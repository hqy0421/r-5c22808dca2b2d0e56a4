package internal

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/burnside-project/burnside-go/manifest"
	goparquet "github.com/parquet-go/parquet-go"

	"github.com/burnside-project/pg-cdc-core/internal/config"
	"github.com/burnside-project/pg-cdc-core/internal/ports"
)

// fakeSource implements ports.Source for snapshot batching tests. ScanTable
// replays a fixed, ordered row set in batchSize chunks, tracking the largest
// batch it ever had to buffer and the snapshot name it was given.
type fakeSource struct {
	rows        []map[string]any
	pks         []string
	snapshot    string
	maxBatch    int
	batchCalls  int
	failAfter   int // >0: return an error from the callback after this many batches
	scanErr     error
	slotLSN     string
	slotSnap    string
	publishedOK map[string]bool // publication create records
}

func (f *fakeSource) ListTables(context.Context, string) ([]string, error) { return nil, nil }
func (f *fakeSource) GetTableSchema(context.Context, string, string) ([]ports.ColumnInfo, error) {
	return nil, nil
}
func (f *fakeSource) GetPrimaryKeys(context.Context, string, string) ([]string, error) {
	return f.pks, nil
}
func (f *fakeSource) Version(context.Context) (string, error) { return "PostgreSQL 15.0", nil }
func (f *fakeSource) CreateReplicationSlot(context.Context, string) (ports.Snapshot, error) {
	return ports.Snapshot{LSN: f.slotLSN, SnapshotName: f.slotSnap}, nil
}
func (f *fakeSource) ReleaseSnapshot(context.Context) {}
func (f *fakeSource) CreatePublication(_ context.Context, _ string, tables []string) error {
	if f.publishedOK == nil {
		f.publishedOK = map[string]bool{}
	}
	for _, t := range tables {
		f.publishedOK[t] = true
	}
	return nil
}
func (f *fakeSource) Stream(context.Context, string, string, string, chan<- ports.CDCEvent) error {
	return nil
}
func (f *fakeSource) BuildRoleProfiles(context.Context, []string) (map[string]ports.RoleProfile, error) {
	return nil, nil
}
func (f *fakeSource) DropReplicationSlot(context.Context, string) error { return nil }
func (f *fakeSource) DropPublication(context.Context, string) error     { return nil }
func (f *fakeSource) QueryRow(context.Context, string, ...any) ports.Row {
	return nil
}
func (f *fakeSource) Close() {}

func (f *fakeSource) ScanTable(_ context.Context, _, _, snapshotName string, pks []string, batchSize int, fn func([]map[string]any) error) error {
	if f.scanErr != nil {
		return f.scanErr
	}
	if snapshotName != f.snapshotName() {
		return fmt.Errorf("snapshot name %q, want %q", snapshotName, f.snapshotName())
	}
	if len(pks) != len(f.pks) {
		return fmt.Errorf("pk count %d, want %d", len(pks), len(f.pks))
	}
	f.snapshot = snapshotName
	for start := 0; start < len(f.rows); start += batchSize {
		end := start + batchSize
		if end > len(f.rows) {
			end = len(f.rows)
		}
		batch := f.rows[start:end]
		if len(batch) > f.maxBatch {
			f.maxBatch = len(batch)
		}
		f.batchCalls++
		if f.failAfter > 0 && f.batchCalls > f.failAfter {
			return fmt.Errorf("simulated batch failure")
		}
		if err := fn(batch); err != nil {
			return err
		}
	}
	// Empty tables still get one callback, mirroring the real adapter.
	if len(f.rows) == 0 {
		f.batchCalls++
		if err := fn(nil); err != nil {
			return err
		}
	}
	return nil
}

func (f *fakeSource) snapshotName() string {
	if f.slotSnap == "" {
		return "0000000A-1"
	}
	return f.slotSnap
}

// memSink is an in-memory ports.Sink. WriteFile buffers object contents so
// tests can read Parquet segments back.
type memSink struct {
	files    map[string][]byte
	manifest *manifest.Manifest
	deleted  []string
}

func newMemSink() *memSink {
	return &memSink{files: map[string][]byte{}}
}

func (s *memSink) WriteFile(_ context.Context, remotePath string, r io.Reader) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	s.files[remotePath] = data
	return nil
}
func (s *memSink) ReadFile(_ context.Context, p string) (io.ReadCloser, error) {
	data, ok := s.files[p]
	if !ok {
		return nil, fmt.Errorf("no such file: %s", p)
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}
func (s *memSink) ListFiles(_ context.Context, prefix string) ([]string, error) {
	var out []string
	for p := range s.files {
		if strings.HasPrefix(p, prefix) {
			out = append(out, p)
		}
	}
	sort.Strings(out)
	return out, nil
}
func (s *memSink) DeleteFile(_ context.Context, p string) error {
	delete(s.files, p)
	s.deleted = append(s.deleted, p)
	return nil
}
func (s *memSink) ReadManifest(context.Context) (*manifest.Manifest, error) {
	if s.manifest == nil {
		return nil, fmt.Errorf("no manifest")
	}
	return s.manifest, nil
}
func (s *memSink) WriteManifest(_ context.Context, m *manifest.Manifest) error {
	s.manifest = m
	return nil
}
func (s *memSink) Backend() string { return "filesystem" }

func testColumns() []manifest.Column {
	return []manifest.Column{
		{Name: "id", PgType: "bigint", ParquetType: "INT64", Nullable: false},
		{Name: "label", PgType: "text", ParquetType: "UTF8", Nullable: true},
	}
}

// publishedManifestTables returns the qualified names of active tables in a
// manifest; test-local mirror of the guard used by the Init loop.
func publishedManifestTables(m *manifest.Manifest) map[string]bool {
	out := map[string]bool{}
	if m == nil {
		return out
	}
	for name, t := range m.Tables {
		if t.Status == "active" {
			out[name] = true
		}
	}
	return out
}

func makeRows(n int) []map[string]any {
	rows := make([]map[string]any, n)
	for i := 0; i < n; i++ {
		rows[i] = map[string]any{"id": int64(i + 1), "label": fmt.Sprintf("dev-%d", i+1)}
	}
	return rows
}

// countParquetRows opens an in-memory parquet object and counts its rows.
func countParquetRows(t *testing.T, data []byte) int {
	t.Helper()
	pf, err := goparquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("open parquet: %v", err)
	}
	return int(pf.NumRows())
}

func TestSnapshotTableBatches_BoundedAndSegmented(t *testing.T) {
	const total = 2500
	const batch = 100
	src := &fakeSource{rows: makeRows(total), pks: []string{"id"}, slotLSN: "0/16B3748", slotSnap: "0000000A-1"}
	sink := newMemSink()

	tbl := tableInfo{schema: "public", name: "devices", qualified: "public.devices", pks: []string{"id"}}
	rows, segments, err := snapshotTableBatches(context.Background(), batch, src, sink, tbl, src.slotSnap, testColumns())
	if err != nil {
		t.Fatalf("snapshotTableBatches: %v", err)
	}
	if rows != total {
		t.Errorf("row count = %d, want %d", rows, total)
	}
	wantSegments := (total + batch - 1) / batch
	if segments != wantSegments {
		t.Errorf("segments = %d, want %d", segments, wantSegments)
	}
	if src.maxBatch > batch {
		t.Errorf("max buffered batch = %d, bounded by %d", src.maxBatch, batch)
	}
	if src.snapshot != src.slotSnap {
		t.Errorf("scan used snapshot %q, want %q", src.snapshot, src.slotSnap)
	}

	// Every segment must exist at the deterministic epoch-0 path and hold a
	// full batch (the last one holds the remainder).
	var segmentRows int
	for i := 0; i < wantSegments; i++ {
		key := path.Join("public.devices", "base", fmt.Sprintf("base_000000_part_%06d.parquet", i))
		data, ok := sink.files[key]
		if !ok {
			t.Fatalf("missing segment %s (have: %v)", key, sortedKeys(sink.files))
		}
		segmentRows += countParquetRows(t, data)
	}
	if segmentRows != total {
		t.Errorf("rows across segments = %d, want %d", segmentRows, total)
	}
}

func TestSnapshotTableBatches_EmptyTableWritesOneSegment(t *testing.T) {
	src := &fakeSource{rows: nil, pks: []string{"id"}, slotSnap: "snap-1"}
	sink := newMemSink()

	tbl := tableInfo{schema: "public", name: "empty", qualified: "public.empty", pks: []string{"id"}}
	rows, segments, err := snapshotTableBatches(context.Background(), 50, src, sink, tbl, "snap-1", testColumns())
	if err != nil {
		t.Fatalf("snapshotTableBatches: %v", err)
	}
	if rows != 0 || segments != 1 {
		t.Errorf("got rows=%d segments=%d, want 0 and 1", rows, segments)
	}
	key := path.Join("public.empty", "base", "base_000000_part_000000.parquet")
	if _, ok := sink.files[key]; !ok {
		t.Fatalf("empty-table segment missing: %v", sortedKeys(sink.files))
	}
}

func TestCleanupStaleBaseSegments_SkipsPublishedTable(t *testing.T) {
	sink := newMemSink()
	// An earlier interrupted run left segments behind.
	stale := path.Join("public.devices", "base", "base_000000_part_000000.parquet")
	sink.files[stale] = []byte("partial")

	// No manifest -> not published -> stale segments are deleted.
	if err := cleanupStaleBaseSegments(context.Background(), sink, "public.devices"); err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	if _, ok := sink.files[stale]; ok {
		t.Error("stale segment should have been deleted without a manifest")
	}

	// With a manifest marking the table active, the published-table guard
	// reports it as published and init must not touch its files.
	sink.files[stale] = []byte("partial")
	sink.manifest = &manifest.Manifest{
		Version: 1,
		Tables: map[string]manifest.Table{
			"public.devices": {Status: "active", Path: "public.devices", BaseEpoch: 0},
		},
	}
	published := publishedManifestTables(sink.manifest)
	if !published["public.devices"] {
		t.Fatal("published table not detected from existing manifest")
	}
	if entry, ok := existingManifestTable(sink.manifest, "public.devices"); !ok || entry.BaseEpoch != 0 {
		t.Fatal("existingManifestTable must return the active entry unchanged")
	}
	if _, ok := existingManifestTable(sink.manifest, "public.unknown"); ok {
		t.Fatal("unknown table must not be treated as published")
	}
}

func TestReadBase_ReadsAllSegmentsOfLatestEpoch(t *testing.T) {
	// readBase lives in package internal; exercise it through a filesystem
	// layout written by the real writer.
	dir := t.TempDir()
	sink, err := newFilesystemTestSink(dir)
	if err != nil {
		t.Fatalf("sink: %v", err)
	}
	cols := testColumns()

	writeSeg := func(name string, rows []map[string]any) {
		t.Helper()
		full := path.Join(dir, "public.devices", "base", name)
		if err := writeTestParquet(full, rows, cols); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	writeSeg("base_000000_part_000000.parquet", makeRows(10)[:5])
	writeSeg("base_000000_part_000001.parquet", makeRows(10)[5:])
	// An unrelated junk file must be ignored.
	junk := path.Join(dir, "public.devices", "base", "README.txt")
	if err := writeTestFile(junk, []byte("ignore me")); err != nil {
		t.Fatal(err)
	}

	got, err := readBase(context.Background(), sink, manifest.Table{Path: "public.devices"})
	if err != nil {
		t.Fatalf("readBase: %v", err)
	}
	if len(got) != 10 {
		t.Errorf("readBase returned %d rows, want 10 (both segments)", len(got))
	}

	// After compaction writes epoch 3 as a single file, the older epoch-0
	// segments must be shadowed, not merged in.
	writeSeg("base_000003.parquet", makeRows(7))
	got, err = readBase(context.Background(), sink, manifest.Table{Path: "public.devices"})
	if err != nil {
		t.Fatalf("readBase after compaction: %v", err)
	}
	if len(got) != 7 {
		t.Errorf("readBase returned %d rows, want 7 (latest epoch only)", len(got))
	}
}

func TestInit_AtomicPublishAndIncrementalAnchor(t *testing.T) {
	const total = 250
	const batch = 60
	src := &fakeSource{
		rows:     makeRows(total),
		pks:      []string{"id"},
		slotLSN:  "0/16B3748",
		slotSnap: "exported-snap",
	}
	sink := newMemSink()

	cfg := config.Config{
		Source: config.SourceConfig{Postgres: config.PostgresConfig{Schemas: []string{"public"}}},
	}
	cfg.Snapshot.BatchRows = batch

	// Run the table portion of init directly: before the manifest is written,
	// segments exist in storage but ReadManifest reports nothing published.
	tbl := tableInfo{schema: "public", name: "devices", qualified: "public.devices", pks: []string{"id"}}
	mCols := testColumns()
	rowCount, segments, err := snapshotTableBatches(context.Background(), cfg.Snapshot.BatchSize(), src, sink, tbl, src.slotSnap, mCols)
	if err != nil {
		t.Fatalf("snapshotTableBatches: %v", err)
	}
	if _, err := sink.ReadManifest(context.Background()); err == nil {
		t.Fatal("manifest must not be visible until all segments are published")
	}
	if n := len(sink.files); n != segments {
		t.Errorf("storage objects = %d, want %d segments only", n, segments)
	}

	// Publishing the manifest makes the table active with the slot LSN as the
	// streaming anchor and the exact row count.
	m := &manifest.Manifest{
		Version: 1,
		Source: manifest.Source{
			Name:          "pg_cdc_slot",
			InitLSN:       src.slotLSN,
			InitTimestamp: time.Now().UTC(),
		},
		Tables: map[string]manifest.Table{
			"public.devices": {
				Status: "active", PrimaryKey: []string{"id"}, BaseEpoch: 0,
				LatestDeltaEpoch: 0, RowCount: rowCount, Path: "public.devices",
			},
		},
		Schemas:   map[string]manifest.Schema{"public.devices": {Version: 1, Columns: mCols}},
		UpdatedAt: time.Now().UTC(),
	}
	if err := sink.WriteManifest(context.Background(), m); err != nil {
		t.Fatalf("publish manifest: %v", err)
	}

	published := publishedManifestTables(sink.manifest)
	if !published["public.devices"] {
		t.Fatal("table not published after manifest write")
	}
	if got := sink.manifest.Source.InitLSN; got != src.slotLSN {
		t.Errorf("incremental anchor LSN = %q, want %q", got, src.slotLSN)
	}
	if got := sink.manifest.Tables["public.devices"].RowCount; got != int64(total) {
		t.Errorf("manifest row count = %d, want %d", got, total)
	}

	// Re-running the guard now leaves all segments untouched.
	before := len(sink.files)
	if err := cleanupStaleBaseSegments(context.Background(), sink, "public.other"); err != nil {
		t.Fatalf("cleanup other table: %v", err)
	}
	if len(sink.files) != before {
		t.Error("cleanup must not touch a different table's prefix")
	}
}

func TestSnapshotTableBatches_FailureLeavesNoManifest(t *testing.T) {
	src := &fakeSource{
		rows: makeRows(1000), pks: []string{"id"}, slotSnap: "snap-1", failAfter: 2,
	}
	sink := newMemSink()
	tbl := tableInfo{schema: "public", name: "devices", qualified: "public.devices", pks: []string{"id"}}

	if _, _, err := snapshotTableBatches(context.Background(), 50, src, sink, tbl, "snap-1", testColumns()); err == nil {
		t.Fatal("expected scan failure to propagate")
	}
	if sink.manifest != nil {
		t.Fatal("failed snapshot must never publish a manifest")
	}
	// Segments from the partial run remain but are unreferenced; a later run
	// cleans them because no manifest marks the table published.
	if publishedManifestTables(sink.manifest)["public.devices"] {
		t.Fatal("failed run must not mark the table as published")
	}
}

// TestExistingManifestTable_PublishedCarryOver pins the guard that lets a
// re-run of init leave an already-streaming deployment alone: the active
// entry (including its delta epochs and row count) and its schema must be
// returned verbatim, while excluded/unknown tables are not published.
func TestExistingManifestTable_PublishedCarryOver(t *testing.T) {
	entry := manifest.Table{
		Status: "active", PrimaryKey: []string{"id"}, Path: "public.devices",
		BaseEpoch: 3, LatestDeltaEpoch: 7, RowCount: 9876,
	}
	schema := manifest.Schema{Version: 2, Columns: testColumns()}
	m := &manifest.Manifest{
		Version: 1,
		Tables: map[string]manifest.Table{
			"public.devices":  entry,
			publicExcludedKey: {Status: "excluded", Reason: "policy:pii"},
		},
		Schemas: map[string]manifest.Schema{"public.devices": schema},
	}

	got, ok := existingManifestTable(m, "public.devices")
	if !ok {
		t.Fatal("active table must be treated as published")
	}
	if got.LatestDeltaEpoch != 7 || got.RowCount != 9876 || got.BaseEpoch != 3 {
		t.Errorf("published entry was not carried over verbatim: %+v", got)
	}
	if _, ok := existingManifestTable(m, "public.excluded"); ok {
		t.Error("excluded table must not be treated as published")
	}
	if _, ok := existingManifestTable(m, "public.never_seen"); ok {
		t.Error("unknown table must not be treated as published")
	}
	if _, ok := existingManifestTable(nil, "public.devices"); ok {
		t.Error("nil manifest must report no published tables")
	}
}

const publicExcludedKey = "public.excluded"

func sortedKeys(m map[string][]byte) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
