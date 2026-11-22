package internal

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"

	btypes "github.com/burnside-project/burnside-go/types"

	"github.com/burnside-project/pg-cdc-core/internal/adapters/sink/filesystem"
	pgadapter "github.com/burnside-project/pg-cdc-core/internal/adapters/source/postgres"
	"github.com/burnside-project/pg-cdc-core/internal/config"
	"github.com/burnside-project/pg-cdc-core/internal/writer"
)

// TestInit_BatchedSnapshot_EndToEnd drives the full Init pipeline against a
// real PostgreSQL (gated on PG_CDC_TEST_DATABASE) and a real filesystem sink:
// bounded keyset reads become numbered base segments, the manifest is only
// published once every segment exists, and compaction picks every segment of
// the latest base epoch back up.
func TestInit_BatchedSnapshot_EndToEnd(t *testing.T) {
	url := os.Getenv("PG_CDC_TEST_DATABASE")
	if url == "" {
		t.Skip("PG_CDC_TEST_DATABASE not set; skipping end-to-end init test")
	}
	ctx := context.Background()

	const schema = "e2e_batched"
	const slot = "e2e_batched_slot"
	const pub = "e2e_batched_pub"

	// Admin connection for setup/teardown (Init closes its own source).
	admin, err := pgadapter.New(ctx, url)
	if err != nil {
		t.Fatalf("admin connect: %v", err)
	}
	defer admin.Close()
	cleanupDB := func() {
		_ = admin.QueryRow(ctx, "SELECT pg_drop_replication_slot($1)", slot).Scan(new(any))
		_ = admin.DropPublication(ctx, pub)
		// Identifier-sanitised schema drop goes through the generic row API;
		// a missing schema/slot simply leaves no rows to scan.
		_ = admin.QueryRow(ctx, fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", schema)).Scan(new(any))
	}
	cleanupDB()
	t.Cleanup(cleanupDB)

	// QueryRow returns ErrNoRows on successful DDL/INSERT ... SELECT; the
	// helper treats that as success.
	seed := func(stmt string) {
		t.Helper()
		row := admin.QueryRow(ctx, stmt)
		if err := row.Scan(new(any)); err != nil && err.Error() != "no rows in result set" {
			t.Fatalf("seed %q: %v", stmt, err)
		}
	}
	seed(fmt.Sprintf("CREATE SCHEMA %s", schema))
	seed(fmt.Sprintf(`CREATE TABLE %s.devices (
		id bigint PRIMARY KEY, tenant int NOT NULL, label text)`, schema))
	seed(fmt.Sprintf(`INSERT INTO %s.devices (id, tenant, label)
		SELECT g, (g %% 5) + 1, 'device-' || g FROM generate_series(1, 1000) g`, schema))
	seed(fmt.Sprintf(`CREATE TABLE %s.empty_t (id bigint PRIMARY KEY, note text)`, schema))

	outDir := t.TempDir()
	cfg := config.Config{
		Source: config.SourceConfig{Postgres: config.PostgresConfig{
			URL: url, Schemas: []string{schema},
		}},
		Storage:     config.StorageConfig{Type: "filesystem", Path: outDir},
		Replication: config.ReplicationConfig{Slot: slot, Publication: pub},
	}
	cfg.Snapshot.BatchRows = 125 // 1000 rows -> 8 full segments

	source, err := pgadapter.New(ctx, url)
	if err != nil {
		t.Fatalf("source connect: %v", err)
	}
	sink, err := filesystem.New(outDir)
	if err != nil {
		t.Fatalf("sink: %v", err)
	}

	if err := Init(ctx, cfg, source, sink); err != nil {
		t.Fatalf("Init: %v", err)
	}

	// --- Segment layout on disk -------------------------------------------
	deviceBase := filepath.Join(outDir, schema+".devices", "base")
	entries, err := os.ReadDir(deviceBase)
	if err != nil {
		t.Fatalf("read base dir: %v", err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	wantNames := []string{
		"base_000000_part_000000.parquet",
		"base_000000_part_000001.parquet",
		"base_000000_part_000002.parquet",
		"base_000000_part_000003.parquet",
		"base_000000_part_000004.parquet",
		"base_000000_part_000005.parquet",
		"base_000000_part_000006.parquet",
		"base_000000_part_000007.parquet",
	}
	if fmt.Sprint(names) != fmt.Sprint(wantNames) {
		t.Errorf("segments = %v, want %v", names, wantNames)
	}
	emptySeg := filepath.Join(outDir, schema+".empty_t", "base", "base_000000_part_000000.parquet")
	if _, err := os.Stat(emptySeg); err != nil {
		t.Errorf("empty table segment missing: %v", err)
	}

	// --- Published manifest -----------------------------------------------
	m, err := sink.ReadManifest(ctx)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	if m.Source.InitLSN == "" {
		t.Error("manifest missing init LSN: streaming cannot resume")
	}
	dt := m.Tables[schema+".devices"]
	if dt.Status != "active" || dt.RowCount != 1000 || len(dt.PrimaryKey) != 1 {
		t.Errorf("devices manifest entry wrong: %+v", dt)
	}
	if et := m.Tables[schema+".empty_t"]; et.Status != "active" || et.RowCount != 0 {
		t.Errorf("empty table manifest entry wrong: %+v", et)
	}

	// --- readBase reconstructs every segment in order ---------------------
	rows, err := readBase(ctx, sink, dt)
	if err != nil {
		t.Fatalf("readBase: %v", err)
	}
	if len(rows) != 1000 {
		t.Errorf("readBase rows = %d, want 1000 across 8 segments", len(rows))
	}
	ids := map[int64]bool{}
	var ordered []int64
	for _, r := range rows {
		id, ok := r["id"].(int64)
		if !ok {
			t.Fatalf("id type %T", r["id"])
		}
		ids[id] = true
		ordered = append(ordered, id)
	}
	if len(ids) != 1000 {
		t.Errorf("unique ids = %d, want 1000", len(ids))
	}
	if !sort.SliceIsSorted(ordered, func(i, j int) bool { return ordered[i] < ordered[j] }) {
		t.Error("segment concatenation lost primary-key order")
	}

	// --- Compaction writes a new epoch and shadows the init segments ------
	schemaEntry := m.Schemas[schema+".devices"]
	deltaRows := []writer.Row{{
		btypes.ColOp: "I",
		"id":         int64(1001), "tenant": int64(1), "label": "compact-new",
	}}
	deltaPath := filepath.Join(outDir, schema+".devices", "deltas", "epoch=000001.parquet")
	if err := writer.WriteDeltaParquet(deltaPath, deltaRows, schemaEntry.Columns, 1); err != nil {
		t.Fatalf("write delta: %v", err)
	}
	// Streaming updates LatestDeltaEpoch per flush; emulate that so compaction
	// actually has work to do.
	entry := m.Tables[schema+".devices"]
	entry.LatestDeltaEpoch = 1
	m.Tables[schema+".devices"] = entry
	if err := sink.WriteManifest(ctx, m); err != nil {
		t.Fatalf("update manifest: %v", err)
	}
	if err := Compact(ctx, sink); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	compacted, err := os.ReadDir(deviceBase)
	if err != nil {
		t.Fatalf("read base dir after compaction: %v", err)
	}
	var hasNewBase bool
	for _, e := range compacted {
		if e.Name() == "base_000001.parquet" {
			hasNewBase = true
		}
	}
	if !hasNewBase {
		t.Errorf("compaction base missing among: %v", compacted)
	}

	m2, err := sink.ReadManifest(ctx)
	if err != nil {
		t.Fatalf("read manifest after compaction: %v", err)
	}
	rows2, err := readBase(ctx, sink, m2.Tables[schema+".devices"])
	if err != nil {
		t.Fatalf("readBase after compaction: %v", err)
	}
	if len(rows2) != 1001 {
		t.Errorf("readBase after compaction = %d rows, want 1001 (epoch-0 segments shadowed)", len(rows2))
	}
}

// TestInit_PublishedTableCarryOver verifies that re-running init against a
// deployment whose manifest already exists leaves the streaming anchor
// intact: an already-published table is not re-snapshotted (its delta epochs,
// row count and base files survive), while a newly added table is snapshotted
// and published in the same run.
func TestInit_PublishedTableCarryOver(t *testing.T) {
	url := os.Getenv("PG_CDC_TEST_DATABASE")
	if url == "" {
		t.Skip("PG_CDC_TEST_DATABASE not set; skipping end-to-end init test")
	}
	ctx := context.Background()

	const schema = "e2e_carryover"
	const slot = "e2e_carryover_slot"
	const pub = "e2e_carryover_pub"

	admin, err := pgadapter.New(ctx, url)
	if err != nil {
		t.Fatalf("admin connect: %v", err)
	}
	defer admin.Close()
	cleanupDB := func() {
		_ = admin.QueryRow(ctx, "SELECT pg_drop_replication_slot($1)", slot).Scan(new(any))
		_ = admin.DropPublication(ctx, pub)
		_ = admin.QueryRow(ctx, fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", schema)).Scan(new(any))
	}
	cleanupDB()
	t.Cleanup(cleanupDB)

	seed := func(stmt string) {
		t.Helper()
		row := admin.QueryRow(ctx, stmt)
		if err := row.Scan(new(any)); err != nil && err.Error() != "no rows in result set" {
			t.Fatalf("seed %q: %v", stmt, err)
		}
	}
	seed(fmt.Sprintf("CREATE SCHEMA %s", schema))
	seed(fmt.Sprintf(`CREATE TABLE %s.alpha (id bigint PRIMARY KEY, v text)`, schema))
	seed(fmt.Sprintf(`INSERT INTO %s.alpha (id, v) SELECT g, 'a-' || g FROM generate_series(1, 200) g`, schema))

	outDir := t.TempDir()
	baseCfg := func() config.Config {
		return config.Config{
			Source: config.SourceConfig{Postgres: config.PostgresConfig{
				URL: url, Schemas: []string{schema},
			}},
			Storage:     config.StorageConfig{Type: "filesystem", Path: outDir},
			Replication: config.ReplicationConfig{Slot: slot, Publication: pub},
		}
	}
	cfg := baseCfg()
	cfg.Snapshot.BatchRows = 75

	src, err := pgadapter.New(ctx, url)
	if err != nil {
		t.Fatalf("source: %v", err)
	}
	sink, err := filesystem.New(outDir)
	if err != nil {
		t.Fatalf("sink: %v", err)
	}
	if err := Init(ctx, cfg, src, sink); err != nil {
		t.Fatalf("first Init: %v", err)
	}

	first, err := sink.ReadManifest(ctx)
	if err != nil {
		t.Fatalf("read first manifest: %v", err)
	}
	firstLSN := first.Source.InitLSN
	if firstLSN == "" {
		t.Fatal("first manifest has no InitLSN anchor")
	}
	alphaEntry := first.Tables[schema+".alpha"]
	alphaEntry.LatestDeltaEpoch = 4 // simulate streaming having advanced
	first.Tables[schema+".alpha"] = alphaEntry
	if err := sink.WriteManifest(ctx, first); err != nil {
		t.Fatalf("advance manifest: %v", err)
	}

	// Add a second table after the first deployment was published.
	seed(fmt.Sprintf(`CREATE TABLE %s.bravo (id bigint PRIMARY KEY, v text)`, schema))
	seed(fmt.Sprintf(`INSERT INTO %s.bravo (id, v) SELECT g, 'b-' || g FROM generate_series(1, 90) g`, schema))

	// Re-run init with a fresh slot (mirrors teardown + re-init).
	if err := admin.DropReplicationSlot(ctx, slot); err != nil {
		t.Fatalf("drop slot: %v", err)
	}
	if err := admin.DropPublication(ctx, pub); err != nil {
		t.Fatalf("drop pub: %v", err)
	}
	cfg2 := baseCfg()
	cfg2.Snapshot.BatchRows = 75
	src2, err := pgadapter.New(ctx, url)
	if err != nil {
		t.Fatalf("source2: %v", err)
	}
	if err := Init(ctx, cfg2, src2, sink); err != nil {
		t.Fatalf("second Init: %v", err)
	}

	second, err := sink.ReadManifest(ctx)
	if err != nil {
		t.Fatalf("read second manifest: %v", err)
	}

	// The incremental anchor must stay at the original LSN so `start` keeps
	// streaming the carried-over table without a gap, even though a new slot
	// was created for this run.
	if second.Source.InitLSN != firstLSN {
		t.Errorf("InitLSN changed from %s to %s; carried-over anchor must be preserved",
			firstLSN, second.Source.InitLSN)
	}

	// The carried-over table keeps its advanced delta state and its original
	// base files; only the new table gets fresh segments.
	alpha2 := second.Tables[schema+".alpha"]
	if alpha2.LatestDeltaEpoch != 4 {
		t.Errorf("alpha delta epoch reset to %d, want 4 (carry-over lost)", alpha2.LatestDeltaEpoch)
	}
	if alpha2.RowCount != 200 {
		t.Errorf("alpha row count = %d, want 200", alpha2.RowCount)
	}
	bravo2, ok := second.Tables[schema+".bravo"]
	if !ok || bravo2.Status != "active" || bravo2.RowCount != 90 {
		t.Fatalf("new table bravo not published correctly: %+v", bravo2)
	}
	alphaBase := filepath.Join(outDir, schema+".alpha", "base")
	alphaFiles, err := os.ReadDir(alphaBase)
	if err != nil {
		t.Fatalf("alpha base dir: %v", err)
	}
	if len(alphaFiles) != 3 { // ceil(200/75)
		t.Errorf("alpha base segments = %d, want 3 untouched files", len(alphaFiles))
	}
	bravoBase := filepath.Join(outDir, schema+".bravo", "base")
	bravoFiles, err := os.ReadDir(bravoBase)
	if err != nil {
		t.Fatalf("bravo base dir: %v", err)
	}
	if len(bravoFiles) != 2 { // ceil(90/75)
		t.Errorf("bravo base segments = %d, want 2", len(bravoFiles))
	}
}
