// Package internal contains the core pg-cdc command logic.
package internal

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/burnside-project/burnside-go/epoch"
	"github.com/burnside-project/burnside-go/manifest"
	btypes "github.com/burnside-project/burnside-go/types"

	"github.com/burnside-project/pg-cdc-core/internal/config"
	"github.com/burnside-project/pg-cdc-core/internal/glue"
	"github.com/burnside-project/pg-cdc-core/internal/ports"
	"github.com/burnside-project/pg-cdc-core/internal/tags"
	"github.com/burnside-project/pg-cdc-core/internal/writer"
)

// Init creates a replication slot, snapshots all discovered tables to base
// Parquet files, and writes a manifest.json. This follows the native
// Postgres replica pattern: snapshot + LSN = zero-gap starting point.
//
// Tables are read in bounded primary-key keyset batches against the slot's
// exported snapshot (see SnapshotConfig); each batch becomes one numbered
// base segment. The manifest is the single atomic publish point — segments
// written by an interrupted run stay invisible without it and are cleaned up
// on the next attempt, while tables already present in an existing manifest
// are carried over untouched so their incremental state stays valid.
func Init(ctx context.Context, cfg config.Config, source ports.Source, sink ports.Sink) error {
	defer source.Close()

	// Get source version
	pgVersion, _ := source.Version(ctx)
	fmt.Printf("Connected to PostgreSQL: %s\n", truncateVersion(pgVersion))

	// Discover tables
	schemas := cfg.Source.Postgres.Schemas
	if len(schemas) == 0 {
		schemas = []string{"public"}
	}

	var allQualified []string
	type rawTable struct {
		schema string
		name   string
	}
	rawTables := make(map[string]rawTable)

	for _, schema := range schemas {
		tableNames, err := source.ListTables(ctx, schema)
		if err != nil {
			return fmt.Errorf("list tables in %s: %w", schema, err)
		}
		for _, name := range tableNames {
			qualified := schema + "." + name
			allQualified = append(allQualified, qualified)
			rawTables[qualified] = rawTable{schema: schema, name: name}
		}
	}

	// Apply tags + policies
	resolutions := tags.Resolve(allQualified, cfg.Tables)

	excludedTables := make(map[string]tags.Resolution)
	var tables []tableInfo
	for _, res := range resolutions {
		if !res.Included {
			fmt.Printf("  skip: %s (%s)\n", res.Table, res.Reason)
			excludedTables[res.Table] = res
			continue
		}
		rt := rawTables[res.Table]
		pks, err := source.GetPrimaryKeys(ctx, rt.schema, rt.name)
		if err != nil || len(pks) == 0 {
			fmt.Printf("  skip: %s (no primary key)\n", res.Table)
			continue
		}
		cols, err := source.GetTableSchema(ctx, rt.schema, rt.name)
		if err != nil {
			return fmt.Errorf("schema for %s: %w", res.Table, err)
		}
		tables = append(tables, tableInfo{
			schema: rt.schema, name: rt.name, qualified: res.Table,
			pks: pks, cols: cols, tags: res.Tags,
		})
	}

	if len(tables) == 0 {
		return fmt.Errorf("no tables with primary keys found in schemas %v", schemas)
	}
	fmt.Printf("Discovered %d tables\n", len(tables))

	// Create publication
	pubName := cfg.Replication.Publication
	if pubName == "" {
		pubName = "pg_cdc_pub"
	}
	var qualifiedNames []string
	for _, t := range tables {
		qualifiedNames = append(qualifiedNames, t.qualified)
	}
	if err := source.CreatePublication(ctx, pubName, qualifiedNames); err != nil {
		return err
	}
	fmt.Printf("Publication: %s\n", pubName)

	// Create replication slot and get snapshot
	slotName := cfg.Replication.Slot
	if slotName == "" {
		slotName = "pg_cdc_slot"
	}
	snapshot, err := source.CreateReplicationSlot(ctx, slotName)
	if err != nil {
		return err
	}
	fmt.Printf("Replication slot: %s (LSN: %s)\n", slotName, snapshot.LSN)

	// Release the snapshot-holding replication connection after all copies.
	defer source.ReleaseSnapshot(ctx)

	// Snapshot each table using the exported snapshot. Tables already present
	// in an existing manifest are considered published and protected: their
	// base segments, delta epochs and schema are carried over untouched, so
	// re-running init never invalidates a running deployment's incremental
	// state. Base files left by an interrupted earlier init run (no manifest
	// entry) are stale and cleaned before re-writing.
	// A missing manifest means a fresh deployment (every sink surfaces that as
	// an error here). A real read failure is only warned about: init remains
	// runnable, but the operator gets a signal that published-table protection
	// could not be evaluated rather than having it silently disabled.
	existingManifest, manifestErr := sink.ReadManifest(ctx)
	if manifestErr != nil {
		fmt.Printf("  warning: could not read existing manifest (%v); treating all tables as new\n", manifestErr)
		existingManifest = nil
	}

	// Snapshot each table using the exported snapshot
	manifestTables := make(map[string]manifest.Table)
	manifestSchemas := make(map[string]manifest.Schema)
	carriedOver := 0

	for _, t := range tables {
		// Build manifest columns
		var mCols []manifest.Column
		for _, col := range t.cols {
			mCols = append(mCols, manifest.Column{
				Name:        col.Name,
				PgType:      col.Type,
				ParquetType: btypes.ParquetType(col.Type),
				Nullable:    col.Nullable,
			})
		}

		if existing, ok := existingManifestTable(existingManifest, t.qualified); ok {
			// Already published: keep its base/delta state and schema exactly
			// as consumers and `start` know them; do not re-read Postgres or
			// touch its base files.
			carriedOver++
			fmt.Printf("  snapshot: %s skipped (already published, base epoch %d)\n", t.qualified, existing.BaseEpoch)
			manifestTables[t.qualified] = existing
			if schema, ok := existingManifest.Schemas[t.qualified]; ok {
				manifestSchemas[t.qualified] = schema
			} else {
				manifestSchemas[t.qualified] = manifest.Schema{Version: 1, Columns: mCols}
			}
			continue
		}

		fmt.Printf("  snapshot: %s ...\n", t.qualified)

		if err := cleanupStaleBaseSegments(ctx, sink, t.qualified); err != nil {
			return fmt.Errorf("cleanup stale base for %s: %w", t.qualified, err)
		}

		rowCount, segments, err := snapshotTableBatches(ctx, cfg.Snapshot.BatchSize(), source, sink, t, snapshot.SnapshotName, mCols)
		if err != nil {
			return err
		}

		fmt.Printf("  snapshot: %s done (%d rows, %d segment(s))\n", t.qualified, rowCount, segments)

		manifestTables[t.qualified] = manifest.Table{
			Status:           "active",
			Tags:             t.tags,
			PrimaryKey:       t.pks,
			SchemaVersion:    1,
			BaseEpoch:        0,
			LatestDeltaEpoch: 0,
			RowCount:         rowCount,
			Path:             t.qualified,
		}
		manifestSchemas[t.qualified] = manifest.Schema{
			Version: 1,
			Columns: mCols,
		}
	}

	// Add excluded tables to manifest (with reason)
	for tableName, res := range excludedTables {
		manifestTables[tableName] = manifest.Table{
			Status: "excluded",
			Tags:   res.Tags,
			Reason: res.Reason,
		}
	}

	// Build ACL-driven role profiles
	var roles map[string]manifest.Role
	if cfg.Profiles.Source == "pg_acl" && len(cfg.Profiles.Roles) > 0 {
		fmt.Printf("Reading ACLs for roles: %v\n", cfg.Profiles.Roles)
		portRoles, err := source.BuildRoleProfiles(ctx, cfg.Profiles.Roles)
		if err != nil {
			fmt.Printf("  warning: failed to read ACLs: %v\n", err)
		} else {
			roles = toManifestRoles(portRoles)
			fmt.Printf("  %d role profiles generated\n", len(roles))
		}
	}

	// Write manifest. When any table was carried over from an existing
	// deployment, keep that deployment's Source anchor: its base files were
	// snapshotted at the original InitLSN and `start` resumes from there, so
	// rewriting the anchor to the freshly created slot's LSN would open a gap.
	// New tables added in this run are snapshot at the new slot and begin
	// streaming at that same point, so they stay zero-gap on their own.
	sourceBlock := manifest.Source{
		Name:            slotName,
		PostgresVersion: truncateVersion(pgVersion),
		InitLSN:         snapshot.LSN,
		InitTimestamp:   time.Now().UTC(),
	}
	if carriedOver > 0 && existingManifest != nil {
		sourceBlock = existingManifest.Source
		fmt.Printf("  keeping incremental anchor at LSN %s for %d carried-over table(s)\n",
			sourceBlock.InitLSN, carriedOver)
	}

	m := &manifest.Manifest{
		Version:   1,
		Source:    sourceBlock,
		Tables:    manifestTables,
		Schemas:   manifestSchemas,
		Roles:     roles,
		UpdatedAt: time.Now().UTC(),
	}

	if err := sink.WriteManifest(ctx, m); err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}

	if err := registerCatalog(ctx, cfg, m); err != nil {
		return fmt.Errorf("catalog register: %w", err)
	}

	fmt.Printf("\nInit complete:\n")
	fmt.Printf("  Slot: %s\n", slotName)
	fmt.Printf("  LSN: %s\n", sourceBlock.InitLSN)
	if carriedOver > 0 {
		fmt.Printf("  (new slot consistent point %s; streaming anchor kept at %s)\n", snapshot.LSN, sourceBlock.InitLSN)
	}
	fmt.Printf("  Tables: %d\n", len(tables))
	fmt.Printf("  Epoch: %s\n", epoch.FormatFilename(0))
	fmt.Printf("  Storage: %s (%s)\n", cfg.Storage.Path, sink.Backend())
	return nil
}

// CatalogRegister reads the manifest from sink and registers every active
// table in the configured catalog (e.g. AWS Glue). Use this to turn on
// catalog.type for a deployment that was already snapshotted without that
// block — re-running `init` would re-snapshot from Postgres and duplicate
// the parquet base files in storage.
//
// Stricter than the init-time call: returns an error if catalog.type is
// empty, since a no-op invocation is almost certainly an operator mistake.
func CatalogRegister(ctx context.Context, cfg config.Config, sink ports.Sink) error {
	if cfg.Catalog.Type == "" {
		return fmt.Errorf("catalog.type is not set in config; nothing to register")
	}
	m, err := sink.ReadManifest(ctx)
	if err != nil {
		return fmt.Errorf("read manifest: %w (run `pg-cdc init` first if this deployment has never been snapshotted)", err)
	}
	return registerCatalog(ctx, cfg, m)
}

// registerCatalog catalogues active manifest tables when cfg.Catalog.Type is
// set. Today only "glue" is supported, and only on the s3 sink — other
// backends are skipped with a notice. Failure surfaces as an error; the
// manifest is already on disk, so re-running init is the recovery path
// (CreateTable falls through to UpdateTable on the second attempt).
func registerCatalog(ctx context.Context, cfg config.Config, m *manifest.Manifest) error {
	if cfg.Catalog.Type == "" {
		return nil
	}
	if cfg.Catalog.Type != "glue" {
		return fmt.Errorf("unsupported catalog type %q", cfg.Catalog.Type)
	}
	if cfg.Storage.StorageType() != "s3" {
		fmt.Printf("  catalog: skipping Glue registration (storage type %q is not s3)\n", cfg.Storage.StorageType())
		return nil
	}

	region := cfg.Catalog.Region
	if region == "" {
		region = cfg.Storage.Region
	}
	g, err := glue.New(glue.Config{
		Database: cfg.Catalog.Database,
		Region:   region,
		Endpoint: cfg.Catalog.Endpoint,
	})
	if err != nil {
		return err
	}

	locFn := func(qualified string) string {
		return fmt.Sprintf("s3://%s/%s%s/", cfg.Storage.Bucket, cfg.Storage.Prefix, qualified)
	}
	if err := g.Register(ctx, m, locFn); err != nil {
		return err
	}

	active := 0
	for _, t := range m.Tables {
		if t.Status == "active" {
			active++
		}
	}
	fmt.Printf("  Glue catalog: %s (%d tables)\n", g.Database(), active)
	return nil
}

func uploadFile(ctx context.Context, sink ports.Sink, remotePath, localPath string) error {
	f, err := os.Open(localPath)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	return sink.WriteFile(ctx, remotePath, f)
}

// initTmpRoot is the local scratch directory for base segments before they
// are published through the sink. Each table gets a unique subdirectory so
// interrupted runs never mix segments across attempts.
const initTmpRoot = "/tmp/pgcdc-init"

// baseSegmentPath returns the storage-relative path of one init base segment.
// Init always writes epoch 0; compaction later writes a single base_<epoch>.
func baseSegmentPath(qualified string, part int) string {
	return filepath.Join(qualified, "base", fmt.Sprintf("base_000000_part_%06d.parquet", part))
}

// existingManifestTable returns the active manifest entry of a table that has
// already been published, so a re-run of init can carry it over instead of
// re-snapshotting it and resetting its base/delta state. A nil manifest (fresh
// deployment) or a non-active entry yields ok=false.
func existingManifestTable(m *manifest.Manifest, qualified string) (manifest.Table, bool) {
	if m == nil {
		return manifest.Table{}, false
	}
	t, ok := m.Tables[qualified]
	if !ok || t.Status != "active" {
		return manifest.Table{}, false
	}
	return t, true
}

// cleanupStaleBaseSegments removes base files left under a table path by an
// earlier init attempt that never published a manifest. Deterministic segment
// names mean a successful re-run would overwrite them anyway; deleting first
// also covers segment-count changes between runs (e.g. batch size tuning).
func cleanupStaleBaseSegments(ctx context.Context, sink ports.Sink, qualified string) error {
	basePrefix := filepath.Join(qualified, "base") + "/"
	files, err := sink.ListFiles(ctx, basePrefix)
	if err != nil {
		return fmt.Errorf("list base segments: %w", err)
	}
	for _, f := range files {
		if err := sink.DeleteFile(ctx, f); err != nil {
			return fmt.Errorf("delete stale segment %s: %w", f, err)
		}
	}
	return nil
}

// snapshotTableBatches reads one table through the exported snapshot in
// bounded keyset batches and writes one base Parquet segment per batch.
// Segments are published to storage as they complete, but the table only
// becomes visible to readers (compaction, MCP views, streaming) when the
// manifest referencing it is written afterwards — that final manifest write
// is the atomic publish point. It returns the total row count and number of
// segments written (at least one, even for an empty table).
func snapshotTableBatches(
	ctx context.Context,
	batchSize int,
	source ports.Source,
	sink ports.Sink,
	t tableInfo,
	snapshotName string,
	mCols []manifest.Column,
) (totalRows int64, segments int, err error) {
	if err := os.MkdirAll(initTmpRoot, 0o755); err != nil {
		return 0, 0, fmt.Errorf("create temp root for %s: %w", t.qualified, err)
	}
	tmpDir, err := os.MkdirTemp(initTmpRoot, strings.ReplaceAll(t.qualified, ".", "_")+"-*")
	if err != nil {
		return 0, 0, fmt.Errorf("create temp dir for %s: %w", t.qualified, err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	part := 0
	scanErr := source.ScanTable(ctx, t.schema, t.name, snapshotName, t.pks, batchSize, func(batch []map[string]any) error {
		remotePath := baseSegmentPath(t.qualified, part)
		tmpPath := filepath.Join(tmpDir, fmt.Sprintf("part_%06d.parquet", part))

		if writeErr := writer.WriteBaseParquet(tmpPath, batch, mCols); writeErr != nil {
			return fmt.Errorf("write base segment %s: %w", remotePath, writeErr)
		}
		if uploadErr := uploadFile(ctx, sink, remotePath, tmpPath); uploadErr != nil {
			return fmt.Errorf("upload %s: %w", remotePath, uploadErr)
		}

		totalRows += int64(len(batch))
		segments++
		part++
		fmt.Printf("    %s: segment %d (%d rows, %d total)\n", t.qualified, part, len(batch), totalRows)
		return nil
	})
	if scanErr != nil {
		return 0, 0, fmt.Errorf("snapshot %s: %w", t.qualified, scanErr)
	}
	return totalRows, segments, nil
}

type tableInfo struct {
	schema    string
	name      string
	qualified string
	pks       []string
	cols      []ports.ColumnInfo
	tags      []string
}

func truncateVersion(v string) string {
	if idx := strings.IndexByte(v, '('); idx > 0 {
		return strings.TrimSpace(v[:idx])
	}
	return v
}

// toManifestRoles converts port-level role profiles to manifest types.
func toManifestRoles(portRoles map[string]ports.RoleProfile) map[string]manifest.Role {
	roles := make(map[string]manifest.Role, len(portRoles))
	for name, profile := range portRoles {
		role := manifest.Role{Tables: make(map[string]manifest.RoleAccess, len(profile.Tables))}
		for tbl, access := range profile.Tables {
			role.Tables[tbl] = manifest.RoleAccess{
				AllColumns: access.AllColumns,
				Columns:    access.Columns,
			}
		}
		roles[name] = role
	}
	return roles
}
