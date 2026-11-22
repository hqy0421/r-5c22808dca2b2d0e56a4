package postgres

import (
	"context"
	"fmt"
	"os"
	"sort"
	"testing"

	"github.com/jackc/pglogrepl"
)

// testURL is the user-local Postgres instance started by
// scripts/test-postgres.sh; the integration test only runs when
// PG_CDC_TEST_DATABASE is set so `go test ./...` stays dependency-free.
func testURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv("PG_CDC_TEST_DATABASE")
	if url == "" {
		t.Skip("PG_CDC_TEST_DATABASE not set; skipping PostgreSQL integration test")
	}
	return url
}

func TestScanTable_Integration(t *testing.T) {
	ctx := context.Background()
	url := testURL(t)

	conn, err := NewConn(ctx, url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close()

	// Fresh schema/table per run.
	if _, err := conn.pool.Exec(ctx, "DROP SCHEMA IF EXISTS batched_snapshot CASCADE"); err != nil {
		t.Fatalf("drop schema: %v", err)
	}
	if _, err := conn.pool.Exec(ctx, "CREATE SCHEMA batched_snapshot"); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	if _, err := conn.pool.Exec(ctx, `
		CREATE TABLE batched_snapshot.devices (
			id bigint PRIMARY KEY,
			tenant int NOT NULL,
			label text
		)`); err != nil {
		t.Fatalf("create table: %v", err)
	}

	const total = 2500
	if _, err := conn.pool.Exec(ctx, `
		INSERT INTO batched_snapshot.devices (id, tenant, label)
		SELECT g, (g % 3) + 1, 'device-' || g FROM generate_series(1, $1) g`, total); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Also create an empty table to verify the single-empty-batch guarantee.
	if _, err := conn.pool.Exec(ctx, "CREATE TABLE batched_snapshot.empty_t (id bigint PRIMARY KEY, v text)"); err != nil {
		t.Fatalf("create empty table: %v", err)
	}

	// Composite-PK table must exist before the snapshot is exported, since
	// SET TRANSACTION SNAPSHOT only sees relations visible at that point.
	if _, err := conn.pool.Exec(ctx, `
		CREATE TABLE batched_snapshot.composite (
			tenant int NOT NULL, id bigint NOT NULL, label text,
			PRIMARY KEY (tenant, id)
		);
		INSERT INTO batched_snapshot.composite (tenant, id, label)
		SELECT (g % 4), g, 'c-' || g FROM generate_series(1, 333) g`); err != nil {
		t.Fatalf("create composite table: %v", err)
	}

	// Export a snapshot via a real logical replication slot.
	replConn, err := conn.connectReplication(ctx)
	if err != nil {
		t.Fatalf("replication connect: %v", err)
	}
	res, err := pglogrepl.CreateReplicationSlot(ctx, replConn, "batched_snapshot_slot", "pgoutput",
		pglogrepl.CreateReplicationSlotOptions{Temporary: true, Mode: pglogrepl.LogicalReplication})
	if err != nil {
		t.Fatalf("create slot: %v", err)
	}
	defer func() { _ = replConn.Close(ctx) }()

	// Mutate the table after the snapshot point: none of these rows may
	// appear in the scan, proving every batch reads the exported snapshot.
	if _, err := conn.pool.Exec(ctx, `
		INSERT INTO batched_snapshot.devices (id, tenant, label)
		SELECT g, 9, 'late-' || g FROM generate_series(100000, 100099) g`); err != nil {
		t.Fatalf("post-snapshot insert: %v", err)
	}

	t.Run("single primary key, bounded batches", func(t *testing.T) {
		const batch = 100
		var (
			gotIDs   []int64
			maxBatch int
			calls    int
		)
		err := conn.ScanTable(ctx, "batched_snapshot", "devices", res.SnapshotName, []string{"id"}, batch,
			func(rows []map[string]any) error {
				calls++
				if len(rows) > maxBatch {
					maxBatch = len(rows)
				}
				for _, r := range rows {
					gotIDs = append(gotIDs, r["id"].(int64))
					if r["tenant"].(int32) == 9 {
						t.Error("scan observed a row inserted after the exported snapshot")
					}
				}
				return nil
			})
		if err != nil {
			t.Fatalf("ScanTable: %v", err)
		}
		if len(gotIDs) != total {
			t.Fatalf("rows = %d, want %d", len(gotIDs), total)
		}
		if maxBatch != batch {
			t.Errorf("max batch = %d, want %d", maxBatch, batch)
		}
		if calls != total/batch {
			t.Errorf("batch callbacks = %d, want %d", calls, total/batch)
		}
		if !sort.SliceIsSorted(gotIDs, func(i, j int) bool { return gotIDs[i] < gotIDs[j] }) {
			t.Error("rows were not returned in primary-key order across batches")
		}
		// Keyset pagination must not lose or duplicate rows.
		seen := map[int64]bool{}
		for _, id := range gotIDs {
			if seen[id] {
				t.Fatalf("duplicate id %d across batch boundary", id)
			}
			seen[id] = true
		}
	})

	t.Run("composite primary key", func(t *testing.T) {
		keys := map[string]bool{}
		var n int
		err := conn.ScanTable(ctx, "batched_snapshot", "composite", res.SnapshotName, []string{"tenant", "id"}, 37,
			func(rows []map[string]any) error {
				for _, r := range rows {
					key := fmt.Sprintf("%d|%d", r["tenant"], r["id"])
					if keys[key] {
						t.Errorf("duplicate composite key %s", key)
					}
					keys[key] = true
					n++
				}
				return nil
			})
		if err != nil {
			t.Fatalf("composite ScanTable: %v", err)
		}
		if n != 333 || len(keys) != 333 {
			t.Errorf("composite rows = %d (unique keys %d), want 333", n, len(keys))
		}
	})

	t.Run("empty table yields one empty batch", func(t *testing.T) {
		calls := 0
		err := conn.ScanTable(ctx, "batched_snapshot", "empty_t", res.SnapshotName, []string{"id"}, 50,
			func(rows []map[string]any) error {
				calls++
				if len(rows) != 0 {
					t.Errorf("empty table batch had %d rows", len(rows))
				}
				return nil
			})
		if err != nil {
			t.Fatalf("empty ScanTable: %v", err)
		}
		if calls != 1 {
			t.Errorf("callbacks = %d, want exactly 1", calls)
		}
	})

	t.Run("invalid batch size rejected", func(t *testing.T) {
		if err := conn.ScanTable(ctx, "batched_snapshot", "devices", res.SnapshotName, []string{"id"}, 0,
			func([]map[string]any) error { return nil }); err == nil {
			t.Error("expected error for non-positive batch size")
		}
	})
}
