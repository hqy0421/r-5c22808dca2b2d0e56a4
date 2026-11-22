# Init & Snapshot

`pg-cdc init` creates a replication slot and snapshots all discoverable tables to base Parquet files. This follows the native PostgreSQL replica pattern — snapshot + LSN = zero-gap starting point.

## What Happens

```
pg-cdc init
  1. Connect to PostgreSQL
  2. Discover tables in configured schemas
  3. Apply tags + policies → filter included/excluded
  4. CREATE PUBLICATION for included tables only
  5. CREATE_REPLICATION_SLOT → capture LSN + export snapshot
  6. For each included table:
     SET TRANSACTION SNAPSHOT per keyset batch → bounded SELECT →
     one typed Parquet base segment per batch
  7. Read PostgreSQL ACLs → build role profiles (if configured)
  8. Write manifest.json with full catalog (the atomic publish point —
     nothing references the base segments until this file exists)
  9. If catalog.type = "glue":
     Register each active table in AWS Glue (idempotent)
```

## Bounded, segmented snapshots

`init` never loads a whole table into memory. Every table is scanned in
primary-key order using keyset pagination (`WHERE (pk…) > cursor LIMIT
batch_rows`); each batch runs in its own `REPEATABLE READ` transaction that
re-attaches the slot's exported snapshot, so every batch sees the same
consistent point-in-time view while peak memory stays proportional to
`snapshot.batch_rows` (default 50000), not to the table size.

Each batch is written to its own numbered segment under the table's `base/`
directory. An empty table still produces one (empty) segment, so every active
table always has a base path.

```
/var/lib/pg-cdc/output/
├── manifest.json
├── public.orders/
│   └── base/
│       ├── base_000000_part_000000.parquet
│       ├── base_000000_part_000001.parquet
│       └── ...                # one file per snapshot batch, epoch 0
├── public.customers/
│   └── base/
│       └── base_000000_part_000000.parquet
└── ...
```

The manifest remains the single source of truth: readers (compaction, the MCP
query views, downstream consumers) only see a table once `manifest.json` is
written, and compaction reads every segment of the latest base epoch. If init
fails midway, the orphaned segments are invisible without a manifest and are
cleaned up on the next attempt; base files of tables already present in an
existing manifest are never deleted.

## Re-running init with an existing manifest

Init is designed to run once per deployment (the slot must be torn down
before a fresh full snapshot), but it is safe to run again against storage
that already holds a manifest — for example to add newly discovered tables
after a teardown:

- Tables already marked `active` in the existing manifest are **carried
  over**, not re-snapshotted: their base segments, delta epochs, row counts
  and schema stay untouched.
- The manifest's `source.init_lsn` anchor is preserved when any table is
  carried over, so `pg-cdc start` keeps streaming those tables from the same
  point and the existing base never diverges from its incremental starting
  point.
- Tables not yet in the manifest are snapshotted at the new slot and
  published alongside the carried-over entries.
- A table absent from the manifest (e.g. segments left by an init run that
  crashed before writing the manifest) has no protection: its stale base
  files are deleted and rewritten from scratch.

## Usage

```bash
pg-cdc init --config pg-cdc.yml
```

## Output Structure

See [Bounded, segmented snapshots](#bounded-segmented-snapshots) above for the
`base/base_000000_part_NNNNNN.parquet` layout init produces.

## Manifest Content

After init, `manifest.json` contains:

- **Source**: PostgreSQL version, init LSN, timestamp
- **Active tables**: status, primary keys, schema (column types), row count, path
- **Excluded tables**: status=excluded, tags, reason (e.g., "policy:pii")
- **Schemas**: column-level type information (pg_type + parquet_type)
- **Roles**: per-role table + column access (if profiles configured)

## Filters at Init Time

Init is where table-inclusion decisions are baked in:

1. **Tags resolve** — each table is matched against tag definitions (glob patterns)
2. **Policies apply** — tagged tables checked against policy (include/exclude)
3. **Publication scoped** — only included tables go into the Postgres publication
4. **Excluded tables recorded** — manifest shows what was excluded and why
5. **ACL profiles written** — role-based access from PostgreSQL GRANTs (when `profiles.source: pg_acl`)

A table excluded at init time will never appear in the Parquet output or be streamed by `pg-cdc start`.

## Re-running Init

A full re-initialization drops the slot first, because a slot can only be
created once:

```bash
pg-cdc teardown --config pg-cdc.yml    # drop slot + publication
pg-cdc init --config pg-cdc.yml         # create new slot + snapshot
```

The storage manifest is left in place by teardown, so the re-run does not
blindly overwrite a running deployment: tables already in the manifest are
carried over and new tables are snapshotted at the new slot. See
[Re-running init with an existing manifest](#re-running-init-with-an-existing-manifest)
for the exact semantics.

## Catalog registration

When `catalog.type: glue` is set in `pg-cdc.yml`, init registers active tables in the configured Glue database after snapshotting. Registration is idempotent — re-running init on an existing deployment updates existing table definitions rather than failing.

> **Enabling Glue on a deployment that was already initialized?** Don't re-run `init` — it will re-snapshot every table from Postgres (duplicating GB of writes and requiring a slot teardown). Use **`pg-cdc catalog register`** instead: it reads the existing manifest and calls the same upsert path without touching Postgres. See the `catalog` subcommand in [`08-operations.md`](08-operations.md).

For DynamoDB-backed ACL registration and Lake Formation reconciliation, see the [commercial edition](commercial-edition.md).
