#!/usr/bin/env bash
# Start a throwaway PostgreSQL 15+ instance for the integration tests
# (TestScanTable_Integration, TestInit_BatchedSnapshot_EndToEnd). No root
# required: the data directory and unix socket live under /tmp.
#
# Usage:
#   scripts/test-postgres.sh start
#   export PG_CDC_TEST_DATABASE="postgres://postgres@localhost/cdc_test?host=/tmp/pgtest&sslmode=disable"
#   go test -race -count=1 ./...
#   scripts/test-postgres.sh stop
set -euo pipefail

PREFIX="${PG_TEST_PREFIX:-/tmp/pgtest}"
DATA_DIR="$PREFIX/data"
SOCKET_DIR="$PREFIX"
DB_NAME="cdc_test"
TEST_URL="postgres://postgres@localhost/$DB_NAME?host=$SOCKET_DIR&sslmode=disable"

# Locate the Debian-packaged binaries when they are not on PATH.
if ! command -v initdb >/dev/null 2>&1 && [ -x /usr/lib/postgresql/15/bin/initdb ]; then
  export PATH="$PATH:/usr/lib/postgresql/15/bin"
fi

start() {
  if [ -d "$DATA_DIR" ]; then
    echo "data directory $DATA_DIR already exists; run '$0 stop' first" >&2
    exit 1
  fi
  mkdir -p "$SOCKET_DIR"
  initdb -D "$DATA_DIR" -U postgres --auth=trust >/dev/null
  {
    echo "listen_addresses = ''"
    echo "unix_socket_directories = '$SOCKET_DIR'"
    echo "wal_level = logical"
    echo "max_replication_slots = 10"
    echo "max_wal_senders = 10"
  } >> "$DATA_DIR/postgresql.conf"
  pg_ctl -D "$DATA_DIR" -l "$PREFIX/server.log" -w start
  createdb -h "$SOCKET_DIR" -U postgres "$DB_NAME"
  echo "PostgreSQL is ready."
  echo "Export this before running the tests:"
  echo "  export PG_CDC_TEST_DATABASE='$TEST_URL'"
}

stop() {
  if [ ! -d "$DATA_DIR" ]; then
    echo "no data directory at $DATA_DIR; nothing to stop"
    exit 0
  fi
  pg_ctl -D "$DATA_DIR" -w stop || true
  rm -rf "$PREFIX"
}

case "${1:-}" in
  start) start ;;
  stop) stop ;;
  url) echo "$TEST_URL" ;;
  *)
    echo "usage: $0 {start|stop|url}" >&2
    exit 2
    ;;
esac
