#!/bin/bash
set -euo pipefail

PRIMARY_HOST="${PRIMARY_HOST:-postgres-primary}"
PRIMARY_PORT="${PRIMARY_PORT:-5432}"
REPLICATION_USER="${REPLICATION_USER:-replicator}"
REPLICATION_PASSWORD="${REPLICATION_PASSWORD:?REPLICATION_PASSWORD is required}"
REPLICATION_SLOT="${REPLICATION_SLOT:-lottery_replica_slot}"

mkdir -p "$PGDATA"
chown -R postgres:postgres "$PGDATA"

if [ ! -s "$PGDATA/PG_VERSION" ]; then
  echo "Waiting for primary PostgreSQL at ${PRIMARY_HOST}:${PRIMARY_PORT}..."
  until pg_isready -h "$PRIMARY_HOST" -p "$PRIMARY_PORT" -U "$REPLICATION_USER" >/dev/null 2>&1; do
    sleep 2
  done

  echo "Taking base backup from primary..."
  rm -rf "${PGDATA:?}"/*
  export PGPASSWORD="$REPLICATION_PASSWORD"
  gosu postgres pg_basebackup \
    -h "$PRIMARY_HOST" \
    -p "$PRIMARY_PORT" \
    -U "$REPLICATION_USER" \
    -D "$PGDATA" \
    -Fp -Xs -P -R \
    -C -S "$REPLICATION_SLOT"
  unset PGPASSWORD
fi

exec /usr/local/bin/docker-entrypoint.sh "$@"
