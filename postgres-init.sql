#!/bin/bash
set -e

# Defaults (can be overridden in docker-compose)
: "${BRIDGE_READER_PASSWORD:=bridge_password_changeme}"
: "${BRIDGE_READER:=bridge_reader}"
: "${TARGET_DB:=postgres}"

echo "[Postgres Init] Setting up logical replication + bridge user..."

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$TARGET_DB" <<EOF

-- ============================================================================
-- Logical replication configuration (global)
-- ============================================================================
ALTER SYSTEM SET wal_level = 'logical';
ALTER SYSTEM SET max_replication_slots = 10;
ALTER SYSTEM SET max_wal_senders = 10;
ALTER SYSTEM SET max_slot_wal_keep_size = '10GB';

SELECT pg_reload_conf();

-- ============================================================================
-- Create bridge_reader
-- ============================================================================
DO \$\$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_roles WHERE rolname = '${BRIDGE_READER}'
    ) THEN
        CREATE USER ${BRIDGE_READER}
            WITH PASSWORD '${BRIDGE_READER_PASSWORD}';
        ALTER USER ${BRIDGE_READER} WITH REPLICATION;
        ALTER USER ${BRIDGE_READER} CONNECTION LIMIT 5;
    END IF;
END
\$\$;

GRANT CONNECT ON DATABASE ${TARGET_DB} TO ${BRIDGE_READER};
GRANT CREATE  ON DATABASE ${TARGET_DB} TO ${BRIDGE_READER};

GRANT USAGE ON SCHEMA public TO ${BRIDGE_READER};

GRANT SELECT ON ALL TABLES IN SCHEMA public TO ${BRIDGE_READER};
ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT SELECT ON TABLES TO ${BRIDGE_READER};

GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO ${BRIDGE_READER};
ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT SELECT ON SEQUENCES TO ${BRIDGE_READER};

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'cdc_pub') THEN
        CREATE PUBLICATION cdc_pub FOR ALL TABLES;
    END IF;
END;
$$;

CREATE TABLE IF NOT EXISTS test_types (
  id SERIAL PRIMARY KEY,
  uid UUID DEFAULT gen_random_uuid(),
  age INT,
  temperature FLOAT8,
  price NUMERIC(20,8),
  is_true BOOLEAN,
  some_text TEXT,
  tags TEXT[],
  matrix INT[][],
  metadata JSONB,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS users (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  email TEXT,
  create_at TIMESTAMPTZ DEFAULT now()
);

EOF

echo "[Postgres Init] Completed."
