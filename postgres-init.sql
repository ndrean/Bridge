-- Initialize PostgreSQL with settings for CDC
-- Create a publication for all tables (for logical replication)
-- This will be used later for CDC
-- Note: Publications are created per database, we'll create it after tables exist
-- Set up some initial configuration
ALTER SYSTEM
SET
  wal_level = 'logical';

ALTER SYSTEM
SET
  max_replication_slots = 10;

ALTER SYSTEM
SET
  max_wal_senders = 10;

-- Limit WAL retention to prevent disk fill
ALTER SYSTEM
SET
  max_slot_wal_keep_size = '10GB';

SELECT
  pg_reload_conf ();

-- ============================================================================
-- Read-Only User for CDC Bridge (Production Security)
-- ============================================================================
-- This section creates a dedicated read-only user for the CDC bridge.
-- Benefits:
-- - No write access to tables (except replication slot management)
-- - Principle of least privilege
-- - Can be used instead of postgres superuser in production

-- Create bridge_reader user with password
CREATE USER bridge_reader WITH PASSWORD 'bridge_password_changeme';

-- Grant replication privileges (required for logical replication)
ALTER USER bridge_reader WITH REPLICATION;

-- Limit concurrent connections from bridge user
ALTER USER bridge_reader CONNECTION LIMIT 5;

-- Note: bridge_reader does NOT need SUPERUSER privilege because:
-- 1. The publication is pre-created below by postgres superuser
-- 2. The replication slot can be created by any user with REPLICATION privilege
-- This is the recommended production security model.

-- Grant connect to database
GRANT CONNECT ON DATABASE postgres TO bridge_reader;

-- Grant CREATE privilege (required for creating publications and replication slots)
GRANT CREATE ON DATABASE postgres TO bridge_reader;

-- Grant usage on schema
GRANT USAGE ON SCHEMA public TO bridge_reader;

-- Grant SELECT on all tables (current and future)
GRANT SELECT ON ALL TABLES IN SCHEMA public TO bridge_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO bridge_reader;

-- Grant usage on sequences (for reading column metadata)
GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO bridge_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON SEQUENCES TO bridge_reader;

-- Note: The user still needs replication privileges to:
-- 1. Create/drop replication slots (pg_create_logical_replication_slot)
-- 2. Read from replication slots (pg_logical_slot_get_changes)
-- 3. Query pg_stat_replication and other replication catalogs

-- ============================================================================
-- Test Tables
-- ============================================================================

-- Test table with various PostgreSQL types
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
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Users table for CDC testing
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create publication for all tables AFTER tables exist
DROP PUBLICATION IF EXISTS cdc_pub;

CREATE PUBLICATION cdc_pub FOR ALL TABLES;
