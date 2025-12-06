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
-- Snapshot Signaling Table
-- ============================================================================
-- This table allows consumers to request snapshots by inserting records.
-- The bridge listens for NOTIFY events and generates incremental snapshots.
CREATE TABLE IF NOT EXISTS snapshot_requests (
    id SERIAL PRIMARY KEY,
    table_name TEXT NOT NULL,
    requested_at TIMESTAMPTZ DEFAULT NOW(),
    requested_by TEXT,  -- Optional: track which consumer requested (e.g., 'browser-client-123')
    status TEXT DEFAULT 'pending',  -- 'pending', 'processing', 'completed', 'failed'
    snapshot_id TEXT,    -- Filled in by bridge when snapshot starts (e.g., 'snap-1733507200')
    completed_at TIMESTAMPTZ,
    error_message TEXT   -- If status='failed', store error details here
);

-- Create index for faster lookups
CREATE INDEX IF NOT EXISTS idx_snapshot_requests_status ON snapshot_requests(status, requested_at);
CREATE INDEX IF NOT EXISTS idx_snapshot_requests_table ON snapshot_requests(table_name, requested_at DESC);

-- Create notification function
-- This function sends a pg_notify whenever a new snapshot request is inserted
CREATE OR REPLACE FUNCTION notify_snapshot_request()
RETURNS TRIGGER AS $$
BEGIN
    -- Send payload with table name, request ID, and requester
    PERFORM pg_notify('snapshot_request',
        json_build_object(
            'id', NEW.id,
            'table_name', NEW.table_name,
            'requested_by', NEW.requested_by
        )::text
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger to auto-notify on INSERT
DROP TRIGGER IF EXISTS snapshot_request_trigger ON snapshot_requests;
CREATE TRIGGER snapshot_request_trigger
AFTER INSERT ON snapshot_requests
FOR EACH ROW
EXECUTE FUNCTION notify_snapshot_request();

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