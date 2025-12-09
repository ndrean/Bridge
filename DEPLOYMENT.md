# Bridge Deployment Guide

This document describes the recommended production deployment model for the CDC bridge.

## Architecture

The bridge uses a **hybrid approach**: it tries to create infrastructure (slots/publications) but gracefully falls back to using pre-existing infrastructure. This provides flexibility for both development and production environments.

## Production Deployment Steps

### 1. Database Administrator Tasks (One-Time Setup)

The database administrator must set up the following infrastructure:

#### a) Create Bridge User with Minimal Privileges

```sql
-- Create dedicated bridge user
CREATE USER bridge_reader WITH PASSWORD 'secure_password_here';

-- Grant replication privilege (required for logical replication)
ALTER USER bridge_reader WITH REPLICATION;

-- Limit concurrent connections
ALTER USER bridge_reader CONNECTION LIMIT 5;

-- Grant connect to database
GRANT CONNECT ON DATABASE your_database TO bridge_reader;

-- Grant SELECT on all tables (current and future)
GRANT USAGE ON SCHEMA public TO bridge_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO bridge_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO bridge_reader;
```

**Note**: `bridge_reader` does NOT need SUPERUSER privilege in production.

#### b) Create Publication for All Tables

```sql
-- Create publication as database superuser (postgres)
CREATE PUBLICATION cdc_pub FOR ALL TABLES;
```

**Why**: `FOR ALL TABLES` publications require SUPERUSER privilege to create. By pre-creating it as an admin, the bridge user only needs REPLICATION privilege at runtime.

#### c) Optionally Pre-Create Replication Slot

```sql
-- Optional: Pre-create replication slot
SELECT pg_create_logical_replication_slot('cdc_slot', 'pgoutput');
```

**Note**: The bridge can create its own slot with just REPLICATION privilege, but pre-creating it gives admins more control.

### 2. NATS Administrator Tasks (One-Time Setup)

#### a) Configure NATS Account and User

In `nats-server.conf`:

```conf
accounts {
  BRIDGE: {
    jetstream: {
      max_memory: 1GB
      max_file: 10GB
      max_streams: 10
      max_consumers: 10
    }
    users: [
      { user: "bridge_user", password: "secure_password_here" }
    ]
  }
}
```

#### b) Create JetStream Streams

```bash
# CDC stream (short retention for real-time events)
nats stream add CDC \
  --server=nats://bridge_user:password@nats-server:4222 \
  --subjects='cdc.>' \
  --storage=file \
  --retention=limits \
  --max-age=1m \
  --max-msgs=1000000 \
  --max-bytes=1G \
  --replicas=1

# INIT stream (longer retention for snapshots)
nats stream add INIT \
  --server=nats://bridge_user:password@nats-server:4222 \
  --subjects='init.>' \
  --storage=file \
  --retention=limits \
  --max-age=7d \
  --max-msgs=10000000 \
  --max-bytes=8G \
  --replicas=1

# Schemas KV bucket
nats kv add schemas \
  --server=nats://bridge_user:password@nats-server:4222 \
  --history=10 \
  --replicas=1
```

### 3. Bridge Configuration

Configure the bridge via environment variables:

```bash
# PostgreSQL connection (using bridge_reader user)
export PG_HOST=postgres.example.com
export PG_PORT=5432
export PG_USER=bridge_reader
export PG_PASSWORD=secure_password_here
export PG_DB=your_database

# NATS connection
export NATS_HOST=nats.example.com
export NATS_USER=bridge_user
export NATS_PASSWORD=secure_password_here

# Optional: Bridge configuration
export SLOT_NAME=cdc_slot        # default: cdc_slot
export PUB_NAME=cdc_pub          # default: cdc_pub
export HTTP_PORT=6543            # default: 6543
```

### 4. Start the Bridge

```bash
./bridge
```

## What the Bridge Does at Startup

1. **Verifies/Creates Replication Slot**:
   - Checks if `cdc_slot` exists
   - If not, tries to create it (requires REPLICATION privilege)
   - If creation fails, checks if it was created externally and uses it

2. **Verifies/Creates Publication**:
   - Checks if `cdc_pub` exists
   - If not, tries to create it (requires SUPERUSER privilege)
   - If creation fails, checks if it was created externally and uses it

3. **Verifies NATS Streams**:
   - Checks if CDC and INIT streams exist
   - Fails fast if streams don't exist (must be created by admin)

4. **Starts Streaming**:
   - Publishes all CDC events from all tables to NATS subjects: `cdc.<table>.<operation>`
   - Listens for snapshot requests on `snapshot.request.>`

## Security Model Summary

| Component | Privilege Required | Why |
|-----------|-------------------|-----|
| **bridge_reader** | REPLICATION | Read WAL, create/use replication slots |
| **bridge_reader** | SELECT | Read table schemas and snapshot data |
| **Publication (create)** | SUPERUSER | FOR ALL TABLES requires SUPERUSER |
| **Publication (use)** | None | Any user can subscribe to existing publications |
| **bridge_user (NATS)** | Consumer | Publish to streams, write to KV buckets |

**Key Insight**: By pre-creating the publication as a superuser, the bridge runtime user only needs REPLICATION + SELECT privileges.

## Development vs Production

### Development (docker-compose.yml)

The `postgres-init.sql` script handles all setup automatically:
- Creates `bridge_reader` user
- Creates `cdc_pub` publication
- Creates test tables

The `nats-init` container creates streams automatically.

### Production

Administrators manually create:
1. Database user with REPLICATION privilege
2. Publication as superuser
3. NATS streams and account

The bridge verifies infrastructure exists and starts streaming.

## Monitoring

The bridge exposes an HTTP server (default port 6543) with:

- `GET /health` - Health check
- `GET /status` - Bridge status (JSON)
- `GET /metrics` - Prometheus metrics
- `GET /streams/info?stream=CDC` - NATS stream information

## Troubleshooting

### "Failed to create replication slot"

**Solution**: Admin must create the slot manually:
```sql
SELECT pg_create_logical_replication_slot('cdc_slot', 'pgoutput');
```

Or grant REPLICATION privilege to bridge user.

### "Failed to create publication"

**Solution**: Admin must create the publication manually:
```sql
CREATE PUBLICATION cdc_pub FOR ALL TABLES;
```

The bridge will detect it exists and use it.

### "Stream not found"

**Solution**: Admin must create NATS streams using the commands in section 2b above.

## References

- PostgreSQL Logical Replication: https://www.postgresql.org/docs/current/logical-replication.html
- NATS JetStream: https://docs.nats.io/nats-concepts/jetstream
- Bridge Source Code: `src/replication_setup.zig`
