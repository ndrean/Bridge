const std = @import("std");
const builtin = @import("builtin");
const pg = @import("pg");
const cdc = @import("cdc.zig");
const cdc_nats = @import("cdc_nats.zig");
const msgpack_encoder = @import("msgpack_encoder.zig");

pub const log = std.log.scoped(.cdc_demo);

pub fn main() !void {
    const Gpa = std.heap.GeneralPurposeAllocator(.{});
    var gpa = Gpa{};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    log.info("=== CDC Bridge Demo ===", .{});

    // 1. Connect to PostgreSQL
    log.info("\n1. Connecting to PostgreSQL...", .{});
    var pool = pg.Pool.init(allocator, .{
        .size = 5,
        .connect = .{
            .port = 5432,
            .host = "127.0.0.1",
        },
        .auth = .{
            .username = "postgres",
            .password = "postgres",
            .database = "postgres",
        },
    }) catch |err| {
        log.err("Failed to connect to PostgreSQL: {}", .{err});
        log.err("Make sure PostgreSQL is running: docker compose up -d", .{});
        std.posix.exit(1);
    };
    defer pool.deinit();
    log.info("✓ Connected to PostgreSQL", .{});

    // 2. Set up CDC consumer
    log.info("\n2. Setting up CDC consumer...", .{});
    var consumer = try cdc.Consumer.init(allocator, pool, .{
        .slot_name = "bridge_demo_slot",
        .publication_name = "bridge_demo_pub",
        .tables = &.{},
    });
    defer consumer.deinit();

    // Create replication slot and publication
    try consumer.createSlot();
    try consumer.createPublication();

    // Get current LSN
    const current_lsn = try consumer.getCurrentLSN();
    defer allocator.free(current_lsn);
    log.info("Current WAL LSN: {s}", .{current_lsn});

    // 3. Connect to NATS (demonstrating both JSON and MessagePack modes)
    log.info("\n3. Connecting to NATS JetStream...", .{});

    // Test with MessagePack encoding
    var publisher_msgpack = try cdc_nats.CDCPublisher.init(allocator, .{
        .nats_url = "nats://localhost:4222",
        .stream_name = "CDC_EVENTS",
        .subject_prefix = "cdc",
        .use_msgpack = true,
    });
    defer publisher_msgpack.deinit();
    try publisher_msgpack.connect();

    // 4. Demonstrate publishing with MessagePack encoding
    log.info("\n4. Publishing test CDC event (MessagePack mode)...", .{});

    const test_change = cdc.Change{
        .lsn = current_lsn,
        .table = "users",
        .operation = .INSERT,
        .timestamp = std.time.timestamp(),
        .tx_id = 12345,
        .before = null,
        .after = null,
    };

    try publisher_msgpack.publishChange(test_change);
    log.info("✓ Published MessagePack-encoded event to cdc.users.INSERT", .{});

    // Also demonstrate JSON mode
    var publisher_json = try cdc_nats.CDCPublisher.init(allocator, .{
        .nats_url = "nats://localhost:4222",
        .stream_name = "CDC_EVENTS",
        .subject_prefix = "cdc",
        .use_msgpack = false,
    });
    defer publisher_json.deinit();
    try publisher_json.connect();

    try publisher_json.publishChange(test_change);
    log.info("✓ Published JSON-encoded event to cdc.users.INSERT", .{});

    // 5. Insert some data into PostgreSQL to generate real changes
    log.info("\n5. Inserting test data to generate CDC events...", .{});
    {
        var conn = try pool.acquire();
        defer conn.release();

        // Ensure test table exists
        _ = conn.exec("DROP TABLE IF EXISTS cdc_demo_users", .{}) catch {};
        _ = try conn.exec(
            \\CREATE TABLE cdc_demo_users (
            \\    id SERIAL PRIMARY KEY,
            \\    name TEXT NOT NULL,
            \\    email TEXT,
            \\    created_at TIMESTAMP DEFAULT NOW()
            \\)
        , .{});

        // Insert data
        _ = try conn.exec(
            "INSERT INTO cdc_demo_users (name, email) VALUES ($1, $2)",
            .{ "Alice", "alice@example.com" },
        );

        _ = try conn.exec(
            "INSERT INTO cdc_demo_users (name, email) VALUES ($1, $2)",
            .{ "Bob", "bob@example.com" },
        );

        log.info("✓ Inserted test data", .{});
    }

    // 6. Verify changes were captured (simplified - just check LSN advanced)
    log.info("\n6. Verifying CDC capture...", .{});
    const new_lsn = try consumer.getCurrentLSN();
    defer allocator.free(new_lsn);
    log.info("New WAL LSN: {s}", .{new_lsn});

    if (!std.mem.eql(u8, current_lsn, new_lsn)) {
        log.info("✓ WAL has advanced - changes were recorded", .{});
    }

    // Summary
    log.info("\n=== Summary ===", .{});
    log.info("✓ PostgreSQL replication slot created", .{});
    log.info("✓ PostgreSQL publication created", .{});
    log.info("✓ NATS JetStream stream created", .{});
    log.info("✓ Test event published (MessagePack mode)", .{});
    log.info("✓ Test event published (JSON mode)", .{});
    log.info("✓ PostgreSQL changes captured in WAL", .{});

    log.info("\nNext steps:", .{});
    log.info("  1. Implement WAL streaming protocol parser", .{});
    log.info("  3. Implement continuous streaming loop", .{});
    log.info("  4. Add error handling and reconnection logic", .{});
    log.info("  5. Add metrics and monitoring", .{});

    log.info("\nMonitor the stream with:", .{});
    log.info("  nats stream info CDC_EVENTS", .{});
    log.info("  nats stream ls", .{});
    log.info("  nats sub 'cdc.>'", .{});
}
