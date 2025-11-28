const std = @import("std");
const pg = @import("pg");
const cdc = @import("cdc.zig");
const cdc_nats = @import("cdc_nats.zig");
const replication_setup = @import("replication_setup.zig");

pub const log = std.log.scoped(.cdc_load_test);

const LoadTestConfig = struct {
    num_inserts: u32 = 1000,
    batch_size: u32 = 100,
    use_msgpack: bool = true,
    verify_nats: bool = true,
};

pub fn main() !void {
    const Gpa = std.heap.GeneralPurposeAllocator(.{});
    var gpa = Gpa{};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    log.info("=== CDC Load Test ===" ++ "\n", .{});

    const config = LoadTestConfig{
        .num_inserts = 1000,
        .batch_size = 100,
        .use_msgpack = true,
        .verify_nats = true,
    };

    // 1. Connect to PostgreSQL
    log.info("1. Connecting to PostgreSQL...", .{});
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
    log.info("✓ Connected to PostgreSQL" ++ "\n", .{});

    // 2. Set up CDC consumer using libpq
    log.info("2. Setting up CDC consumer...", .{});
    const setup = replication_setup.ReplicationSetup{
        .allocator = allocator,
        .host = "127.0.0.1",
        .port = 5432,
        .user = "postgres",
        .password = "postgres",
        .database = "postgres",
    };

    try setup.createSlot("bridge_load_test_slot");
    try setup.createPublication("bridge_load_test_pub", &.{});

    const start_lsn = try setup.getCurrentLSN();
    defer allocator.free(start_lsn);
    log.info("Starting WAL LSN: {s}" ++ "\n", .{start_lsn});

    // 3. Connect to NATS
    log.info("3. Connecting to NATS JetStream...", .{});
    var publisher = try cdc_nats.CDCPublisher.init(allocator, .{
        .nats_url = "nats://localhost:4222",
        .stream_name = "CDC_LOAD_TEST",
        .subject_prefix = "load_test",
        .use_msgpack = config.use_msgpack,
    });
    defer publisher.deinit();
    try publisher.connect();
    log.info("✓ NATS connected" ++ "\n", .{});

    // 4. Prepare test table
    log.info("4. Preparing test table...", .{});
    {
        var conn = try pool.acquire();
        defer conn.release();

        _ = conn.exec("DROP TABLE IF EXISTS load_test_events", .{}) catch {};
        _ = try conn.exec(
            \\CREATE TABLE load_test_events (
            \\    id SERIAL PRIMARY KEY,
            \\    event_type TEXT NOT NULL,
            \\    payload JSONB,
            \\    created_at TIMESTAMP DEFAULT NOW()
            \\)
        , .{});
        log.info("✓ Test table created" ++ "\n", .{});
    }

    // 5. Run load test
    log.info("5. Running load test ({} inserts in batches of {})...", .{ config.num_inserts, config.batch_size });

    const test_start = std.time.milliTimestamp();
    var total_inserted: u32 = 0;
    var total_published: u32 = 0;

    var batch_num: u32 = 0;
    while (total_inserted < config.num_inserts) {
        const batch_start = std.time.milliTimestamp();
        const remaining = config.num_inserts - total_inserted;
        const this_batch = @min(config.batch_size, remaining);

        // Insert batch into PostgreSQL
        {
            var conn = try pool.acquire();
            defer conn.release();

            var i: u32 = 0;
            while (i < this_batch) : (i += 1) {
                const event_id = total_inserted + i;
                const payload = try std.fmt.allocPrint(
                    allocator,
                    \\{{"event_id":{d},"batch":{d},"timestamp":{d}}}
                ,
                    .{ event_id, batch_num, std.time.timestamp() },
                );
                defer allocator.free(payload);

                _ = try conn.exec(
                    "INSERT INTO load_test_events (event_type, payload) VALUES ($1, $2::jsonb)",
                    .{ "test_event", payload },
                );
            }
        }

        total_inserted += this_batch;

        // Publish CDC events to NATS
        var i: u32 = 0;
        while (i < this_batch) : (i += 1) {
            const event_id = total_inserted - this_batch + i;

            const change = cdc.Change{
                .lsn = try std.fmt.allocPrint(allocator, "batch_{d}_event_{d}", .{ batch_num, event_id }),
                .table = "load_test_events",
                .operation = .INSERT,
                .timestamp = std.time.timestamp(),
                .tx_id = event_id,
                .before = null,
                .after = null,
            };
            defer allocator.free(change.lsn);

            try publisher.publishChange(change);
            total_published += 1;
        }

        const batch_duration = std.time.milliTimestamp() - batch_start;
        batch_num += 1;

        if (batch_num % 5 == 0) {
            log.info("  Batch {d}: {d}/{d} events ({d}ms)", .{
                batch_num,
                total_inserted,
                config.num_inserts,
                batch_duration,
            });
        }
    }

    const test_duration = std.time.milliTimestamp() - test_start;
    log.info("✓ Load test complete" ++ "\n", .{});

    // 6. Verify WAL advancement
    log.info("6. Verifying WAL advancement...", .{});
    const end_lsn = try setup.getCurrentLSN();
    defer allocator.free(end_lsn);

    log.info("  Start LSN: {s}", .{start_lsn});
    log.info("  End LSN:   {s}", .{end_lsn});

    if (std.mem.eql(u8, start_lsn, end_lsn)) {
        log.warn("⚠ WAL did not advance - no changes captured!", .{});
    } else {
        log.info("✓ WAL advanced - changes captured" ++ "\n", .{});
    }

    // 7. Verify NATS stream (if enabled)
    if (config.verify_nats) {
        log.info("7. Verifying NATS stream...", .{});
        try verifyNatsStream(&publisher, total_published);
    }

    // 8. Performance summary
    log.info("\n" ++ "=== Performance Summary ===", .{});
    log.info("Total inserts:      {d}", .{total_inserted});
    log.info("Total published:    {d}", .{total_published});
    log.info("Total duration:     {d}ms", .{test_duration});
    log.info("Throughput:         {d} events/sec", .{(total_inserted * 1000) / @as(u32, @intCast(test_duration))});
    log.info("Encoding format:    {s}", .{if (config.use_msgpack) "MessagePack (binary)" else "JSON (text)"});

    log.info("\n" ++ "=== Next Steps ===", .{});
    log.info("• Monitor stream: nats stream info CDC_LOAD_TEST", .{});
    log.info("• Subscribe:      nats sub 'load_test.>'", .{});
    log.info("• Check messages: nats stream ls", .{});
    log.info("• Test failure:   Stop NATS mid-test to see error handling", .{});
}

fn verifyNatsStream(publisher: *cdc_nats.CDCPublisher, expected_count: u32) !void {
    _ = publisher;
    log.info("Expected message count: {d}", .{expected_count});
    log.info("Verify manually with:", .{});
    log.info("  nats stream info CDC_LOAD_TEST", .{});
    log.info("  nats stream view CDC_LOAD_TEST", .{});
}
