const std = @import("std");
const pg = @import("pg");
const wal_stream = @import("wal_stream.zig");
const pgoutput = @import("pgoutput.zig");
const replication_setup = @import("replication_setup.zig");

pub const log = std.log.scoped(.wal_stream_test);

pub fn main() !void {
    const Gpa = std.heap.GeneralPurposeAllocator(.{});
    var gpa = Gpa{};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    log.info("=== WAL Streaming Test ===" ++ "\n", .{});

    // 1. Create replication slot and publication using libpq
    log.info("1. Setting up replication infrastructure...", .{});
    const setup = replication_setup.ReplicationSetup{
        .allocator = allocator,
        .host = "127.0.0.1",
        .port = 5432,
        .user = "postgres",
        .password = "postgres",
        .database = "postgres",
    };

    try setup.createSlot("bridge_stream_slot");
    try setup.createPublication("bridge_stream_pub", &.{});
    log.info("✓ Replication infrastructure ready" ++ "\n", .{});

    // Still need pg.zig pool for inserting test data
    var pool = pg.Pool.init(allocator, .{
        .size = 2,
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

    // 2. Connect to replication stream
    log.info("2. Connecting to WAL stream...", .{});
    var stream = wal_stream.ReplicationStream.init(allocator, .{
        .host = "127.0.0.1",
        .port = 5432,
        .user = "postgres",
        .password = "postgres",
        .database = "postgres",
        .slot_name = "bridge_stream_slot",
        .publication_name = "bridge_stream_pub",
    });
    defer stream.deinit();

    try stream.connect();
    log.info("✓ WAL stream connected" ++ "\n", .{});

    // 3. Start streaming from current position
    log.info("3. Starting replication...", .{});
    try stream.startStreaming(null);
    log.info("✓ Replication stream started" ++ "\n", .{});

    // 4. Insert some test data to generate WAL records
    log.info("4. Generating test data...", .{});
    {
        var conn = try pool.acquire();
        defer conn.release();

        _ = conn.exec("DROP TABLE IF EXISTS wal_stream_test", .{}) catch {};
        _ = try conn.exec(
            \\CREATE TABLE wal_stream_test (
            \\    id SERIAL PRIMARY KEY,
            \\    data TEXT
            \\)
        , .{});

        _ = try conn.exec("INSERT INTO wal_stream_test (data) VALUES ($1)", .{"Test record 1"});
        _ = try conn.exec("INSERT INTO wal_stream_test (data) VALUES ($1)", .{"Test record 2"});
        _ = try conn.exec("INSERT INTO wal_stream_test (data) VALUES ($1)", .{"Test record 3"});

        log.info("✓ Test data inserted" ++ "\n", .{});
    }

    // 5. Receive WAL messages
    log.info("5. Receiving WAL messages (10 second timeout)...", .{});
    var msg_count: u32 = 0;
    const start_time = std.time.milliTimestamp();
    const timeout_ms = 10000;

    while (std.time.milliTimestamp() - start_time < timeout_ms) {
        if (stream.receiveMessage()) |maybe_msg| {
            if (maybe_msg) |msg| {
                var wal_msg = msg;
                defer wal_msg.deinit(allocator);

                msg_count += 1;

                // Parse pgoutput messages
                if (wal_msg.type == .xlogdata and wal_msg.payload.len > 0) {
                    var parser = pgoutput.Parser.init(allocator, wal_msg.payload);
                    if (parser.parse()) |parsed_msg| {
                        var pg_msg = parsed_msg;
                        defer {
                            // Free allocated memory in parsed message
                            switch (pg_msg) {
                                .relation => |*r| r.deinit(allocator),
                                .insert => |*ins| ins.deinit(allocator),
                                .update => |*upd| upd.deinit(allocator),
                                .delete => |*del| del.deinit(allocator),
                                else => {},
                            }
                        }

                        log.info("Message #{d}: {s}", .{ msg_count, @tagName(pg_msg) });
                        switch (pg_msg) {
                            .begin => |b| {
                                log.info("  BEGIN xid={d} lsn={x}", .{ b.xid, b.final_lsn });
                            },
                            .commit => |c| {
                                log.info("  COMMIT lsn={x}", .{c.commit_lsn});
                            },
                            .relation => |r| {
                                log.info("  RELATION {s}.{s} (id={d}, {d} columns)", .{ r.namespace, r.name, r.relation_id, r.columns.len });
                            },
                            .insert => |ins| {
                                log.info("  INSERT relation_id={d} cols={d}", .{ ins.relation_id, ins.tuple_data.columns.len });
                            },
                            .update => |upd| {
                                log.info("  UPDATE relation_id={d}", .{upd.relation_id});
                            },
                            .delete => |del| {
                                log.info("  DELETE relation_id={d}", .{del.relation_id});
                            },
                            else => {},
                        }
                    } else |err| {
                        log.warn("Failed to parse pgoutput message: {}", .{err});
                        log.info("  Raw payload ({d} bytes): {x}", .{ wal_msg.payload.len, wal_msg.payload[0..@min(32, wal_msg.payload.len)] });
                    }
                } else if (wal_msg.type == .keepalive) {
                    log.debug("Keepalive message #{d}", .{msg_count});
                }

                // Send acknowledgment
                if (wal_msg.wal_end > 0) {
                    try stream.sendStatusUpdate(wal_msg.wal_end);
                }
            } else {
                // No message available, sleep briefly
                std.Thread.sleep(100 * std.time.ns_per_ms);
            }
        } else |err| {
            if (err == error.StreamEnded) {
                log.info("Stream ended naturally", .{});
                break;
            }
            log.err("Error receiving message: {}", .{err});
            return err;
        }

        // Stop after receiving 20 messages or timeout
        if (msg_count >= 20) {
            log.info("Received 20 messages, stopping", .{});
            break;
        }
    }

    log.info("\n" ++ "=== Summary ===", .{});
    log.info("Total WAL messages received: {d}", .{msg_count});
    log.info("✓ WAL streaming test complete", .{});

    if (msg_count == 0) {
        log.warn("⚠ No WAL messages received - this might indicate:", .{});
        log.warn("  - No changes to replicated tables", .{});
        log.warn("  - Replication slot not advancing", .{});
        log.warn("  - Connection or protocol issues", .{});
    }
}
