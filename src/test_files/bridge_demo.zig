const std = @import("std");
const pg = @import("pg");
const wal_stream = @import("wal_stream.zig");
const pgoutput = @import("pgoutput.zig");
const nats_publisher = @import("nats_publisher.zig");
const replication_setup = @import("replication_setup.zig");
const msgpack = @import("msgpack");

pub const log = std.log.scoped(.bridge_demo);

/// Encode a simple CDC event to MessagePack
///
/// Caller is responsible for freeing the returned slice
fn encodeCDCEvent(allocator: std.mem.Allocator, table: []const u8, operation: []const u8) ![]u8 {
    var buffer: [512]u8 = undefined;

    const compat = msgpack.compat;
    var write_buffer = compat.fixedBufferStream(&buffer);
    var read_buffer = compat.fixedBufferStream(&buffer);

    const BufferType = compat.BufferStream;
    var packer = msgpack.Pack(
        *BufferType,
        *BufferType,
        BufferType.WriteError,
        BufferType.ReadError,
        BufferType.write,
        BufferType.read,
    ).init(&write_buffer, &read_buffer);

    // Create map with table and operation
    var root = msgpack.Payload.mapPayload(allocator);
    defer root.free(allocator);

    try root.mapPut("table", try msgpack.Payload.strToPayload(table, allocator));
    try root.mapPut("operation", try msgpack.Payload.strToPayload(operation, allocator));

    // Write the payload
    try packer.write(root);

    // Copy to owned slice
    const written = write_buffer.pos;
    return try allocator.dupe(u8, buffer[0..written]);
}

pub fn main() !void {
    const Gpa = std.heap.GeneralPurposeAllocator(.{});
    var gpa = Gpa{};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    log.info("=== PostgreSQL CDC Bridge to NATS Demo ===\n", .{});

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

    try setup.createSlot("bridge_demo_slot");
    try setup.createPublication("bridge_demo_pub", &.{});
    log.info("✓ Replication infrastructure ready\n", .{});

    // Still need pg.zig pool for inserting test data
    // var pool = pg.Pool.init(allocator, .{
    //     .size = 2,
    //     .connect = .{
    //         .port = 5432,
    //         .host = "127.0.0.1",
    //     },
    //     .auth = .{
    //         .username = "postgres",
    //         .password = "postgres",
    //         .database = "postgres",
    //     },
    // }) catch |err| {
    //     log.err("Failed to connect to PostgreSQL: {}", .{err});
    //     log.err("Make sure PostgreSQL is running: docker compose up -d", .{});
    //     std.posix.exit(1);
    // };
    // defer pool.deinit();

    // 2. Connect to NATS JetStream
    log.info("2. Connecting to NATS JetStream...", .{});
    var publisher = try nats_publisher.Publisher.init(allocator, .{
        .url = "nats://localhost:4222",
    });
    defer publisher.deinit();
    try publisher.connect();

    // Create CDC stream
    const stream_config = nats_publisher.StreamConfig{
        .name = "CDC_BRIDGE",
        .subjects = &.{"cdc.>"},
    };
    try nats_publisher.createStream(publisher.js.?, allocator, stream_config);
    log.info("✓ NATS JetStream connected\n", .{});

    // 3. Get current LSN to skip historical data
    log.info("3. Getting current LSN position...", .{});
    const current_lsn = try setup.getCurrentLSN();
    defer allocator.free(current_lsn);
    log.info("Current LSN: {s}\n", .{current_lsn});

    // 4. Connect to replication stream starting from current LSN
    log.info("4. Connecting to WAL replication stream...", .{});
    var pg_stream = wal_stream.ReplicationStream.init(allocator, .{
        .host = "127.0.0.1",
        .port = 5432,
        .user = "postgres",
        .password = "postgres",
        .database = "postgres",
        .slot_name = "bridge_demo_slot",
        .publication_name = "bridge_demo_pub",
    });
    defer pg_stream.deinit();

    try pg_stream.connect();
    try pg_stream.startStreaming(current_lsn);
    log.info("✓ WAL replication stream started from LSN {s}\n", .{current_lsn});

    // 6. Stream CDC events to NATS
    // Expected: 500 INSERTs + 250 UPDATEs + ~99 DELETEs = ~849 CDC events
    // (One DELETE happens in same transaction as CREATE due to i%5==0 at i=0)
    const expected_cdc_events = 849;
    log.info("6. Streaming CDC events to NATS (expecting ~{d} events, 60s timeout)...\n", .{expected_cdc_events});
    var msg_count: u32 = 0;
    var cdc_events: u32 = 0;
    var last_lsn: u64 = 0;
    var last_ack_lsn: u64 = 0; // Track last acknowledged LSN for keepalives
    const start_time = std.time.milliTimestamp();
    const timeout_ms = 60000;

    // Track relation metadata (table info)
    var relation_map = std.AutoHashMap(u32, pgoutput.RelationMessage).init(allocator);
    defer {
        var it = relation_map.valueIterator();
        while (it.next()) |rel| {
            var r = rel.*;
            r.deinit(allocator);
        }
        relation_map.deinit();
    }

    while (std.time.milliTimestamp() - start_time < timeout_ms) {
        if (pg_stream.receiveMessage()) |maybe_msg| {
            if (maybe_msg) |wal_msg_val| {
                var wal_msg = wal_msg_val;
                defer wal_msg.deinit(allocator);

                msg_count += 1;

                // Parse and publish pgoutput messages
                if (wal_msg.type == .xlogdata and wal_msg.payload.len > 0) {
                    var parser = pgoutput.Parser.init(allocator, wal_msg.payload);
                    if (parser.parse()) |parsed_msg| {
                        var pg_msg = parsed_msg;
                        defer {
                            switch (pg_msg) {
                                .relation => {}, // Don't deinit relations - we store them in the map
                                .insert => |*ins| ins.deinit(allocator),
                                .update => |*upd| upd.deinit(allocator),
                                .delete => |*del| del.deinit(allocator),
                                else => {},
                            }
                        }

                        switch (pg_msg) {
                            .relation => |rel| {
                                // Store relation metadata for future use
                                try relation_map.put(rel.relation_id, rel);
                                log.info("RELATION: {s}.{s} (id={d}, {d} columns)", .{ rel.namespace, rel.name, rel.relation_id, rel.columns.len });
                            },
                            .begin => |b| {
                                log.info("BEGIN: xid={d} lsn={x}", .{ b.xid, b.final_lsn });
                            },
                            .insert => |ins| {
                                if (relation_map.get(ins.relation_id)) |rel| {
                                    log.info("INSERT: {s}.{s}", .{ rel.namespace, rel.name });

                                    // Publish to NATS
                                    const subject = try std.fmt.allocPrintSentinel(
                                        allocator,
                                        "cdc.{s}.insert",
                                        .{rel.name},
                                        0,
                                    );
                                    defer allocator.free(subject);

                                    // Encode as MessagePack
                                    const payload = try encodeCDCEvent(allocator, rel.name, "INSERT");
                                    defer allocator.free(payload);

                                    // Generate message ID from WAL LSN for idempotent delivery
                                    const msg_id = try std.fmt.allocPrint(
                                        allocator,
                                        "{x}-{s}-insert",
                                        .{ wal_msg.wal_end, rel.name },
                                    );
                                    defer allocator.free(msg_id);

                                    try publisher.publish(subject, payload, msg_id);
                                    cdc_events += 1;
                                    log.info("  → Published to NATS: cdc.{s} (msg_id: {s})", .{ subject, msg_id });
                                }
                            },
                            .update => |upd| {
                                if (relation_map.get(upd.relation_id)) |rel| {
                                    log.info("UPDATE: {s}.{s}", .{ rel.namespace, rel.name });

                                    const subject = try std.fmt.allocPrintSentinel(
                                        allocator,
                                        "cdc.{s}.update",
                                        .{rel.name},
                                        0,
                                    );
                                    defer allocator.free(subject);

                                    const payload = try encodeCDCEvent(allocator, rel.name, "UPDATE");
                                    defer allocator.free(payload);

                                    // Generate message ID from WAL LSN for idempotent delivery
                                    const msg_id = try std.fmt.allocPrint(
                                        allocator,
                                        "{x}-{s}-update",
                                        .{ wal_msg.wal_end, rel.name },
                                    );
                                    defer allocator.free(msg_id);

                                    try publisher.publish(subject, payload, msg_id);
                                    cdc_events += 1;
                                    log.info("  → Published to NATS: cdc.{s} (msg_id: {s})", .{ subject, msg_id });
                                }
                            },
                            .delete => |del| {
                                if (relation_map.get(del.relation_id)) |rel| {
                                    log.info("DELETE: {s}.{s}", .{ rel.namespace, rel.name });

                                    const subject = try std.fmt.allocPrintSentinel(
                                        allocator,
                                        "cdc.{s}.delete",
                                        .{rel.name},
                                        0,
                                    );
                                    defer allocator.free(subject);

                                    const payload = try encodeCDCEvent(allocator, rel.name, "DELETE");
                                    defer allocator.free(payload);

                                    // Generate message ID from WAL LSN for idempotent delivery
                                    const msg_id = try std.fmt.allocPrint(
                                        allocator,
                                        "{x}-{s}-delete",
                                        .{ wal_msg.wal_end, rel.name },
                                    );
                                    defer allocator.free(msg_id);

                                    try publisher.publish(subject, payload, msg_id);
                                    cdc_events += 1;
                                    log.info("  → Published to NATS: cdc.{s} (msg_id: {s})", .{ subject, msg_id });
                                }
                            },
                            .commit => |c| {
                                // Track LSN progression
                                if (c.commit_lsn != last_lsn) {
                                    const lsn_diff = c.commit_lsn - last_lsn;
                                    log.info("COMMIT: lsn={x} (delta: +{d})", .{ c.commit_lsn, lsn_diff });
                                    last_lsn = c.commit_lsn;
                                } else {
                                    log.info("COMMIT: lsn={x}", .{c.commit_lsn});
                                }
                            },
                            else => {},
                        }
                    } else |err| {
                        log.warn("Failed to parse pgoutput message: {}", .{err});
                    }
                }

                // Send acknowledgment for all messages with wal_end
                if (wal_msg.wal_end > 0) {
                    try pg_stream.sendStatusUpdate(wal_msg.wal_end);
                    last_ack_lsn = wal_msg.wal_end;
                }
            } else {
                // No message available - short sleep to avoid busy waiting
                std.Thread.sleep(10 * std.time.ns_per_ms);
            }
        } else |err| {
            if (err == error.StreamEnded) {
                log.info("Stream ended", .{});
                break;
            }
            log.err("Error receiving message: {}", .{err});
            return err;
        }

        // Stop once we've received all expected CDC events
        if (cdc_events >= expected_cdc_events) {
            log.info("\n✓ Received all {d} expected CDC events, stopping...", .{cdc_events});
            break;
        }
    }

    log.info("\n=== Summary ===", .{});
    log.info("Total WAL messages: {d}", .{msg_count});
    log.info("CDC events published to NATS: {d}", .{cdc_events});
    log.info("✓ CDC Bridge Demo Complete!\n", .{});

    log.info("Monitor NATS stream with:", .{});
    log.info("  nats stream info CDC_BRIDGE", .{});
    log.info("  nats sub 'cdc.>'", .{});
}
