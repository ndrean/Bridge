//! Snapshot request listener and generator
//!
//! Runs in a dedicated thread to:
//! 1. LISTEN for PostgreSQL NOTIFY events on 'snapshot_request' channel
//! 2. Generate incremental snapshots in chunks
//! 3. Publish snapshot chunks to NATS INIT stream

const std = @import("std");
const c = @cImport({
    @cInclude("libpq-fe.h");
});
const pg_conn = @import("pg_conn.zig");
const nats_publisher = @import("nats_publisher.zig");
const config = @import("config.zig");
const msgpack = @import("msgpack");

pub const log = std.log.scoped(.snapshot_listener);

/// Snapshot request payload from PostgreSQL NOTIFY
const SnapshotRequest = struct {
    id: u32,
    table_name: []const u8,
    requested_by: ?[]const u8,
};

/// Main entry point: Listen for snapshot requests and generate snapshots
pub fn listenForSnapshotRequests(
    allocator: std.mem.Allocator,
    pg_config: *const pg_conn.PgConf,
    publisher: *nats_publisher.Publisher,
    should_stop: *std.atomic.Value(bool),
) !void {
    log.info("🔔 Starting snapshot listener thread", .{});

    // Create a separate PostgreSQL connection (NOT replication mode)
    const conninfo = try pg_config.connInfo(allocator, false);
    defer allocator.free(conninfo);

    const conn = c.PQconnectdb(conninfo.ptr);
    if (conn == null) {
        log.err("Failed to connect to PostgreSQL for snapshot listening", .{});
        return error.ConnectionFailed;
    }
    defer c.PQfinish(conn);

    if (c.PQstatus(conn) != c.CONNECTION_OK) {
        const err_msg = c.PQerrorMessage(conn);
        log.err("PostgreSQL connection failed: {s}", .{err_msg});
        return error.ConnectionFailed;
    }

    // Start listening for snapshot_request notifications
    const listen_query = "LISTEN snapshot_request";
    const listen_result = c.PQexec(conn, listen_query.ptr);
    defer c.PQclear(listen_result);

    if (c.PQresultStatus(listen_result) != c.PGRES_COMMAND_OK) {
        const err_msg = c.PQerrorMessage(conn);
        log.err("Failed to LISTEN: {s}", .{err_msg});
        return error.ListenFailed;
    }

    log.info("🔔 Listening for snapshot requests on channel 'snapshot_request'", .{});

    while (!should_stop.load(.seq_cst)) {
        // Check for notifications (non-blocking)
        _ = c.PQconsumeInput(conn);

        var notify: ?*c.PGnotify = c.PQnotifies(conn);
        while (notify) |n| {
            defer c.PQfreemem(n);

            const payload = std.mem.span(n.extra);
            log.info("📩 Snapshot request notification: {s}", .{payload});

            // Parse JSON payload
            handleSnapshotRequest(allocator, pg_config, publisher, conn, payload) catch |err| {
                log.err("Failed to handle snapshot request: {}", .{err});
            };

            notify = c.PQnotifies(conn);
        }

        // Sleep before next poll
        std.Thread.sleep(config.Snapshot.poll_interval_ms * std.time.ns_per_ms);
    }

    log.info("🥁 Snapshot listener thread stopped", .{});
}

/// Parse snapshot request and generate snapshot
fn handleSnapshotRequest(
    allocator: std.mem.Allocator,
    pg_config: *const pg_conn.PgConf,
    publisher: *nats_publisher.Publisher,
    conn: ?*c.PGconn,
    payload: []const u8,
) !void {
    // Parse JSON: {"id": 1, "table_name": "users", "requested_by": "client-123"}
    const parsed = std.json.parseFromSlice(
        SnapshotRequest,
        allocator,
        payload,
        .{ .ignore_unknown_fields = true },
    ) catch |err| {
        log.err("Failed to parse JSON payload: {}", .{err});
        return err;
    };
    defer parsed.deinit();

    const request = parsed.value;

    log.info("🔄 Processing snapshot request #{d} for table '{s}' (requested_by: {s})", .{
        request.id,
        request.table_name,
        request.requested_by orelse "unknown",
    });

    // Update status to 'processing'
    const snapshot_id = try generateSnapshotId(allocator);
    defer allocator.free(snapshot_id);

    try updateRequestStatus(
        conn,
        request.id,
        "processing",
        snapshot_id,
        null,
    );

    // Generate snapshot
    generateIncrementalSnapshot(
        allocator,
        pg_config,
        publisher,
        conn,
        request.table_name,
        snapshot_id,
    ) catch |err| {
        log.err("Snapshot generation failed for table '{s}': {}", .{ request.table_name, err });

        // Update status to 'failed'
        const err_msg = try std.fmt.allocPrint(allocator, "Snapshot failed: {}", .{err});
        defer allocator.free(err_msg);

        try updateRequestStatus(conn, request.id, "failed", snapshot_id, err_msg);
        return err;
    };

    // Update status to 'completed'
    try updateRequestStatus(conn, request.id, "completed", snapshot_id, null);

    log.info("✅ Snapshot request #{d} completed successfully", .{request.id});
}

/// Generate incremental snapshot in chunks and publish to NATS
fn generateIncrementalSnapshot(
    allocator: std.mem.Allocator,
    pg_config: *const pg_conn.PgConf,
    publisher: *nats_publisher.Publisher,
    _: ?*c.PGconn, // Original connection (not used, we create a new one for snapshot query)
    table_name: []const u8,
    snapshot_id: []const u8,
) !void {
    log.info("🔄 Generating incremental snapshot for table '{s}' (snapshot_id={s})", .{
        table_name,
        snapshot_id,
    });

    // Create a separate connection for snapshot query
    const conninfo = try pg_config.connInfo(allocator, false);
    defer allocator.free(conninfo);

    const conn = c.PQconnectdb(conninfo.ptr);
    if (conn == null) return error.ConnectionFailed;
    defer c.PQfinish(conn);

    if (c.PQstatus(conn) != c.CONNECTION_OK) {
        return error.ConnectionFailed;
    }

    // Get current LSN for snapshot consistency
    const lsn_query = "SELECT pg_current_wal_lsn()::text";
    const lsn_result = c.PQexec(conn, lsn_query.ptr);
    defer c.PQclear(lsn_result);

    if (c.PQresultStatus(lsn_result) != c.PGRES_TUPLES_OK) {
        return error.QueryFailed;
    }

    const lsn_str = std.mem.span(c.PQgetvalue(lsn_result, 0, 0));

    // Query table in chunks
    var batch: u32 = 0;
    var total_rows: u64 = 0;

    while (true) {
        const offset = batch * config.Snapshot.chunk_size;

        // Build query: SELECT * FROM table ORDER BY id LIMIT chunk_size OFFSET offset
        const query = try std.fmt.allocPrintSentinel(
            allocator,
            "SELECT * FROM {s} ORDER BY id LIMIT {d} OFFSET {d}",
            .{ table_name, config.Snapshot.chunk_size, offset },
            0,
        );
        defer allocator.free(query);

        const chunk_result = c.PQexec(conn, query.ptr);
        defer c.PQclear(chunk_result);

        if (c.PQresultStatus(chunk_result) != c.PGRES_TUPLES_OK) {
            const err_msg = c.PQerrorMessage(conn);
            log.err("Snapshot query failed: {s}", .{err_msg});
            return error.QueryFailed;
        }

        const num_rows = c.PQntuples(chunk_result);
        if (num_rows == 0) break;

        total_rows += @intCast(num_rows);

        // Encode chunk as MessagePack
        const encoded = try encodeChunkToMessagePack(chunk_result, allocator);
        defer allocator.free(encoded);

        // Publish chunk to NATS: init.users.snap-1733507200.0
        const subject = try std.fmt.allocPrint(
            allocator,
            "init.{s}.{s}.{d}\x00",
            .{ table_name, snapshot_id, batch },
        );
        defer allocator.free(subject);

        const subject_z: [:0]const u8 = subject[0 .. subject.len - 1 :0];

        // Message ID for deduplication
        const msg_id_buf = try std.fmt.allocPrint(
            allocator,
            "init-{s}-{s}-{d}",
            .{ table_name, snapshot_id, batch },
        );
        defer allocator.free(msg_id_buf);

        try publisher.publish(subject_z, encoded, msg_id_buf);
        try publisher.flushAsync();

        log.info("📦 Published snapshot chunk {d} ({d} rows) → {s}", .{
            batch,
            num_rows,
            subject_z,
        });

        batch += 1;
    }

    // Publish metadata: init.users.meta
    try publishSnapshotMetadata(
        allocator,
        publisher,
        table_name,
        snapshot_id,
        lsn_str,
        batch,
        total_rows,
    );

    log.info("✅ Snapshot complete: {s} ({d} batches, {d} rows)", .{
        snapshot_id,
        batch,
        total_rows,
    });
}

/// Encode PostgreSQL result set to MessagePack array
fn encodeChunkToMessagePack(result: ?*c.PGresult, allocator: std.mem.Allocator) ![]const u8 {
    const num_rows = c.PQntuples(result);
    const num_cols = c.PQnfields(result);

    var buffer = std.ArrayList(u8).empty;
    defer buffer.deinit(allocator);

    const ArrayListStream = struct {
        list: *std.ArrayList(u8),
        allocator: std.mem.Allocator,

        const WriteError = std.mem.Allocator.Error;
        const ReadError = error{};

        pub fn write(self: *@This(), bytes: []const u8) WriteError!usize {
            try self.list.appendSlice(self.allocator, bytes);
            return bytes.len;
        }

        pub fn read(self: *@This(), out: []u8) ReadError!usize {
            _ = self;
            _ = out;
            return 0;
        }
    };

    var write_stream = ArrayListStream{ .list = &buffer, .allocator = allocator };
    var read_stream = ArrayListStream{ .list = &buffer, .allocator = allocator };

    var packer = msgpack.Pack(
        *ArrayListStream,
        *ArrayListStream,
        ArrayListStream.WriteError,
        ArrayListStream.ReadError,
        ArrayListStream.write,
        ArrayListStream.read,
    ).init(&write_stream, &read_stream);

    // Encode as array of maps
    var rows_array = try msgpack.Payload.arrPayload(@intCast(num_rows), allocator);
    defer rows_array.free(allocator);

    for (0..@intCast(num_rows)) |row| {
        var row_map = msgpack.Payload.mapPayload(allocator);

        for (0..@intCast(num_cols)) |col| {
            // Column name
            const col_name = c.PQfname(result, @intCast(col));
            const col_name_slice = std.mem.span(col_name);

            // Column value
            if (c.PQgetisnull(result, @intCast(row), @intCast(col)) == 1) {
                try row_map.mapPut(col_name_slice, msgpack.Payload{ .nil = {} });
            } else {
                const value = c.PQgetvalue(result, @intCast(row), @intCast(col));
                const value_slice = std.mem.span(value);
                try row_map.mapPut(col_name_slice, try msgpack.Payload.strToPayload(value_slice, allocator));
            }
        }

        rows_array.arr[row] = row_map;
    }

    try packer.write(rows_array);
    return try buffer.toOwnedSlice(allocator);
}

/// Publish snapshot metadata to NATS
fn publishSnapshotMetadata(
    allocator: std.mem.Allocator,
    publisher: *nats_publisher.Publisher,
    table_name: []const u8,
    snapshot_id: []const u8,
    lsn: []const u8,
    batch_count: u32,
    row_count: u64,
) !void {
    // Build JSON metadata
    const meta = try std.fmt.allocPrint(
        allocator,
        "{{\"snapshot_id\":\"{s}\",\"lsn\":\"{s}\",\"timestamp\":{d},\"batch_count\":{d},\"row_count\":{d},\"table\":\"{s}\"}}",
        .{ snapshot_id, lsn, std.time.timestamp(), batch_count, row_count, table_name },
    );
    defer allocator.free(meta);

    const subject = try std.fmt.allocPrint(
        allocator,
        "init.{s}.meta\x00",
        .{table_name},
    );
    defer allocator.free(subject);

    const subject_z: [:0]const u8 = subject[0 .. subject.len - 1 :0];

    try publisher.publish(subject_z, meta, null);
    try publisher.flushAsync();

    log.info("📋 Published snapshot metadata → {s}", .{subject_z});
}

/// Update snapshot request status in database
fn updateRequestStatus(
    conn: ?*c.PGconn,
    request_id: u32,
    status: []const u8,
    snapshot_id: []const u8,
    error_message: ?[]const u8,
) !void {
    const query = if (error_message) |err_msg|
        try std.fmt.allocPrintSentinel(
            std.heap.c_allocator,
            "UPDATE snapshot_requests SET status='{s}', snapshot_id='{s}', error_message='{s}', completed_at=NOW() WHERE id={d}",
            .{ status, snapshot_id, err_msg, request_id },
            0,
        )
    else
        try std.fmt.allocPrintSentinel(
            std.heap.c_allocator,
            "UPDATE snapshot_requests SET status='{s}', snapshot_id='{s}', completed_at=NOW() WHERE id={d}",
            .{ status, snapshot_id, request_id },
            0,
        );
    defer std.heap.c_allocator.free(query);

    const result = c.PQexec(conn, query.ptr);
    defer c.PQclear(result);

    if (c.PQresultStatus(result) != c.PGRES_COMMAND_OK) {
        const err_msg = c.PQerrorMessage(conn);
        log.err("Failed to update snapshot request status: {s}", .{err_msg});
        return error.UpdateFailed;
    }
}

/// Generate snapshot ID based on current timestamp
fn generateSnapshotId(allocator: std.mem.Allocator) ![]const u8 {
    return try std.fmt.allocPrint(allocator, "snap-{d}", .{std.time.timestamp()});
}
