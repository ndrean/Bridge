//! Snapshot request listener and generator
//!
//! Runs in a dedicated thread to:
//! 1. Subscribe to NATS 'snapshot.request.>' subject
//! 2. Generate incremental snapshots in chunks using COPY CSV
//! 3. Publish snapshot chunks to NATS INIT stream

const std = @import("std");
const c = @cImport({
    @cInclude("libpq-fe.h");
    @cInclude("nats.h");
});
const pg_conn = @import("pg_conn.zig");
const nats_publisher = @import("nats_publisher.zig");
const publication_mod = @import("publication.zig");
const config = @import("config.zig");
const msgpack = @import("msgpack");
const pg_copy_csv = @import("pg_copy_csv.zig");
const encoder_mod = @import("encoder.zig");

pub const log = std.log.scoped(.snapshot_listener);

/// Snapshot request context passed to NATS callback
const SnapshotContext = struct {
    allocator: std.mem.Allocator,
    pg_config: *const pg_conn.PgConf,
    publisher: *nats_publisher.Publisher,
    monitored_tables: []const []const u8,
};

/// Snapshot listener with thread management
pub const SnapshotListener = struct {
    allocator: std.mem.Allocator,
    pg_config: *const pg_conn.PgConf,
    publisher: *nats_publisher.Publisher,
    should_stop: *std.atomic.Value(bool),
    monitored_tables: []const []const u8,
    thread: ?std.Thread = null,

    /// Initialize snapshot listener (does not start the thread)
    pub fn init(
        allocator: std.mem.Allocator,
        pg_config: *const pg_conn.PgConf,
        publisher: *nats_publisher.Publisher,
        should_stop: *std.atomic.Value(bool),
        monitored_tables: []const []const u8,
    ) SnapshotListener {
        return .{
            .allocator = allocator,
            .pg_config = pg_config,
            .publisher = publisher,
            .should_stop = should_stop,
            .monitored_tables = monitored_tables,
            .thread = null,
        };
    }

    /// Start the snapshot listener thread
    pub fn start(self: *SnapshotListener) !void {
        if (self.thread != null) {
            return error.AlreadyStarted;
        }
        self.thread = try std.Thread.spawn(.{}, listenLoop, .{self});
    }

    /// Join the snapshot listener thread (waits for completion)
    pub fn join(self: *SnapshotListener) void {
        if (self.thread) |thread| {
            thread.join();
            self.thread = null;
        }
    }

    /// Deinit - cleanup resources (call after join)
    pub fn deinit(self: *SnapshotListener) void {
        // No resources to clean up currently
        _ = self;
    }

    /// Background listening loop (internal)
    fn listenLoop(self: *SnapshotListener) !void {
        try listenForSnapshotRequests(
            self.allocator,
            self.pg_config,
            self.publisher,
            self.should_stop,
            self.monitored_tables,
        );
    }
};

/// Publish snapshot error to NATS for consumer feedback
fn publishSnapshotError(
    allocator: std.mem.Allocator,
    publisher: *nats_publisher.Publisher,
    table_name: []const u8,
    error_type: []const u8,
    available_tables: []const []const u8,
) !void {
    // Build error subject: init.error.{table}
    const subject = try std.fmt.allocPrint(allocator, "init.error.{s}", .{table_name});
    defer allocator.free(subject);

    // Use unified encoder (always MessagePack for snapshots)
    var encoder = encoder_mod.Encoder.init(allocator, .msgpack);
    defer encoder.deinit();

    var map = encoder.createMap();
    defer map.free(allocator);

    try map.put("error", try encoder.createString(error_type));
    try map.put("table", try encoder.createString(table_name));

    // Create array for available_tables
    var tables_array = try encoder.createArray(available_tables.len);
    for (available_tables, 0..) |table, i| {
        try tables_array.setIndex(i, try encoder.createString(table));
    }
    try map.put("available_tables", tables_array);

    const payload = try encoder.encode(map);
    defer allocator.free(payload);

    // Publish error message
    try publisher.publish(subject, payload, null);

    log.info("📤 Published snapshot error for table '{s}' to {s}", .{ table_name, subject });
}

/// NATS message callback for snapshot requests
fn onSnapshotRequest(
    _: ?*c.natsConnection,
    sub: ?*c.natsSubscription,
    msg: ?*c.natsMsg,
    closure: ?*anyopaque,
) callconv(.c) void {
    _ = sub;

    const ctx: *SnapshotContext = @ptrCast(@alignCast(closure));

    defer c.natsMsg_Destroy(msg);

    // Extract table name from subject: snapshot.request.<table>
    const subject_ptr = c.natsMsg_GetSubject(msg);
    const subject = std.mem.span(subject_ptr);

    const table_name = blk: {
        if (std.mem.startsWith(u8, subject, config.Snapshot.request_subject_prefix)) {
            break :blk subject[config.Snapshot.request_subject_prefix.len..];
        }
        log.err("⚠️ Invalid snapshot request subject: {s}", .{subject});
        return;
    };

    log.info("📩 Snapshot request via NATS: table='{s}'", .{table_name});

    // Validate table is in monitored tables list
    const is_monitored = publication_mod.isTableMonitored(table_name, ctx.monitored_tables);

    if (!is_monitored) {
        log.warn("⚠️ Snapshot requested for non-monitored table '{s}' (not in publication)", .{table_name});

        // Publish error to NATS so consumer gets feedback
        publishSnapshotError(
            ctx.allocator,
            ctx.publisher,
            table_name,
            "table_not_in_publication",
            ctx.monitored_tables,
        ) catch |err| {
            log.err("Failed to publish snapshot error: {}", .{err});
        };

        return;
    }

    // Get request metadata from message payload (MessagePack: requested_by, etc.)
    const data_ptr = c.natsMsg_GetData(msg);
    const data_len = c.natsMsg_GetDataLength(msg);

    const requested_by = if (data_len > 0) blk: {
        const payload = data_ptr[0..@intCast(data_len)];
        // Try to parse MessagePack for requested_by field
        // For now, just use "nats-consumer"
        _ = payload;
        break :blk "nats-consumer";
    } else "unknown";

    log.info("🔄 Processing snapshot request for table '{s}' (requested_by: {s})", .{
        table_name,
        requested_by,
    });

    // Generate snapshot ID
    const snapshot_id = generateSnapshotId(ctx.allocator) catch |err| {
        log.err("Failed to generate snapshot ID: {}", .{err});
        return;
    };
    defer ctx.allocator.free(snapshot_id);

    // Generate snapshot
    generateIncrementalSnapshot(
        ctx.allocator,
        ctx.pg_config,
        ctx.publisher,
        null, // No PostgreSQL connection needed (we create our own)
        table_name,
        snapshot_id,
    ) catch |err| {
        log.err("Snapshot generation failed for table '{s}': {}", .{ table_name, err });
        return;
    };

    log.info("✅ Snapshot request for '{s}' completed successfully", .{table_name});
}

/// Main entry point: Subscribe to NATS for snapshot requests
pub fn listenForSnapshotRequests(
    allocator: std.mem.Allocator,
    pg_config: *const pg_conn.PgConf,
    publisher: *nats_publisher.Publisher,
    should_stop: *std.atomic.Value(bool),
    monitored_tables: []const []const u8,
) !void {
    log.info("🔔 Starting NATS snapshot listener thread", .{});

    // Create context for NATS callback
    var ctx = SnapshotContext{
        .allocator = allocator,
        .pg_config = pg_config,
        .publisher = publisher,
        .monitored_tables = monitored_tables,
    };

    // Subscribe to snapshot.request.> (wildcard for all tables)
    var sub: ?*c.natsSubscription = null;
    const status = c.natsConnection_Subscribe(
        &sub,
        @ptrCast(publisher.nc),
        config.Snapshot.request_subject_wildcard,
        onSnapshotRequest,
        &ctx,
    );

    if (status != c.NATS_OK) {
        log.err("Failed to subscribe to {s}: {s}", .{
            config.Snapshot.request_subject_wildcard,
            std.mem.span(c.natsStatus_GetText(status)),
        });
        return error.SubscribeFailed;
    }
    defer c.natsSubscription_Destroy(sub);

    log.info("🔔 Subscribed to NATS subject 'snapshot.request.>' for snapshot requests", .{});

    // Keep thread alive until stop signal
    while (!should_stop.load(.seq_cst)) {
        std.Thread.sleep(100 * std.time.ns_per_ms);
    }

    log.info("🥁 Snapshot listener thread stopped", .{});
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

    // Use COPY CSV format to fetch rows in chunks
    var batch: u32 = 0;
    var total_rows: u64 = 0;
    var offset_rows: u64 = 0;

    while (true) {
        // Build COPY CSV query with LIMIT/OFFSET for chunking
        const copy_query = try std.fmt.allocPrintSentinel(
            allocator,
            "COPY (SELECT * FROM {s} ORDER BY id LIMIT {d} OFFSET {d}) TO STDOUT WITH (FORMAT csv, HEADER true)",
            .{ table_name, config.Snapshot.chunk_size, offset_rows },
            0,
        );
        defer allocator.free(copy_query);

        // Parse CSV COPY data
        var parser = pg_copy_csv.CopyCsvParser.init(allocator, @ptrCast(conn));
        defer parser.deinit();

        parser.executeCopy(copy_query) catch |err| {
            log.err("COPY CSV command failed: {}", .{err});
            return error.CopyFailed;
        };

        // Collect rows into array
        var rows_list = std.ArrayList(pg_copy_csv.CsvRow){};
        defer {
            for (rows_list.items) |*row| {
                row.deinit();
            }
            rows_list.deinit(allocator);
        }

        var row_iterator = parser.rows();
        while (try row_iterator.next()) |row| {
            try rows_list.append(allocator, row);
        }

        const num_rows = rows_list.items.len;
        if (num_rows == 0) break;

        total_rows += num_rows;

        // Get column names from parser header
        const col_names = parser.columnNames() orelse return error.NoHeader;

        // Encode chunk as MessagePack with metadata wrapper
        const encoded = try encodeCsvRowsToMessagePack(
            allocator,
            rows_list.items,
            col_names,
            table_name,
            snapshot_id,
            lsn_str,
            batch,
        );
        defer allocator.free(encoded);

        // Publish chunk to NATS: init.snap.users.snap-1733507200.0
        const subject = try std.fmt.allocPrint(
            allocator,
            config.Snapshot.data_subject_pattern ++ "\x00",
            .{ table_name, snapshot_id, batch },
        );
        defer allocator.free(subject);

        const subject_z: [:0]const u8 = subject[0 .. subject.len - 1 :0];

        // Message ID for deduplication
        const msg_id_buf = try std.fmt.allocPrint(
            allocator,
            config.Snapshot.data_msg_id_pattern,
            .{ table_name, snapshot_id, batch },
        );
        defer allocator.free(msg_id_buf);

        try publisher.publish(subject_z, encoded, msg_id_buf);
        try publisher.flushAsync();

        log.info("📦 Published snapshot chunk {d} ({d} rows, {d} bytes CSV) → {s}", .{
            batch,
            num_rows,
            encoded.len,
            subject_z,
        });

        batch += 1;
        offset_rows += num_rows;

        // If we got fewer rows than chunk_size, we're done
        if (num_rows < config.Snapshot.chunk_size) {
            break;
        }
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

/// Encode CSV rows to MessagePack with metadata wrapper
/// Wraps snapshot data with table name, operation type, LSN, and chunk info
fn encodeCsvRowsToMessagePack(
    allocator: std.mem.Allocator,
    rows: []const pg_copy_csv.CsvRow,
    col_names: [][]const u8,
    table_name: []const u8,
    snapshot_id: []const u8,
    lsn: []const u8,
    chunk: u32,
) ![]const u8 {
    // Use unified encoder (always MessagePack for snapshots)
    var encoder = encoder_mod.Encoder.init(allocator, .msgpack);
    defer encoder.deinit();

    // Build data array (array of row maps)
    var data_array = try encoder.createArray(rows.len);

    for (rows, 0..) |row, row_idx| {
        var row_map = encoder.createMap();

        for (row.fields, 0..) |csv_field, col_idx| {
            if (col_idx >= col_names.len) continue;

            const col_name = col_names[col_idx];

            if (csv_field.isNull()) {
                try row_map.put(col_name, encoder.createNull());
            } else if (csv_field.value) |text_val| {
                // CSV values are already text, just encode them
                try row_map.put(col_name, try encoder.createString(text_val));
            }
        }

        try data_array.setIndex(row_idx, row_map);
    }

    // Build metadata wrapper map
    var wrapper_map = encoder.createMap();
    defer wrapper_map.free(allocator);

    try wrapper_map.put("table", try encoder.createString(table_name));
    try wrapper_map.put("operation", try encoder.createString("snapshot"));
    try wrapper_map.put("snapshot_id", try encoder.createString(snapshot_id));
    try wrapper_map.put("chunk", encoder.createInt(@intCast(chunk)));
    try wrapper_map.put("lsn", try encoder.createString(lsn));
    try wrapper_map.put("data", data_array);

    return try encoder.encode(wrapper_map);
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
    // Use unified encoder (always MessagePack for snapshots)
    var encoder = encoder_mod.Encoder.init(allocator, .msgpack);
    defer encoder.deinit();

    var meta_map = encoder.createMap();
    defer meta_map.free(allocator);

    try meta_map.put("snapshot_id", try encoder.createString(snapshot_id));
    try meta_map.put("lsn", try encoder.createString(lsn));
    try meta_map.put("timestamp", encoder.createInt(std.time.timestamp()));
    try meta_map.put("batch_count", encoder.createInt(@intCast(batch_count)));
    try meta_map.put("row_count", encoder.createInt(@intCast(row_count)));
    try meta_map.put("table", try encoder.createString(table_name));

    const encoded = try encoder.encode(meta_map);
    defer allocator.free(encoded);

    const subject = try std.fmt.allocPrint(
        allocator,
        config.Snapshot.meta_subject_pattern ++ "\x00",
        .{table_name},
    );
    defer allocator.free(subject);

    const subject_z: [:0]const u8 = subject[0 .. subject.len - 1 :0];

    try publisher.publish(subject_z, encoded, null);
    try publisher.flushAsync();

    log.info("📋 Published snapshot metadata → {s}", .{subject_z});
}

/// Generate snapshot ID based on current timestamp
fn generateSnapshotId(allocator: std.mem.Allocator) ![]const u8 {
    return try std.fmt.allocPrint(allocator, "snap-{d}", .{std.time.timestamp()});
}
