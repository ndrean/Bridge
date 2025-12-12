//! Schema publishing to NATS
//!
//! This module handles publishing table schema information to NATS:
//! - Initial schema publishing to KV store (at bridge startup)
//! - Schema change notifications to CDC stream (when relation_id changes)
//!
//! Schema information is sourced from PostgreSQL's information_schema and
//! published as MessagePack payloads for consumers to parse.

const std = @import("std");
const config = @import("config.zig");
const nats_publisher = @import("nats_publisher.zig");
const nats_kv = @import("nats_kv.zig");
const pgoutput = @import("pgoutput.zig");
const msgpack = @import("msgpack");
const encoder_mod = @import("encoder.zig");
const c = @cImport({
    @cInclude("libpq-fe.h");
});
const pg_conn = @import("pg_conn.zig");

pub const log = std.log.scoped(.schema_publisher);

/// Column information from information_schema
const ColumnInfo = struct {
    name: []const u8,
    position: u32,
    data_type: []const u8,
    is_nullable: bool,
    column_default: ?[]const u8,
};

/// Publish schema information to NATS CDC stream
///
/// Called when a schema change is detected (relation_id changed).
/// Publishes schema metadata to "schema.{table_name}" subject.
///
/// Args:
///   publisher: NATS publisher instance
///   relation: Relation message from pgoutput (contains column info)
///   allocator: Memory allocator
///   format: Encoding format (.msgpack or .json)
pub fn publishSchema(
    publisher: *nats_publisher.Publisher,
    relation: *const pgoutput.RelationMessage,
    allocator: std.mem.Allocator,
    format: encoder_mod.Format,
) !void {
    const schema_version = std.time.timestamp();

    log.info("📋 Publishing schema for table '{s}' (relation_id={d}, version={d})", .{
        relation.name,
        relation.relation_id,
        schema_version,
    });

    // Build subject: schema.{table_name}
    const subject = try std.fmt.allocPrintSentinel(
        allocator,
        "schema.{s}",
        .{relation.name},
        0,
    );
    defer allocator.free(subject);

    // Build payload with unified encoder
    var encoder = encoder_mod.Encoder.init(allocator, format);
    defer encoder.deinit();

    var schema_map = encoder.createMap();
    defer schema_map.free(allocator);

    // Add schema fields
    try schema_map.put("table", try encoder.createString(relation.name));
    try schema_map.put("namespace", try encoder.createString(relation.namespace));
    try schema_map.put("relation_id", encoder.createInt(@intCast(relation.relation_id)));
    try schema_map.put("schema_version", encoder.createInt(schema_version));
    try schema_map.put("replica_identity", encoder.createInt(@intCast(relation.replica_identity)));

    // Add columns array
    var columns_array = try encoder.createArray(relation.columns.len);
    for (relation.columns, 0..) |col, i| {
        var col_map = encoder.createMap();
        try col_map.put("name", try encoder.createString(col.name));
        try col_map.put("type_id", encoder.createInt(@intCast(col.type_id)));
        try col_map.put("type_modifier", encoder.createInt(@intCast(col.type_modifier)));
        try col_map.put("flags", encoder.createInt(@intCast(col.flags)));
        try columns_array.setIndex(i, col_map);
    }
    try schema_map.put("columns", columns_array);

    const encoded = try encoder.encode(schema_map);
    defer allocator.free(encoded);

    // Publish to schema stream with relation_id as message ID (for deduplication)
    var msg_id_buf: [64]u8 = undefined;
    const msg_id = try std.fmt.bufPrint(&msg_id_buf, "schema-{s}-{d}", .{ relation.name, relation.relation_id });

    try publisher.publish(subject, encoded, msg_id);
    try publisher.flushAsync();

    log.info("✅ Schema published: {s} ({d} columns)", .{ relation.name, relation.columns.len });
}

/// Query and publish initial schemas for monitored tables to NATS KV
///
/// This runs once at bridge startup to ensure consumers have schema information.
/// Queries PostgreSQL's information_schema for the specified monitored tables
/// and publishes their column metadata to the NATS KV store.
///
/// Args:
///   allocator: Memory allocator
///   pg_config: PostgreSQL connection configuration
///   publisher: NATS publisher instance
///   monitored_tables: List of tables from the publication to publish schemas for
///   format: Encoding format (.msgpack or .json)
pub fn publishInitialSchemas(
    allocator: std.mem.Allocator,
    pg_config: *const pg_conn.PgConf,
    publisher: *nats_publisher.Publisher,
    monitored_tables: []const []const u8,
    format: encoder_mod.Format,
) !void {
    log.info("📋 Querying and publishing initial schemas to NATS KV...", .{});

    // Create a separate PostgreSQL connection (NOT replication mode)
    const conninfo = try pg_config.connInfo(allocator, false);
    defer allocator.free(conninfo);

    const conn = c.PQconnectdb(conninfo.ptr);
    if (conn == null) {
        log.err("Failed to connect to PostgreSQL for schema query", .{});
        return error.ConnectionFailed;
    }
    defer c.PQfinish(conn);

    if (c.PQstatus(conn) != c.CONNECTION_OK) {
        const err_msg = c.PQerrorMessage(conn);
        log.err("PostgreSQL connection failed: {s}", .{err_msg});
        return error.ConnectionFailed;
    }

    // Build IN clause for monitored tables: ('table1', 'table2', ...)
    var in_clause = std.ArrayList(u8){};
    defer in_clause.deinit(allocator);

    try in_clause.appendSlice(allocator, "(");
    for (monitored_tables, 0..) |table, i| {
        if (i > 0) try in_clause.appendSlice(allocator, ", ");

        // Extract just table name if it's "schema.table" format
        const table_name = blk: {
            if (std.mem.indexOf(u8, table, ".")) |idx| {
                break :blk table[idx + 1 ..];
            }
            break :blk table;
        };

        try in_clause.appendSlice(allocator, "'");
        try in_clause.appendSlice(allocator, table_name);
        try in_clause.appendSlice(allocator, "'");
    }
    try in_clause.appendSlice(allocator, ")");

    // Query information_schema for monitored tables only
    const query = try std.fmt.allocPrintSentinel(
        allocator,
        \\SELECT
        \\    t.table_schema,
        \\    t.table_name,
        \\    c.column_name,
        \\    c.ordinal_position,
        \\    c.data_type,
        \\    c.is_nullable,
        \\    c.column_default
        \\FROM information_schema.tables t
        \\JOIN information_schema.columns c
        \\    ON t.table_schema = c.table_schema
        \\    AND t.table_name = c.table_name
        \\WHERE t.table_schema = 'public'
        \\    AND t.table_type = 'BASE TABLE'
        \\    AND t.table_name IN {s}
        \\ORDER BY t.table_name, c.ordinal_position
    ,
        .{in_clause.items},
        0,
    );
    defer allocator.free(query);

    const result = c.PQexec(conn, query.ptr);
    defer c.PQclear(result);

    if (c.PQresultStatus(result) != c.PGRES_TUPLES_OK) {
        const err_msg = c.PQerrorMessage(conn);
        log.err(
            "⚠️ Schema query failed: {s}",
            .{err_msg},
        );
        return error.QueryFailed;
    }

    const num_rows: usize = @intCast(c.PQntuples(result));
    if (num_rows == 0) {
        log.warn("⚠️No tables found in public schema", .{});
        return;
    }

    // Group columns by table
    var table_schemas = std.StringHashMap(std.ArrayList(ColumnInfo)).init(allocator);
    defer {
        var it = table_schemas.iterator();
        while (it.next()) |entry| {
            allocator.free(entry.key_ptr.*);
            entry.value_ptr.deinit(allocator);
        }
        table_schemas.deinit();
    }

    // Parse rows and group by table
    for (0..num_rows) |ui| {
        const i: c_int = @intCast(ui);
        const table_schema = std.mem.span(c.PQgetvalue(result, i, 0));
        const table_name = std.mem.span(c.PQgetvalue(result, i, 1));
        const column_name = std.mem.span(c.PQgetvalue(result, i, 2));
        const position_str = std.mem.span(c.PQgetvalue(result, i, 3));
        const data_type = std.mem.span(c.PQgetvalue(result, i, 4));
        const is_nullable_str = std.mem.span(c.PQgetvalue(result, i, 5));
        const column_default_ptr = c.PQgetvalue(result, i, 6);

        const position = try std.fmt.parseInt(u32, position_str, 10);
        const is_nullable = std.mem.eql(u8, is_nullable_str, "YES");
        const column_default = if (c.PQgetisnull(result, i, 6) == 1)
            null
        else
            std.mem.span(column_default_ptr);

        // Build full table name: schema.table
        const full_table_name = try std.fmt.allocPrint(allocator, "{s}.{s}", .{ table_schema, table_name });

        // Get or create column list for this table
        const entry = try table_schemas.getOrPut(full_table_name);
        if (!entry.found_existing) {
            entry.value_ptr.* = std.ArrayList(ColumnInfo){};
        } else {
            allocator.free(full_table_name); // Already have this key
        }

        try entry.value_ptr.append(allocator, .{
            .name = try allocator.dupe(u8, column_name),
            .position = position,
            .data_type = try allocator.dupe(u8, data_type),
            .is_nullable = is_nullable,
            .column_default = if (column_default) |d| try allocator.dupe(u8, d) else null,
        });
    }

    // Publish each table's schema to KV store
    var it = table_schemas.iterator();
    while (it.next()) |entry| {
        const table_name = entry.key_ptr.*;
        const columns = entry.value_ptr.*;
        defer {
            for (columns.items) |col| {
                allocator.free(col.name);
                allocator.free(col.data_type);
                if (col.column_default) |d| allocator.free(d);
            }
        }

        try publishTableSchema(allocator, publisher, table_name, columns.items, format);
    }

    log.info("✅ Published initial schemas for {d} tables", .{table_schemas.count()});
}

/// Publish a single table's schema to NATS KV store
///
/// Internal helper function called by publishInitialSchemas.
/// Encodes column metadata and stores in KV with key: table_name
fn publishTableSchema(
    allocator: std.mem.Allocator,
    publisher: *nats_publisher.Publisher,
    table_name: []const u8,
    columns: []const ColumnInfo,
    format: encoder_mod.Format,
) !void {
    // Extract just the table name (remove schema prefix if present)
    const table_only = blk: {
        if (std.mem.indexOf(u8, table_name, ".")) |idx| {
            break :blk table_name[idx + 1 ..];
        }
        break :blk table_name;
    };

    // Build payload with unified encoder
    var encoder = encoder_mod.Encoder.init(allocator, format);
    defer encoder.deinit();

    var schema_map = encoder.createMap();
    defer schema_map.free(allocator);

    // Add schema fields
    try schema_map.put("table", try encoder.createString(table_only));
    try schema_map.put("schema", try encoder.createString(table_name));
    try schema_map.put("timestamp", encoder.createInt(std.time.timestamp()));

    // Add columns array
    var columns_array = try encoder.createArray(columns.len);
    for (columns, 0..) |col, i| {
        var col_map = encoder.createMap();
        try col_map.put("name", try encoder.createString(col.name));
        try col_map.put("position", encoder.createInt(@intCast(col.position)));
        try col_map.put("data_type", try encoder.createString(col.data_type));
        try col_map.put("is_nullable", encoder.createBool(col.is_nullable));

        if (col.column_default) |default_val| {
            try col_map.put("column_default", try encoder.createString(default_val));
        } else {
            try col_map.put("column_default", encoder.createNull());
        }

        try columns_array.setIndex(i, col_map);
    }
    try schema_map.put("columns", columns_array);

    const encoded = try encoder.encode(schema_map);
    defer allocator.free(encoded);

    // Open KV store for schemas
    var kv_store = try nats_kv.KVStore.open(publisher.js, config.Nats.schema_kv_bucket, allocator);
    defer kv_store.deinit();

    // Put schema into KV with key: table_name
    const key = try std.fmt.allocPrintSentinel(allocator, "{s}", .{table_only}, 0);
    defer allocator.free(key);

    const revision = try kv_store.put(key, encoded);

    log.info("📋 Published schema to KV → schemas.{s} ({d} columns, {d} bytes, revision={d})", .{
        table_only,
        columns.len,
        encoded.len,
        revision,
    });
}
