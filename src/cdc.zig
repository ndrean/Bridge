const std = @import("std");
const pg = @import("pg");

pub const log = std.log.scoped(.cdc);

/// CDC Change represents a single change event from PostgreSQL
pub const Change = struct {
    /// Transaction ID
    tx_id: ?u64 = null,

    /// Log Sequence Number
    lsn: []const u8,

    /// Table name
    table: []const u8,

    /// Operation type
    operation: Operation,

    /// Old values (for UPDATE/DELETE)
    before: ?std.json.Value = null,

    /// New values (for INSERT/UPDATE)
    after: ?std.json.Value = null,

    /// Timestamp
    timestamp: i64,

    pub const Operation = enum {
        INSERT,
        UPDATE,
        DELETE,
        BEGIN,
        COMMIT,
    };
};

/// Replication slot configuration
pub const ReplicationConfig = struct {
    slot_name: []const u8 = "bridge_cdc_slot",
    publication_name: []const u8 = "bridge_publication",
    /// Tables to replicate (empty = all tables)
    tables: []const []const u8 = &.{},
};

/// CDC Consumer manages PostgreSQL logical replication
///
/// Exposes methods to create replication slots, publications,
/// and to stream changes.
/// - `init`: Initialize the consumer with a connection pool and config
/// - `deinit`: Clean up resources
/// - `createSlot`: Create replication slot if it doesn't exist
/// - `createPublication`: Create publication for tables
/// - `streamChanges`: Start streaming replication changes
/// - `getCurrentLSN`: Get current WAL LSN position
pub const Consumer = struct {
    allocator: std.mem.Allocator,
    conn: *pg.Conn,
    config: ReplicationConfig,

    pub fn init(
        allocator: std.mem.Allocator,
        pool: *pg.Pool,
        config: ReplicationConfig,
    ) !Consumer {
        const conn = try pool.acquire();
        return Consumer{
            .allocator = allocator,
            .conn = conn,
            .config = config,
        };
    }

    pub fn deinit(self: *Consumer) void {
        self.conn.release();
    }

    /// Create replication slot if it doesn't exist
    pub fn createSlot(self: *Consumer) !void {
        log.info("Creating replication slot '{s}'...", .{self.config.slot_name});

        // Check if slot already exists
        const check_query =
            \\SELECT slot_name FROM pg_replication_slots
            \\WHERE slot_name = $1
        ;

        var check_result = try self.conn.query(check_query, .{self.config.slot_name});
        defer check_result.deinit();

        const exists = (try check_result.next()) != null;

        if (exists) {
            log.info("Replication slot '{s}' already exists", .{self.config.slot_name});
            return;
        }

        // Create the slot using pgoutput plugin
        const create_query =
            \\SELECT pg_create_logical_replication_slot($1, 'pgoutput')
        ;

        _ = self.conn.exec(create_query, .{self.config.slot_name}) catch |err| {
            if (self.conn.err) |pg_err| {
                log.err("Failed to create replication slot: {s}", .{pg_err.message});
            }
            return err;
        };

        log.info("✓ Replication slot '{s}' created", .{self.config.slot_name});
    }

    /// Create publication for tables
    pub fn createPublication(self: *Consumer) !void {
        log.info("Creating publication '{s}'...", .{self.config.publication_name});

        // Check if publication exists
        const check_query =
            \\SELECT pubname FROM pg_publication
            \\WHERE pubname = $1
        ;

        var check_result = try self.conn.query(check_query, .{self.config.publication_name});
        defer check_result.deinit();

        const exists = (try check_result.next()) != null;

        if (exists) {
            log.info("Publication '{s}' already exists", .{self.config.publication_name});
            return;
        }

        // Create publication
        var create_query = std.ArrayList(u8).init(self.allocator);
        defer create_query.deinit();

        const writer = create_query.writer();
        try writer.print("CREATE PUBLICATION {s} FOR ", .{self.config.publication_name});

        if (self.config.tables.len == 0) {
            try writer.writeAll("ALL TABLES");
        } else {
            try writer.writeAll("TABLE ");
            for (self.config.tables, 0..) |table, i| {
                if (i > 0) try writer.writeAll(", ");
                try writer.writeAll(table);
            }
        }

        _ = self.conn.exec(create_query.items, .{}) catch |err| {
            if (self.conn.err) |pg_err| {
                log.err("Failed to create publication: {s}", .{pg_err.message});
            }
            return err;
        };

        log.info("✓ Publication '{s}' created", .{self.config.publication_name});
    }

    /// Start streaming replication changes
    /// Note: This is a simplified version. Full implementation would need to:
    /// 1. Use START_REPLICATION SLOT command
    /// 2. Parse pgoutput protocol messages
    /// 3. Handle keep-alive messages
    /// 4. Send status updates back to PostgreSQL
    pub fn streamChanges(self: *Consumer, callback: *const fn (Change) anyerror!void) !void {
        log.info("Starting replication stream from slot '{s}'...", .{self.config.slot_name});

        // This is a placeholder - actual implementation would use PostgreSQL's
        // replication protocol which requires:
        // 1. Opening a replication connection (not a regular connection)
        // 2. Sending START_REPLICATION SLOT command
        // 3. Parsing binary protocol messages
        // 4. Decoding pgoutput format

        // For now, we'll demonstrate with a trigger-based approach as a POC
        log.warn("Full streaming replication not yet implemented", .{});
        log.warn("Consider using LISTEN/NOTIFY or polling as interim solution", .{});

        _ = callback;
    }

    /// Get current WAL LSN position
    pub fn getCurrentLSN(self: *Consumer) ![]const u8 {
        var row = (try self.conn.row("SELECT pg_current_wal_lsn()::text", .{})) orelse {
            return error.NoLSN;
        };
        defer row.deinit() catch {};

        const lsn = row.get([]const u8, 0);

        // Dupe the LSN since it will be invalid after row.deinit()
        return try self.allocator.dupe(u8, lsn);
    }
};
