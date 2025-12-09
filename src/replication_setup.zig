//! Replication setup tasks, including creating/dropping replication slots and publications,
const std = @import("std");
const c = @cImport({
    @cInclude("libpq-fe.h");
});
const pg_conn = @import("pg_conn.zig");

pub const log = std.log.scoped(.replication_setup);
/// Struct to handle replication setup tasks and includes get current WAL LSN.
///
/// Includes creating|droping replication slots and publications.
pub const ReplicationSetup = struct {
    allocator: std.mem.Allocator,
    pg_config: pg_conn.PgConf,

    /// Create a replication slot if it doesn't exist (hybrid: try create, fallback to verify)
    ///
    /// This function attempts to create a replication slot. If it already exists, it verifies
    /// and continues. If creation fails due to permissions, it checks if the slot exists
    /// (perhaps created by an admin) and uses it.
    ///
    /// Privilege requirements:
    /// - To CREATE: Requires REPLICATION privilege
    /// - To VERIFY: Requires ability to query pg_replication_slots
    pub fn createSlot(
        self: *const ReplicationSetup,
        slot_name: []const u8,
    ) !void {
        const conn = try self.connect();
        defer c.PQfinish(conn);

        // Check if slot exists
        const check_query = try std.fmt.allocPrintSentinel(
            self.allocator,
            "SELECT slot_name FROM pg_replication_slots WHERE slot_name = '{s}'",
            .{slot_name},
            0,
        );
        defer self.allocator.free(check_query);

        const check_result = try runQuery(
            conn,
            check_query,
        );
        defer c.PQclear(check_result);
        log.debug("Checking for replication slot '{s}'...", .{slot_name});

        const exists = c.PQntuples(check_result) > 0;

        if (exists) {
            log.info("✅ Replication slot '{s}' already exists", .{slot_name});
            return;
        }

        // Try to create the slot
        const create_query = try std.fmt.allocPrintSentinel(
            self.allocator,
            "SELECT pg_create_logical_replication_slot('{s}', 'pgoutput')",
            .{slot_name},
            0,
        );
        defer self.allocator.free(create_query);

        const create_result = runQuery(conn, create_query) catch |err| {
            // Creation failed - check if slot now exists (race condition or permission issue)
            const recheck_result = runQuery(conn, check_query) catch {
                log.err("🔴 Failed to create replication slot '{s}' and cannot verify existence", .{slot_name});
                log.err("   → Ensure an admin has created the slot or grant REPLICATION privilege", .{});
                return err;
            };
            defer c.PQclear(recheck_result);

            if (c.PQntuples(recheck_result) > 0) {
                log.info("✅ Replication slot '{s}' exists (created externally)", .{slot_name});
                return;
            }

            log.err("🔴 Failed to create replication slot '{s}'", .{slot_name});
            log.err("   → Ask your database administrator to run:", .{});
            log.err("      SELECT pg_create_logical_replication_slot('{s}', 'pgoutput');", .{slot_name});
            return err;
        };
        defer c.PQclear(create_result);

        log.info("✅ Replication slot '{s}' created", .{slot_name});
    }

    /// Create a publication if it doesn't exist (hybrid: try create, fallback to verify)
    ///
    /// This function attempts to create a FOR ALL TABLES publication. If creation fails
    /// due to permissions, it checks if the publication exists (perhaps created by an admin)
    /// and uses it. The tables parameter is for informational/logging purposes only.
    ///
    /// Privilege requirements:
    /// - To CREATE FOR ALL TABLES: Requires SUPERUSER privilege
    /// - To CREATE FOR TABLE: Requires ownership of each table
    /// - To VERIFY: Requires ability to query pg_publication
    ///
    /// Production deployment recommendation:
    /// Have your database administrator pre-create the publication:
    ///   CREATE PUBLICATION cdc_pub FOR ALL TABLES;
    /// Then the bridge only needs REPLICATION privilege to use it.
    pub fn createPublication(
        self: *const ReplicationSetup,
        pub_name: []const u8,
        tables: []const []const u8,
    ) !void {
        const conn = try self.connect();
        defer c.PQfinish(conn);

        // Check if publication exists
        const check_query = try std.fmt.allocPrintSentinel(
            self.allocator,
            "SELECT pubname FROM pg_publication WHERE pubname = '{s}'",
            .{pub_name},
            0,
        );
        defer self.allocator.free(check_query);

        const check_result = try runQuery(conn, check_query);
        defer c.PQclear(check_result);

        const exists = c.PQntuples(check_result) > 0;

        if (exists) {
            log.info("✅  Publication '{s}' already exists, listening on ALL TABLES", .{pub_name});
            return;
        }

        // Try to create publication with FOR ALL TABLES
        // This requires SUPERUSER privilege
        const create_query = try std.fmt.allocPrintSentinel(
            self.allocator,
            "CREATE PUBLICATION {s} FOR ALL TABLES",
            .{pub_name},
            0,
        );
        defer self.allocator.free(create_query);

        const create_result = runQuery(conn, create_query) catch |err| {
            // Creation failed - check if publication now exists (race condition)
            const recheck_result = runQuery(conn, check_query) catch {
                log.err("🔴 Failed to create publication '{s}' and cannot verify existence", .{pub_name});
                log.err("   → Ensure an admin has created the publication or grant SUPERUSER privilege", .{});
                return err;
            };
            defer c.PQclear(recheck_result);

            if (c.PQntuples(recheck_result) > 0) {
                log.info("✅ Publication '{s}' exists (created externally)", .{pub_name});
                return;
            }

            log.err("🔴 Failed to create publication '{s}'", .{pub_name});
            log.err("   → Ask your database administrator to run:", .{});
            log.err("      CREATE PUBLICATION {s} FOR ALL TABLES;", .{pub_name});
            log.err("   → Or grant SUPERUSER privilege to the bridge user (not recommended for production)", .{});
            return err;
        };
        defer c.PQclear(create_result);

        if (tables.len > 0) {
            const table_list = try std.mem.join(self.allocator, ", ", tables);
            defer self.allocator.free(table_list);
            log.info("✅ Publication '{s}' created for ALL TABLES (monitoring: {s})", .{ pub_name, table_list });
        } else {
            log.info("✅ Publication '{s}' created for ALL TABLES", .{pub_name});
        }

        // OPTION: Table-specific publications (PRESERVED BUT COMMENTED OUT)
        // Uncomment this block if you need table-specific publications instead of FOR ALL TABLES
        // WARNING: Requires ownership of each table listed
        //
        // const create_query = if (tables.len == 0)
        //     try std.fmt.allocPrintSentinel(
        //         self.allocator,
        //         "CREATE PUBLICATION {s} FOR ALL TABLES",
        //         .{pub_name},
        //         0,
        //     )
        // else blk: {
        //     // Join tables with ", "
        //     const table_list = try std.mem.join(self.allocator, ", ", tables);
        //     defer self.allocator.free(table_list);
        //
        //     const query = try std.fmt.allocPrint(
        //         self.allocator,
        //         "CREATE PUBLICATION {s} FOR TABLE {s}",
        //         .{ pub_name, table_list },
        //     );
        //     defer self.allocator.free(query);
        //
        //     break :blk try self.allocator.dupeZ(u8, query);
        // };
        // defer self.allocator.free(create_query);
    }

    /// Drop a replication slot
    pub fn dropSlot(self: *const ReplicationSetup, slot_name: []const u8) !void {
        log.info("Dropping replication slot '{s}'...", .{slot_name});

        const query = try std.fmt.allocPrintSentinel(
            self.allocator,
            "SELECT pg_drop_replication_slot('{s}')",
            .{slot_name},
            0,
        );
        defer self.allocator.free(query);

        const conn = try self.connect();
        defer c.PQfinish(conn);

        const result = try runQuery(conn, query);
        defer c.PQclear(result);

        log.info("✓ Replication slot '{s}' dropped", .{slot_name});
    }

    /// Drop a publication
    pub fn dropPublication(self: *const ReplicationSetup, pub_name: []const u8) !void {
        log.info("Dropping publication '{s}'...", .{pub_name});

        const query = try std.fmt.allocPrintSentinel(
            self.allocator,
            "DROP PUBLICATION IF EXISTS {s}",
            .{pub_name},
            0,
        );
        defer self.allocator.free(query);

        const conn = try self.connect();
        defer c.PQfinish(conn);

        const result = try runQuery(conn, query);
        defer c.PQclear(result);

        log.info("✓ Publication '{s}' dropped", .{pub_name});
    }

    fn runQuery(conn: *c.PGconn, query: []const u8) !*c.PGresult {
        const result = c.PQexec(conn, query.ptr) orelse {
            log.err("Query execution failed: PQexec returned null", .{});
            return error.QueryFailed;
        };

        if (c.PQresultStatus(result) != c.PGRES_TUPLES_OK and c.PQresultStatus(result) != c.PGRES_COMMAND_OK) {
            const err_msg = c.PQerrorMessage(conn);
            log.err("Query failed: {s}", .{err_msg});
            c.PQclear(result);
            return error.QueryFailed;
        }

        return result;
    }

    pub fn connect(self: *const ReplicationSetup) !*c.PGconn {
        // Build connection string
        const conninfo = try self.pg_config.connInfo(
            self.allocator,
            self.pg_config.replication,
        );
        defer self.allocator.free(conninfo);

        if (conninfo.len == 0) {
            log.err("Invalid configuration: connection string is empty", .{});
            return error.InvalidConfig;
        }

        const conn = c.PQconnectdb(conninfo.ptr) orelse {
            log.err("🔴 Connection failed: PQconnectdb returned null", .{});
            return error.ConnectionFailed;
        };

        if (c.PQstatus(conn) != c.CONNECTION_OK) {
            const err_msg = c.PQerrorMessage(conn);
            log.err("🔴 Connection failed: {s}", .{err_msg});
            c.PQfinish(conn);
            return error.ConnectionFailed;
        }

        return conn;
    }
};
