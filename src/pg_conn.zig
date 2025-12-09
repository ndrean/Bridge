//! PostgreSQL configuration and connection management.
//!
//! Methods for PG init configuration and connection_string, and connection management.
const std = @import("std");

const c = @cImport({
    @cInclude("libpq-fe.h");
});

pub const log = std.log.scoped(.pg_conn);

/// PostgreSQL connection configuration
/// Centralized storage for database credentials used across the application
pub const PgConf = struct {
    host: []const u8,
    port: u16,
    user: []const u8,
    password: []const u8,
    database: []const u8,
    /// Enable replication mode (adds replication=database to connection string)
    replication: bool = false,

    /// Build a PostgreSQL connection string
    ///
    /// Caller is responsible for freeing the returned string
    pub fn connInfo(self: *const PgConf, allocator: std.mem.Allocator, replication: bool) ![:0]const u8 {
        const conninfo_str = if (replication)
            try std.fmt.allocPrint(
                allocator,
                "host={s} port={d} user={s} password={s} dbname={s} replication=database",
                .{ self.host, self.port, self.user, self.password, self.database },
            )
        else
            try std.fmt.allocPrint(
                allocator,
                "host={s} port={d} user={s} password={s} dbname={s}",
                .{ self.host, self.port, self.user, self.password, self.database },
            );
        defer allocator.free(conninfo_str);

        // Return null-terminated string for C APIs
        return try allocator.dupeZ(u8, conninfo_str);
    }

    /// Setup PostgreSQL connection configuration from environment variables
    ///
    /// Returns a PgConf with owned strings. Caller must call deinit() to free.
    ///
    /// Environment variables (matches .env.prod naming):
    /// - PG_HOST: PostgreSQL host
    /// - PG_PORT: PostgreSQL port
    /// - POSTGRES_BRIDGE_USER: Bridge user (fallback to PG_USER)
    /// - POSTGRES_BRIDGE_PASSWORD: Bridge password (fallback to PG_PASSWORD)
    /// - PG_DB: Database name
    pub fn init_from_env(allocator: std.mem.Allocator) !PgConf {
        const pg_host = std.process.getEnvVarOwned(allocator, "PG_HOST") catch |err| blk: {
            log.info("PG_HOST env var not set ({t}), using default 127.0.0.1", .{err});
            break :blk try allocator.dupe(u8, "127.0.0.1");
        };
        errdefer allocator.free(pg_host);

        const port_str = std.process.getEnvVarOwned(allocator, "PG_PORT") catch |err| blk: {
            log.info("PG_PORT env var not set ({t}), using default 5432", .{err});
            break :blk try allocator.dupe(u8, "5432");
        };
        defer allocator.free(port_str); // Port string only needed for parsing

        // Try bridge-specific user first, fallback to PG_USER
        const user_name = std.process.getEnvVarOwned(allocator, "POSTGRES_BRIDGE_USER") catch blk: {
            const generic_user = std.process.getEnvVarOwned(allocator, "PG_USER") catch |err| blk2: {
                log.info("POSTGRES_BRIDGE_USER and PG_USER env vars not set ({t}), using default username", .{err});
                break :blk2 try allocator.dupe(u8, "postgres");
            };
            break :blk generic_user;
        };
        errdefer allocator.free(user_name);

        // Try bridge-specific password first, fallback to PG_PASSWORD
        const password = std.process.getEnvVarOwned(allocator, "POSTGRES_BRIDGE_PASSWORD") catch blk: {
            const generic_password = std.process.getEnvVarOwned(allocator, "PG_PASSWORD") catch |err| blk2: {
                log.info("POSTGRES_BRIDGE_PASSWORD and PG_PASSWORD env vars not set ({}), using default", .{err});
                break :blk2 try allocator.dupe(u8, "postgres");
            };
            break :blk generic_password;
        };
        errdefer allocator.free(password);

        const database = std.process.getEnvVarOwned(allocator, "PG_DB") catch |err| blk: {
            log.info("PG_DB env var not set ({}), using default postgres", .{err});
            break :blk try allocator.dupe(u8, "postgres");
        };
        errdefer allocator.free(database);

        return .{
            .host = pg_host,
            .port = try std.fmt.parseInt(u16, port_str, 10),
            .user = user_name,
            .password = password,
            .database = database,
        };
    }

    /// Free owned strings in PgConf
    pub fn deinit(self: *PgConf, allocator: std.mem.Allocator) void {
        allocator.free(self.host);
        allocator.free(self.user);
        allocator.free(self.password);
        allocator.free(self.database);
    }
};

/// Connect to PostgreSQL with the given configuration
///
/// Returns a PGconn pointer that must be closed with PQfinish()
/// Caller is responsible for calling c.PQfinish(conn) when done
pub fn connect(allocator: std.mem.Allocator, pg_conf: PgConf) !*c.PGconn {
    // Build connection string
    const conninfo = try pg_conf.connInfo(
        allocator,
        pg_conf.replication,
    );
    defer allocator.free(conninfo);

    if (conninfo.len == 0) return error.InvalidConfig;

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
