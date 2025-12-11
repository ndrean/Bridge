//! Publication table management and validation
//!
//! This module handles PostgreSQL publication metadata and table validation.
//! It provides utilities to check if a table is within the monitored publication scope.

const std = @import("std");

/// Check if a table is in the monitored tables list from publication
///
/// Handles both "schema.table" and "table" formats:
/// - Exact match: "users" matches "users"
/// - Schema-qualified match: "public.users" matches "users"
/// - Case-sensitive comparison
///
/// Args:
///   table_name: The table name to check (can be "table" or "schema.table")
///   monitored_tables: List of tables from the publication (schema.table format)
///
/// Returns:
///   true if table is monitored, false otherwise
pub fn isTableMonitored(table_name: []const u8, monitored_tables: []const []const u8) bool {
    for (monitored_tables) |monitored| {
        // Handle both "schema.table" and "table" formats
        // Check if monitored ends with table_name (handles "public.users" matching "users")
        if (std.mem.endsWith(u8, monitored, table_name)) {
            // Exact match or schema.table format
            if (std.mem.eql(u8, monitored, table_name) or
                (monitored.len > table_name.len and monitored[monitored.len - table_name.len - 1] == '.'))
            {
                return true;
            }
        }
    }
    return false;
}

/// Publication metadata wrapper
///
/// Provides a typed interface to publication table lists with validation methods.
/// This is a lightweight wrapper around the table list from pg_publication_tables.
pub const PublicationTables = struct {
    tables: []const []const u8, // Owned strings in "schema.table" format
    allocator: std.mem.Allocator,

    /// Initialize from a table list (takes ownership of the slice)
    pub fn init(allocator: std.mem.Allocator, tables: []const []const u8) PublicationTables {
        return .{
            .tables = tables,
            .allocator = allocator,
        };
    }

    /// Check if a table is monitored by this publication
    pub fn isMonitored(self: *const PublicationTables, table_name: []const u8) bool {
        return isTableMonitored(table_name, self.tables);
    }

    /// Get the number of monitored tables
    pub fn count(self: *const PublicationTables) usize {
        return self.tables.len;
    }

    /// Free resources (does NOT free the table strings themselves - caller owns those)
    pub fn deinit(self: *PublicationTables) void {
        // Note: We don't free individual table strings here because they're owned
        // by PublicationInfo in replication_setup.zig which frees them on deinit
        _ = self;
    }
};

// Tests
test "isTableMonitored - exact match" {
    const tables = [_][]const u8{ "users", "orders", "products" };
    try std.testing.expect(isTableMonitored("users", &tables) == true);
    try std.testing.expect(isTableMonitored("orders", &tables) == true);
    try std.testing.expect(isTableMonitored("invalid", &tables) == false);
}

test "isTableMonitored - schema-qualified match" {
    const tables = [_][]const u8{ "public.users", "public.orders" };
    try std.testing.expect(isTableMonitored("users", &tables) == true);
    try std.testing.expect(isTableMonitored("orders", &tables) == true);
    try std.testing.expect(isTableMonitored("public.users", &tables) == true);
}

test "isTableMonitored - mixed formats" {
    const tables = [_][]const u8{ "public.users", "orders" };
    try std.testing.expect(isTableMonitored("users", &tables) == true);
    try std.testing.expect(isTableMonitored("orders", &tables) == true);
    try std.testing.expect(isTableMonitored("public.users", &tables) == true);
}

test "isTableMonitored - no false positives" {
    const tables = [_][]const u8{ "public.users", "orders" };
    // Should NOT match partial strings
    try std.testing.expect(isTableMonitored("user", &tables) == false);
    try std.testing.expect(isTableMonitored("order", &tables) == false);
    try std.testing.expect(isTableMonitored("rs", &tables) == false);
}

test "PublicationTables - basic operations" {
    const allocator = std.testing.allocator;
    const tables = [_][]const u8{ "public.users", "public.orders" };

    var pub_tables = PublicationTables.init(allocator, &tables);
    defer pub_tables.deinit();

    try std.testing.expect(pub_tables.count() == 2);
    try std.testing.expect(pub_tables.isMonitored("users") == true);
    try std.testing.expect(pub_tables.isMonitored("invalid") == false);
}
