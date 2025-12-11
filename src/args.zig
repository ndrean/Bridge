//! Command-line arguments parsing for CDC Bridge application
const std = @import("std");
const pg = @import("config.zig").Postgres;
const nats = @import("config.zig").Nats;
const encoder = @import("encoder.zig");
const log = std.log.scoped(.args);

/// Command-line arguments structure
pub const Args = struct {
    http_port: u16,
    slot_name: []const u8,
    publication_name: []const u8,
    encoding_format: encoder.Format,

    /// Parse command-line arguments
    pub fn parseArgs(allocator: std.mem.Allocator) !Args {
        // Parse command-line arguments
        var args = try std.process.argsWithAllocator(allocator);
        _ = args.skip(); // Skip program name
        var http_port: u16 = 6543; // default
        var slot_name: []const u8 = pg.default_slot_name; // default
        var publication_name: []const u8 = pg.default_publication_name; // default
        var encoding_format: encoder.Format = .msgpack; // default

        while (args.next()) |arg| {
            if (std.mem.eql(u8, arg, "--port")) {
                if (args.next()) |value| {
                    http_port = std.fmt.parseInt(u16, value, 10) catch {
                        log.err("--port requires a valid port number (1-65535)", .{});
                        return error.InvalidArguments;
                    };
                }
            } else if (std.mem.eql(u8, arg, "--slot")) {
                if (args.next()) |value| {
                    slot_name = value;
                }
            } else if (std.mem.eql(u8, arg, "--pub")) {
                if (args.next()) |value| {
                    publication_name = value;
                }
            } else if (std.mem.eql(u8, arg, "--json")) {
                encoding_format = .json;
            }
        }

        return Args{
            .http_port = http_port,
            .slot_name = slot_name,
            .publication_name = publication_name,
            .encoding_format = encoding_format,
        };
    }
};
