//! Unified encoder abstraction for MessagePack and JSON
//!
//! This module provides a common interface for encoding data structures
//! as either MessagePack or JSON, allowing runtime selection via CLI flag.
//!
//! Usage:
//!   var encoder = Encoder.init(allocator, .msgpack);
//!   defer encoder.deinit();
//!
//!   var map = try encoder.createMap();
//!   try map.put("key", try encoder.createString("value"));
//!   const bytes = try encoder.encode(map);

const std = @import("std");
const msgpack = @import("msgpack");

pub const log = std.log.scoped(.encoder);

/// Encoding format selection
pub const Format = enum {
    msgpack,
    json,
};

/// Unified encoder supporting both MessagePack and JSON
pub const Encoder = struct {
    allocator: std.mem.Allocator,
    format: Format,

    pub fn init(allocator: std.mem.Allocator, format: Format) Encoder {
        return .{
            .allocator = allocator,
            .format = format,
        };
    }

    pub fn deinit(self: *Encoder) void {
        _ = self;
        // No cleanup needed currently
    }

    /// Create a map/object value
    pub fn createMap(self: *Encoder) Value {
        return switch (self.format) {
            .msgpack => .{ .msgpack = msgpack.Payload.mapPayload(self.allocator) },
            .json => .{ .json = std.json.Value{ .object = std.json.ObjectMap.init(self.allocator) } },
        };
    }

    /// Create an array value
    pub fn createArray(self: *Encoder, len: usize) !Value {
        return switch (self.format) {
            .msgpack => .{ .msgpack = try msgpack.Payload.arrPayload(len, self.allocator) },
            .json => blk: {
                var arr = std.json.Array.init(self.allocator);
                try arr.ensureTotalCapacity(len);
                break :blk .{ .json = std.json.Value{ .array = arr } };
            },
        };
    }

    /// Create a string value
    pub fn createString(self: *Encoder, s: []const u8) !Value {
        return switch (self.format) {
            .msgpack => .{ .msgpack = try msgpack.Payload.strToPayload(s, self.allocator) },
            .json => .{ .json = std.json.Value{ .string = try self.allocator.dupe(u8, s) } },
        };
    }

    /// Create an integer value
    pub fn createInt(self: *Encoder, i: i64) Value {
        return switch (self.format) {
            .msgpack => .{ .msgpack = msgpack.Payload{ .int = i } },
            .json => .{ .json = std.json.Value{ .integer = i } },
        };
    }

    /// Create a float value
    pub fn createFloat(self: *Encoder, f: f64) Value {
        return switch (self.format) {
            .msgpack => .{ .msgpack = msgpack.Payload{ .float = f } },
            .json => .{ .json = std.json.Value{ .float = f } },
        };
    }

    /// Create a boolean value
    pub fn createBool(self: *Encoder, b: bool) Value {
        return switch (self.format) {
            .msgpack => .{ .msgpack = msgpack.Payload{ .bool = b } },
            .json => .{ .json = std.json.Value{ .bool = b } },
        };
    }

    /// Create a null value
    pub fn createNull(self: *Encoder) Value {
        return switch (self.format) {
            .msgpack => .{ .msgpack = msgpack.Payload{ .nil = {} } },
            .json => .{ .json = std.json.Value.null },
        };
    }

    /// Encode a value to bytes
    pub fn encode(self: *Encoder, value: Value) ![]const u8 {
        return switch (self.format) {
            .msgpack => try self.encodeMsgpack(value),
            .json => try self.encodeJson(value),
        };
    }

    /// Encode as MessagePack
    fn encodeMsgpack(self: *Encoder, value: Value) ![]const u8 {
        const msgpack_value = switch (value) {
            .msgpack => |mp| mp,
            .json => return error.FormatMismatch,
        };

        // Use ArrayList for dynamic buffer
        var buffer = std.ArrayList(u8).empty;
        errdefer buffer.deinit(self.allocator);

        const ArrayListStream = struct {
            list: *std.ArrayList(u8),
            allocator: std.mem.Allocator,

            const WriteError = std.mem.Allocator.Error;
            const ReadError = error{};

            pub fn write(stream: *@This(), bytes: []const u8) WriteError!usize {
                try stream.list.appendSlice(stream.allocator, bytes);
                return bytes.len;
            }

            pub fn read(stream: *@This(), out: []u8) ReadError!usize {
                _ = stream;
                _ = out;
                return 0;
            }
        };

        var write_stream = ArrayListStream{ .list = &buffer, .allocator = self.allocator };
        var read_stream = ArrayListStream{ .list = &buffer, .allocator = self.allocator };

        var packer = msgpack.Pack(
            *ArrayListStream,
            *ArrayListStream,
            ArrayListStream.WriteError,
            ArrayListStream.ReadError,
            ArrayListStream.write,
            ArrayListStream.read,
        ).init(&write_stream, &read_stream);

        try packer.write(msgpack_value);
        return try buffer.toOwnedSlice(self.allocator);
    }

    /// Encode as JSON
    fn encodeJson(self: *Encoder, value: Value) ![]const u8 {
        const json_value = switch (value) {
            .json => |js| js,
            .msgpack => return error.FormatMismatch,
        };

        var buffer = std.ArrayList(u8).init(self.allocator);
        errdefer buffer.deinit();

        try std.json.stringify(json_value, .{}, buffer.writer());
        return try buffer.toOwnedSlice();
    }
};

/// Tagged union for either MessagePack or JSON values
pub const Value = union(Format) {
    msgpack: msgpack.Payload,
    json: std.json.Value,

    /// Put a key-value pair into a map
    pub fn put(self: *Value, key: []const u8, value: Value) !void {
        switch (self.*) {
            .msgpack => |*mp| {
                const val_mp = switch (value) {
                    .msgpack => |v| v,
                    .json => return error.FormatMismatch,
                };
                try mp.mapPut(key, val_mp);
            },
            .json => |*js| {
                // JSON Value should be an object
                if (js.* != .object) return error.NotAnObject;

                const val_js = switch (value) {
                    .json => |v| v,
                    .msgpack => return error.FormatMismatch,
                };

                try js.object.put(key, val_js);
            },
        }
    }

    /// Set an array element at index
    pub fn setIndex(self: *Value, index: usize, value: Value) !void {
        switch (self.*) {
            .msgpack => |*mp| {
                const val_mp = switch (value) {
                    .msgpack => |v| v,
                    .json => return error.FormatMismatch,
                };
                mp.arr[index] = val_mp;
            },
            .json => |*js| {
                // JSON Value should be an array
                if (js.* != .array) return error.NotAnArray;

                const val_js = switch (value) {
                    .json => |v| v,
                    .msgpack => return error.FormatMismatch,
                };

                js.array.items[index] = val_js;
            },
        }
    }

    /// Free the value's resources
    pub fn free(self: *Value, allocator: std.mem.Allocator) void {
        switch (self.*) {
            .msgpack => |*mp| mp.free(allocator),
            .json => |*js| {
                switch (js.*) {
                    .object => |*obj| obj.deinit(),
                    .array => |*arr| arr.deinit(),
                    .string => |s| allocator.free(s),
                    else => {},
                }
            },
        }
    }
};
