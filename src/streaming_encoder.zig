//! Streaming MessagePack encoder that writes directly to a pre-allocated buffer
//! This eliminates intermediate allocations by encoding data on-the-fly

const std = @import("std");
const Writer = std.Io.Writer;

pub const StreamingEncoder = struct {
    /// Fixed buffer for writing
    buffer: []u8,
    /// Current write position
    pos: usize,
    /// Writer using new std.Io.Writer.fixed API
    writer: Writer,

    pub fn init(buffer: []u8) StreamingEncoder {
        return .{
            .buffer = buffer,
            .pos = 0,
            .writer = Writer.fixed(buffer),
        };
    }

    /// Reset the encoder to reuse the same buffer for the next chunk
    pub fn reset(self: *StreamingEncoder) void {
        self.pos = 0;
        self.writer = Writer.fixed(self.buffer);
    }

    /// Get the bytes written so far
    pub fn getWritten(self: *StreamingEncoder) []const u8 {
        // Don't sync from writer if pos was manually set (e.g., after shifting)
        // Just return the current position
        return self.buffer[0..self.pos];
    }

    /// Get the current write position (syncs from writer)
    pub fn getPos(self: *StreamingEncoder) usize {
        // Sync pos with writer and return it
        self.pos = self.writer.buffered().len;
        return self.pos;
    }

    /// Set the write position explicitly (for manual adjustments after shifting)
    pub fn setPos(self: *StreamingEncoder, new_pos: usize) void {
        self.pos = new_pos;
    }

    // ========================================================================
    // MessagePack Header Writers
    // ========================================================================

    /// Write a MessagePack array header
    pub fn writeArrayHeader(self: *StreamingEncoder, len: usize) !void {
        if (len < 16) {
            try self.writer.writeByte(@as(u8, @intCast(0x90 | len)));
        } else if (len < 65536) {
            try self.writer.writeByte(0xdc);
            try self.writer.writeInt(u16, @intCast(len), .big);
        } else {
            try self.writer.writeByte(0xdd);
            try self.writer.writeInt(u32, @intCast(len), .big);
        }
    }

    /// Write a MessagePack map header
    pub fn writeMapHeader(self: *StreamingEncoder, len: usize) !void {
        if (len < 16) {
            try self.writer.writeByte(@as(u8, @intCast(0x80 | len)));
        } else if (len < 65536) {
            try self.writer.writeByte(0xde);
            try self.writer.writeInt(u16, @intCast(len), .big);
        } else {
            try self.writer.writeByte(0xdf);
            try self.writer.writeInt(u32, @intCast(len), .big);
        }
    }

    /// Write a MessagePack string
    pub fn writeString(self: *StreamingEncoder, v: []const u8) !void {
        if (v.len < 32) {
            try self.writer.writeByte(@as(u8, @intCast(0xa0 | v.len)));
        } else if (v.len < 256) {
            try self.writer.writeByte(0xd9);
            try self.writer.writeByte(@intCast(v.len));
        } else if (v.len < 65536) {
            try self.writer.writeByte(0xda);
            try self.writer.writeInt(u16, @intCast(v.len), .big);
        } else {
            try self.writer.writeByte(0xdb);
            try self.writer.writeInt(u32, @intCast(v.len), .big);
        }
        try self.writer.writeAll(v);
    }

    /// Write a MessagePack integer
    pub fn writeInt(self: *StreamingEncoder, value: i64) !void {
        if (value >= 0 and value < 128) {
            // Positive fixint
            try self.writer.writeByte(@intCast(value));
        } else if (value < 0 and value >= -32) {
            // Negative fixint
            try self.writer.writeByte(@as(u8, @bitCast(@as(i8, @intCast(value)))));
        } else if (value >= 0 and value < 256) {
            try self.writer.writeByte(0xcc);
            try self.writer.writeByte(@intCast(value));
        } else if (value >= 0 and value < 65536) {
            try self.writer.writeByte(0xcd);
            try self.writer.writeInt(u16, @intCast(value), .big);
        } else if (value >= 0) {
            try self.writer.writeByte(0xcf);
            try self.writer.writeInt(u64, @intCast(value), .big);
        } else if (value >= -128) {
            try self.writer.writeByte(0xd0);
            try self.writer.writeByte(@bitCast(@as(i8, @intCast(value))));
        } else if (value >= -32768) {
            try self.writer.writeByte(0xd1);
            try self.writer.writeInt(i16, @intCast(value), .big);
        } else {
            try self.writer.writeByte(0xd3);
            try self.writer.writeInt(i64, value, .big);
        }
    }

    /// Write a MessagePack null
    pub fn writeNull(self: *StreamingEncoder) !void {
        try self.writer.writeByte(0xc0);
    }

    /// Write a MessagePack boolean
    pub fn writeBool(self: *StreamingEncoder, value: bool) !void {
        try self.writer.writeByte(if (value) 0xc3 else 0xc2);
    }

    // ========================================================================
    // High-level snapshot encoding helpers
    // ========================================================================

    /// Begin a snapshot batch (writes outer array header)
    pub fn beginBatch(self: *StreamingEncoder, estimated_rows: usize) !void {
        try self.writeArrayHeader(estimated_rows);
    }

    /// Begin a row (writes row array header)
    pub fn beginRow(self: *StreamingEncoder, column_count: usize) !void {
        try self.writeArrayHeader(column_count);
    }

    /// Write a field value (string or null)
    pub fn writeField(self: *StreamingEncoder, value: ?[]const u8) !void {
        if (value) |v| {
            try self.writeString(v);
        } else {
            try self.writeNull();
        }
    }
};

// ========================================================================
// Tests
// ========================================================================

test "StreamingEncoder - write array header" {
    var buffer: [256]u8 = undefined;
    var encoder = StreamingEncoder.init(&buffer);

    // Small array (fixarray)
    try encoder.writeArrayHeader(5);
    const written = encoder.getWritten();
    try std.testing.expectEqual(@as(u8, 0x95), written[0]); // fixarray 5
}

test "StreamingEncoder - write string" {
    var buffer: [256]u8 = undefined;
    var encoder = StreamingEncoder.init(&buffer);

    try encoder.writeString("hello");
    const written = encoder.getWritten();
    try std.testing.expectEqual(@as(u8, 0xa5), written[0]); // fixstr 5
    try std.testing.expectEqualStrings("hello", written[1..6]);
}

test "StreamingEncoder - write null" {
    var buffer: [256]u8 = undefined;
    var encoder = StreamingEncoder.init(&buffer);

    try encoder.writeNull();
    const written = encoder.getWritten();
    try std.testing.expectEqual(@as(u8, 0xc0), written[0]);
}

test "StreamingEncoder - write field" {
    var buffer: [256]u8 = undefined;
    var encoder = StreamingEncoder.init(&buffer);

    try encoder.writeField("test");
    try encoder.writeField(null);

    const written = encoder.getWritten();
    try std.testing.expectEqual(@as(u8, 0xa4), written[0]); // fixstr 4
    try std.testing.expectEqualStrings("test", written[1..5]);
    try std.testing.expectEqual(@as(u8, 0xc0), written[5]); // null
}

test "StreamingEncoder - snapshot row encoding" {
    var buffer: [256]u8 = undefined;
    var encoder = StreamingEncoder.init(&buffer);

    // Encode a batch with 1 row, 3 columns
    try encoder.beginBatch(1);
    try encoder.beginRow(3);
    try encoder.writeField("1");
    try encoder.writeField("Alice");
    try encoder.writeField("30");

    const written = encoder.getWritten();

    // Verify structure: [[[...]]]
    try std.testing.expectEqual(@as(u8, 0x91), written[0]); // array(1) - batch
    try std.testing.expectEqual(@as(u8, 0x93), written[1]); // array(3) - row
}
