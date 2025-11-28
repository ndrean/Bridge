const std = @import("std");
const cdc = @import("cdc.zig");

pub const log = std.log.scoped(.pgoutput);

/// pgoutput protocol parser
/// Parses PostgreSQL logical replication binary messages
/// Spec: https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html
pub const PgOutputMessage = union(enum) {
    begin: BeginMessage,
    commit: CommitMessage,
    relation: RelationMessage,
    insert: InsertMessage,
    update: UpdateMessage,
    delete: DeleteMessage,
    origin: OriginMessage,
    type: TypeMessage,
    truncate: TruncateMessage,
};

pub const BeginMessage = struct {
    final_lsn: u64,
    timestamp: i64,
    xid: u32,
};

pub const CommitMessage = struct {
    flags: u8,
    commit_lsn: u64,
    end_lsn: u64,
    timestamp: i64,
};

pub const RelationMessage = struct {
    relation_id: u32,
    namespace: []const u8,
    name: []const u8,
    replica_identity: u8,
    columns: []ColumnInfo,

    pub const ColumnInfo = struct {
        flags: u8,
        name: []const u8,
        type_id: u32,
        type_modifier: i32,
    };

    pub fn deinit(self: *RelationMessage, allocator: std.mem.Allocator) void {
        allocator.free(self.namespace);
        allocator.free(self.name);
        for (self.columns) |col| {
            allocator.free(col.name);
        }
        allocator.free(self.columns);
    }
};

pub const InsertMessage = struct {
    relation_id: u32,
    tuple_data: TupleData,

    pub fn deinit(self: *InsertMessage, allocator: std.mem.Allocator) void {
        self.tuple_data.deinit(allocator);
    }
};

pub const UpdateMessage = struct {
    relation_id: u32,
    old_tuple: ?TupleData,
    new_tuple: TupleData,

    pub fn deinit(self: *UpdateMessage, allocator: std.mem.Allocator) void {
        if (self.old_tuple) |*old| {
            old.deinit(allocator);
        }
        self.new_tuple.deinit(allocator);
    }
};

pub const DeleteMessage = struct {
    relation_id: u32,
    old_tuple: TupleData,

    pub fn deinit(self: *DeleteMessage, allocator: std.mem.Allocator) void {
        self.old_tuple.deinit(allocator);
    }
};

pub const OriginMessage = struct {
    commit_lsn: u64,
    name: []const u8,
};

pub const TypeMessage = struct {
    type_id: u32,
    namespace: []const u8,
    name: []const u8,
};

pub const TruncateMessage = struct {
    options: u8,
    relation_ids: []u32,
};

pub const TupleData = struct {
    columns: []?[]const u8,

    pub fn deinit(self: *TupleData, allocator: std.mem.Allocator) void {
        for (self.columns) |col| {
            if (col) |data| {
                allocator.free(data);
            }
        }
        allocator.free(self.columns);
    }
};

/// Parser state for decoding pgoutput messages
pub const Parser = struct {
    allocator: std.mem.Allocator,
    data: []const u8,
    pos: usize,

    pub fn init(allocator: std.mem.Allocator, data: []const u8) Parser {
        return .{
            .allocator = allocator,
            .data = data,
            .pos = 0,
        };
    }

    pub fn parse(self: *Parser) !PgOutputMessage {
        if (self.pos >= self.data.len) {
            return error.EndOfData;
        }

        const msg_type = self.data[self.pos];
        self.pos += 1;

        return switch (msg_type) {
            'B' => .{ .begin = try self.parseBegin() },
            'C' => .{ .commit = try self.parseCommit() },
            'R' => .{ .relation = try self.parseRelation() },
            'I' => .{ .insert = try self.parseInsert() },
            'U' => .{ .update = try self.parseUpdate() },
            'D' => .{ .delete = try self.parseDelete() },
            'O' => .{ .origin = try self.parseOrigin() },
            'Y' => .{ .type = try self.parseType() },
            'T' => .{ .truncate = try self.parseTruncate() },
            else => {
                log.warn("Unknown pgoutput message type: {c} (0x{x})", .{ msg_type, msg_type });
                return error.UnknownMessageType;
            },
        };
    }

    fn parseBegin(self: *Parser) !BeginMessage {
        const final_lsn = try self.readU64();
        const timestamp = try self.readI64();
        const xid = try self.readU32();

        return BeginMessage{
            .final_lsn = final_lsn,
            .timestamp = timestamp,
            .xid = xid,
        };
    }

    fn parseCommit(self: *Parser) !CommitMessage {
        const flags = try self.readU8();
        const commit_lsn = try self.readU64();
        const end_lsn = try self.readU64();
        const timestamp = try self.readI64();

        return CommitMessage{
            .flags = flags,
            .commit_lsn = commit_lsn,
            .end_lsn = end_lsn,
            .timestamp = timestamp,
        };
    }

    fn parseRelation(self: *Parser) !RelationMessage {
        const relation_id = try self.readU32();
        const namespace = try self.readString();
        const name = try self.readString();
        const replica_identity = try self.readU8();
        const num_columns = try self.readU16();

        var columns = try self.allocator.alloc(RelationMessage.ColumnInfo, num_columns);
        errdefer self.allocator.free(columns);

        var i: usize = 0;
        while (i < num_columns) : (i += 1) {
            const flags = try self.readU8();
            const col_name = try self.readString();
            const type_id = try self.readU32();
            const type_modifier = try self.readI32();

            columns[i] = .{
                .flags = flags,
                .name = col_name,
                .type_id = type_id,
                .type_modifier = type_modifier,
            };
        }

        return RelationMessage{
            .relation_id = relation_id,
            .namespace = namespace,
            .name = name,
            .replica_identity = replica_identity,
            .columns = columns,
        };
    }

    fn parseInsert(self: *Parser) !InsertMessage {
        const relation_id = try self.readU32();
        const tuple_type = try self.readU8();

        if (tuple_type != 'N') {
            log.warn("Expected 'N' (new tuple), got: {c}", .{tuple_type});
            return error.InvalidTupleType;
        }

        const tuple_data = try self.parseTupleData();

        return InsertMessage{
            .relation_id = relation_id,
            .tuple_data = tuple_data,
        };
    }

    fn parseUpdate(self: *Parser) !UpdateMessage {
        const relation_id = try self.readU32();
        const key_or_old = try self.readU8();

        var old_tuple: ?TupleData = null;
        if (key_or_old == 'K' or key_or_old == 'O') {
            old_tuple = try self.parseTupleData();
            _ = try self.readU8(); // Read 'N' for new tuple
        }

        const new_tuple = try self.parseTupleData();

        return UpdateMessage{
            .relation_id = relation_id,
            .old_tuple = old_tuple,
            .new_tuple = new_tuple,
        };
    }

    fn parseDelete(self: *Parser) !DeleteMessage {
        const relation_id = try self.readU32();
        const key_or_old = try self.readU8();

        if (key_or_old != 'K' and key_or_old != 'O') {
            log.warn("Expected 'K' or 'O', got: {c}", .{key_or_old});
            return error.InvalidTupleType;
        }

        const old_tuple = try self.parseTupleData();

        return DeleteMessage{
            .relation_id = relation_id,
            .old_tuple = old_tuple,
        };
    }

    fn parseOrigin(self: *Parser) !OriginMessage {
        const commit_lsn = try self.readU64();
        const name = try self.readString();

        return OriginMessage{
            .commit_lsn = commit_lsn,
            .name = name,
        };
    }

    fn parseType(self: *Parser) !TypeMessage {
        const type_id = try self.readU32();
        const namespace = try self.readString();
        const name = try self.readString();

        return TypeMessage{
            .type_id = type_id,
            .namespace = namespace,
            .name = name,
        };
    }

    fn parseTruncate(self: *Parser) !TruncateMessage {
        const options = try self.readU8();
        const num_relations = try self.readU32();

        var relation_ids = try self.allocator.alloc(u32, num_relations);
        errdefer self.allocator.free(relation_ids);

        var i: usize = 0;
        while (i < num_relations) : (i += 1) {
            relation_ids[i] = try self.readU32();
        }

        return TruncateMessage{
            .options = options,
            .relation_ids = relation_ids,
        };
    }

    /// Parse tuple data
    fn parseTupleData(self: *Parser) !TupleData {
        const num_columns = try self.readU16();
        var columns = try self.allocator.alloc(?[]const u8, num_columns);
        errdefer self.allocator.free(columns);

        var i: usize = 0;
        while (i < num_columns) : (i += 1) {
            const col_type = try self.readU8();

            columns[i] = switch (col_type) {
                'n' => null, // NULL value
                'u' => null, // Unchanged TOASTed value
                't' => blk: {
                    const len = try self.readU32();
                    break :blk try self.readBytes(len);
                },
                else => {
                    log.warn("Unknown column type: {c}", .{col_type});
                    return error.UnknownColumnType;
                },
            };
        }

        return TupleData{ .columns = columns };
    }

    // Low-level read functions

    /// Read a single byte
    fn readU8(self: *Parser) !u8 {
        if (self.pos >= self.data.len) {
            return error.UnexpectedEndOfData;
        }
        const val = self.data[self.pos];
        self.pos += 1;
        return val;
    }

    /// Read a 16-bit unsigned integer in big-endian order
    fn readU16(self: *Parser) !u16 {
        if (self.pos + 2 > self.data.len) {
            return error.UnexpectedEndOfData;
        }
        const val = std.mem.readInt(u16, self.data[self.pos..][0..2], .big);
        self.pos += 2;
        return val;
    }

    /// Read a 32-bit unsigned integer in big-endian order
    fn readU32(self: *Parser) !u32 {
        if (self.pos + 4 > self.data.len) {
            return error.UnexpectedEndOfData;
        }
        const val = std.mem.readInt(u32, self.data[self.pos..][0..4], .big);
        self.pos += 4;
        return val;
    }

    /// Read a 32-bit signed integer in big-endian order
    fn readI32(self: *Parser) !i32 {
        if (self.pos + 4 > self.data.len) {
            return error.UnexpectedEndOfData;
        }
        const val = std.mem.readInt(i32, self.data[self.pos..][0..4], .big);
        self.pos += 4;
        return val;
    }

    /// Read a 64-bit unsigned integer in big-endian order
    fn readU64(self: *Parser) !u64 {
        if (self.pos + 8 > self.data.len) {
            return error.UnexpectedEndOfData;
        }
        const val = std.mem.readInt(u64, self.data[self.pos..][0..8], .big);
        self.pos += 8;
        return val;
    }

    /// Read a 64-bit signed integer in big-endian order
    fn readI64(self: *Parser) !i64 {
        if (self.pos + 8 > self.data.len) {
            return error.UnexpectedEndOfData;
        }
        const val = std.mem.readInt(i64, self.data[self.pos..][0..8], .big);
        self.pos += 8;
        return val;
    }

    /// Read a null-terminated string
    ///
    /// Caller is responsible for freeing the returned string
    fn readString(self: *Parser) ![]const u8 {
        const start = self.pos;
        while (self.pos < self.data.len and self.data[self.pos] != 0) {
            self.pos += 1;
        }

        if (self.pos >= self.data.len) {
            return error.UnexpectedEndOfData;
        }

        const str = self.data[start..self.pos];
        self.pos += 1; // Skip null terminator

        return try self.allocator.dupe(u8, str);
    }

    /// Read a sequence of bytes of given length
    ///
    /// Caller is responsible for freeing the returned byte slice
    fn readBytes(self: *Parser, len: u32) ![]const u8 {
        const end = self.pos + len;
        if (end > self.data.len) {
            return error.UnexpectedEndOfData;
        }

        const bytes = self.data[self.pos..end];
        self.pos = end;

        return try self.allocator.dupe(u8, bytes);
    }
};
