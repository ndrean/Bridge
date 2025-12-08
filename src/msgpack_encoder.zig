// const std = @import("std");
// const msgpack = @import("msgpack");
// const cdc = @import("cdc.zig");

// pub const log = std.log.scoped(.msgpack_encoder);

// /// Encode a CDC Change event to MessagePack binary format
// pub fn encodeChange(allocator: std.mem.Allocator, change: cdc.Change) ![]u8 {
//     var buffer: [2048]u8 = undefined;

//     // Use the compatibility layer for cross-version support
//     const compat = msgpack.compat;
//     var write_buffer = compat.fixedBufferStream(&buffer);
//     var read_buffer = compat.fixedBufferStream(&buffer);

//     const BufferType = compat.BufferStream;
//     var packer = msgpack.Pack(
//         *BufferType,
//         *BufferType,
//         BufferType.WriteError,
//         BufferType.ReadError,
//         BufferType.write,
//         BufferType.read,
//     ).init(&write_buffer, &read_buffer);

//     // Create root map
//     var root = msgpack.Payload.mapPayload(allocator);
//     defer root.free(allocator);

//     // Add lsn
//     try root.mapPut("lsn", try msgpack.Payload.strToPayload(change.lsn, allocator));

//     // Add table
//     try root.mapPut("table", try msgpack.Payload.strToPayload(change.table, allocator));

//     // Add operation
//     const operation_str = switch (change.operation) {
//         .INSERT => "INSERT",
//         .UPDATE => "UPDATE",
//         .DELETE => "DELETE",
//         .BEGIN => "BEGIN",
//         .COMMIT => "COMMIT",
//     };
//     try root.mapPut("operation", try msgpack.Payload.strToPayload(operation_str, allocator));

//     // Add timestamp
//     try root.mapPut("timestamp", msgpack.Payload.intToPayload(change.timestamp));

//     // Add tx_id if present
//     if (change.tx_id) |tx_id| {
//         try root.mapPut("tx_id", msgpack.Payload.uintToPayload(tx_id));
//     }

//     // Write the payload
//     try packer.write(root);

//     // Return a copy of the written data
//     const written_len = write_buffer.pos;
//     return try allocator.dupe(u8, buffer[0..written_len]);
// }

// /// Decode MessagePack binary back to CDC Change event
// pub fn decodeChange(allocator: std.mem.Allocator, data: []const u8) !cdc.Change {
//     var buffer: [2048]u8 = undefined;
//     @memcpy(buffer[0..data.len], data);

//     // Use the compatibility layer
//     const compat = msgpack.compat;
//     var write_buffer = compat.fixedBufferStream(&buffer);
//     var read_buffer = compat.fixedBufferStream(&buffer);

//     const BufferType = compat.BufferStream;
//     var packer = msgpack.Pack(
//         *BufferType,
//         *BufferType,
//         BufferType.WriteError,
//         BufferType.ReadError,
//         BufferType.write,
//         BufferType.read,
//     ).init(&write_buffer, &read_buffer);

//     // Read and decode
//     read_buffer.pos = 0;
//     const payload = try packer.read(allocator);
//     defer payload.free(allocator);

//     // Extract fields
//     const lsn = (try payload.mapGet("lsn")).?.str.value();
//     const table = (try payload.mapGet("table")).?.str.value();
//     const operation_str = (try payload.mapGet("operation")).?.str.value();
//     const timestamp = (try payload.mapGet("timestamp")).?.int;

//     // Parse operation
//     const operation: cdc.Change.Operation = if (std.mem.eql(u8, operation_str, "INSERT"))
//         .INSERT
//     else if (std.mem.eql(u8, operation_str, "UPDATE"))
//         .UPDATE
//     else if (std.mem.eql(u8, operation_str, "DELETE"))
//         .DELETE
//     else if (std.mem.eql(u8, operation_str, "BEGIN"))
//         .BEGIN
//     else if (std.mem.eql(u8, operation_str, "COMMIT"))
//         .COMMIT
//     else
//         return error.InvalidOperation;

//     // Extract optional tx_id
//     var tx_id: ?u64 = null;
//     if (try payload.mapGet("tx_id")) |tx_id_payload| {
//         tx_id = tx_id_payload.uint;
//     }

//     return cdc.Change{
//         .lsn = try allocator.dupe(u8, lsn),
//         .table = try allocator.dupe(u8, table),
//         .operation = operation,
//         .timestamp = timestamp,
//         .tx_id = tx_id,
//         .before = null,
//         .after = null,
//     };
// }

// test "encode and decode CDC change" {
//     const allocator = std.testing.allocator;

//     const change = cdc.Change{
//         .lsn = "0/12345678",
//         .table = "users",
//         .operation = .INSERT,
//         .timestamp = 1700000000,
//         .tx_id = 12345,
//         .before = null,
//         .after = null,
//     };

//     // Encode
//     const encoded = try encodeChange(allocator, change);
//     defer allocator.free(encoded);

//     log.info("Encoded {} bytes", .{encoded.len});

//     // Decode
//     const decoded = try decodeChange(allocator, encoded);
//     defer {
//         allocator.free(decoded.lsn);
//         allocator.free(decoded.table);
//     }

//     // Verify
//     try std.testing.expectEqualStrings(change.lsn, decoded.lsn);
//     try std.testing.expectEqualStrings(change.table, decoded.table);
//     try std.testing.expectEqual(change.operation, decoded.operation);
//     try std.testing.expectEqual(change.timestamp, decoded.timestamp);
//     try std.testing.expectEqual(change.tx_id, decoded.tx_id);
// }
