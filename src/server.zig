// const std = @import("std");
// const Stream = std.Io.net.Stream;
// const Socket = std.Io.net.Socket;
// const Protocol = std.Io.net.Protocol;

// pub const log = std.log.scoped(.bridge_demo);

// pub const Server = struct {
//     host: []const u8,
//     port: u16,
//     addr: std.Io.net.IpAddress,
//     io: std.Io,

//     pub fn init(io: std.Io) !Server {
//         const host: []const u8 = "127.0.0.1";
//         const port: u16 = 9090;
//         const addr = try std.Io.net.IpAddress.parseIp4(host, port);

//         return .{ .host = host, .port = port, .addr = addr, .io = io };
//     }

//     pub fn listen(self: Server) !std.Io.net.Server {
//         std.debug.print("Server Addr: {s}:{any}\n", .{ self.host, self.port });
//         return try self.addr.listen(self.io, .{ .mode = Socket.Mode.stream, .protocol = Protocol.tcp });
//     }
// };

// const Map = std.static_string_map.StaticStringMap;
// const MethodMap = Map(Method).initComptime(.{
//     .{ "GET", Method.GET },
//     .{ "POST", Method.POST },
// });

// pub const Method = enum {
//     GET,
//     POST,
//     pub fn init(text: []const u8) !Method {
//         return MethodMap.get(text).?;
//     }
//     pub fn is_supported(m: []const u8) bool {
//         const method = MethodMap.get(m);
//         if (method) |_| {
//             return true;
//         }
//         return false;
//     }
// };

// const Request = struct {
//     method: Method,
//     version: []const u8,
//     uri: []const u8,
//     pub fn init(method: Method, uri: []const u8, version: []const u8) Request {
//         return Request{
//             .method = method,
//             .uri = uri,
//             .version = version,
//         };
//     }
// };

// pub fn read_request(io: std.Io, conn: Stream, buffer: []u8) !void {
//     var recv_buffer: [1024]u8 = undefined;
//     var reader = conn.reader(io, &recv_buffer);
//     const length: usize = 400;
//     const input = try reader.interface.peekArray(length);
//     @memcpy(buffer[0..length], input[0..length]);
// }

// pub fn parse_request(text: []u8) Request {
//     const line_index = std.mem.indexOfScalar(u8, text, '\n') orelse text.len;
//     var iterator = std.mem.splitScalar(u8, text[0..line_index], ' ');
//     const method = try Method.init(iterator.next().?);
//     const uri = iterator.next().?;
//     const version = iterator.next().?;
//     const request = Request.init(method, uri, version);
//     return request;
// }

// pub fn send_200(conn: Stream, io: std.Io) !void {
//     const message = ("HTTP/1.1 200 OK\n");
//     var stream_writer = conn.writer(io, &.{});
//     _ = try stream_writer.interface.write(message);
// }

// pub fn send_404(conn: Stream, io: std.Io) !void {
//     const message = ("HTTP/1.1 404 Not Found\n");
//     var stream_writer = conn.writer(io, &.{});
//     _ = try stream_writer.interface.write(message);
// }

// pub fn run() !void {
//     var alloc = std.heap.GeneralPurposeAllocator(.{}){};
//     const gpa = alloc.allocator();
//     var threaded: std.Io.Threaded = .init(gpa);
//     const io = threaded.io();
//     defer threaded.deinit();

//     const server = try Server.init(io);
//     var listening = try server.listen();
//     const connection = try listening.accept(io);
//     defer connection.close(io);
//     log.info("HTTP server listening on http://127.0.0.1:{d}", .{server.port});
//     log.info("Available endpoints:", .{});
//     log.info("  GET  /health  - Health check", .{});
//     log.info("  GET  /status  - Bridge status", .{});
//     log.info("  POST /shutdown - Graceful shutdown", .{});

//     var request_buffer: [400]u8 = undefined;
//     try Request.read_request(io, connection, request_buffer[0..request_buffer.len]);
//     const request = Request.parse_request(request_buffer[0..request_buffer.len]);
//     if (request.method == Method.GET) {
//         if (std.mem.eql(u8, request.uri, "/")) {
//             log.info("GET", .{});
//             // try Response.send_200(connection, io);
//         } else {
//             log.info("404", .{});
//             // try Response.send_404(connection, io);
//         }
//     } else if (request.method == Method.POST) {
//         log.info("POST", .{});
//         // try Response.send_200(connection, io);
//     } else {
//         log.info("404", .{});
//         // try Response.send_404(connection, io);
//     }
// }
