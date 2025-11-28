const std = @import("std");

// Import NATS C library
const c = @cImport({
    @cInclude("nats.h");
});

pub fn main() !void {
    std.debug.print("Testing NATS connection...\n", .{});

    var nc: ?*c.natsConnection = null;
    var opts: ?*c.natsOptions = null;
    var status: c.natsStatus = undefined;

    // Create options
    status = c.natsOptions_Create(&opts);
    if (status != c.NATS_OK) {
        std.debug.print("Error creating options: {s}\n", .{c.natsStatus_GetText(status)});
        return error.NatsOptionsCreateFailed;
    }
    defer c.natsOptions_Destroy(opts);

    // Set server URL
    status = c.natsOptions_SetURL(opts, "nats://localhost:4222");
    if (status != c.NATS_OK) {
        std.debug.print("Error setting URL: {s}\n", .{c.natsStatus_GetText(status)});
        return error.NatsSetURLFailed;
    }

    // Connect to NATS
    std.debug.print("Connecting to NATS at nats://localhost:4222...\n", .{});
    status = c.natsConnection_Connect(&nc, opts);
    if (status != c.NATS_OK) {
        std.debug.print("Error connecting: {s}\n", .{c.natsStatus_GetText(status)});
        std.debug.print("\nMake sure NATS server is running:\n", .{});
        std.debug.print("  brew install nats-server\n", .{});
        std.debug.print("  nats-server -c nats-server.conf\n", .{});
        return error.NatsConnectionFailed;
    }
    defer c.natsConnection_Destroy(nc);

    std.debug.print("✓ Connected successfully!\n", .{});

    // Publish a simple test message
    const subject = "test.hello";
    const message = "Hello from Zig!";

    std.debug.print("Publishing message to '{s}'...\n", .{subject});
    status = c.natsConnection_PublishString(nc, subject, message);
    if (status != c.NATS_OK) {
        std.debug.print("Error publishing: {s}\n", .{c.natsStatus_GetText(status)});
        return error.NatsPublishFailed;
    }

    std.debug.print("✓ Message published successfully!\n", .{});
    std.debug.print("\nNATS integration is working!\n", .{});
}
