const std = @import("std");

/// Thread-safe metrics shared between bridge and HTTP server
pub const Metrics = struct {
    mutex: std.Thread.Mutex,
    start_time: i64, // Unix timestamp in seconds

    // Message counters
    wal_messages_received: u64,
    cdc_events_published: u64,

    // LSN tracking
    last_ack_lsn: u64,
    current_lsn_hex: [32]u8, // Hex string representation "0/1A2B3C4D"
    current_lsn_len: u8,

    // Connection state
    is_connected: bool,
    reconnect_count: u32,
    last_reconnect_time: i64, // Unix timestamp, 0 if never reconnected

    // Latency tracking (simple)
    last_processing_time_us: u64, // Microseconds for last message

    // WAL lag metrics
    slot_active: bool, // Is replication slot active?
    wal_lag_bytes: u64, // Bytes of WAL retained for this slot
    last_wal_check_time: i64, // Unix timestamp of last WAL lag check

    pub fn init() Metrics {
        return .{
            .mutex = .{},
            .start_time = std.time.timestamp(),
            .wal_messages_received = 0,
            .cdc_events_published = 0,
            .last_ack_lsn = 0,
            .current_lsn_hex = std.mem.zeroes([32]u8),
            .current_lsn_len = 0,
            .is_connected = false,
            .reconnect_count = 0,
            .last_reconnect_time = 0,
            .last_processing_time_us = 0,
            .slot_active = false,
            .wal_lag_bytes = 0,
            .last_wal_check_time = 0,
        };
    }

    /// Thread-safe increment of WAL message counter
    pub fn incrementWalMessages(self: *Metrics) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        self.wal_messages_received += 1;
    }

    /// Thread-safe increment of CDC events counter
    pub fn incrementCdcEvents(self: *Metrics) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        self.cdc_events_published += 1;
    }

    /// Thread-safe update of LSN position
    pub fn updateLsn(self: *Metrics, lsn: u64) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        self.last_ack_lsn = lsn;

        // Format as hex string "0/{x}"
        const hex_str = std.fmt.bufPrint(&self.current_lsn_hex, "0/{x}", .{lsn}) catch "";
        self.current_lsn_len = @intCast(hex_str.len);
    }

    /// Thread-safe connection state update
    pub fn setConnected(self: *Metrics, connected: bool) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        self.is_connected = connected;
    }

    /// Thread-safe reconnection tracking
    pub fn recordReconnect(self: *Metrics) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        self.reconnect_count += 1;
        self.last_reconnect_time = std.time.timestamp();
        self.is_connected = true;
    }

    /// Thread-safe processing time update
    pub fn recordProcessingTime(self: *Metrics, microseconds: u64) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        self.last_processing_time_us = microseconds;
    }

    /// Thread-safe WAL lag update
    pub fn updateWalLag(self: *Metrics, slot_active: bool, lag_bytes: u64) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        self.slot_active = slot_active;
        self.wal_lag_bytes = lag_bytes;
        self.last_wal_check_time = std.time.timestamp();
    }

    /// Get current uptime in seconds (thread-safe read)
    pub fn getUptimeSeconds(self: *Metrics) i64 {
        self.mutex.lock();
        defer self.mutex.unlock();
        return std.time.timestamp() - self.start_time;
    }

    /// Thread-safe snapshot of metrics for display
    pub const Snapshot = struct {
        uptime_seconds: i64,
        wal_messages_received: u64,
        cdc_events_published: u64,
        last_ack_lsn: u64,
        current_lsn_str: []const u8,
        is_connected: bool,
        reconnect_count: u32,
        last_reconnect_time: i64,
        last_processing_time_us: u64,
        slot_active: bool,
        wal_lag_bytes: u64,
        last_wal_check_time: i64,
    };

    /// Get a consistent snapshot of all metrics
    pub fn snapshot(self: *Metrics, allocator: std.mem.Allocator) !Snapshot {
        self.mutex.lock();
        defer self.mutex.unlock();

        const lsn_str = try allocator.dupe(u8, self.current_lsn_hex[0..self.current_lsn_len]);

        return .{
            .uptime_seconds = std.time.timestamp() - self.start_time,
            .wal_messages_received = self.wal_messages_received,
            .cdc_events_published = self.cdc_events_published,
            .last_ack_lsn = self.last_ack_lsn,
            .current_lsn_str = lsn_str,
            .is_connected = self.is_connected,
            .reconnect_count = self.reconnect_count,
            .last_reconnect_time = self.last_reconnect_time,
            .last_processing_time_us = self.last_processing_time_us,
            .slot_active = self.slot_active,
            .wal_lag_bytes = self.wal_lag_bytes,
            .last_wal_check_time = self.last_wal_check_time,
        };
    }
};
