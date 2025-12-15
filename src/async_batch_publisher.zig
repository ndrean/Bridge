//! Async batch publisher that offloads flushing to a dedicated thread.
//!
//! It holds the complete pipeline: Decode pgoutput tuple → Create CDC event → Enqueue → Batch encoding → Publish to NATS → LSN confirmation tracking

const std = @import("std");
const c_imports = @import("c_imports.zig");
const c = c_imports.c;
const batch_publisher = @import("batch_publisher.zig");
const nats_publisher = @import("nats_publisher.zig");
const pgoutput = @import("pgoutput.zig");
const SPSCQueue = @import("spsc_queue.zig").SPSCQueue;
const Config = @import("config.zig");
const encoder_mod = @import("encoder.zig");
const Metrics = @import("metrics.zig").Metrics;

pub const log = std.log.scoped(.async_batch_publisher);

/// Async batch publisher that offloads flushing to a dedicated thread
pub const AsyncBatchPublisher = struct {
    allocator: std.mem.Allocator,
    publisher: *nats_publisher.Publisher,
    config: batch_publisher.BatchConfig,
    format: encoder_mod.Format,
    metrics: ?*Metrics, // Optional metrics reference

    // Lock-free event queue (SPSC: Single Producer Single Consumer)
    // Producer: Main thread adding WAL events
    // Consumer: Flush thread publishing to NATS
    event_queue: SPSCQueue(batch_publisher.CDCEvent),

    // Atomic state shared between threads
    last_confirmed_lsn: std.atomic.Value(u64), // Last LSN confirmed by NATS
    fatal_error: std.atomic.Value(bool), // Set when NATS reconnection fails
    flush_complete: std.atomic.Value(bool), // Set when flush thread finishes final flush

    // Flush thread
    flush_thread: ?std.Thread,
    should_stop: std.atomic.Value(bool),

    pub fn init(
        allocator: std.mem.Allocator,
        publisher: *nats_publisher.Publisher,
        config: batch_publisher.BatchConfig,
        format: encoder_mod.Format,
        metrics: ?*Metrics,
        runtime_config: *const Config.RuntimeConfig,
    ) !AsyncBatchPublisher {
        // Initialize lock-free queue with power-of-2 capacity from runtime config
        const event_queue = try SPSCQueue(batch_publisher.CDCEvent).init(
            allocator,
            runtime_config.batch_ring_buffer_size,
        );

        return AsyncBatchPublisher{
            .allocator = allocator,
            .publisher = publisher,
            .config = config,
            .format = format,
            .metrics = metrics,
            .event_queue = event_queue,
            .last_confirmed_lsn = std.atomic.Value(u64).init(0),
            .fatal_error = std.atomic.Value(bool).init(false),
            .flush_complete = std.atomic.Value(bool).init(false),
            .flush_thread = null,
            .should_stop = std.atomic.Value(bool).init(false),
        };
    }

    /// Start the background flush thread. Must be called after init() and after
    /// the AsyncBatchPublisher is in its final memory location (not on stack).
    pub fn start(self: *AsyncBatchPublisher) !void {
        // Spawn flush thread - self must be at stable address
        self.flush_thread = try std.Thread.spawn(.{}, flushLoop, .{self});
    }

    /// Join the flush thread (waits for completion)
    pub fn join(self: *AsyncBatchPublisher) void {
        // Signal flush thread to stop
        self.should_stop.store(true, .seq_cst);

        // Wait for flush thread to finish
        if (self.flush_thread) |thread| {
            thread.join();
            self.flush_thread = null;
        }
    }

    /// Deinit - cleanup resources (call after join)
    pub fn deinit(self: *AsyncBatchPublisher) void {
        // Clean up any remaining events in the queue
        while (self.event_queue.pop()) |event| {
            var mut_event = event;
            mut_event.deinit(self.allocator);
        }

        // Deinit the queue itself
        self.event_queue.deinit();

        log.info("🥁 Async batch publisher stopped", .{});
    }

    /// Add an event to the lock-free queue. Wait-free operation (no locks, no blocking).
    /// Takes ownership of the `data` ArrayList - caller must not free it.
    pub fn addEvent(
        self: *AsyncBatchPublisher,
        subject: []const u8,
        table: []const u8,
        operation: []const u8,
        msg_id: []const u8,
        relation_id: u32,
        data: ?std.ArrayList(pgoutput.Column),
        lsn: u64,
    ) !void {
        log.debug("📥 Adding event to queue: {s} {s}", .{ operation, table });

        // Only copy the strings (subject, table, operation, msg_id)
        // Take ownership of the columns ArrayList directly (zero-copy for column data)
        const owned_subject = try self.allocator.dupeZ(u8, subject);
        errdefer self.allocator.free(owned_subject);

        const owned_table = try self.allocator.dupe(u8, table);
        errdefer self.allocator.free(owned_table);

        const owned_operation = try self.allocator.dupe(u8, operation);
        errdefer self.allocator.free(owned_operation);

        const owned_msg_id = try self.allocator.dupe(u8, msg_id);
        errdefer self.allocator.free(owned_msg_id);

        const event = batch_publisher.CDCEvent{
            .subject = owned_subject,
            .table = owned_table,
            .operation = owned_operation,
            .msg_id = owned_msg_id,
            .relation_id = relation_id,
            .data = data, // Transfer ownership - no copy!
            .lsn = lsn,
        };
        // If push fails with unexpected error, clean up the event (including the hashmap we now own)
        errdefer {
            var mut_event = event;
            mut_event.deinit(self.allocator);
        }

        // Push to lock-free queue with backpressure retry
        var retry_count: usize = 0;
        while (true) {
            self.event_queue.push(event) catch |err| {
                if (err == error.QueueFull) {
                    retry_count += 1;

                    // Log warning on first retry, then periodically
                    if (retry_count == 1 or retry_count % 100 == 0) {
                        log.warn(
                            "⚠️ Event queue full! Applying backpressure (retry #{d}). Queue capacity: {d}",
                            .{ retry_count, Config.Batch.ring_buffer_size },
                        );
                    }

                    // Yield CPU to flush thread
                    std.Thread.yield() catch {};
                    continue; // Retry
                }

                // Unexpected error - propagate (errdefer will clean up)
                return err;
            };

            // Success!
            if (retry_count > 0) {
                log.info("Queue space available after {d} retries, resuming", .{retry_count});
            }
            break;
        }

        log.debug("Event added to lock-free queue", .{});
    }

    /// Process CDC event from pgoutput format and enqueue for publishing
    /// This encapsulates the entire CDC event processing pipeline.
    /// Returns the event's LSN for tracking, or null if the event was filtered out.
    pub fn processCdcEvent(
        self: *AsyncBatchPublisher,
        rel: pgoutput.RelationMessage,
        tuple_data: pgoutput.TupleData,
        operation: []const u8,
        wal_end: u64,
    ) !void {
        // Decode tuple data to get actual column values
        // Use allocator so decoded values survive and are owned by the event
        var decoded_values = pgoutput.decodeTuple(
            self.allocator,
            tuple_data,
            rel.columns,
        ) catch |err| {
            log.warn("⚠️ Failed to decode tuple: {}", .{err});
            return;
        };
        // NOTE: addEvent() takes ownership of decoded_values.
        // The flush thread will free them after publishing.
        errdefer {
            // Only free on error - if addEvent() fails
            for (decoded_values.items) |column| {
                switch (column.value) {
                    .text => |txt| self.allocator.free(txt),
                    .numeric => |num| self.allocator.free(num),
                    .array => |arr| self.allocator.free(arr),
                    .jsonb => |jsn| self.allocator.free(jsn),
                    .bytea => |byt| self.allocator.free(byt),
                    else => {}, // int32, int64, float64, boolean, null don't need freeing
                }
            }
            decoded_values.deinit(self.allocator);
        }

        // Extract ID value for logging (if present)
        // Optimize: check length first before memcmp (most column names aren't "id")
        var id_buf: [64]u8 = undefined;
        const id_str = blk: {
            for (decoded_values.items) |column| {
                // Quick rejection: check length first (avoid memcmp for wrong-length names)
                if (column.name.len == 2 and column.name[0] == 'i' and column.name[1] == 'd') {
                    break :blk switch (column.value) {
                        .int32 => |v| std.fmt.bufPrint(&id_buf, "{d}", .{v}) catch "?",
                        .int64 => |v| std.fmt.bufPrint(&id_buf, "{d}", .{v}) catch "?",
                        .text => |v| if (v.len <= id_buf.len) v else "?",
                        else => "?",
                    };
                }
            }
            break :blk null;
        };

        // Convert operation to lowercase for NATS subject
        const operation_lower = switch (operation[0]) {
            'I' => "insert", // INSERT
            'U' => "update", // UPDATE
            'D' => "delete", // DELETE
            else => unreachable, // Only these 3 operations exist in CDC
        };

        // Create NATS subject
        var subject_buf: [Config.Buffers.subject_buffer_size]u8 = undefined;
        const subject = try std.fmt.bufPrintZ(
            &subject_buf,
            "{s}.{s}.{s}",
            .{ Config.Nats.subject_cdc_prefix, rel.name, operation_lower },
        );

        // Generate message ID from WAL LSN for idempotent delivery
        var msg_id_buf: [Config.Buffers.msg_id_buffer_size]u8 = undefined;
        const msg_id = try std.fmt.bufPrint(
            &msg_id_buf,
            "{x}-{s}-{s}",
            .{ wal_end, rel.name, operation_lower },
        );

        // Add to batch publisher with column data, relation_id, and LSN
        try self.addEvent(
            subject,
            rel.name,
            operation,
            msg_id,
            rel.relation_id,
            decoded_values,
            wal_end,
        );

        // Update metrics if available
        if (self.metrics) |m| {
            m.incrementCdcEvents();
        }

        // Log single line with table, operation, and ID
        if (id_str) |id| {
            log.info("{s} {s}.{s} id={s} → {s}", .{ operation, rel.namespace, rel.name, id, subject });
        } else {
            log.info("{s} {s}.{s} → {s}", .{ operation, rel.namespace, rel.name, subject });
        }
    }

    /// Get the last LSN that was successfully confirmed by NATS
    /// Called only when ACK thresholds are met (bytes/time/keepalive)
    pub fn getLastConfirmedLsn(self: *AsyncBatchPublisher) u64 {
        return self.last_confirmed_lsn.load(.seq_cst);
    }

    /// Get current queue usage (0.0 = empty, 1.0 = full)
    pub fn getQueueUsage(self: *AsyncBatchPublisher) f64 {
        const current_len = self.event_queue.len();
        const capacity = self.event_queue.capacity;
        return @as(f64, @floatFromInt(current_len)) / @as(f64, @floatFromInt(capacity));
    }

    /// Check if a fatal error occurred (e.g., NATS reconnection timeout)
    pub fn hasFatalError(self: *AsyncBatchPublisher) bool {
        return self.fatal_error.load(.seq_cst);
    }

    /// Check if the flush thread has completed all pending work
    pub fn isFlushComplete(self: *AsyncBatchPublisher) bool {
        return self.flush_complete.load(.seq_cst);
    }

    /// Background thread that continuously drains events from lock-free queue and flushes to NATS
    fn flushLoop(self: *AsyncBatchPublisher) void {
        log.info("ℹ️ Lock-free flush thread started", .{});

        var batches_processed: usize = 0;
        var last_flush_time = std.time.milliTimestamp();

        // Persistent batch that accumulates events across iterations
        var batch = std.ArrayList(batch_publisher.CDCEvent){};
        var current_payload_size: usize = 0; // Track approximate payload size
        defer {
            // Clean up on thread exit
            for (batch.items) |*event| {
                event.deinit(self.allocator);
            }
            batch.deinit(self.allocator);
        }

        while (!self.should_stop.load(.seq_cst)) {
            // Drain events from the queue until we hit a threshold
            while (batch.items.len < self.config.max_events and
                current_payload_size < self.config.max_payload_bytes)
            {
                const event = self.event_queue.pop() orelse break;

                // Approximate payload size (table + operation + subject strings)
                const event_size = event.table.len + event.operation.len + event.subject.len;

                batch.append(self.allocator, event) catch |err| {
                    log.err("⚠️ Failed to append to batch: {}", .{err});
                    // Clean up event on error
                    var mut_event = event;
                    mut_event.deinit(self.allocator);
                    break;
                };

                current_payload_size += event_size;
            }
            const now = std.time.milliTimestamp();
            const time_elapsed = now - last_flush_time;

            // Flush if we have events AND (batch is full OR payload too large OR timeout reached)
            const should_flush = batch.items.len > 0 and
                (batch.items.len >= self.config.max_events or
                    current_payload_size >= self.config.max_payload_bytes or
                    time_elapsed >= self.config.max_wait_ms);

            if (should_flush) {
                batches_processed += 1;
                log.info("Flush thread processing batch #{d} with {d} events", .{ batches_processed, batch.items.len });

                // Log the msg_ids of events being flushed
                if (batch.items.len > 0) {
                    log.debug("Batch #{d} contains msg_ids: {s} ... {s}", .{
                        batches_processed,
                        batch.items[0].msg_id,
                        batch.items[batch.items.len - 1].msg_id,
                    });
                }

                // Flush the current batch - this will free the events but retain capacity
                self.flushBatch(&batch) catch |err| {
                    log.err("⚠️ Failed to flush batch: {}", .{err});
                };

                // Clear the batch (capacity is retained for reuse)
                // Events were already freed by flushBatch
                current_payload_size = 0;

                last_flush_time = now;

                // Update queue usage metrics after flushing batch
                // Queue usage has meaningfully changed - we just drained events
                if (self.metrics) |m| {
                    const queue_usage = self.getQueueUsage();
                    m.updateQueueUsage(queue_usage);
                }
            } else if (batch.items.len == 0) {
                // No events available
                // Check if we should stop immediately (no pending work)
                if (self.should_stop.load(.seq_cst)) {
                    break;
                }
                // Sleep briefly to avoid busy-waiting
                std.Thread.sleep(1 * std.time.ns_per_ms);
            } else {
                // Have events but timeout not reached
                // If shutting down, flush immediately instead of waiting
                if (self.should_stop.load(.seq_cst)) {
                    log.info("Shutdown detected with {d} pending events - flushing now", .{batch.items.len});
                    self.flushBatch(&batch) catch |err| {
                        log.err("⚠️ Failed to flush pending batch on shutdown: {}", .{err});
                    };
                    current_payload_size = 0;
                    break;
                }
                // Keep them and sleep briefly
                std.Thread.sleep(1 * std.time.ns_per_ms);
            }
        }

        // Drain any remaining events on shutdown
        log.info("Flush thread shutting down, draining remaining events...", .{});

        // Drain remaining events into the existing batch
        while (self.event_queue.pop()) |event| {
            batch.append(self.allocator, event) catch |err| {
                log.err("⚠️ Failed to append final event: {}", .{err});
                var mut_event = event;
                mut_event.deinit(self.allocator);
                break;
            };
        }

        if (batch.items.len > 0) {
            log.info("Flushing final batch with {d} events", .{batch.items.len});
            self.flushBatch(&batch) catch |err| {
                log.err("⚠️ Failed to flush final batch: {}", .{err});
            };
        }

        // Signal that flush thread has completed all work
        self.flush_complete.store(true, .seq_cst);
        log.info("✅ Flush thread completed - all events published", .{});

        // Release NATS thread-local storage
        // This is required when user-created threads call NATS C library APIs
        c.nats_ReleaseThreadMemory();
    }

    /// Flush a batch to NATS (runs in flush thread)
    /// Takes a pointer to the batch ArrayList and clears it after flushing (retaining capacity)
    fn flushBatch(self: *AsyncBatchPublisher, batch: *std.ArrayList(batch_publisher.CDCEvent)) !void {
        if (batch.items.len == 0) {
            return;
        }

        const flush_start = std.time.milliTimestamp();
        const event_count = batch.items.len;

        log.info("⚡ Flushing batch of {d} events", .{event_count});

        // Use temporary BatchPublisher for actual encoding/publishing logic
        // IMPORTANT: We pass batch.* (by value) which creates a shallow copy
        // But we'll manage cleanup carefully to avoid double-free
        var temp_publisher = batch_publisher.BatchPublisher{
            .allocator = self.allocator,
            .publisher = self.publisher,
            .config = self.config,
            .format = self.format,
            .events = batch.*, // Shallow copy - temp_publisher.events now shares buffer with batch
            .current_payload_size = 0,
            .last_flush_time = 0,
            .last_confirmed_lsn = 0,
        };

        // Call the existing flush implementation
        // This will free the events and call clearRetainingCapacity()
        const confirmed_lsn = temp_publisher.flush() catch |err| {
            log.err("Flush failed: {}", .{err});

            // Check if this is a NATS timeout - trigger fatal error to shutdown bridge
            if (err == error.FlushFailed) {
                self.fatal_error.store(true, .seq_cst);
                log.err("🔴 FATAL: NATS reconnection timeout exceeded - bridge must shutdown to prevent WAL overflow", .{});
            }

            // On error, events may be partially freed
            // Clear the batch to prevent use-after-free
            batch.clearRetainingCapacity();
            return err;
        };

        // Update last confirmed LSN after successful flush
        if (confirmed_lsn > 0) {
            self.last_confirmed_lsn.store(confirmed_lsn, .seq_cst);
            log.debug("Updated last confirmed LSN to {x}", .{confirmed_lsn});
        }

        // temp_publisher.flush() already freed the events and called clearRetainingCapacity()
        // Since batch and temp_publisher.events share the same buffer, we need to sync them
        // The buffer now has len=0 but retains capacity for reuse
        batch.clearRetainingCapacity();

        // Log flush timing
        const flush_elapsed = std.time.milliTimestamp() - flush_start;
        log.info("Flushed {d} events in {d}ms", .{ event_count, flush_elapsed });
        if (flush_elapsed > 5) {
            log.warn("⚠️ Slow flush: {d}ms for {d} events", .{ flush_elapsed, event_count });
        }
    }
};
