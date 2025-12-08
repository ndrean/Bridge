//! PostgreSQL COPY CSV format parser
//!
//! Much simpler than binary format - just CSV parsing with PostgreSQL-specific escaping
//!
//! Format: COPY (...) TO STDOUT WITH (FORMAT csv, HEADER true)
//! - First line: column names (comma-separated)
//! - Following lines: data rows (comma-separated, quoted if needed)
//! - NULL values: empty string or explicit \N
//! - Escaping: double quotes for quotes, standard CSV rules

const std = @import("std");
const c = @cImport({
    @cInclude("libpq-fe.h");
});

pub const log = std.log.scoped(.pg_copy_csv);

/// Parser for COPY CSV format
pub const CopyCsvParser = struct {
    allocator: std.mem.Allocator,
    conn: ?*c.PGconn,
    header: ?[][]const u8 = null,
    buffer: std.ArrayList(u8),

    pub fn init(allocator: std.mem.Allocator, conn: ?*c.PGconn) CopyCsvParser {
        return .{
            .allocator = allocator,
            .conn = conn,
            .buffer = std.ArrayList(u8){},
        };
    }

    pub fn deinit(self: *CopyCsvParser) void {
        if (self.header) |h| {
            for (h) |col| self.allocator.free(col);
            self.allocator.free(h);
        }
        self.buffer.deinit(self.allocator);
    }

    /// Execute COPY command and read all data
    pub fn executeCopy(self: *CopyCsvParser, query: [:0]const u8) !void {
        // Execute COPY command
        const result = c.PQexec(self.conn, query.ptr);
        defer c.PQclear(result);

        if (c.PQresultStatus(result) != c.PGRES_COPY_OUT) {
            const err_msg = c.PQerrorMessage(self.conn);
            log.err("COPY command failed: {s}", .{err_msg});
            return error.CopyFailed;
        }

        // Read all COPY data into buffer
        self.buffer.clearRetainingCapacity();

        while (true) {
            var buf_ptr: [*c]u8 = undefined;
            const len = c.PQgetCopyData(self.conn, &buf_ptr, 0);

            if (len == -1) {
                // End of COPY data
                break;
            } else if (len == -2) {
                // Error
                const err_msg = c.PQerrorMessage(self.conn);
                log.err("PQgetCopyData failed: {s}", .{err_msg});
                return error.CopyDataFailed;
            } else if (len > 0) {
                const chunk = buf_ptr[0..@intCast(len)];
                try self.buffer.appendSlice(self.allocator, chunk);
                c.PQfreemem(buf_ptr);
            }
        }

        log.debug("COPY CSV received {d} bytes", .{self.buffer.items.len});

        // Parse header (first line)
        try self.parseHeader();
    }

    /// Parse CSV header line
    fn parseHeader(self: *CopyCsvParser) !void {
        const data = self.buffer.items;

        // Find first newline
        const newline_pos = std.mem.indexOfScalar(u8, data, '\n') orelse {
            if (data.len == 0) return; // Empty result set
            return error.NoHeaderFound;
        };

        const header_line = data[0..newline_pos];

        // Parse CSV header
        var cols = std.ArrayList([]const u8){};
        defer cols.deinit(self.allocator);

        var it = std.mem.splitScalar(u8, header_line, ',');
        while (it.next()) |col_name| {
            const trimmed = std.mem.trim(u8, col_name, " \r");
            try cols.append(self.allocator, try self.allocator.dupe(u8, trimmed));
        }

        self.header = try cols.toOwnedSlice(self.allocator);

        log.debug("COPY CSV header: {d} columns", .{self.header.?.len});
    }

    /// Row iterator for CSV data
    pub const RowIterator = struct {
        parser: *CopyCsvParser,
        line_start: usize,

        pub fn next(self: *RowIterator) !?CsvRow {
            const data = self.parser.buffer.items;

            // Skip header on first call
            if (self.line_start == 0) {
                const first_newline = std.mem.indexOfScalar(u8, data, '\n') orelse return null;
                self.line_start = first_newline + 1;
            }

            // Check if we've reached the end
            if (self.line_start >= data.len) {
                return null;
            }

            // Find next newline
            const line_end = if (std.mem.indexOfScalarPos(u8, data, self.line_start, '\n')) |pos|
                pos
            else
                data.len;

            const line = data[self.line_start..line_end];
            self.line_start = line_end + 1;

            // Skip empty lines
            if (line.len == 0) {
                return try self.next();
            }

            // Parse CSV line
            return try self.parser.parseCsvLine(line);
        }
    };

    /// Parse a single CSV line into fields
    fn parseCsvLine(self: *CopyCsvParser, line: []const u8) !CsvRow {
        var fields = std.ArrayList(CsvField){};
        errdefer {
            for (fields.items) |field| {
                if (field.value) |v| self.allocator.free(v);
            }
            fields.deinit(self.allocator);
        }

        var col_idx: usize = 0;
        var it = std.mem.splitScalar(u8, line, ',');

        while (it.next()) |raw_field| : (col_idx += 1) {
            const trimmed = std.mem.trim(u8, raw_field, " \r");

            // Handle NULL
            if (trimmed.len == 0 or std.mem.eql(u8, trimmed, "\\N")) {
                try fields.append(self.allocator, .{ .value = null });
                continue;
            }

            // Handle quoted values
            if (trimmed.len >= 2 and trimmed[0] == '"' and trimmed[trimmed.len - 1] == '"') {
                // Unquote and unescape
                const quoted = trimmed[1 .. trimmed.len - 1];
                const unescaped = try self.unescapeCsv(quoted);
                try fields.append(self.allocator, .{ .value = unescaped });
            } else {
                // Plain value
                try fields.append(self.allocator, .{ .value = try self.allocator.dupe(u8, trimmed) });
            }
        }

        return .{
            .fields = try fields.toOwnedSlice(self.allocator),
            .allocator = self.allocator,
        };
    }

    /// Unescape CSV quoted value (replace "" with ")
    fn unescapeCsv(self: *CopyCsvParser, input: []const u8) ![]const u8 {
        var result = std.ArrayList(u8){};
        defer result.deinit(self.allocator);

        var i: usize = 0;
        while (i < input.len) {
            if (input[i] == '"' and i + 1 < input.len and input[i + 1] == '"') {
                // Double quote -> single quote
                try result.append(self.allocator, '"');
                i += 2;
            } else {
                try result.append(self.allocator, input[i]);
                i += 1;
            }
        }

        return try result.toOwnedSlice(self.allocator);
    }

    /// Get row iterator
    pub fn rows(self: *CopyCsvParser) RowIterator {
        return .{
            .parser = self,
            .line_start = 0,
        };
    }

    /// Get column names
    pub fn columnNames(self: *CopyCsvParser) ?[][]const u8 {
        return self.header;
    }
};

/// A field in a CSV row
pub const CsvField = struct {
    value: ?[]const u8,

    pub fn isNull(self: CsvField) bool {
        return self.value == null;
    }
};

/// A row from CSV data
pub const CsvRow = struct {
    fields: []CsvField,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *CsvRow) void {
        for (self.fields) |csv_field| {
            if (csv_field.value) |v| {
                self.allocator.free(v);
            }
        }
        self.allocator.free(self.fields);
    }

    pub fn getField(self: CsvRow, index: usize) ?CsvField {
        if (index < self.fields.len) {
            return self.fields[index];
        }
        return null;
    }

    pub fn fieldCount(self: CsvRow) usize {
        return self.fields.len;
    }
};
