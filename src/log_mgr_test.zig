const std = @import("std");
const fmt = std.fmt;
const mem = std.mem;

const print = std.debug.print;

const storage = @import("storage.zig");
const FileMgr = storage.FileMgr;
const Page = storage.Page;
const BlockID = storage.BlockID;
const OFFSET_BYTE_SIZE = storage.OFFSET_BYTE_SIZE;

const logmgr = @import("log_mgr.zig");
const LogMgr = logmgr.LogMgr;

const LogMgrTest = struct {
    blk_size: u64,
    file_mgr: FileMgr,
    log_mgr: LogMgr,
    allocator: mem.Allocator,

    pub fn create_records(self: *LogMgrTest, start: u64, end: u64) !void {
        print("Creating records\n", .{});
        for (start..end + 1) |idx| {
            const log_str: []const u8 = try fmt.allocPrint(self.allocator, "record{}", .{idx});
            defer self.allocator.free(log_str);

            const buf = try self.allocator.alloc(u8, log_str.len + OFFSET_BYTE_SIZE + OFFSET_BYTE_SIZE);

            var page: Page = .{
                .allocator = self.allocator,
                .buf = buf,
            };
            defer page.free();

            _ = page.set_bytes(0, &log_str);
            _ = page.set_int(log_str.len, 100 + idx);

            _ = try self.log_mgr.append(buf);

            // print("Completed writing LSN: {}\n", .{lsn});
        }

        print("Wrote all records\n", .{});
    }

    pub fn print_log_records(self: *LogMgrTest) !void {
        // _ = self;
        var itr = try self.log_mgr.iterator();
        while (try itr.next()) |rec| {
            // _ = rec;
            print("rec: {any}\n", .{rec});
            self.allocator.free(rec);
        }
    }
};

test "log mgr" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    defer {
        _ = gpa.deinit();
        delete_test_dir(allocator);
    }


    const blk_size = 400;

    var file_mgr = try FileMgr.new(allocator, "/Users/feniljain/test-dir-2", blk_size);
    defer file_mgr.free();

    var log_mgr = try LogMgr.new(allocator, &file_mgr, "test-log-file");
    defer log_mgr.free();

    var log_mgr_test: LogMgrTest = .{
        .blk_size = blk_size,
        .file_mgr = file_mgr,
        .log_mgr = log_mgr,
        .allocator = allocator,
    };

    try log_mgr_test.create_records(1, 35);
    try log_mgr_test.print_log_records();

    // print_log_records();
    // create_records();
    // log_mgr.flush();
    // print_log_records();

    print("[PASS] log mgr test\n", .{});
}

fn delete_test_dir(allocator: mem.Allocator) void {
    print("delete_test_dir::start\n", .{});

    const argv = [_][]const u8{
        "rm",
        "-rf",
        "/Users/feniljain/test-dir-2",
    };
    _ = std.process.Child.run(.{
        .allocator = allocator,
        .argv = &argv,
    }) catch |err| {
        print("error while deleting dir: {any}", .{err});
    };
}
