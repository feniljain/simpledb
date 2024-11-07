const std = @import("std");
const mem = std.mem;
const print = std.debug.print;

const storage = @import("storage.zig");
const FileMgr = storage.FileMgr;
const string = storage.string;

// TODO: Check which all functions are marked `serialized` in book
// and understand how those would be enfored in our codebase?

pub const SimpleDB = struct {
    file_mgr: FileMgr,
    allocator: mem.Allocator,

    pub fn new(
        allocator: mem.Allocator,
        db_dir_path: string,
        block_size: u64,
        buffer_size: u64,
    ) !SimpleDB {
        _ = buffer_size;

        const file_mgr = try FileMgr.new(allocator, db_dir_path, block_size);

        return SimpleDB{
            .file_mgr = file_mgr,
            .allocator = allocator,
        };
    }

    pub fn free(self: *SimpleDB) void {
        self.file_mgr.free();
    }
};

pub fn main() !void {
    std.debug.print("SimpleDB\n", .{});

    // var db = try SimpleDB.new(allocator, "/Users/feniljain/test-dir", 400, 0);
    // defer db.free();

    // TODO: Implement a TCP server
}
