// API: https://riak.com/assets/bitcask-intro.pdf

const OpenOptions = struct {
    /// if this process is going to be a writer
    /// and not just a reader
    read_write: bool,
    /// if this writer would prefer to sync the
    /// write file after every write operation
    sync_on_put: bool,
    /// Max size of each data file
    max_file_size: u32,
};

// ref: https://github.com/erikgrinaker/toydb/blob/main/docs/architecture/storage.md#bitcask-storage-engine
const KeyValPair = struct {
    key_len: u32,
    value_len: i32,
    key: []const u8,
    value: []const u8,
};

pub const BitCask = struct {
    dir: fs.Dir,
    allocator: mem.Allocator,

    // TODO: How to make sure only one process
    // opens it with read_write?
    // OS level dir locks?
    // multiple readers are fine.
    pub fn open(dir_name: []const u8) !BitCask {
        // var gpa = heap.GeneralPurposeAllocator(.{}).init;
        // const allocator = gpa.allocator();

        // const allocator = std.testing.allocator;
        const allocator = std.heap.smp_allocator;

        const dir = try fs.cwd().openDir(dir_name, .{ .access_sub_paths = true, .iterate = true, .no_follow = false });

        // var walker = try dir.walk(allocator);
        // while (try walker.next()) |entry| {
        //     std.debug.print("entry: {}", .{entry});
        //     allocator.free(entry);
        // }

        return BitCask{ .dir = dir, .allocator = allocator };
    }

    pub fn get(self: *BitCask, key: []const u8) ![]u8 {
        _ = key;

        const file_name = try std.fmt.allocPrint(self.allocator, "data-{d}", .{1});
        defer self.allocator.free(file_name);

        // TODO: change flags to appropriate values
        var file = try self.dir.openFile(file_name, .{ .mode = fs.File.OpenMode.read_write, .lock = .none, .lock_nonblocking = false, .allow_ctty = false });

        try file.seekTo(0);

        const key_len_byts = (try self.allocator.alloc(u8, 4))[0..4];
        _ = try file.read(key_len_byts);
        const key_len = mem.readInt(u32, key_len_byts, Endian.big);

        const value_len_byts = (try self.allocator.alloc(u8, 4))[0..4];
        _ = try file.read(value_len_byts);
        const value_len = mem.readInt(u32, value_len_byts, Endian.big);

        try file.seekBy(key_len);

        const value = try self.allocator.alloc(u8, value_len);
        _ = try file.read(value);

        return value;
    }

    pub fn put(self: BitCask, key: []const u8, value: []const u8) !void {
        const file_name = try std.fmt.allocPrint(self.allocator, "data-{d}", .{1});
        defer self.allocator.free(file_name);

        // TODO: mark truncate as false, and exclusive as true
        var file = try self.dir.createFile(file_name, .{ .read = true, .truncate = true, .exclusive = false });

        var keyvalbyts = try self.allocator.alloc(u8, 4 + 4 + key.len + value.len);
        mem.writeInt(u32, keyvalbyts[0..4], @intCast(key.len), Endian.big);
        mem.writeInt(i32, keyvalbyts[4..8], @intCast(value.len), Endian.big);

        @memcpy(keyvalbyts[8..(8 + key.len)], key);
        @memcpy(keyvalbyts[(8 + key.len)..(8 + key.len + value.len)], value);

        try file.writeAll(keyvalbyts);
    }

    pub fn delete(self: *BitCask, key: []const u8) !void {
        _ = self;
        _ = key;
    }

    pub fn list_keys(self: *BitCask) ![][]u8 {
        _ = self;
        return [1][1]u8{[_]u8{'a'}};
    }

    // pub fn fold(self: *BitCask) !void {
    // }

    pub fn merge(self: *BitCask) !void {
        _ = self;
    }

    pub fn sync(self: *BitCask) !void {
        _ = self;
    }

    pub fn close(self: *BitCask) !void {
        // _ = try self.allocator.deinit();
        self.dir.close();
    }
};

test "test_bitcask_put_get" {
    var bitcask = try BitCask.open("./data");

    const key: string = "melody";
    const value: string = "itni choclaty kyun hai";

    try bitcask.put(key, value);

    const received_value = try bitcask.get(key);
    try expect(std.mem.eql(u8, value, received_value));

    try bitcask.close();
}

test "test_bitcask_multiple_put_get" {
    var bitcask = try BitCask.open("./data");

    const key_1: string = "melody";
    const value_1: string = "itni choclaty kyun hai";

    try bitcask.put(key_1, value_1);

    const key_2: string = "AJR";
    const value_2: string = "Turning out";

    try bitcask.put(key_2, value_2);

    const received_value_2 = try bitcask.get(key_2);
    try expect(std.mem.eql(u8, value_2, received_value_2));

    const received_value_1 = try bitcask.get(key_1);
    try expect(std.mem.eql(u8, value_1, received_value_1));

    try bitcask.close();
}

// https://github.com/oven-sh/bun/blob/3b7d1f7be28ecafabb8828d2d53f77898f45312f/src/open.zig#L437
const string = []const u8;

const std = @import("std");
const Endian = @import("std").builtin.Endian;
const heap = @import("std").heap;
const mem = @import("std").mem;
const fs = @import("std").fs;
const expect = std.testing.expect;
