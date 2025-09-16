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
// KeyValPair::key_len:u32, value_len: u32, key: []const u8, value: []const u8

pub const KeyDirValue = struct {
    value_len: u32,
    value_offset: u32,
};

// Simpler version of BitCask:
// - No log files
// - No multiple data files
pub const BitCask = struct {
    file: fs.File,
    allocator: mem.Allocator,
    keydir: *HashMap(KeyDirValue),

    pub fn open(dir_name: []const u8) !BitCask {
        // var gpa = heap.GeneralPurposeAllocator(.{}).init;
        // const allocator = gpa.allocator();

        // const allocator = std.testing.allocator;
        const allocator = std.heap.smp_allocator;

        var dir = try fs.cwd().openDir(dir_name, .{ .access_sub_paths = true, .iterate = true, .no_follow = false });
        defer dir.close();

        const file_name = try std.fmt.allocPrint(allocator, "data-{d}", .{1});
        defer allocator.free(file_name);

        // TODO: mark truncate as false, and exclusive as true, how to make
        // file access as exclusive?
        const file = try dir.createFile(file_name, .{ .read = true, .truncate = true, .exclusive = false });

        var keydir = HashMap(KeyDirValue).init(allocator);

        return BitCask{ .file = file, .allocator = allocator, .keydir = &keydir };
    }

    pub fn get(self: *BitCask, key: []const u8) ![]u8 {
        _ = key;

        try self.file.seekTo(0);

        const key_len_byts = (try self.allocator.alloc(u8, 4))[0..4];
        _ = try self.file.read(key_len_byts);
        const key_len = mem.readInt(u32, key_len_byts, Endian.big);

        const value_len_byts = (try self.allocator.alloc(u8, 4))[0..4];
        _ = try self.file.read(value_len_byts);
        const value_len = mem.readInt(u32, value_len_byts, Endian.big);

        try self.file.seekBy(key_len);

        const value = try self.allocator.alloc(u8, value_len);
        _ = try self.file.read(value);

        return value;
    }

    pub fn put(self: BitCask, key: []const u8, value: []const u8) !void {
        var keyvalbyts = try self.allocator.alloc(u8, 4 + 4 + key.len + value.len);

        const value_len: u32 = @intCast(value.len);
        mem.writeInt(u32, keyvalbyts[0..4], @intCast(key.len), Endian.big);
        mem.writeInt(u32, keyvalbyts[4..8], value_len, Endian.big);

        @memcpy(keyvalbyts[8..(8 + key.len)], key);
        @memcpy(keyvalbyts[(8 + key.len)..(8 + key.len + value.len)], value);

        // try self.keydir.put(key, .{ .value_len = value_len, .value_offset = 0 });
        try self.file.writeAll(keyvalbyts);
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
        self.file.close();
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
const HashMap = std.array_hash_map.StringArrayHashMap;
const Endian = std.builtin.Endian;
const heap = std.heap;
const mem = std.mem;
const fs = std.fs;
const expect = std.testing.expect;
