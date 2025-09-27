// API: https://riak.com/assets/bitcask-intro.pdf

// TODO: use this
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

pub const ValueLocation = struct {
    value_len: u64,
    value_offset: u64,
};

const BitCaskError = error {
    KeyNotFound,
};

// Simpler version of BitCask:
// - No log files
// - No multiple data files
pub const BitCask = struct {
    file: fs.File,
    allocator: mem.Allocator = smp_allocator,
    keydir: HashMap(ValueLocation) = HashMap(ValueLocation).init(smp_allocator),

    pub fn open(dir_name: []const u8) !BitCask {
        // var gpa = heap.GeneralPurposeAllocator(.{}).init;
        // const allocator = gpa.allocator();

        // const allocator = std.testing.allocator;

        var dir = try fs.cwd().openDir(dir_name, .{ .access_sub_paths = true, .iterate = true, .no_follow = false });
        defer dir.close();

        const file_name = try std.fmt.allocPrint(smp_allocator, "data-{d}", .{1});
        defer smp_allocator.free(file_name);

        var new_file_created = false;

        const file = dir.openFile(
            file_name,
            .{ .mode = fs.File.OpenMode.read_write }) catch blk: {
                // TODO: should we make file access as exclusive?
                const file = try dir.createFile(file_name, .{ .read = true, .truncate = true, .exclusive = false });
                new_file_created = true;
                break :blk file;
            };


        var bc = BitCask{ .file = file };

        if(!new_file_created) {
            try bc.build_keydir();
        }

        return bc;
    }

    pub fn build_keydir(self: *BitCask) !void {
        const file_len = try self.file.getEndPos();

        var curr_pos: u64 = 0;
        try self.file.seekTo(curr_pos);
        const lenbyts = try self.allocator.alloc(u8, 4);

        std.debug.print("DEBUG::curr_pos::{d} - file_len::{d}\n", .{curr_pos, file_len});

        // TODO
        const is_file_corrupted: bool = false;

        // FIX: set is_file_corrupted instead of
        // returning from method
        while(curr_pos < file_len) {
            _ = try self.file.read(lenbyts);
            const key_len = mem.readInt(u32, lenbyts[0..4], Endian.big);
            curr_pos += 4;

            std.debug.print("DEBUG::key_len::{d}\n", .{key_len});

            const key = try self.allocator.alloc(u8, key_len);
            _ = try self.file.read(key);
            curr_pos += key_len;

            std.debug.print("DEBUG::key::{s}\n", .{key});

            _ = try self.file.read(lenbyts);
            const value_len = mem.readInt(u32, lenbyts[0..4], Endian.big);
            curr_pos += 4;

            std.debug.print("DEBUG::value_len::{d}\n", .{value_len});

            // don't read tombstone
            if(value_len != 0) {
                // insert into keydir
                try self.keydir.put(key, .{ .value_offset = curr_pos, .value_len = value_len });

                // skip reading value
                curr_pos += value_len;
                _ = try self.file.seekTo(curr_pos);
            }
        }

        std.debug.print("DEBUG::curr_pos::{d} - file_len::{d}\n", .{curr_pos, file_len});
        assert(curr_pos <= file_len);
        if(is_file_corrupted or curr_pos != file_len) {
            // if current_position is not perfectly as file_len
            // that means corruption has happened,
            // truncate remaining file
            try self.file.setEndPos(curr_pos);
            std.debug.print("DEBUG::corrupted file, truncating", .{});
        }
    }

    pub fn get(self: *BitCask, key: []const u8) ![]u8 {
        const keydirval_opt = self.keydir.get(key);
        if(keydirval_opt == null) {
            return BitCaskError.KeyNotFound;
        }

        const keydirval = keydirval_opt.?;

        try self.file.seekTo(keydirval.value_offset);

        if(keydirval.value_len == 0) {
            return BitCaskError.KeyNotFound;
        }

        const value = try self.allocator.alloc(u8, keydirval.value_len);
        _ = try self.file.read(value);

        return value;
    }

    fn put_internal(self: *BitCask, key: []const u8, optional_value: ?[]const u8) !ValueLocation {
        try self.file.seekFromEnd(0);

        var value_len: u32 = 0;
        if(optional_value != null) {
            const value = optional_value.?;
            value_len = @intCast(value.len);
        }
        const key_len: u64 = @intCast(key.len);

        var keyvalbyts = try self.allocator.alloc(u8, 4 + 4 + key_len + value_len);

        mem.writeInt(u32, keyvalbyts[0..4], @intCast(key.len), Endian.big);
        @memcpy(keyvalbyts[8..(8 + key.len)], key);

        mem.writeInt(u32, keyvalbyts[4..8], value_len, Endian.big);
        if(optional_value != null) {
            const value = optional_value.?;
            @memcpy(keyvalbyts[(8 + key.len)..(8 + key.len + value.len)], value);
        }

        const file_offset = try self.file.getEndPos();

        try self.file.writeAll(keyvalbyts);

        return .{ .value_offset = file_offset + 8 + key_len, .value_len = value_len };
    }

    pub fn put(self: *BitCask, key: []const u8, value: []const u8) !void {
        const value_offset = try self.put_internal(key, value);
        try self.keydir.put(key, value_offset);
    }

    pub fn delete(self: *BitCask, key: []const u8) !void {
        _ = try self.put_internal(key, null);
        _ = self.keydir.swapRemove(key);
    }

    pub fn list_keys(self: *BitCask) ![][]u8 {
        _ = self;
        return [1][1]u8{[_]u8{'a'}};
    }

    // Only partial implementation as
    // current impl only works on single
    // file right now, so we just compact
    // that
    pub fn merge(self: *BitCask) !void {
        _ = self;
    }

    pub fn close(self: *BitCask) !void {
        // _ = try self.allocator.deinit();
        self.file.close();
    }

    // pub fn fold(self: *BitCask) !void {
    // }

    // pub fn sync(self: *BitCask) !void {
    //     _ = self;
    // }
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

test "test_bitcask_delete" {
    var bitcask = try BitCask.open("./data");

    const key_1: string = "melody";
    const value_1: string = "itni choclaty kyun hai";

    try bitcask.put(key_1, value_1);

    const key_2: string = "AJR";
    const value_2: string = "Turning out";

    try bitcask.put(key_2, value_2);

    var received_value_2 = try bitcask.get(key_2);
    try expect(std.mem.eql(u8, value_2, received_value_2));

    const received_value_1 = try bitcask.get(key_1);
    try expect(std.mem.eql(u8, value_1, received_value_1));

    try bitcask.delete(key_1);

    received_value_2 = try bitcask.get(key_2);
    try expect(std.mem.eql(u8, value_2, received_value_2));

    try std.testing.expectError(BitCaskError.KeyNotFound, bitcask.get(key_1));

    try bitcask.close();
}

test "test_bitcask_build_keydir" {
    var bitcask = try BitCask.open("./data");

    const key_1: string = "melody";
    const value_1: string = "itni choclaty kyun hai";

    try bitcask.put(key_1, value_1);

    const received_value_1 = try bitcask.get(key_1);
    try expect(std.mem.eql(u8, value_1, received_value_1));

    try bitcask.close();

    var bitcask_1 = try BitCask.open("./data");

    const received_value_2 = try bitcask_1.get(key_1);
    try expect(std.mem.eql(u8, value_1, received_value_2));

    try bitcask_1.close();
}

// // TODO(test): corrupt file
// test "test_bitcask_build_keydir_corrupt_file" {
//     var bitcask = try BitCask.open("./data");
//
//     const key_1: string = "melody";
//     const value_1: string = "itni choclaty kyun hai";
//
//     try bitcask.put(key_1, value_1);
//
//     const received_value_1 = try bitcask.get(key_1);
//     try expect(std.mem.eql(u8, value_1, received_value_1));
//
//     // corrupt the file by writing just key_len
//     const lenbyts = try bitcask.allocator.alloc(u8, 4);
//     mem.writeInt(u32, lenbyts[0..4], 10, Endian.big);
//     try bitcask.file.writeAll(lenbyts);
//
//     try bitcask.close();
//
//     var bitcask_1 = try BitCask.open("./data");
//
//     const received_value_2 = try bitcask_1.get(key_1);
//     try expect(std.mem.eql(u8, value_1, received_value_2));
//
//
//     try bitcask_1.close();
// }

// TODO:
// - build_keydir
// - list_keys
// - merge
// - iterator

// https://github.com/oven-sh/bun/blob/3b7d1f7be28ecafabb8828d2d53f77898f45312f/src/open.zig#L437
const string = []const u8;
const smp_allocator = std.heap.smp_allocator;

const std = @import("std");
const assert = std.debug.assert;
const HashMap = std.array_hash_map.StringArrayHashMap;
const Endian = std.builtin.Endian;
const heap = std.heap;
const mem = std.mem;
const fs = std.fs;
const expect = std.testing.expect;
