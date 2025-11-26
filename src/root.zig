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
    keydir: StringHashMap(ValueLocation) = StringHashMap(ValueLocation).init(smp_allocator),
    dir_name: []const u8,

    pub fn open(dir_name: []const u8) !BitCask {
        // var gpa = heap.GeneralPurposeAllocator(.{}).init;
        // const allocator = gpa.allocator();

        // const allocator = std.testing.allocator;

        const file_name = try std.fmt.allocPrint(smp_allocator, "data-{d}", .{1});
        defer smp_allocator.free(file_name);

        return try BitCask.openWithFile(dir_name, file_name);
    }

    pub fn openWithFile(dir_name: []const u8, file_name: []const u8) !BitCask {
        var dir = try fs.cwd().openDir(dir_name, .{ .access_sub_paths = true, .iterate = true, .no_follow = false });
        defer dir.close();

        var new_file_created = false;

        const file = dir.openFile(
            file_name,
            .{ .mode = fs.File.OpenMode.read_write }) catch blk: {
                // TODO: should we make file access as exclusive?
                const file = try dir.createFile(file_name, .{ .read = true, .truncate = true, .exclusive = false });
                new_file_created = true;
                break :blk file;
            };


        var bc = BitCask { .file = file, .dir_name = dir_name };

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

        var is_file_corrupted = false;

        // make sure we can at least read key_len
        // and value_len
        while((curr_pos + 8) < file_len) {
            _ = try self.file.read(lenbyts);
            const key_len = mem.readInt(u32, lenbyts[0..4], Endian.big);
            curr_pos += 4;

            _ = try self.file.read(lenbyts);
            const value_len = mem.readInt(u32, lenbyts[0..4], Endian.big);
            curr_pos += 4;

            if(curr_pos + key_len > file_len) {
                is_file_corrupted = true;
                // this entry is corrupted, remove
                // key_len and value_len of this entry
                curr_pos -= 8;
                break;
            }

            const key = try self.allocator.alloc(u8, key_len);
            _ = try self.file.readAll(key);
            curr_pos += key_len;

            // don't read tombstone
            if(value_len != 0) {
                // insert into keydir
                try self.keydir.put(key, .{ .value_offset = curr_pos, .value_len = value_len });

                if(curr_pos + value_len > file_len) {
                    is_file_corrupted = true;
                    // this entry is corrupted, remove
                    // key_len, value_len and key of
                    // this entry
                    curr_pos -= (8 + key_len);
                    break;
                }

                // skip reading value
                curr_pos += value_len;
                _ = try self.file.seekTo(curr_pos);
            }
        }

        if(is_file_corrupted or curr_pos != file_len) {
            // if current_position is not perfectly as file_len
            // that means corruption has happened,
            // truncate remaining file
            try self.file.setEndPos(curr_pos);
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
        const key_len: u32 = @intCast(key.len);

        var keyvalbyts = try self.allocator.alloc(u8, 4 + 4 + key_len + value_len);

        mem.writeInt(u32, keyvalbyts[0..4], key_len, Endian.big);
        @memcpy(keyvalbyts[8..(8 + key_len)], key);

        mem.writeInt(u32, keyvalbyts[4..8], value_len, Endian.big);
        if(optional_value != null) {
            const value = optional_value.?;
            @memcpy(keyvalbyts[(8 + key_len)..(8 + key_len + value_len)], value);
        }

        const file_offset = try self.file.getEndPos();

        try self.file.writeAll(keyvalbyts);

        return .{ .value_offset = file_offset + 8 + @as(u64, key_len), .value_len = value_len };
    }

    pub fn put(self: *BitCask, key: []const u8, value: []const u8) !void {
        const value_offset = try self.put_internal(key, value);
        try self.keydir.put(key, value_offset);
    }

    pub fn delete(self: *BitCask, key: []const u8) !void {
        _ = try self.put_internal(key, null);
        _ = self.keydir.swapRemove(key);
    }

    pub fn list_keys(self: *BitCask) ![][]const u8 {
        // As this returns the underlying array
        // used by keydir, we will need to clone
        // it
        return try self.allocator.dupe([]const u8, self.keydir.keys());
    }

    // Only partial implementation as
    // current impl only works on single
    // file right now, so we just compact
    // that
    pub fn merge(old_bitcask: *BitCask) !BitCask {
        // create a temp file for new data
        const temp_data_file_name = try std.fmt.allocPrint(smp_allocator, "data-{d}-temp", .{1});
        const data_file_name = try std.fmt.allocPrint(smp_allocator, "data-{d}", .{1});

        // create new bitcask instance with this temp file
        var new_bitcask = try BitCask.openWithFile(old_bitcask.dir_name, temp_data_file_name);

        // create an iterator over keydir,
        // get all values and write them to
        // new datafile
        var itr = old_bitcask.keydir.iterator();
        while (itr.next()) |entry| {
            const key = entry.key_ptr.*;
            const value = try old_bitcask.get(key);

            try new_bitcask.put(key, value);
        }

        try old_bitcask.close();

        var dir = try fs.cwd().openDir(old_bitcask.dir_name, .{ .access_sub_paths = true, .iterate = true, .no_follow = false });
        defer dir.close();

        try dir.rename(temp_data_file_name, data_file_name);
        
        return new_bitcask;
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

test "test_bitcask_corrupt_file_build_keydir" {
    var bitcask = try BitCask.open("./data");

    const key_1: string = "melody";
    const value_1: string = "itni choclaty kyun hai";

    try bitcask.put(key_1, value_1);

    const received_value_1 = try bitcask.get(key_1);
    try expect(std.mem.eql(u8, value_1, received_value_1));

    const uncorrupted_file_len = try bitcask.file.getEndPos();

    const lenbyts = try bitcask.allocator.alloc(u8, 4);
    mem.writeInt(u32, lenbyts[0..4], 10, Endian.big); // write key_len
    try bitcask.file.writeAll(lenbyts);

    mem.writeInt(u32, lenbyts[0..4], 10, Endian.big); // write value_len
    try bitcask.file.writeAll(lenbyts);

    // write bogus smaller than key_len key data,
    // indicating corruption
    mem.writeInt(u32, lenbyts[0..4], 10, Endian.big);
    try bitcask.file.writeAll(lenbyts);

    try bitcask.close();

    var bitcask_1 = try BitCask.open("./data");

    const received_value_2 = try bitcask_1.get(key_1);
    try expect(std.mem.eql(u8, value_1, received_value_2));

    const new_file_len = try bitcask_1.file.getEndPos();
    // make sure file did get truncated
    try expect(uncorrupted_file_len == new_file_len);

    try bitcask_1.close();
}

test "test_bitcask_list_keys" {
    var bitcask = try BitCask.open("./data");

    const key_1: string = "melody";
    const value_1: string = "itni choclaty kyun hai";

    try bitcask.put(key_1, value_1);

    const key_2: string = "AJR";
    const value_2: string = "Turning out";

    try bitcask.put(key_2, value_2);

    const keys = try bitcask.list_keys();

    try expect(std.mem.eql(u8, keys[0], key_1));
    try expect(std.mem.eql(u8, keys[1], key_2));

    try bitcask.close();
}

test "test_bitcask_iterator" {
    var bitcask = try BitCask.open("./data");

    const key_1: string = "melody";
    const value_1: string = "itni choclaty kyun hai";

    try bitcask.put(key_1, value_1);

    var itr = bitcask.keydir.iterator();

    // while (itr.next()) |entry| {}

    const entry = itr.next().?;
    const key = entry.key_ptr.*;
    // const val = entry.value_ptr.*;

    try expect(std.mem.eql(u8, key, key_1));

    try bitcask.close();
}

test "test_bitcask_merge" {
    var bitcask = try BitCask.open("./data");

    const key_1: string = "melody";
    const value_1: string = "itni choclaty kyun hai";

    try bitcask.put(key_1, value_1);

    const key_2: string = "AJR";
    const value_2: string = "Turning out";

    try bitcask.put(key_2, value_2);

    const file_len_before_merge = try bitcask.file.getEndPos();

    try bitcask.delete(key_1);

    var new_bitcask = try bitcask.merge();

    const file_len_after_merge = try new_bitcask.file.getEndPos();

    // file len after merge should exactly be a (key len + value len + 2 * len bytes i.e. 4)
    // lesser than before merge
    try expect(file_len_before_merge == (file_len_after_merge + key_1.len + value_1.len + 4 + 4));

    try new_bitcask.close();
}

// https://github.com/oven-sh/bun/blob/3b7d1f7be28ecafabb8828d2d53f77898f45312f/src/open.zig#L437
const string = []const u8;
const smp_allocator = std.heap.smp_allocator;

const std = @import("std");
const assert = std.debug.assert;
const StringHashMap = std.array_hash_map.StringArrayHashMap;
const Endian = std.builtin.Endian;
const heap = std.heap;
const mem = std.mem;
const fs = std.fs;
const expect = std.testing.expect;
