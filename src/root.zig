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
        _ = self;
        _ = key;

        return "";
    }

    pub fn put(self: BitCask, key: []const u8, value: []const u8) !void {
        const file_name = try std.fmt.allocPrint(self.allocator, "data-{d}", .{1});
        defer self.allocator.free(file_name);

        // TODO: mark truncate as false, and exclude as true
        var file = try self.dir.createFile(file_name, .{ .read = true, .truncate = true, .exclusive = false });

        // const keyval: KeyValPair = .{ .key_len = @intCast(key.len), .value_len = @intCast(value.len), .key = key, .value = value };
        // std.debug.print("pair: {any}", .{@sizeOf(keyval)});
        // try file.writeAll(mem.asBytes(keyval));

        var keyvalbyts = try self.allocator.alloc(u8, 4 + 4 + key.len + value.len);
        mem.writeInt(u32, keyvalbyts[0..4], @intCast(key.len), Endian.big);
        mem.writeInt(i32, keyvalbyts[4..8], @intCast(value.len), Endian.big);
        std.debug.print("{any}", .{@TypeOf(&keyvalbyts[8..(8 + key.len)])});
        std.debug.print("{any}", .{@TypeOf(key)});
        @memset(keyvalbyts[8..(8 + key.len)], std.mem.sliceAsBytes(key));
        // @memset(keyvalbyts[(8 + key.len)..(8 + key.len + value.len)], value);

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

test "test bitcask put/get" {
    var bitcask = try BitCask.open("./data");

    const key: string = "melody";
    const value: string = "itni choclaty kyun hai";

    try bitcask.put(key, value);

    const received_value = try bitcask.get(key);
    try expect(std.mem.eql(u8, value, received_value));

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
