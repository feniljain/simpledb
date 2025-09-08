//! By convention, root.zig is the root source file when making a library.

// API: https://riak.com/assets/bitcask-intro.pdf

const OpenOptions = struct {
    /// if this process is going to be a writer
    /// and not just a reader
    read_write: bool,
    /// if this writer would prefer to sync the
    /// write file after every write operation
    sync_on_put: bool,
};

const BitCask = struct {
    // TODO: How to make sure only one process
    // opens it with read_write? 
    // OS level dir locks?
    // multiple readers are fine.
    pub fn open(dir_name: []const u8) !BitCask {
        _ = dir_name;

        return BitCask {};
    }

    pub fn get(self: *BitCask, key: []const u8) ![]const u8 {
        _ = self;
        _ = key;

        return "";
    }

    pub fn put(self: *BitCask, key: []const u8, value: []const u8) !void {
        _ = self;
        _ = key;
        _ = value;
    }

    pub fn delete(self: *BitCask, key: []const u8) !void {
        _ = self;
        _ = key;
    }

    pub fn list_keys(self: *BitCask) ![][]const u8 {
        _ = self;
        return [1][1]u8{[_]u8{ 'a' }};
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
        _ = self;
    }
};


test "test bitcask put/get" {
    var bitcask = try BitCask.open("./data");

    const key = "melody";
    const value = "itni choclaty kyun hai";

    try bitcask.put(key, value);

    const received_value = try bitcask.get(key);
    try expect(std.mem.eql(u8, value, received_value));
}

const std = @import("std");
const expect = std.testing.expect;
