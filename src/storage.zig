const std = @import("std");
const mem = std.mem;
const builtin = std.mem;
const posix = posix;
const fs = std.fs;
const print = std.debug.print;
const StringHashMap = std.hash_map.StringHashMap;

const string = []const u8;

const OFFSET_BYTE_SIZE = 8;

inline fn construct_path(alloc: mem.Allocator, db_dir_path: string, file_name: string) !string {
    const concat_arr = [3]string{ db_dir_path, "/", file_name };
    const file_path = try mem.concat(alloc, u8, &concat_arr);

    return file_path;
}

pub const BlockID = struct {
    file_name: string,
    idx: u64,

    pub fn new(file_name: string, idx: u64) BlockID {
        return BlockID{
            .file_name = file_name,
            .idx = idx,
        };
    }

    pub fn equals(block_id_left: BlockID, block_id_right: BlockID) bool {
        return ((block_id_left.file_name == block_id_right.file_name) and
            (block_id_left.idx == block_id_right.idx));
    }
};

pub const Page = struct {
    buf: []u8,
    allocator: mem.Allocator,

    pub fn from_blocksize(allocator: mem.Allocator, size: u64) !Page {
        const buf: []u8 = try allocator.alloc(u8, size);
        @memset(buf, 0);

        return Page{
            .buf = buf,
            .allocator = allocator,
        };
    }

    // we don't need from_bytes() as one can construct that directly

    // primitives are stored in the form of:
    // <length:u64><bytearray:length>
    // example for storing [0b00, 0b01] // byte arrray of length 2
    //                     v actual array
    // 0b00 0b00 0b00 0b10 0b00 0b01
    //                  ^ length 2
    pub fn get_int(self: *Page, offset: u64) i64 {
        return mem.readInt(
            i64,
            mem.sliceAsBytes(self.buf[offset + OFFSET_BYTE_SIZE .. offset + OFFSET_BYTE_SIZE + 8])[0..8],
            .big,
        );
    }

    // TODO: Should caller pass a buffer to write to, or Should
    // this method allocate?
    pub fn get_bytes(self: *Page, offset: u64) ![]u8 {
        const data_len = mem.readInt(
            u64,
            mem.sliceAsBytes(self.buf[offset .. offset + OFFSET_BYTE_SIZE])[0..8],
            .big,
        );

        const bytes: []u8 = try self.allocator.alloc(u8, data_len);
        mem.copyForwards(
            u8,
            bytes,
            self.buf[offset + OFFSET_BYTE_SIZE .. offset + OFFSET_BYTE_SIZE + data_len],
        );

        return bytes;
    }

    // OPTIMIZATION: instead of using static 8 bytes for each size integer, think of packing integers, apply
    // the same for size storage in `set_bytes`
    pub fn set_int(self: *Page, offset: u64, value: i64) u64 {
        mem.writeInt(u64, mem.sliceAsBytes(self.buf[offset .. offset + OFFSET_BYTE_SIZE])[0..8], 8, .big);
        mem.writeInt(i64, mem.sliceAsBytes(self.buf[offset + OFFSET_BYTE_SIZE .. offset + OFFSET_BYTE_SIZE + 8])[0..8], value, .big);
        return offset + OFFSET_BYTE_SIZE + 8;
    }

    pub fn set_bytes(self: *Page, offset: u64, bytes: []const u8) u64 {
        mem.writeInt(u64, mem.sliceAsBytes(self.buf[offset .. offset + OFFSET_BYTE_SIZE])[0..8], bytes.len, .big);
        mem.copyForwards(u8, self.buf[offset + OFFSET_BYTE_SIZE .. offset + OFFSET_BYTE_SIZE + bytes.len], bytes);

        return offset + OFFSET_BYTE_SIZE + bytes.len;
    }

    pub fn free(self: *Page) void {
        self.allocator.free(self.buf);
    }
};

const FileMgr = struct {
    db_dir: fs.Dir,
    block_size: u64,
    allocator: mem.Allocator,
    open_files: StringHashMap(fs.File),
    // is_new: bool // we will try to make this just an externally returned var

    pub fn new(allocator: mem.Allocator, db_dir_path: string, block_size: u64) !FileMgr {
        // dir already exist?
        // - no, create it
        // - yes, remote temp files(tables) in dir

        const db_dir = if (fs.openDirAbsolute(
            db_dir_path,
            .{ .iterate = true },
        )) |db_dir_| if_blk: {
            var db_dir_iter = db_dir_.iterate();
            while (try db_dir_iter.next()) |ele| {
                if (mem.indexOf(u8, ele.name, "temp") != null) {
                    // OPTIMIZATION: Don't go to heap(see use of allocator) for this operation,
                    // do it on stack itself
                    const file_path = try construct_path(allocator, db_dir_path, ele.name);
                    try fs.deleteFileAbsolute(file_path);

                    allocator.free(file_path);
                }
            }

            break :if_blk db_dir_;
        } else |err| switch (err) {
            fs.File.OpenError.FileNotFound => else_blk: {
                try fs.makeDirAbsolute(db_dir_path);

                const db_dir = try fs.openDirAbsolute(
                    db_dir_path,
                    .{ .iterate = true },
                );

                break :else_blk db_dir;
            },
            else => {
                return err;
            },
        };

        return FileMgr{
            .db_dir = db_dir,
            .block_size = block_size,
            .allocator = allocator,
            .open_files = StringHashMap(fs.File).init(allocator),
        };
    }

    pub fn write(self: *FileMgr, blk: *BlockID, p: *Page) !void {
        const file = try self.get_file(blk.file_name);
        try file.seekTo(blk.idx * self.block_size);
        _ = try file.write(p.buf);
    }

    pub fn read(self: *FileMgr, blk: *BlockID, p: *Page) !void {
        const file = try self.get_file(blk.file_name);
        try file.seekTo(blk.idx * self.block_size);
        _ = try file.read(p.buf);
    }

    fn get_file(self: *FileMgr, file_name: string) !fs.File {
        const file_opt = self.open_files.get(file_name);
        if (file_opt != null) {
            return file_opt.?;
        }

        const file = try self.db_dir.createFile(file_name, .{
            .read = true,
            .truncate = false,
            .exclusive = false,
            .lock = .exclusive,
            .lock_nonblocking = true,
        });

        try self.open_files.put(file_name, file);

        return file;
    }

    pub fn length(self: *FileMgr, file_name: string) !u64 {
        const file = try self.get_file(file_name);
        try file.seekTo(0);
        const file_size = (try file.stat()).size;
        return file_size / self.block_size;
    }

    pub fn free(self: *FileMgr) void {
        var val_iter = self.open_files.iterator();
        while (val_iter.next()) |ele| {
            ele.value_ptr.close();
        }

        self.open_files.deinit();

        self.db_dir.close();
    }
};

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

test "basic block + file + page read/write" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    defer {
        _ = gpa.deinit();
    }

    var db = try SimpleDB.new(allocator, "/Users/feniljain/test-dir", 400, 0);
    defer db.free();

    var blk = BlockID.new("testfile", 0);

    var file_mgr = &db.file_mgr;

    var p1 = try Page.from_blocksize(allocator, file_mgr.block_size);
    defer p1.free();

    const val = "abcdefghijklm";
    const pos1 = 0;

    var next_offset = p1.set_bytes(0, val);

    const pos2 = next_offset;
    const i: i64 = 1234;

    next_offset = p1.set_int(next_offset, i);
 
    try file_mgr.write(&blk, &p1);

    var p2 = try Page.from_blocksize(allocator, file_mgr.block_size);
    defer p2.free();

    try file_mgr.read(&blk, &p2);

    const buf = try p2.get_bytes(pos1);
    defer allocator.free(buf);

    print("length of file in terms of blocks: {any}\n", .{file_mgr.length(blk.file_name)});
    print("offset {any} contains {s}\n", .{ pos1, buf });
    print("offset {any} contains {any}\n", .{ pos2, p2.get_int(pos2) });

    const argv = [_][]const u8{"rm","-rf","/Users/feniljain/test-dir",};
    _ = try std.process.Child.run(.{
        .allocator = allocator,
        .argv = &argv,
    });
}
