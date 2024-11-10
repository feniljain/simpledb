const std = @import("std");
const mem = std.mem;
const print = std.debug.print;

const storage = @import("storage.zig");
const string = storage.string;
const FileMgr = storage.FileMgr;
const Page = storage.Page;
const BlockID = storage.BlockID;
const OFFSET_BYTE_SIZE = storage.OFFSET_BYTE_SIZE;

pub const LogMgr = struct {
    allocator: mem.Allocator,
    file_mgr: *FileMgr,
    log_file_name: string,
    curr_blk: BlockID,
    latest_lsn: u64 = 0,
    last_saved_lsn: u64 = 0,

    // Log Page: [<offset_to_write_from><log-record-n>....<log-record-2><log-record-1>]
    // - Logs are written from end of page to starting
    // - First 4 bytes (an integer's space) is reserved to store what offset from back
    //  to write from next. This starts from page_size (== block_size) and keeps
    //  decrementing from there till no space is left to store new log records
    log_page: Page,

    pub fn new(allocator: mem.Allocator, file_mgr: *FileMgr, log_file_name: string) !LogMgr {
        const log_page = try Page.from_blocksize(allocator, file_mgr.block_size);

        const log_size = try file_mgr.length(log_file_name);

        const dummy_blk = BlockID.new("dummy-temp-block", 0);

        var log_mgr: LogMgr = .{
            .allocator = allocator,
            .log_page = log_page,
            .file_mgr = file_mgr,
            .log_file_name = log_file_name,
            .curr_blk = dummy_blk,
        };

        const curr_blk = if (log_size == 0) if_blk: {
            const curr_blk = try log_mgr.append_new_block();
            break :if_blk curr_blk;
        } else else_blk: {
            const curr_blk = BlockID.new(log_file_name, log_size - 1);
            break :else_blk curr_blk;
        };

        log_mgr.curr_blk = curr_blk;

        return log_mgr;
    }

    // pub fn flush(lsn: u64) void {}

    pub fn iterator(self: *LogMgr) !LogsIterator {
        return LogsIterator.init(self.allocator, self.log_file_name, self.file_mgr);
    }

    pub fn append(self: *LogMgr, log_rec: []const u8) !u64 {

        // how much space is left in block?
        //
        // enough ->
        // - just push buf to log page
        // - increment lsn
        //
        // not enough ->
        // - flush current block
        // - append a new block
        // - set curr_blk to new_blk
        // - just push buf to log page
        // - increment lsn

        var offset_to_write_from = self.log_page.get_int(0);
        // extra OFFSET_BYTE_SIZE is for the integer we store in the start
        // of log page
        const byt_space_remaining = offset_to_write_from - OFFSET_BYTE_SIZE;
        const rec_size = log_rec.len;

        // here OFFSET_BYTE_SIZE denotes storing length of record
        // in `set_bytes` impl of page
        const byts_needed = rec_size + OFFSET_BYTE_SIZE;

        if (byts_needed > byt_space_remaining) {
            // not enough space
            try self.flush();
            const new_blk = try self.append_new_block();
            self.curr_blk = new_blk;
            offset_to_write_from = self.log_page.get_int(0);
        }

        _ = self.log_page.set_bytes(offset_to_write_from - byts_needed, &log_rec);
        _ = self.log_page.set_int(0, offset_to_write_from - byts_needed);
        self.latest_lsn += 1;

        return self.latest_lsn;
    }

    pub fn append_new_block(self: *LogMgr) !BlockID {
        var blk = try self.file_mgr.append(self.log_file_name);
        _ = self.log_page.set_int(0, self.file_mgr.block_size);
        try self.file_mgr.write(&blk, &self.log_page);
        return blk;
    }

    pub fn flush(self: *LogMgr) !void {
        try self.file_mgr.write(&self.curr_blk, &self.log_page);
        self.last_saved_lsn = self.latest_lsn;
    }

    pub fn free(self: *LogMgr) void {
        self.log_page.free();
    }
};

pub const LogsIterator = struct {
    log_file_name: string,
    blk: BlockID,
    next_offset: u64,
    n_blk: u64,
    read_page: Page,
    file_mgr: *FileMgr,

    pub fn init(allocator: mem.Allocator, log_file_name: string, file_mgr: *FileMgr) !LogsIterator {
        const n_blk = try file_mgr.length(log_file_name) - 1;
        var page = try Page.from_blocksize(allocator, file_mgr.block_size);
        var blk = BlockID.new(log_file_name, 0);

        try file_mgr.read(&blk, &page);
        const next_offset = page.get_int(0);

        return LogsIterator{
            .log_file_name = log_file_name,
            .blk = blk,
            .next_offset = next_offset,
            .n_blk = n_blk,
            .read_page = page,
            .file_mgr = file_mgr,
        };

        // find number of blocks in it
        // store which block are we currently on
        // read from that block into a local page
    }

    pub fn next(self: *LogsIterator) !?[]const u8 {
        // free page at the end of iterator cycle

        // did we reach end of blocks and page both?
        // yes -> free page, return null
        //
        // no ->
        // did we reach end of page?
        // yes->
        // - switch to next block
        // - read page from block
        // - set next_offset
        // - get_bytes()
        // - set next_offset
        //
        // no ->
        // - get_bytes()
        // - set next_offset

        const end_of_page = self.next_offset >= self.file_mgr.block_size;
        if (self.blk.idx == self.n_blk and end_of_page) {
            self.read_page.free();
            return null;
        }

        if (end_of_page) {
            self.blk = BlockID.new(self.log_file_name, self.blk.idx + 1);
            try self.file_mgr.read(&self.blk, &self.read_page);
            self.next_offset = self.read_page.get_int(0);
            if(self.next_offset == self.file_mgr.block_size and self.blk.idx == self.n_blk) {
                self.read_page.free();
                return null;
            }
        }

        const buf = try self.read_page.get_bytes(self.next_offset);
        self.next_offset += buf.len + OFFSET_BYTE_SIZE; // TODO: double check

        return buf;
    }
};
