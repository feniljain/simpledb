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
    // iterator??

    pub fn append(self: *LogMgr, log_rec: []const u8) !u64 {
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
