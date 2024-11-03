const std = @import("std");
const mem = std.mem;

const storage = @import("storage.zig");
const string = storage.string;
const FileMgr = storage.FileMgr;
const Page = storage.Page;
const BlockID = storage.BlockID;

pub const LogMgr = struct {
    file_mgr: FileMgr,
    log_file_name: string,
    log_page: Page,
    curr_blk: BlockID,
    latest_lsn: u64 = 0,
    last_saved_lsn: u64 = 0,

    pub fn new(allocator: mem.Allocator, file_mgr: FileMgr, log_file_name: string) LogMgr {
        const log_page = try Page.from_blocksize(allocator, file_mgr.block_size);

        const log_size =  file_mgr.length(log_file_name);

        if(log_size == 0) {
            curr_blk = append_new_block();
        } else {
            curr_blk = BlockID.new();
        }

        return .{
            .log_page = log_page,
            .file_mgr = file_mgr,
            .log_file = log_file,
        };
    }
};

test "log mgr" {}
