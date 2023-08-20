// PageId (u32) takes 8 bytes to store
pub const PID_SIZE: usize = 4;
// 8 KB
pub const PAGE_SIZE: usize = 8192;
// 64 bit target
pub const USIZE_SIZE: usize = 8;

// just the number of items in the page
pub const PAGE_HEADER_SIZE: usize = 2;

pub type PageId = u32;
pub type BufferId = u32;
// offset within page, actually only 16 bits is enough, but use usize to avoid type casts when used as an index
pub type ItemPointer = usize;

// #[derive(Default)]
// pub type PageData = [u8; PAGE_SIZE];

pub const META_PID: PageId = 0;
// the maximum pgnum that is used by the db for its own purposes. For now, only page 0 is used as the
// header page. It means all other page numbers can be used.
pub const MAX_NON_DATA_PID: PageId = 0;

pub type Key = Vec<u8>;
pub type Value = Vec<u8>;

pub const N_BUSY_EVENTS: usize = 8; // number of condition variables used for waiting read completion

pub const METADATA_SIZE: usize = 4 * 4;


pub const MAX_VALUE_LEN: usize = PAGE_SIZE / 4; // assume that pages may fit at least 3 items
pub const MAX_KEY_LEN: usize = u8::MAX as usize; // should fit in one byte