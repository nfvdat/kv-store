// PageId (u64) takes 8 bytes to store
pub const PID_SIZE: usize = 8;
// 8 KB
pub const PAGE_SIZE: usize = 8192;
// 64 bit target
pub const USIZE_SIZE: usize = 8;

pub type PageId = u64;

// #[derive(Default)]
// pub type PageData = [u8; PAGE_SIZE];

pub const META_PID: PageId = 0;
// the maximum pgnum that is used by the db for its own purposes. For now, only page 0 is used as the
// header page. It means all other page numbers can be used.
pub const MAX_NON_DATA_PID: PageId = 0;