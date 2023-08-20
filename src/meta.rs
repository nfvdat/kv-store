use crate::config::{PageId, PID_SIZE, METADATA_SIZE};
use crate::pagedata::PageData;

// #[derive(Default)]
pub struct Meta {
    pub freelist_pid: PageId,
}

#[derive(Copy, Clone)]
pub struct Metadata {
    pub free: PageId, // L1 list of free pages
    pub size: PageId, // size of database (pages)
    pub root: PageId, // B-Tree root page
    pub height: u32,  // height of B-Tree
}

impl Metadata {
    pub fn pack(self) -> [u8; METADATA_SIZE] {
        unsafe { std::mem::transmute::<Metadata, [u8; METADATA_SIZE]>(self) }
    }
    pub fn unpack(buf: &[u8]) -> Metadata {
        unsafe {
            std::mem::transmute::<[u8; METADATA_SIZE], Metadata>(
                buf[0..METADATA_SIZE].try_into().unwrap(),
            )
        }
    }
}

impl Meta {
    pub fn serialize(&self) -> PageData {
        let mut page = PageData::new();
        let mut pos: usize = 0;

        page.data[pos..pos + PID_SIZE].copy_from_slice(&self.freelist_pid.to_be_bytes());
        pos += PID_SIZE;

        page
    }

    pub fn deserialize(page: &PageData) -> Self {
        let mut pos: usize = 0;

        // unwrap is safe since this always returns Ok
        let freelist_pid = PageId::from_be_bytes(page.data[pos..pos + PID_SIZE].try_into().unwrap());
        pos += PID_SIZE;

        Self {
            freelist_pid
        }
    }
}
