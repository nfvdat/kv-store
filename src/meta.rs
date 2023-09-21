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
    pub fn unpack(page: &[u8]) -> Metadata {
        // unsafe {
        //     std::mem::transmute::<[u8; METADATA_SIZE], Metadata>(
        //         buf[0..METADATA_SIZE].try_into().unwrap(),
        //     )
        // }

        let mut pos: usize = 0;

        // unwrap is safe since this always returns Ok
        let free = PageId::from_be_bytes(page[pos..pos + PID_SIZE].try_into().unwrap());
        pos += PID_SIZE;

        let size = PageId::from_be_bytes(page[pos..pos + PID_SIZE].try_into().unwrap());
        pos += PID_SIZE;

        let root = PageId::from_be_bytes(page[pos..pos + PID_SIZE].try_into().unwrap());
        pos += PID_SIZE;

        // u32
        let height = PageId::from_be_bytes(page[pos..pos + 4].try_into().unwrap());
        pos += 4;

        Self {
            free,
            size,
            root,
            height,
        }
    }

    pub fn pack(self) -> [u8; METADATA_SIZE] {
        // unsafe { std::mem::transmute::<Metadata, [u8; METADATA_SIZE]>(self) }
        let mut page = [0u8; METADATA_SIZE];
        let mut pos: usize = 0;

        page[pos..pos + PID_SIZE].copy_from_slice(&self.free.to_be_bytes());
        pos += PID_SIZE;

        page[pos..pos + PID_SIZE].copy_from_slice(&self.size.to_be_bytes());
        pos += PID_SIZE;

        page[pos..pos + PID_SIZE].copy_from_slice(&self.root.to_be_bytes());
        pos += PID_SIZE;

        // u32
        page[pos..pos + 4].copy_from_slice(&self.height.to_be_bytes());
        pos += 4;

        page
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
