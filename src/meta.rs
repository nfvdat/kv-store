use crate::config::{PageId, PID_SIZE};
use crate::disk::PageData;

// #[derive(Default)]
pub struct Meta {
    pub freelist_pid: PageId,
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
