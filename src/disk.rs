use anyhow::Result;
use std::fs::File;
use std::fs::OpenOptions;
use std::os::unix::fs::FileExt;
use std::path::Path;

use crate::config::META_PID;
use crate::config::{PageId, PAGE_SIZE};
use crate::freelist::FreeList;
use crate::meta::Meta;

pub struct PageData {
    pub data: [u8; PAGE_SIZE],
}

impl PageData {
    pub fn new() -> PageData {
        PageData {
            data: [0u8; PAGE_SIZE],
        }
    }
}

struct DiskManager {
}

impl DiskManager {
    fn read_page(file: &File, pid: PageId) -> Result<PageData> {
        let mut page = PageData::new();
        file.read_exact_at(&mut page.data, PAGE_SIZE as u64 * pid)?;
        Ok(page)
    }

    fn write_page(file: &File, pid: PageId, page: &PageData) -> Result<()> {
        file.write_all_at(&page.data, PAGE_SIZE as u64 * pid)?;
        Ok(())
    }

    fn read_freelist(file: &File, freelist_pid: PageId) -> Result<FreeList> {
        let freelist_page = Self::read_page(file, freelist_pid)?;
        Ok(FreeList::deserialize(&freelist_page))
    }

    fn write_freelist(file: &File, freelist_pid: PageId, freelist: &FreeList) -> Result<()> {
        Self::write_page(file, freelist_pid, &freelist.serialize())?;
        Ok(())
    }

    fn read_meta(file: &File) -> Result<Meta> {
        let meta_page = Self::read_page(file, META_PID)?;
        Ok(Meta::deserialize(&meta_page))
    }

    fn write_meta(file: &File, meta: &Meta) -> Result<()> {
        Self::write_page(file, META_PID, &meta.serialize())?;
        Ok(())
    }
}
