use crate::config::{PageId, MAX_NON_DATA_PID, PID_SIZE, USIZE_SIZE};
use crate::pagedata::PageData;

// freelist manages the manages free and used pages.
pub struct FreeList {
    // max_pid holds the latest page num allocated. releasedPages holds all the ids that were released during
    // delete. New page ids are first given from the releasedPageIDs to avoid growing the file. If it's empty, then
    // maxPage is incremented and a new page is created thus increasing the file size.
    max_pid: PageId,
    released_pids: Vec<PageId>,
}

impl FreeList {
    // TODO: use default in some places?
    // pub fn new() -> Self {
    //     Self {
    //         max_pid: MAX_NON_DATA_PID,
    //         released_pids: Vec::new(),
    //     }
    // }

    // getNextPage returns page ids for writing New page ids are first given from the releasedPageIDs to avoid growing
    // the file. If it's empty, then maxPage is incremented and a new page is created thus increasing the file size.
    fn get_next_pid(&mut self) -> PageId {
        if let Some(pid) = self.released_pids.pop() {
            pid
        } else {
            self.max_pid += 1;
            self.max_pid
        }
    }

    fn release_pid(&mut self, pid: PageId) {
        self.released_pids.push(pid);
    }

    pub fn serialize(&self) -> PageData {
        let mut page = PageData::new();
        let mut pos: usize = 0;

        page.data[pos..pos + PID_SIZE].copy_from_slice(&self.max_pid.to_be_bytes());
        pos += PID_SIZE;

        page.data[pos..pos + USIZE_SIZE].copy_from_slice(&self.released_pids.len().to_be_bytes());
        pos += USIZE_SIZE;

        for pid in self.released_pids.iter() {
            page.data[pos..pos + PID_SIZE].copy_from_slice(&pid.to_be_bytes());
            pos += PID_SIZE;
        }

        page
    }

    pub fn deserialize(page: &PageData) -> Self {
        let mut pos: usize = 0;

        let max_pid = PageId::from_be_bytes(page.data[pos..pos + PID_SIZE].try_into().unwrap());
        pos += PID_SIZE;

        let released_pids_len =
            usize::from_be_bytes(page.data[pos..pos + USIZE_SIZE].try_into().unwrap());
        pos += USIZE_SIZE;

        let mut released_pids = Vec::new();
        for _ in 0..released_pids_len {
            released_pids.push(PageId::from_be_bytes(
                page.data[pos..pos + PID_SIZE].try_into().unwrap(),
            ));
            pos += PID_SIZE;
        }

        FreeList {
            max_pid,
            released_pids,
        }
    }
}
