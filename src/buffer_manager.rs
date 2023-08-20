use anyhow::Result;

use crate::config::{BufferId, PageId};

// Flags for page state
pub const PAGE_RAW: u16 = 1; // buffer content is uninitialized
pub const PAGE_BUSY: u16 = 2; // buffer is loaded for the disk
pub const PAGE_DIRTY: u16 = 4; // buffer was updates
pub const PAGE_WAIT: u16 = 8; // some thread waits until buffer is loaded
pub const PAGE_SYNCED: u16 = 16; // dirty pages was saved to log

#[derive(Clone, Copy, Default)]
pub struct Buffer {
    pub pid: PageId,
    collision: BufferId, // collision chain
    // LRU l2-list
    pub next: BufferId,
    pub prev: BufferId,
    pub access_count: u16,
    pub state: u16, // bitmask of PAGE_RAW, PAGE_DIRTY, ...
}

impl Buffer {
    pub fn new() -> Buffer {
        Default::default()
    }
}

pub struct BufferManager {
    // LRU l2-list
    pub head: BufferId,
    pub tail: BufferId,

    pub free_pages: BufferId,  // L1-list of free pages
    pub dirty_pages: BufferId, // L2-list of dirty pages
    pub next_sync: BufferId,   // next page to be written to WAL

    pub used: BufferId,    // used part of page pool
    pub pinned: BufferId,  // amount of pinned pages
    pub dirtied: BufferId, // amount of dirty pages
    pub cached: BufferId,  // amount of cached pages

    pub hash_table: Vec<BufferId>, // array containing indexes of collision chains
    pub pages: Vec<Buffer>,    // page data
}

impl BufferManager {
    //
    // Link buffer to the head of LRU list (make it acceptable for eviction)
    //
    pub fn unpin(&mut self, id: BufferId) {
        debug_assert!(self.pages[id as usize].access_count == 1);
        self.pages[id as usize].access_count = 0;
        self.pages[id as usize].next = self.head;
        self.pages[id as usize].prev = 0;
        self.pinned -= 1;
        if self.head != 0 {
            self.pages[self.head as usize].prev = id;
        } else {
            self.tail = id;
        }
        self.head = id;
    }

    //
    // Unlink buffer from LRU list and so pin it in memory (protect from eviction)
    //
    fn pin(&mut self, id: BufferId) {
        debug_assert!(self.pages[id as usize].access_count == 0);
        let next = self.pages[id as usize].next;
        let prev = self.pages[id as usize].prev;
        if prev == 0 {
            self.head = next;
        } else {
            self.pages[prev as usize].next = next;
        }
        if next == 0 {
            self.tail = prev;
        } else {
            self.pages[next as usize].prev = prev;
        }
        self.pinned += 1;
    }

    //
    // Insert page in hash table
    //
    fn insert(&mut self, id: BufferId) {
        let h = self.pages[id as usize].pid as usize % self.hash_table.len();
        self.pages[id as usize].collision = self.hash_table[h];
        self.hash_table[h] = id;
    }

    //
    // Remove page from hash table
    //
    fn remove(&mut self, id: BufferId) {
        let h = self.pages[id as usize].pid as usize % self.hash_table.len();
        let mut p = self.hash_table[h];
        if p == id {
            self.hash_table[h] = self.pages[id as usize].collision;
        } else {
            while self.pages[p as usize].collision != id {
                p = self.pages[p as usize].collision;
            }
            self.pages[p as usize].collision = self.pages[id as usize].collision;
        }
    }

    //
    // Throw away buffer from cache (used by transaction rollback)
    //
    pub fn throw_buffer(&mut self, id: BufferId) {
        self.remove(id);
        self.pages[id as usize].next = self.free_pages;
        self.free_pages = id;
        self.cached -= 1;
    }

    //
    // If buffer is not yet marked as dirty then mark it as dirty and pin until the end of transaction
    //
    pub fn modify_buffer(
        &mut self,
        id: BufferId,
        wal_flush_threshold: BufferId,
    ) -> Result<Option<(BufferId, PageId)>> {
        debug_assert!(self.pages[id as usize].access_count > 0);
        let mut next_sync: Option<(BufferId, PageId)> = None;
        if (self.pages[id as usize].state & PAGE_DIRTY) == 0 {
            self.pages[id as usize].access_count += 1; // pin dirty page in memory
            self.pages[id as usize].state = PAGE_DIRTY;
            self.dirtied += 1;
            if self.dirtied > wal_flush_threshold {
                let mut sync = self.next_sync;
                while sync != 0 {
                    assert_eq!(self.pages[sync as usize].state, PAGE_DIRTY);
                    if self.pages[sync as usize].access_count == 1 {
                        self.pages[sync as usize].state |= PAGE_SYNCED;
                        self.next_sync = self.pages[sync as usize].prev;
                        let pid = self.pages[sync as usize].pid;
                        next_sync = Some((sync, pid));
                        break;
                    }
                    sync = self.pages[sync as usize].prev;
                }
            }
        } else {
            // we have to write page to the log once again
            self.pages[id as usize].state &= !PAGE_SYNCED;

            let prev = self.pages[id as usize].prev;

            // Move page to the beginning of L2 list
            if prev == 0 {
                // already first page: do nothing
                return Ok(None);
            }

            // If this page was scheduled for flush, then use previous page instead
            if self.next_sync == id {
                self.next_sync = prev;
            }

            // unlink page
            let next = self.pages[id as usize].next;
            self.pages[prev as usize].next = next;
            if next != 0 {
                self.pages[next as usize].prev = prev;
            }
        }
        // link to the beginning of dirty list
        if self.dirty_pages != 0 {
            self.pages[self.dirty_pages as usize].prev = id;
        }
        if self.next_sync == 0 {
            self.next_sync = id;
        }
        self.pages[id as usize].next = self.dirty_pages;
        self.pages[id as usize].prev = 0;
        self.dirty_pages = id;
        Ok(next_sync)
    }

    //
    // Decrement buffer's access counter and release buffer if it is last reference
    //
    pub fn release_buffer(&mut self, id: BufferId) {
        debug_assert!(self.pages[id as usize].access_count > 0);
        if self.pages[id as usize].access_count == 1 {
            debug_assert!((self.pages[id as usize].state & PAGE_DIRTY) == 0);
            self.unpin(id);
        } else {
            self.pages[id as usize].access_count -= 1;
        }
    }

    //
    // Find buffer with specified page or allocate new buffer
    //
    pub fn get_buffer(&mut self, pid: PageId) -> Result<BufferId> {
        let hash = pid as usize % self.hash_table.len();
        let mut h = self.hash_table[hash];
        while h != 0 {
            if self.pages[h as usize].pid == pid {
                let access_count = self.pages[h as usize].access_count;
                debug_assert!(access_count < u16::MAX - 1);
                if access_count == 0 {
                    self.pin(h);
                }
                self.pages[h as usize].access_count = access_count + 1;
                return Ok(h);
            }
            h = self.pages[h as usize].collision;
        }
        // page not found in cache
        h = self.free_pages;
        if h != 0 {
            // has some free pages
            self.free_pages = self.pages[h as usize].next;
            self.cached += 1;
            self.pinned += 1;
        } else {
            h = self.used;
            if (h as usize) < self.hash_table.len() {
                self.used += 1;
                self.cached += 1;
                self.pinned += 1;
            } else {
                // Replace least recently used page
                let victim = self.tail;
                anyhow::ensure!(victim != 0);
                debug_assert!(self.pages[victim as usize].access_count == 0);
                debug_assert!((self.pages[victim as usize].state & PAGE_DIRTY) == 0);
                self.pin(victim);
                self.remove(victim);
                h = victim;
            }
        }
        self.pages[h as usize].access_count = 1;
        self.pages[h as usize].pid = pid;
        self.pages[h as usize].state = PAGE_RAW;
        self.insert(h);
        Ok(h)
    }
}