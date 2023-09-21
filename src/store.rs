use std::fs::{File, OpenOptions};
// use std::os::unix::fs::FileExt;
use std::path::Path;
use std::cmp::Ordering;
use fs2::FileExt;
use std::sync::{Condvar, Mutex, RwLock};
use std::os::unix::prelude::FileExt as UnixFileExt;
use crc32c::*;
use std::iter;

use anyhow::Result;

use crate::meta::Metadata;
use crate::buffer_manager::{BufferManager, PAGE_RAW, PAGE_BUSY, PAGE_WAIT, PAGE_DIRTY, PAGE_SYNCED, Buffer};
use crate::config::{N_BUSY_EVENTS, BufferId, PageId, PAGE_SIZE, METADATA_SIZE, Key, Value, ItemPointer, MAX_KEY_LEN, MAX_VALUE_LEN};
use crate::pagedata::PageData;
use crate::transaction::{TransactionStatus, Transaction};

#[derive(PartialEq)]
enum AccessMode {
    ReadOnly,
    WriteOnly,
}

struct PageGuard<'a> {
    buf: BufferId,
    pid: PageId,
    store: &'a Store,
}

impl<'a> Drop for PageGuard<'a> {
    fn drop(&mut self) {
        self.store.release_page(self.buf);
    }
}

#[derive(PartialEq, Copy, Clone, Debug)]
enum DatabaseState {
    InRecovery,
    Opened,
    Closed,
    Corrupted,
}
pub struct Database {
    pub meta: Metadata,           // cached metadata (stored in root page)
    meta_updated: bool,       // whether metadata was updated
    state: DatabaseState,     // database state
    wal_pos: u64,             // current position in log file
    tx_crc: u32,              // accumulated CRC of the current transaction
    tx_size: usize,           // current transaction size
}

#[derive(Copy, Clone, Debug)]
pub struct StoreConfig {
    /// Buffer pool (pages)
    pub cache_size: usize,
    /// Maximal size of WAL. When it is reached, database file is synced and WAL is rotated
    /// (write starts from the beginning)
    pub checkpoint_interval: u64,
    /// Threshold for flushing dirty pages to WAL (to reduce commit time)
    pub wal_flush_threshold: BufferId,
}

impl StoreConfig {
    pub fn default() -> StoreConfig {
        StoreConfig {
            cache_size: 128 * 1024,                         // 1Gb
            checkpoint_interval: 1u64 * 1024 * 1024 * 1024, // 1Gb
            wal_flush_threshold: BufferId::MAX,
        }
    }
}

pub struct Store {
    db: RwLock<Database>,
    buf_mgr: Mutex<BufferManager>,
    busy_events: [Condvar; N_BUSY_EVENTS],
    pool: Vec<RwLock<PageData>>,
    conf: StoreConfig,
    file: File,
    log: Option<File>,
}

//
// Storage internal methods implementations
//
impl Store {
    //
    // Unpin page (called by PageGuard)
    //
    fn release_page(&self, buf: BufferId) {
        let mut bm = self.buf_mgr.lock().unwrap();
        bm.release_buffer(buf);
    }

    //
    // Allocate new page in store and get buffer for it
    //
    fn new_page(&self, db: &mut Database) -> Result<PageGuard<'_>> {
        let free = db.meta.free;
        let buf;
        let mut bm = self.buf_mgr.lock().unwrap();
        if free != 0 {
            buf = bm.get_buffer(free)?;
            let mut page = self.pool[buf as usize].write().unwrap();
            if (bm.pages[buf as usize].state & PAGE_RAW) != 0 {
                self.file
                    .read_exact_at(&mut page.data, free as u64 * PAGE_SIZE as u64)?;
            }
            db.meta.free = page.get_u32(0);
            page.data.fill(0u8);
        } else {
            // extend store
            buf = bm.get_buffer(db.meta.size)?;
            db.meta.size += 1;
            let mut page = self.pool[buf as usize].write().unwrap();
            page.data.fill(0u8);
        }
        db.meta_updated = true;
        self.modify_buffer(db, &mut bm, buf)?;

        Ok(PageGuard {
            buf,
            pid: bm.pages[buf as usize].pid,
            store: &self,
        })
    }

    //
    // Read page in buffer and return PageGuard with pinned buffer.
    // Buffer will be automatically released on exiting from scope
    //
    fn get_page(&self, pid: PageId, mode: AccessMode) -> Result<PageGuard<'_>> {
        let mut bm = self.buf_mgr.lock().unwrap();
        let buf = bm.get_buffer(pid)?;
        if (bm.pages[buf as usize].state & PAGE_BUSY) != 0 {
            // Some other thread is loading buffer: just wait until it done
            bm.pages[buf as usize].state |= PAGE_WAIT;
            loop {
                debug_assert!((bm.pages[buf as usize].state & PAGE_WAIT) != 0);
                bm = self.busy_events[buf as usize % N_BUSY_EVENTS]
                    .wait(bm)
                    .unwrap();
                if (bm.pages[buf as usize].state & PAGE_BUSY) == 0 {
                    break;
                }
            }
        } else if (bm.pages[buf as usize].state & PAGE_RAW) != 0 {
            if mode != AccessMode::WriteOnly {
                // Read buffer if not in write-only mode
                bm.pages[buf as usize].state = PAGE_BUSY;
                drop(bm); // read page without holding lock
                {
                    let mut page = self.pool[buf as usize].write().unwrap();
                    self.file
                        .read_exact_at(&mut page.data, pid as u64 * PAGE_SIZE as u64)?;
                }
                bm = self.buf_mgr.lock().unwrap();
                if (bm.pages[buf as usize].state & PAGE_WAIT) != 0 {
                    // Somebody is waiting for us
                    self.busy_events[buf as usize % N_BUSY_EVENTS].notify_all();
                }
            }
            bm.pages[buf as usize].state = 0;
        }
        if mode != AccessMode::ReadOnly {
            bm.modify_buffer(buf, BufferId::MAX)?;
        }
        Ok(PageGuard {
            buf,
            pid,
            store: &self,
        })
    }

    //
    // Mark buffer as modified, pin it in memory and if it is needed,
    // write least recently modified page to WAL
    //
    fn modify_buffer(
        &self,
        db: &mut Database,
        bm: &mut BufferManager,
        buf: BufferId,
    ) -> Result<()> {
        if let Some((sync_buf, sync_pid)) = bm.modify_buffer(buf, self.conf.wal_flush_threshold)? {
            assert_eq!(bm.pages[sync_buf as usize].state, PAGE_DIRTY | PAGE_SYNCED);
            self.write_page_to_wal(db, sync_buf, sync_pid)?;
        }
        Ok(())
    }

    //
    // Mark page as dirty and pin it in-memory until end of transaction
    //
    fn modify_page(&self, db: &mut Database, buf: BufferId) -> Result<()> {
        let mut bm = self.buf_mgr.lock().unwrap();
        self.modify_buffer(db, &mut bm, buf)
    }

    pub fn start_transaction(&self) -> Transaction<'_> {
        Transaction {
            status: TransactionStatus::InProgress,
            store: self,
            db: self.db.write().unwrap(),
        }
    }

    fn write_page_to_wal(&self, db: &mut Database, buf: BufferId, pid: PageId) -> Result<()> {
        if let Some(log) = &self.log {
            let mut tx_buf = [0u8; PAGE_SIZE + 4];
            let page = self.pool[buf as usize].read().unwrap();
            tx_buf[0..4].copy_from_slice(&pid.to_be_bytes());
            tx_buf[4..].copy_from_slice(&page.data);
            db.tx_crc = crc32c_append(db.tx_crc, &tx_buf);
            log.write_all_at(&tx_buf, db.wal_pos)?;
            db.wal_pos += (4 + PAGE_SIZE) as u64;
            db.tx_size += 4 + PAGE_SIZE;
        }
        Ok(())
    }

    pub fn commit(&self, db: &mut Database) -> Result<()> {
        let mut bm = self.buf_mgr.lock().unwrap();

        if db.meta_updated {
            let meta = db.meta.pack();
            let mut page = self.pool[0].write().unwrap();
            page.data[0..METADATA_SIZE].copy_from_slice(&meta);
        }
        if let Some(log) = &self.log {
            // Write dirty pages to log file
            let mut dirty = bm.dirty_pages;
            while dirty != 0 && (bm.pages[dirty as usize].state & PAGE_SYNCED) == 0 {
                assert_eq!(bm.pages[dirty as usize].state, PAGE_DIRTY);
                self.write_page_to_wal(db, dirty, bm.pages[dirty as usize].pid)?;
                dirty = bm.pages[dirty as usize].next;
            }
            if bm.dirty_pages != 0 {
                let mut buf = [0u8; METADATA_SIZE + 8];
                {
                    let page = self.pool[0].read().unwrap();
                    buf[4..4 + METADATA_SIZE].copy_from_slice(&page.data[0..METADATA_SIZE]);
                }
                let crc = crc32c_append(db.tx_crc, &buf[..4 + METADATA_SIZE]);
                buf[4 + METADATA_SIZE..].copy_from_slice(&crc.to_be_bytes());
                log.write_all_at(&buf, db.wal_pos)?;
                db.wal_pos += (8 + METADATA_SIZE) as u64;
                log.sync_all()?;
                db.tx_crc = 0;
                db.tx_size = 0;

                // Write pages to the data file
                self.flush_buffers(&mut bm, db.meta_updated)?;

                if db.wal_pos >= self.conf.checkpoint_interval {
                    // Sync data file and restart from the beginning of WAL.
                    // So not truncate WAL to avoid file extension overhead.
                    self.file.sync_all()?;
                    db.wal_pos = 0;
                }
            }
        } else {
            // No WAL mode: just write dirty pages to the disk
            self.flush_buffers(&mut bm, db.meta_updated)?;
        }
        db.meta_updated = false;
        Ok(())
    }

    //
    // Flush dirty pages to the disk. Return true if database is changed.
    //
    fn flush_buffers(&self, bm: &mut BufferManager, save_meta: bool) -> Result<bool> {
        let mut dirty = bm.dirty_pages;
        if save_meta {
            assert!(dirty != 0); // if we changed meta, then we should change or create at least one page
            let page = self.pool[0].read().unwrap();
            self.file.write_all_at(&page.data, 0)?;
        }
        while dirty != 0 {
            let pid = bm.pages[dirty as usize].pid;
            let file_offs = pid as u64 * PAGE_SIZE as u64;
            let page = self.pool[dirty as usize].read().unwrap();
            let next = bm.pages[dirty as usize].next;
            self.file.write_all_at(&page.data, file_offs)?;
            debug_assert!((bm.pages[dirty as usize].state & PAGE_DIRTY) != 0);
            bm.pages[dirty as usize].state = 0;
            bm.unpin(dirty);
            dirty = next;
        }
        if bm.dirty_pages != 0 {
            bm.dirty_pages = 0;
            bm.dirtied = 0;
            bm.next_sync = 0;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    //
    // Rollback current transaction
    //
    pub fn rollback(&self, db: &mut Database) -> Result<()> {
        let mut bm = self.buf_mgr.lock().unwrap();
        let mut dirty = bm.dirty_pages;
        // Just throw away all dirty pages from buffer cache to force reloading of original pages
        while dirty != 0 {
            debug_assert!((bm.pages[dirty as usize].state & PAGE_DIRTY) != 0);
            debug_assert!(bm.pages[dirty as usize].access_count == 1);
            let next = bm.pages[dirty as usize].next;
            bm.throw_buffer(dirty);
            dirty = next;
        }
        bm.dirty_pages = 0;
        bm.dirtied = 0;
        bm.next_sync = 0;
        db.wal_pos -= db.tx_size as u64;
        db.tx_crc = 0;
        db.tx_size = 0;

        if db.meta_updated {
            // reread metadata from disk
            let mut page = self.pool[0].write().unwrap();
            self.file.read_exact_at(&mut page.data, 0)?;
            db.meta = Metadata::unpack(&page.data);
            db.meta_updated = false;
        }
        Ok(())
    }

    ///
    /// Open database store. If store file doesn't exist, then it is created.
    /// If path to transaction log is not specified, then WAL (write-ahead-log) is not used.
    /// It will significantly increase performance but can cause database corruption in case of power failure or system crash.
    ///
    pub fn open(db_path: &Path, log_path: Option<&Path>, conf: StoreConfig) -> Result<Store> {
        let mut buf = [0u8; PAGE_SIZE];
        let (file, meta) = if let Ok(file) = OpenOptions::new().write(true).read(true).open(db_path)
        {
            // open existed file
            file.try_lock_exclusive()?;
            file.read_exact_at(&mut buf, 0)?;
            let meta = Metadata::unpack(&buf);
            anyhow::ensure!(meta.size >= 1);
            (file, meta)
        } else {
            let file = OpenOptions::new()
                .write(true)
                .read(true)
                .create(true)
                .open(db_path)?;
            file.try_lock_exclusive()?;
            // create new file
            let meta = Metadata {
                free: 0,
                size: 1,
                root: 0,
                height: 0,
            };
            let metadata = meta.pack();
            buf[0..METADATA_SIZE].copy_from_slice(&metadata);
            file.write_all_at(&mut buf, 0)?;
            (file, meta)
        };
        let log = if let Some(path) = log_path {
            let log = OpenOptions::new()
                .write(true)
                .read(true)
                .create(true)
                .open(path)?;
            log.try_lock_exclusive()?;
            Some(log)
        } else {
            None
        };
        let store = Store {
            busy_events: [(); N_BUSY_EVENTS].map(|_| Condvar::new()),
            buf_mgr: Mutex::new(BufferManager {
                head: 0,
                tail: 0,
                free_pages: 0,
                dirty_pages: 0,
                next_sync: 0,
                used: 1, // pinned root page
                cached: 1,
                pinned: 1,
                dirtied: 0,
                hash_table: vec![0; conf.cache_size],
                pages: vec![Buffer::new(); conf.cache_size],
            }),
            pool: iter::repeat_with(|| RwLock::new(PageData::new()))
                .take(conf.cache_size)
                .collect(),
            file,
            log,
            conf,
            db: RwLock::new(Database {
                meta,
                meta_updated: false,
                state: DatabaseState::InRecovery,
                wal_pos: 0,
                tx_crc: 0,
                tx_size: 0,
            }),
        };
        store.recovery()?;
        Ok(store)
    }

    //
    // Recover database from WAL (if any)
    //
    fn recovery(&self) -> Result<()> {
        let mut db = self.db.write().unwrap();
        if let Some(log) = &self.log {
            let mut buf = [0u8; 4];
            let mut crc = 0u32;
            let mut wal_pos = 0u64;
            loop {
                let len = log.read_at(&mut buf, wal_pos)?;
                if len != 4 {
                    // end of log
                    break;
                }
                wal_pos += 4;
                let pid = PageId::from_be_bytes(buf);
                crc = crc32c_append(crc, &buf);
                if pid != 0 {
                    let pin = self.get_page(pid, AccessMode::WriteOnly)?;
                    let mut page = self.pool[pin.buf as usize].write().unwrap();
                    let len = log.read_at(&mut page.data, wal_pos)?;
                    if len != PAGE_SIZE {
                        break;
                    }
                    wal_pos += len as u64;
                    crc = crc32c_append(crc, &page.data);
                } else {
                    let mut meta_buf = [0u8; METADATA_SIZE];
                    let len = log.read_at(&mut meta_buf, wal_pos)?;
                    if len != METADATA_SIZE {
                        break;
                    }
                    wal_pos += len as u64;
                    crc = crc32c_append(crc, &meta_buf);
                    let len = log.read_at(&mut buf, wal_pos)?;
                    if len != 4 {
                        break;
                    }
                    wal_pos += 4;
                    if u32::from_be_bytes(buf) != crc {
                        // CRC mismatch
                        break;
                    }
                    {
                        let mut page = self.pool[0].write().unwrap();
                        page.data[0..METADATA_SIZE].copy_from_slice(&meta_buf);
                        db.meta_updated = true;
                    }
                    let mut bm = self.buf_mgr.lock().unwrap();
                    self.flush_buffers(&mut bm, true)?;
                    db.meta_updated = false;
                    crc = 0u32;
                }
            }
            self.rollback(&mut db)?;

            // reset WAL
            self.file.sync_all()?;
            db.wal_pos = 0;
            log.set_len(0)?; // truncate log
        }
        // reread metadata
        let mut page = self.pool[0].write().unwrap();
        self.file.read_exact_at(&mut page.data, 0)?;
        db.meta = Metadata::unpack(&page.data);

        db.state = DatabaseState::Opened;

        Ok(())
    }

    //
    // Allocate new B-Tree leaf page with single (key,value) element
    //
    fn btree_allocate_leaf_page(
        &self,
        db: &mut Database,
        key: &Key,
        value: &Value,
    ) -> Result<PageId> {
        let pin = self.new_page(db)?;
        let mut page = self.pool[pin.buf as usize].write().unwrap();
        page.set_n_items(0);
        page.insert_item(0, key, value);
        Ok(pin.pid)
    }

    //
    // Allocate new B-Tree internal page referencing two children
    //
    fn btree_allocate_internal_page(
        &self,
        db: &mut Database,
        key: &Key,
        left_child: PageId,
        right_child: PageId,
    ) -> Result<PageId> {
        let pin = self.new_page(db)?;
        let mut page = self.pool[pin.buf as usize].write().unwrap();
        page.set_n_items(0);
        debug_assert!(left_child != 0);
        debug_assert!(right_child != 0);
        page.insert_item(0, key, &left_child.to_be_bytes().to_vec());
        page.insert_item(1, &vec![], &right_child.to_be_bytes().to_vec());
        Ok(pin.pid)
    }

    //
    // Insert item at the specified position in B-Tree page.
    // If B-Tree pages is full then split it, evenly distribute items between pages: smaller items moved to new page, larger items left on original page.
    // Value of largest key on new page and its identifiers are returned in case of overflow.
    //
    fn btree_insert_in_page(
        &self,
        db: &mut Database,
        page: &mut PageData,
        ip: ItemPointer,
        key: &Key,
        value: &Value,
    ) -> Result<Option<(Key, PageId)>> {
        if !page.insert_item(ip, key, value) {
            // page is full then divide page
            let pin = self.new_page(db)?;
            let mut new_page = self.pool[pin.buf as usize].write().unwrap();
            let split = page.split(&mut new_page, ip);
            let ok = if ip > split {
                page.insert_item(ip - split - 1, key, value)
            } else {
                new_page.insert_item(ip, key, value)
            };
            anyhow::ensure!(ok);
            Ok(Some((new_page.get_last_key(), pin.pid)))
        } else {
            Ok(None)
        }
    }

    //
    // Remove key from B-Tree. Recursively traverse B-Tree and return true in case of underflow.
    // Right now we do not redistribute nodes between pages or merge pages, underflow is reported only if page becomes empty.
    // If key is not found, then nothing is performed and no error is reported.
    //
    fn btree_remove(&self, db: &mut Database, pid: PageId, key: &Key, height: u32) -> Result<bool> {
        let pin = self.get_page(pid, AccessMode::ReadOnly)?;
        let mut page = self.pool[pin.buf as usize].write().unwrap();
        let mut l: ItemPointer = 0;
        let n = page.get_n_items();
        let mut r = n;
        while l < r {
            let m = (l + r) >> 1;
            if page.compare_key(m, key) == Ordering::Greater {
                l = m + 1;
            } else {
                r = m;
            }
        }
        debug_assert!(l == r);
        if height == 1 {
            // leaf page
            if r < n && page.compare_key(r, key) == Ordering::Equal {
                self.modify_page(db, pin.buf)?;
                page.remove_key(r, true);
            }
        } else {
            // recurse to next level
            debug_assert!(r < n);
            let underflow = self.btree_remove(db, page.get_child(r), key, height - 1)?;
            if underflow {
                self.modify_page(db, pin.buf)?;
                page.remove_key(r, false);
            }
        }
        if page.get_n_items() == 0 {
            // free page
            page.set_u32(0, db.meta.free);
            db.meta.free = pid;
            db.meta_updated = true;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    //
    // Insert item in B-Tree. Recursively traverse B-Tree and return position of new page in case of overflow.
    //
    fn btree_insert(
        &self,
        db: &mut Database,
        pid: PageId,
        key: &Key,
        value: &Value,
        height: u32,
    ) -> Result<Option<(Key, PageId)>> {
        let pin = self.get_page(pid, AccessMode::ReadOnly)?;
        let mut page = self.pool[pin.buf as usize].write().unwrap();
        let mut l: ItemPointer = 0;
        let n = page.get_n_items();
        let mut r = n;
        while l < r {
            let m = (l + r) >> 1;
            if page.compare_key(m, key) == Ordering::Greater {
                l = m + 1;
            } else {
                r = m;
            }
        }
        debug_assert!(l == r);
        if height == 1 {
            // leaf page
            self.modify_page(db, pin.buf)?;
            if r < n && page.compare_key(r, key) == Ordering::Equal {
                // replace old value with new one: just remove old one and reinsert new key-value pair
                page.remove_key(r, true);
            }
            self.btree_insert_in_page(db, &mut page, r, key, value)
        } else {
            // recurse to next level
            debug_assert!(r < n);
            let overflow = self.btree_insert(db, page.get_child(r), key, value, height - 1)?;
            if let Some((key, child)) = overflow {
                // insert new page before original
                self.modify_page(db, pin.buf)?;
                debug_assert!(child != 0);
                self.btree_insert_in_page(db, &mut page, r, &key, &child.to_be_bytes().to_vec())
            } else {
                Ok(None)
            }
        }
    }

    //
    // Insert or update key in the store
    //
    pub fn do_upsert(&self, db: &mut Database, key: &Key, value: &Value) -> Result<()> {
        anyhow::ensure!(key.len() != 0 && key.len() <= MAX_KEY_LEN && value.len() <= MAX_VALUE_LEN);
        if db.meta.root == 0 {
            db.meta.root = self.btree_allocate_leaf_page(db, key, value)?;
            db.meta.height = 1;
            db.meta_updated = true;
        } else if let Some((key, page)) =
            self.btree_insert(db, db.meta.root, key, value, db.meta.height)?
        {
            // overflow
            db.meta.root = self.btree_allocate_internal_page(db, &key, page, db.meta.root)?;
            db.meta.height += 1;
            db.meta_updated = true;
        }
        Ok(())
    }

    //
    // Remove key from the store. Does nothing it key not exists.
    //
    pub fn do_remove(&self, db: &mut Database, key: &Key) -> Result<()> {
        if db.meta.root != 0 {
            let underflow = self.btree_remove(db, db.meta.root, key, db.meta.height)?;
            if underflow {
                db.meta.height = 0;
                db.meta.root = 0;
                db.meta_updated = true;
            }
        }
        Ok(())
    }

    pub fn traverse(&self, pid: PageId, prev_key: &mut Key, height: u32) -> Result<u64> {
        let pin = self.get_page(pid, AccessMode::ReadOnly)?;
        let page = self.pool[pin.buf as usize].read().unwrap();
        let n_items = page.get_n_items();
        let mut count = 0u64;
        if height == 1 {
            for i in 0..n_items {
                anyhow::ensure!(page.compare_key(i, prev_key) == Ordering::Less);
                *prev_key = page.get_key(i);
            }
            count += n_items as u64;
        } else {
            for i in 0..n_items {
                count += self.traverse(page.get_child(i), prev_key, height - 1)?;
                let ord = page.compare_key(i, prev_key);
                anyhow::ensure!(ord == Ordering::Less || ord == Ordering::Equal);
            }
        }
        Ok(count)
    }

    ///
    /// Close store. Close data and WAL files and truncate WAL file.
    ///
    pub fn close(&self) -> Result<()> {
        if let Ok(mut db) = self.db.write() {
            // avoid poisoned lock
            if db.state == DatabaseState::Opened {
                let mut delayed_commit = false;
                if let Ok(bm) = self.buf_mgr.lock() {
                    // avoid poisoned mutex
                    if bm.dirty_pages != 0 {
                        delayed_commit = true;
                    }
                }
                if delayed_commit {
                    self.commit(&mut db)?;
                }
                // Sync data file and truncate log in case of normal shutdown
                self.file.sync_all()?;
                if let Some(log) = &self.log {
                    log.set_len(0)?; // truncate WAL
                }
                db.state = DatabaseState::Closed;
            }
        }
        Ok(())
    }

    //
    // Locate greater or equal key.
    // Returns true and initializes path to this element if such key is found,
    // reset path and returns false otherwise.
    //
    pub fn find(&self, pid: PageId, key: &Key, height: u32) -> Result<Option<Value>> {
        let pin = self.get_page(pid, AccessMode::ReadOnly)?;
        let page = self.pool[pin.buf as usize].read().unwrap();
        let n = page.get_n_items();
        let mut l: ItemPointer = 0;
        let mut r = n;

        while l < r {
            let m = (l + r) >> 1;
            if page.compare_key(m, key) == Ordering::Greater {
                l = m + 1;
            } else {
                r = m;
            }
        }
        debug_assert!(l == r);
        if height == 1 {
            // leaf page
            if r < n {
                let item = page.get_item(r);
                if &item.0 == key {
                    Ok(Some(item.1))
                } else {
                    Ok(None)
                }
            } else {
                Ok(None)
            }
        } else {
            debug_assert!(r < n);
            while r < n {
                debug_assert!(page.get_child(r) != 0);
                if let Some(val) = self.find(page.get_child(r), key, height - 1)? {
                    return Ok(Some(val));
                }
                r += 1;
            }
            Ok(None)
        }
    }

    ///
    /// Shutdown store. Unlike close it does't commit delayed transactions, flush data file and truncatate WAL.
    ///
    pub fn shutdown(&self) -> Result<()> {
        let mut db = self.db.write().unwrap();
        anyhow::ensure!(db.state == DatabaseState::Opened);
        db.state = DatabaseState::Closed;
        Ok(())
    }

    pub fn get(&self, key: &Key) -> Result<Option<Value>> {
        let db = self.db.read().unwrap();
        self.find(db.meta.root, &key, db.meta.height)
    }
}

impl Drop for Store {
    fn drop(&mut self) {
        self.close().unwrap();
    }
}