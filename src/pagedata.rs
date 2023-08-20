use std::cmp::Ordering;

use crate::config::{PAGE_SIZE, PAGE_HEADER_SIZE, PageId, Key, Value, ItemPointer};

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

impl PageData {
    fn get_offs(&self, ip: ItemPointer) -> usize {
        self.get_u16(PAGE_HEADER_SIZE + ip * 2) as usize
    }

    fn set_offs(&mut self, ip: ItemPointer, offs: usize) {
        self.set_u16(PAGE_HEADER_SIZE + ip * 2, offs as u16)
    }

    pub fn get_child(&self, ip: ItemPointer) -> PageId {
        let offs = self.get_offs(ip);
        let key_len = self.data[offs] as usize;
        self.get_u32(offs + key_len + 1)
    }

    pub fn get_key(&self, ip: ItemPointer) -> Key {
        let offs = self.get_offs(ip);
        let key_len = self.data[offs] as usize;
        self.data[offs + 1..offs + 1 + key_len].to_vec()
    }

    pub fn get_last_key(&self) -> Key {
        let n_items = self.get_n_items();
        self.get_key(n_items - 1)
    }

    pub fn get_item(&self, ip: ItemPointer) -> (Key, Value) {
        let (item_offs, item_len) = self.get_item_offs_len(ip);
        let key_len = self.data[item_offs] as usize;
        (
            self.data[item_offs + 1..item_offs + 1 + key_len].to_vec(),
            self.data[item_offs + 1 + key_len..item_offs + item_len].to_vec(),
        )
    }

    fn get_item_offs_len(&self, ip: ItemPointer) -> (usize, usize) {
        let offs = self.get_offs(ip);
        let next_offs = if ip == 0 {
            PAGE_SIZE
        } else {
            self.get_offs(ip - 1)
        };
        debug_assert!(next_offs > offs);
        (offs, next_offs - offs)
    }

    pub fn set_u16(&mut self, offs: usize, data: u16) {
        self.copy(offs, &data.to_be_bytes());
    }

    pub fn set_u32(&mut self, offs: usize, data: u32) {
        self.copy(offs, &data.to_be_bytes());
    }

    pub fn get_u16(&self, offs: usize) -> u16 {
        u16::from_be_bytes(self.data[offs..offs + 2].try_into().unwrap())
    }

    pub fn get_u32(&self, offs: usize) -> u32 {
        u32::from_be_bytes(self.data[offs..offs + 4].try_into().unwrap())
    }

    pub fn get_n_items(&self) -> ItemPointer {
        self.get_u16(0) as ItemPointer
    }

    fn get_size(&self) -> ItemPointer {
        let n_items = self.get_n_items();
        if n_items == 0 {
            0
        } else {
            PAGE_SIZE - self.get_offs(n_items - 1)
        }
    }

    pub fn set_n_items(&mut self, n_items: ItemPointer) {
        self.set_u16(0, n_items as u16)
    }

    fn copy(&mut self, offs: usize, data: &[u8]) {
        let len = data.len();
        self.data[offs..offs + len].copy_from_slice(&data);
    }

    pub fn compare_key(&self, ip: ItemPointer, key: &Key) -> Ordering {
        let offs = self.get_offs(ip);
        let key_len = self.data[offs] as usize;
        if key_len == 0 {
            // special handling of +inf in right-most internal nodes
            Ordering::Less
        } else {
            key[..].cmp(&self.data[offs + 1..offs + 1 + key_len])
        }
    }

    pub fn remove_key(&mut self, ip: ItemPointer, leaf: bool) {
        let n_items = self.get_n_items();
        let size = self.get_size();
        let (item_offs, item_len) = self.get_item_offs_len(ip);
        for i in ip + 1..n_items {
            self.set_offs(i - 1, self.get_offs(i) + item_len);
        }
        let items_origin = PAGE_SIZE - size;
        if !leaf && n_items > 1 && ip + 1 == n_items {
            // If we are removing last child of internal page then copy it's key to the previous item
            let prev_item_offs = item_offs + item_len;
            let key_len = self.data[item_offs] as usize;
            let prev_key_len = self.data[prev_item_offs] as usize;
            let new_offs = prev_item_offs + prev_key_len - key_len;
            self.set_offs(ip - 1, new_offs);
            self.data
                .copy_within(item_offs..item_offs + prev_key_len + 1, new_offs);
        } else {
            self.data
                .copy_within(items_origin..item_offs, items_origin + item_len);
        }
        self.set_n_items(n_items - 1);
    }

    //
    // Insert item on the page is there is enough free space, otherwise return false
    //
    pub fn insert_item(&mut self, ip: ItemPointer, key: &Key, value: &[u8]) -> bool {
        let n_items = self.get_n_items();
        let size = self.get_size();
        let key_len = key.len();
        let item_len = 1 + key_len + value.len();
        if (n_items + 1) * 2 + size + item_len <= PAGE_SIZE - PAGE_HEADER_SIZE {
            // fit in page
            for i in (ip..n_items).rev() {
                self.set_offs(i + 1, self.get_offs(i) - item_len);
            }
            let item_offs = if ip != 0 {
                self.get_offs(ip - 1) - item_len
            } else {
                PAGE_SIZE - item_len
            };
            self.set_offs(ip, item_offs);
            let items_origin = PAGE_SIZE - size;
            self.data
                .copy_within(items_origin..item_offs + item_len, items_origin - item_len);
            self.data[item_offs] = key_len as u8;
            self.data[item_offs + 1..item_offs + 1 + key_len].copy_from_slice(&key);
            self.data[item_offs + 1 + key_len..item_offs + item_len].copy_from_slice(&value);
            self.set_n_items(n_items + 1);
            true
        } else {
            false
        }
    }

    //
    // Split page into two approximately equal parts. Smallest keys are moved to the new page,
    // largest - left on original page.
    // Returns split position
    //
    pub fn split(&mut self, new_page: &mut PageData, ip: ItemPointer) -> ItemPointer {
        let n_items = self.get_n_items();
        let size = self.get_size();
        let mut r = n_items;

        if ip == r {
            // Optimization for insert of sequential keys: move all data to new page,
            // leaving original page empty. It will cause complete filling of B-Tree pages.
            r -= 1;
        } else {
            // Divide page in two approximately equal parts.
            let margin = PAGE_SIZE - size / 2;
            let mut l: ItemPointer = 0;
            while l < r {
                let m = (l + r) >> 1;
                if self.get_offs(m) > margin {
                    // items are allocated from right to left
                    l = m + 1;
                } else {
                    r = m;
                }
            }
            debug_assert!(l == r);
        }
        // Move first r+1 elements to the new page
        let moved_size = PAGE_SIZE - self.get_offs(r);

        // copy item pointers
        new_page.data[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + (r + 1) * 2]
            .copy_from_slice(&self.data[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + (r + 1) * 2]);
        // copy items
        let dst = PAGE_SIZE - moved_size;
        new_page.data[dst..].copy_from_slice(&self.data[dst..]);

        // Adjust item pointers on old page
        for i in r + 1..n_items {
            self.set_offs(i - r - 1, self.get_offs(i) + moved_size);
        }
        let src = PAGE_SIZE - size;
        self.data.copy_within(src..dst, src + moved_size);
        new_page.set_n_items(r + 1);
        self.set_n_items(n_items - r - 1);
        r
    }
}