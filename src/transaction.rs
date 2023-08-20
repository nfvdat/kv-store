use anyhow::Result;
use std::sync::RwLockWriteGuard;

use crate::{store::{Store, Database}, config::{Key, Value}};

///
/// Status of transaction
///
#[derive(PartialEq)]
pub enum TransactionStatus {
    InProgress,
    Committed,
    Aborted,
}

///
/// Explicitly started transaction. Storage can be updated in autocommit mode
/// or using explicitly started transaction.
///
pub struct Transaction<'a> {
    pub status: TransactionStatus,
    pub store: &'a Store,
    pub db: RwLockWriteGuard<'a, Database>,
}

impl<'a> Transaction<'_> {
    ///
    /// Commit transaction
    ///
    pub fn commit(&mut self) -> Result<()> {
        anyhow::ensure!(self.status == TransactionStatus::InProgress);
        self.store.commit(&mut self.db)?;
        self.status = TransactionStatus::Committed;
        Ok(())
    }

    ///
    /// Delay commit of transaction
    ///
    pub fn delay(&mut self) -> Result<()> {
        anyhow::ensure!(self.status == TransactionStatus::InProgress);
        // mark transaction as committed to prevent implicit rollback by destructor
        self.status = TransactionStatus::Committed;
        Ok(())
    }

    ///
    /// Rollback transaction undoing all changes
    ///
    pub fn rollback(&mut self) -> Result<()> {
        anyhow::ensure!(self.status == TransactionStatus::InProgress);
        self.store.rollback(&mut self.db)?;
        self.status = TransactionStatus::Aborted;
        Ok(())
    }

    ///
    /// Lookup key in the storage.
    ///
    pub fn get(&self, key: &Key) -> Result<Option<Value>> {
        anyhow::ensure!(self.status == TransactionStatus::InProgress);
        self.store.find(self.db.meta.root, &key, self.db.meta.height)
    }

    ///
    /// Insert new key in the storage or update existed key as part of this transaction.
    ///
    pub fn put(&mut self, key: &Key, value: &Value) -> Result<()> {
        anyhow::ensure!(self.status == TransactionStatus::InProgress);
        self.store.do_upsert(&mut self.db, key, value)?;
        Ok(())
    }

    ///
    /// Remove key from storage as part of this transaction.
    /// Does nothing if key not exist.
    ///
    pub fn remove(&mut self, key: &Key) -> Result<()> {
        anyhow::ensure!(self.status == TransactionStatus::InProgress);
        self.store.do_remove(&mut self.db, key)?;
        Ok(())
    }

    ///
    /// Traverse B-Tree, check B-Tree invariants and return total number of keys in B-Tree
    ///
    pub fn verify(&self) -> Result<u64> {
        anyhow::ensure!(self.status == TransactionStatus::InProgress);
        if self.db.meta.root != 0 {
            let mut prev_key = Vec::new();
            self.store
                .traverse(self.db.meta.root, &mut prev_key, self.db.meta.height)
        } else {
            Ok(0)
        }
    }
}

impl<'a> Drop for Transaction<'a> {
    fn drop(&mut self) {
        if self.status == TransactionStatus::InProgress {
            self.store.rollback(&mut self.db).unwrap();
        }
    }
}
