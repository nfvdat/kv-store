use anyhow::{bail, Result};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::convert::TryInto;
use std::iter;
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use skv::{Store, StoreConfig};
use skv::Transaction;
use skv::{Key, Value};

const RAND_SEED: u64 = 2021;
const N_RECORDS_LARGE: usize = 1000000;
const N_RECORDS_SMALL: usize = 10000;

#[test]
fn test_basic_ops() {
    let store = open_store("test1.db", Some("test1.log"));
    {
        let mut trans = store.start_transaction();
        trans.put(&v(b"1"), &v(b"one")).unwrap();
        trans.put(&v(b"2"), &v(b"two")).unwrap();
        trans.put(&v(b"3"), &v(b"three")).unwrap();
        trans.put(&v(b"4"), &v(b"four")).unwrap();
        trans.put(&v(b"5"), &v(b"five")).unwrap();
        assert_eq!(trans.get(&v(b"1")).unwrap().unwrap(), v(b"one"));
        trans.commit().unwrap();
    }
    assert_eq!(store.get(&v(b"1")).unwrap().unwrap(), v(b"one"));
    {
        let mut trans = store.start_transaction();
        trans.put(&v(b"2"), &v(b"two-two")).unwrap();
        trans.commit().unwrap();
    }
    assert_eq!(store.get(&v(b"1")).unwrap().unwrap(), v(b"one"));
    assert_eq!(store.get(&v(b"2")).unwrap().unwrap(), v(b"two-two"));
    assert_eq!(store.get(&v(b"3")).unwrap().unwrap(), v(b"three"));
    assert_eq!(store.get(&v(b"4")).unwrap().unwrap(), v(b"four"));
    assert_eq!(store.get(&v(b"5")).unwrap().unwrap(), v(b"five"));

    {
        let mut trans = store.start_transaction();
        trans.remove(&v(b"3")).unwrap();
        trans.commit().unwrap();
    }

    assert_eq!(store.get(&v(b"1")).unwrap().unwrap(), v(b"one"));
    assert_eq!(store.get(&v(b"2")).unwrap().unwrap(), v(b"two-two"));
    assert_eq!(store.get(&v(b"3")).unwrap(), None);
    assert_eq!(store.get(&v(b"4")).unwrap().unwrap(), v(b"four"));
}

fn open_store(data_file: &str, log_file: Option<&str>) -> Store {
    let data_path = Path::new(data_file);
    let log_path = log_file.map(|wal| Path::new(wal));
    let _ = std::fs::remove_file(&data_path);
    if let Some(log) = log_path {
        let _ = std::fs::remove_file(&log);
    }
    Store::open(data_path, log_path, StoreConfig::default()).unwrap()
}

fn v(b: &[u8]) -> Key {
    b.to_vec()
}