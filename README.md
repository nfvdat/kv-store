A simple persistent embedded key-value store built in Rust.

- Design: B-Trees, page cache, write-ahead log.
- Supports ACID transactions with multiple-readers-single-writer concurrency protocol.
- Simple `get/put/remove` interface. Iterators are WIP.

#### Example Usage

```
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
```
