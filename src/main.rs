use rust_embedded_kv_store::Db;
use std::io::{self};

fn main() -> io::Result<()> {
    // Start from a clean slate while experimenting
    let _ = std::fs::remove_file("data.log");
    let _ = std::fs::remove_file("wal.log");

    let mut db = Db::new()?;

    println!("get(foo) before set: {:?}", db.get(b"foo")?);

    // Create a transaction with a single set operation
    {
        let mut tx = db.begin_transaction();
        tx.set(b"foo", b"bar");
        tx.commit()?;
    }
    println!("get(foo) after set: {:?}", db.get(b"foo")?.map(String::from_utf8));

    // Overwrite with a new transaction
    {
        let mut tx = db.begin_transaction();
        tx.set(b"foo", b"baz");
        tx.commit()?;
    }
    println!("get(foo) after overwrite: {:?}", db.get(b"foo")?.map(String::from_utf8));

    // Transaction with multiple operations
    {
        let mut tx = db.begin_transaction();
        tx.set(b"key2", b"value2");
        tx.set(b"key3", b"value3");
        tx.commit()?;
    }
    println!("get(key2): {:?}", db.get(b"key2")?.map(String::from_utf8));
    println!("get(key3): {:?}", db.get(b"key3")?.map(String::from_utf8));

    // Delete operation
    {
        let mut tx = db.begin_transaction();
        tx.delete(b"foo");
        tx.commit()?;
    }
    println!("get(foo) after delete: {:?}", db.get(b"foo")?);

    // Reopen to verify index rebuilding from data log
    drop(db);
    let mut db2 = Db::new()?;
    println!("get(foo) after reopen: {:?}", db2.get(b"foo")?);
    println!("get(key2) after reopen: {:?}", db2.get(b"key2")?.map(String::from_utf8));
    println!("get(key3) after reopen: {:?}", db2.get(b"key3")?.map(String::from_utf8));

    Ok(())
}
