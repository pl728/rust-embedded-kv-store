use rust_embedded_kv_store::KvStore;
use std::io::{self};

fn main() -> io::Result<()> {
    // Start from a clean slate while experimenting
    let _ = std::fs::remove_file("data.log");

    let mut store = KvStore::new()?;

    println!("get(foo) before put: {:?}", store.get("foo")?);

    store.put("foo".to_string(), "bar".to_string())?;
    println!("get(foo) after put: {:?}", store.get("foo")?);

    store.put("foo".to_string(), "baz".to_string())?;
    println!("get(foo) after overwrite: {:?}", store.get("foo")?);

    store.put("key2".to_string(), "value2".to_string())?;
    println!("get(key2): {:?}", store.get("key2")?);

    store.delete("foo".to_string())?;
    println!("get(foo) after delete: {:?}", store.get("foo")?);

    // Reopen to verify index rebuilding from log
    drop(store);
    let mut store2 = KvStore::new()?;
    println!("get(foo) after reopen: {:?}", store2.get("foo")?);
    println!("get(key2) after reopen: {:?}", store2.get("key2")?);

    Ok(())
}
