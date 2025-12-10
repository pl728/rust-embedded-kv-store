# Rust Embedded KV Store

Log-structured key-value store with WAL support.

## Windows File Opening Issue

On Windows, reader files in `Db::new()` require `.write(true)` even for read-only operations, or you'll get:

```
Error: Os { code: 87, kind: InvalidInput, message: "The parameter is incorrect." }
```

**Fix:**

```rust
let mut data_read_file = OpenOptions::new()
    .write(true)  // Required on Windows
    .read(true)
    .create(true)
    .open(path)?;
```

This is needed when opening the same file multiple times for separate read/write handles.
