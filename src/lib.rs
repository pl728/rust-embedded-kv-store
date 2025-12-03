use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, BufReader, Write};

struct KvStore {
    reader: BufReader<File>,
    writer: BufWriter<File>,
}

impl KvStore {
    const PATH: &'static str = "data.log";

    const OP_PUT: u8 = 0;
    const OP_DELETE: u8 = 1;

    pub fn new() -> io::Result<Self> {
        let rfile = OpenOptions::new()
            .read(true)
            .create(true)
            .open(Self::PATH)?;
        
        let wfile = OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .open(Self::PATH)?;
        
        let reader = BufReader::new(rfile);
        let writer = BufWriter::new(wfile);

        Ok(Self { reader, writer })
    }

    pub fn put(&mut self, key: String, value: String) -> io::Result<()> {
        let op: u8 = Self::OP_PUT;
        let key_bytes = key.as_bytes();
        let val_bytes = value.as_bytes();

        let key_len = key_bytes.len() as u32;
        let key_len_bytes = key_len.to_le_bytes();

        let val_len = val_bytes.len() as u32;
        let val_len_bytes = val_len.to_le_bytes();

        self.writer.write_all(&[op])?;
        self.writer.write_all(&key_len_bytes)?;
        self.writer.write_all(&val_len_bytes)?;
        self.writer.write_all(key_bytes)?;
        self.writer.write_all(val_bytes)?;

        self.writer.flush()?;
        Ok(())
    }

    pub fn delete(&mut self, key: String) -> io::Result<()> {
        let op: u8 = Self::OP_DELETE;
        let key_bytes = key.as_bytes();
        
        let key_len = key_bytes.len() as u32;
        let key_len_bytes = key_len.to_le_bytes();

        let val_len_bytes = 0u32.to_le_bytes();

        self.writer.write_all(&[op])?;
        self.writer.write_all(&key_len_bytes)?;
        self.writer.write_all(&val_len_bytes)?;
        self.writer.write_all(key_bytes)?;

        self.writer.flush()?;

        Ok(())
    }
}