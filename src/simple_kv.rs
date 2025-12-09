use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, BufReader, Write, Seek, Read, SeekFrom};
use std::collections::BTreeMap;

pub struct KvStore {
    reader: BufReader<File>,
    writer: BufWriter<File>,
    index: BTreeMap<String, u64>,
    writer_pos: u64,
}

impl KvStore {
    const PATH: &'static str = "./data.log";

    const OP_PUT: u8 = 0;
    const OP_DELETE: u8 = 1;

    pub fn new() -> io::Result<Self> {
        let mut rfile = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(Self::PATH)?;

        let (index, writer_pos) = Self::build_index(&mut rfile)?;

        rfile.seek(SeekFrom::Start(0))?;
        
        let wfile = OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .open(Self::PATH)?;
        
        let reader = BufReader::new(rfile);
        let writer = BufWriter::new(wfile);

        Ok(Self { reader, writer, index, writer_pos })
    }

    fn build_index(file: &mut File) -> io::Result<(BTreeMap<String, u64>, u64)> {
        let mut index = BTreeMap::new();
        let mut offset: u64 = 0;

        loop {
            let entry_start = offset;
            let mut op_buf = [0u8; 1];
            match file.read_exact(&mut op_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
            let op = op_buf[0];
            offset += 1;
            
            let mut len_buf = [0u8; 4];
            file.read_exact(&mut len_buf)?;
            let key_len = u32::from_le_bytes(len_buf) as u64;
            offset += 4;

            file.read_exact(&mut len_buf)?;
            let val_len = u32::from_le_bytes(len_buf) as u64;
            offset += 4;

            let mut key_buf = vec![0u8; key_len as usize];
            file.read_exact(&mut key_buf)?;
            offset += key_len;

            let mut val_buf = vec![0u8; val_len as usize];
            if val_len > 0 {
                file.read_exact(&mut val_buf)?;
            }
            offset += val_len;

            let key = String::from_utf8(key_buf).unwrap();

            match op {
                Self::OP_PUT => {
                    index.insert(key, entry_start);
                },
                Self::OP_DELETE => {
                    index.remove(&key);
                },
                _ => {}
            }
        }

        Ok((index, offset))
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

        self.index.insert(key, self.writer_pos);
        self.writer_pos += 1 + 4 + 4 + key_len as u64 + val_len as u64;

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

        self.index.remove(&key);
        self.writer_pos += 1 + 4 + 4 + key_len as u64;

        Ok(())
    }

    pub fn get(&mut self, key: &str) -> io::Result<Option<String>> {
        let Some(&offset) = self.index.get(key) else {
            return Ok(None);
        };

        self.reader.seek(SeekFrom::Start(offset))?;

        let mut op_buf = [0u8; 1];
        self.reader.read_exact(&mut op_buf)?;
        let _op = op_buf[0];

        let mut len_buf = [0u8; 4];
        self.reader.read_exact(&mut len_buf)?;
        let key_len = u32::from_le_bytes(len_buf) as usize;
        self.reader.read_exact(&mut len_buf)?;
        let val_len = u32::from_le_bytes(len_buf) as usize;

        let mut key_buf = vec![0u8; key_len];
        let mut val_buf = vec![0u8; val_len];

        self.reader.read_exact(&mut key_buf)?;
        self.reader.read_exact(&mut val_buf)?;

        let val = String::from_utf8(val_buf).unwrap();

        Ok(Some(val))
    }
}