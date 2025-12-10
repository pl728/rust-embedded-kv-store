use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Result, BufWriter, BufReader, Write, Seek, Read, SeekFrom};

type Bytes = Vec<u8>;

const OP_BEGIN: u8 = 0;
const OP_PUT: u8 = 1;
const OP_DELETE: u8 = 2;
const OP_COMMIT: u8 = 3;

pub struct Db {
    wal_reader: BufReader<File>,
    wal_writer: BufWriter<File>,
    data_reader: BufReader<File>,
    data_writer: BufWriter<File>,
    index: BTreeMap<Bytes, u64>,
    data_writer_pos: u64,
}

impl Db {
    const DATA_PATH: &'static str = "./data.log";
    const WAL_PATH: &'static str = "./wal.log";

    pub fn new() -> Result<Self> {
        let mut wal_read_file = OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .open(Self::WAL_PATH)?;
        
        let mut wal_write_file = OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .open(Self::WAL_PATH)?;
        
        let mut data_read_file = OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .open(Self::DATA_PATH)?;
        
        let (index, data_writer_pos) = Self::build_index(&mut data_read_file)?;
        data_read_file.seek(SeekFrom::Start(0))?;
        
        let mut data_write_file = OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .open(Self::DATA_PATH)?;
        
        let wal_reader = BufReader::new(wal_read_file);
        let wal_writer = BufWriter::new(wal_write_file);
        let data_reader = BufReader::new(data_read_file);
        let data_writer = BufWriter::new(data_write_file);
        
        Ok(Self { wal_reader, wal_writer, data_reader, data_writer, index, data_writer_pos })
    }

    fn build_index(file: &mut File) -> io::Result<(BTreeMap<Bytes, u64>, u64)> {
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

            match op {
                OP_PUT => {
                    index.insert(key_buf, entry_start);
                },
                OP_DELETE => {
                    index.remove(&key_buf);
                },
                _ => {}
            }
        }

        Ok((index, offset))
    }

    fn commit(&mut self, ops: Vec<Ops>) -> Result<()> {
        // write to WAL (begin, set/delete, commit)
        // Write to DATA
        // update index
        self.append_begin()?;
        println!("Wrote OP_BEGIN to WAL");

        for op in &ops {
            // 
            match op {
                Ops::Set(k, v) => {
                    self.append_wal_set(k, v)?;
                }, 
                Ops::Delete(k) => {
                    self.append_wal_delete(k)?;
                }
            }
        }

        println!("Wrote OPS to WAL buffer");

        self.append_commit()?;
        println!("Wrote OP_COMMIT to WAL buffer");

        self.wal_writer.flush()?;
        self.wal_writer.get_ref().sync_all()?;

        println!("Synced buffer contents with disk");

        for op in ops {
            match op {
                Ops::Set(k, v) => {
                    self.append_data_set(k, v)?;
                }, 
                Ops::Delete(k) => {
                    self.append_data_delete(k)?;
                }
            }
        }

        Ok(())
    }

    fn append_begin(&mut self) -> Result<()> {
        self.wal_writer.write_all(&[OP_BEGIN])?;
        Ok(())
    }

    fn append_commit(&mut self) -> Result<()> {
        self.wal_writer.write_all(&[OP_COMMIT])?;
        Ok(())
    }

    fn append_wal_set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.wal_writer.write_all(&[OP_PUT])?;
        let klen = (key.len() as u32).to_le_bytes();
        let vlen = (value.len() as u32).to_le_bytes();
        self.wal_writer.write_all(&klen)?;
        self.wal_writer.write_all(&vlen)?;
        self.wal_writer.write_all(key)?;
        self.wal_writer.write_all(value)?;
        Ok(())
    }

    fn append_wal_delete(&mut self, key: &[u8]) -> Result<()> {
        self.wal_writer.write_all(&[OP_DELETE])?;
        let klen = (key.len() as u32).to_le_bytes();
        let vlen = 0u32.to_le_bytes();
        self.wal_writer.write_all(&klen)?;
        self.wal_writer.write_all(&vlen)?;
        self.wal_writer.write_all(key)?;
        Ok(())
    }

    fn append_data_set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let op: u8 = OP_PUT;

        let key_bytes = key.as_slice();
        let val_bytes = value.as_slice();

        let key_len = key_bytes.len() as u32;
        let key_len_bytes = key_len.to_le_bytes();

        let val_len = val_bytes.len() as u32;
        let val_len_bytes = val_len.to_le_bytes();

        self.data_writer.write_all(&[op])?;
        self.data_writer.write_all(&key_len_bytes)?;
        self.data_writer.write_all(&val_len_bytes)?;
        self.data_writer.write_all(key_bytes)?;
        self.data_writer.write_all(val_bytes)?;

        self.data_writer.flush()?;

        self.index.insert(key, self.data_writer_pos);
        self.data_writer_pos += 1 + 4 + 4 + key_len as u64 + val_len as u64;

        Ok(())
    }

    fn append_data_delete(&mut self, key: Vec<u8>) -> Result<()> {
        let op: u8 = OP_DELETE;
        let key_bytes = key.as_slice();

        let key_len = key_bytes.len() as u32;
        let key_len_bytes = key_len.to_le_bytes();

        let val_len_bytes = 0u32.to_le_bytes();

        self.data_writer.write_all(&[op])?;
        self.data_writer.write_all(&key_len_bytes)?;
        self.data_writer.write_all(&val_len_bytes)?;
        self.data_writer.write_all(key_bytes)?;

        self.data_writer.flush()?;

        self.index.remove(&key);
        self.data_writer_pos += 1 + 4 + 4 + key_len as u64;

        Ok(())
    }

    pub fn get<K>(&mut self, key: K) -> Result<Option<Bytes>>
    where
        K: AsRef<[u8]>,
    {
        let key_bytes = key.as_ref();
        let Some(&offset) = self.index.get(key_bytes) else {
            return Ok(None);
        };

        self.data_reader.seek(SeekFrom::Start(offset))?;

        let mut op_buf = [0u8; 1];
        self.data_reader.read_exact(&mut op_buf)?;
        let _op = op_buf[0];

        let mut len_buf = [0u8; 4];
        self.data_reader.read_exact(&mut len_buf)?;
        let key_len = u32::from_le_bytes(len_buf) as usize;
        self.data_reader.read_exact(&mut len_buf)?;
        let val_len = u32::from_le_bytes(len_buf) as usize;

        let mut key_buf = vec![0u8; key_len];
        let mut val_buf = vec![0u8; val_len];

        self.data_reader.read_exact(&mut key_buf)?;
        self.data_reader.read_exact(&mut val_buf)?;

        Ok(Some(val_buf))
    }

    pub fn begin_transaction(&mut self) -> Transaction<'_> {
        Transaction {
            db: self,
            operations: Vec::new()
        }
    }
}


enum Ops {
    Set(Bytes, Bytes), 
    Delete(Bytes)
}

pub struct Transaction<'db> {
    db: &'db mut Db,
    operations: Vec<Ops>, 
}

impl<'db> Transaction<'db> {
    pub fn set<K, V>(&mut self, key: K, value: V)
    where 
        K: AsRef<[u8]>, 
        V: AsRef<[u8]>,
    {
        let k = key.as_ref().to_vec();
        let v = value.as_ref().to_vec();
        self.operations.push(Ops::Set(k, v));
        println!("Added SET to Transaction OPS");
    }

    pub fn delete<K>(&mut self, key: K)
    where 
        K: AsRef<[u8]>, 
    {
        let k = key.as_ref().to_vec();
        self.operations.push(Ops::Delete(k));
        println!("Added DELETE to Transaction OPS");
    }

    pub fn commit(self) -> Result<()> {
        self.db.commit(self.operations)
    }
}