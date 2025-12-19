use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Result, Error, BufWriter, BufReader, Write, Seek, Read, SeekFrom};

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
        let mut data_file = OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .open(Self::DATA_PATH)?;
        
        let mut wal_file = OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .open(Self::WAL_PATH)?;
        
        Self::process_wal(&mut wal_file, &mut data_file)?;
        data_file.seek(SeekFrom::Start(0))?;
        let (index, data_writer_pos) = Self::build_index(&mut data_file)?;

        let mut wal_read_file = OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .open(Self::WAL_PATH)?;
        
        let mut wal_write_file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(Self::WAL_PATH)?;
        
        let mut data_read_file = OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .open(Self::DATA_PATH)?;
                
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

    fn process_wal(wal: &mut File, data: &mut File) -> Result<()> {
        // read the entire wal file [OP][..][COMMIT]
        wal.seek(SeekFrom::Start(0))?;
        data.seek(SeekFrom::End(0))?;
        let mut in_txn = false;
        let mut txn: Vec<Op> = Vec::new();
        loop {
            let mut op_buf = [0u8; 1];
            match wal.read_exact(&mut op_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
            match op_buf[0] {
                OP_BEGIN => {
                    in_txn = true;
                    txn.clear();
                }, 
                OP_PUT => {
                    if !in_txn {
                        return Err(Error::new(io::ErrorKind::InvalidData, "PUT outside txn"));
                    }
                    let mut key_len = [0u8; 4];
                    if !read_exact_or_break(wal, &mut key_len)? { break; }
                    let klen = u32::from_le_bytes(key_len) as usize;

                    let mut val_len = [0u8; 4];
                    if !read_exact_or_break(wal, &mut val_len)? { break; }
                    let vlen = u32::from_le_bytes(val_len) as usize;

                    let mut key_buf = vec![0u8; klen];
                    let mut val_buf = vec![0u8; vlen];
                    if !read_exact_or_break(wal, &mut key_buf)? { break; }
                    if !read_exact_or_break(wal, &mut val_buf)? { break; }

                    txn.push(Op::Set(key_buf, val_buf));

                },
                OP_DELETE => {
                    if !in_txn {
                        return Err(Error::new(io::ErrorKind::InvalidData, "DELETE outside txn"));
                    }
                    let mut key_len = [0u8; 4];
                    if !read_exact_or_break(wal, &mut key_len)? { break; }
                    let klen = u32::from_le_bytes(key_len) as usize;

                    let mut val_len = [0u8; 4];
                    if !read_exact_or_break(wal, &mut val_len)? { break; }
                    let vlen = u32::from_le_bytes(val_len) as usize;
                    if vlen != 0 {
                        return Err(Error::new(io::ErrorKind::InvalidData, "DELETE vlen != 0"));
                    }

                    let mut key_buf = vec![0u8; klen];
                    if !read_exact_or_break(wal, &mut key_buf)? { break; }

                    txn.push(Op::Delete(key_buf));
                    
                },
                OP_COMMIT => {
                    if !in_txn {
                        return Err(Error::new(io::ErrorKind::InvalidData, "COMMIT outside txn"));
                    }
                    for t in txn.drain(..) {
                        match t {
                            Op::Set(key, value) => {
                                let op: u8 = OP_PUT;

                                let key_bytes = key.as_slice();
                                let val_bytes = value.as_slice();

                                let key_len = key_bytes.len() as u32;
                                let key_len_bytes = key_len.to_le_bytes();

                                let val_len = val_bytes.len() as u32;
                                let val_len_bytes = val_len.to_le_bytes();

                                data.write_all(&[op])?;
                                data.write_all(&key_len_bytes)?;
                                data.write_all(&val_len_bytes)?;
                                data.write_all(key_bytes)?;
                                data.write_all(val_bytes)?;
                            },
                            Op::Delete(key) => {
                                let op: u8 = OP_DELETE;
                                let key_bytes = key.as_slice();

                                let key_len = key_bytes.len() as u32;
                                let key_len_bytes = key_len.to_le_bytes();

                                let val_len_bytes = 0u32.to_le_bytes();

                                data.write_all(&[op])?;
                                data.write_all(&key_len_bytes)?;
                                data.write_all(&val_len_bytes)?;
                                data.write_all(key_bytes)?;
                            }
                        }
                        
                    }
                    data.flush()?;
                    data.sync_all()?;
                    in_txn = false;
                },
                other => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("unknown opcode: {other}"),
                    ));
                }
            }
        }
        wal.set_len(0)?;
        wal.seek(SeekFrom::Start(0))?;
        wal.sync_all()?;

        Ok(())
    }

    fn build_index(file: &mut File) -> io::Result<(BTreeMap<Bytes, u64>, u64)> {
        let mut index = BTreeMap::new();
        let mut offset: u64 = 0;

        loop {
            let entry_start = offset;
            let mut op_buf = [0u8; 1];
            if !read_exact_or_break(file, &mut op_buf)? { break; }
            let op = op_buf[0];
            offset += 1;
            
            let mut len_buf = [0u8; 4];
            if !read_exact_or_break(file, &mut len_buf)? { break; }
            let key_len = u32::from_le_bytes(len_buf) as u64;
            offset += 4;

            if !read_exact_or_break(file, &mut len_buf)? { break; }
            let val_len = u32::from_le_bytes(len_buf) as u64;
            offset += 4;

            let mut key_buf = vec![0u8; key_len as usize];
            if !read_exact_or_break(file, &mut key_buf)? { break; }
            offset += key_len;

            let mut val_buf = vec![0u8; val_len as usize];
            if val_len > 0 {
                if !read_exact_or_break(file, &mut val_buf)? { break; }
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

    fn commit(&mut self, ops: Vec<Op>) -> Result<()> {
        // write to WAL (begin, set/delete, commit)
        // Write to DATA
        // update index
        self.append_begin()?;
        println!("Wrote OP_BEGIN to WAL");

        for op in &ops {
            // 
            match op {
                Op::Set(k, v) => {
                    self.append_wal_set(k, v)?;
                }, 
                Op::Delete(k) => {
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
                Op::Set(k, v) => {
                    self.append_data_set(k, v)?;
                }, 
                Op::Delete(k) => {
                    self.append_data_delete(k)?;
                }
            }
        }

        self.data_writer.flush()?;
        self.data_writer.get_ref().sync_all()?;

        self.clear_wal()?;

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

    fn clear_wal(&mut self) -> io::Result<()> {
        self.wal_writer.flush()?;
        let f = self.wal_writer.get_mut();
        f.set_len(0)?;
        f.seek(SeekFrom::Start(0))?;
        f.sync_all()?;
        Ok(())
    }

    pub fn get<K>(&mut self, key: K) -> Result<Option<Bytes>>
    where K: AsRef<[u8]>,
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


enum Op {
    Set(Bytes, Bytes), 
    Delete(Bytes)
}

pub struct Transaction<'db> {
    db: &'db mut Db,
    operations: Vec<Op>, 
}

impl<'db> Transaction<'db> {
    pub fn set<K, V>(&mut self, key: K, value: V)
    where 
        K: AsRef<[u8]>, 
        V: AsRef<[u8]>,
    {
        let k = key.as_ref().to_vec();
        let v = value.as_ref().to_vec();
        self.operations.push(Op::Set(k, v));
        println!("Added SET to Transaction OPS");
    }

    pub fn delete<K>(&mut self, key: K)
    where 
        K: AsRef<[u8]>, 
    {
        let k = key.as_ref().to_vec();
        self.operations.push(Op::Delete(k));
        println!("Added DELETE to Transaction OPS");
    }

    pub fn commit(self) -> Result<()> {
        self.db.commit(self.operations)
    }
}

fn read_exact_or_break(file: &mut File, buf: &mut [u8]) -> io::Result<bool> {
    match file.read_exact(buf) {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => Ok(false),
        Err(e) => Err(e),
    }
}