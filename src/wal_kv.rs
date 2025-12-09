use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Result, BufWriter, BufReader, Write, Seek, Read, SeekFrom};

const OP_BEGIN: u8 = 0;
const OP_PUT: u8 = 1;
const OP_DELETE: u8 = 2;
const OP_COMMIT: u8 = 3;

pub struct Db {
    wal_reader: BufReader<File>,
    wal_writer: BufWriter<File>,
    data_reader: BufReader<File>,
    data_writer: BufWriter<File>,
    index: BTreeMap<String, u64>,
    data_writer_pos: u64,
}

impl Db {
    const DATA_PATH: &'static str = "./data.log";
    const WAL_PATH: &'static str = "./wal.log";

    pub fn new() -> Result<Self> {
        let mut wal_read_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(Self::WAL_PATH)?;
        
        let mut wal_write_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(Self::WAL_PATH)?;
        
        let mut data_read_file = OpenOptions::new()
            .write(true)
            .append(true)
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

    fn build_index(file: &mut File) -> Result<(BTreeMap<String, u64>, u64)> {
        todo!
    }

    pub fn commit(&mut self, tx: Transaction) {
        // write to WAL
        // Write to DATA
        // update index
    }

    fn append_begin() -> Result<()> {
        todo!
    }

    fn append_set() -> Result<()> {
        todo!
    }

    fn append_delete() -> Result<()> {
        todo!
    }

    fn append_commit() -> Result<()> {
        todo!
    }
}

type Bytes = Vec<u8>;

enum Ops {
    Set(Bytes, Bytes), 
    Delete(Bytes)
}

pub struct Transaction<> {
    operations: Vec<Ops>, 
}

impl<'db> Transaction<'db> {
    pub fn new() -> Self {
        Self { operations: Vec::new() }
    }

    pub fn set(&mut self, key: K, value: V) -> Result<()>
    where 
        K: AsRef<[u8]>, 
        V: AsRef<[u8]>,
    {
        let k = key.as_ref().to_vec();
        let v = value.as_ref().to_vec();
        self.operations.push(Ops::Set(k, v));
    }

    pub fn delete(&mut self, key: K) -> Result<()>
    where 
        K: AsRef<[u8]>, 
    {
        let k = key.as_ref().to_vec();
        self.operations.push(Ops::Delete(k));
    }
}