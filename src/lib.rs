use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, BufReader};

struct KvStore {
    file: File,
    reader: BufReader<File>,
    writer: BufWriter<File>,
}

impl KvStore {
    const PATH: &'static str = "data.log";

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

        Ok(Self { file, reader, writer })
    }
}