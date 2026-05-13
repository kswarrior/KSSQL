use std::fs::{File, OpenOptions};
use std::io::{Write, Seek, SeekFrom, Read};
use std::path::Path;
use anyhow::Result;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug)]
pub enum WalEntry {
    PageUpdate { page_id: u32, data: Vec<u8> },
}

pub struct Wal {
    file: File,
}

impl Wal {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(path)?;

        Ok(Wal { file })
    }

    pub fn log_page_update(&mut self, page_id: u32, data: &[u8]) -> Result<()> {
        let entry = WalEntry::PageUpdate {
            page_id,
            data: data.to_vec(),
        };
        let encoded = bincode::serialize(&entry)?;
        let len = encoded.len() as u32;

        self.file.write_all(&len.to_le_bytes())?;
        self.file.write_all(&encoded)?;
        self.file.flush()?;
        self.file.sync_all()?;
        Ok(())
    }

    pub fn read_all(&mut self) -> Result<Vec<WalEntry>> {
        self.file.seek(SeekFrom::Start(0))?;
        let mut entries = Vec::new();
        let mut len_buf = [0u8; 4];

        while self.file.read_exact(&mut len_buf).is_ok() {
            let len = u32::from_le_bytes(len_buf);
            let mut encoded = vec![0u8; len as usize];
            self.file.read_exact(&mut encoded)?;
            let entry: WalEntry = bincode::deserialize(&encoded)?;
            entries.push(entry);
        }
        Ok(entries)
    }

    pub fn clear(&mut self) -> Result<()> {
        self.file.set_len(0)?;
        self.file.seek(SeekFrom::Start(0))?;
        self.file.sync_all()?;
        Ok(())
    }
}
