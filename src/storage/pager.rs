use std::fs::{File, OpenOptions};
use std::io::{Read, Write, Seek, SeekFrom};
use std::path::Path;
use anyhow::Result;

pub const PAGE_SIZE: usize = 4096;

pub struct Pager {
    file: File,
    file_length: u64,
}

impl Pager {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        let file_length = file.metadata()?.len();

        if file_length % PAGE_SIZE as u64 != 0 {
            return Err(anyhow::anyhow!("Database file is corrupt: length is not a multiple of page size"));
        }

        Ok(Pager {
            file,
            file_length,
        })
    }

    pub fn read_page(&mut self, page_id: u32) -> Result<[u8; PAGE_SIZE]> {
        let offset = page_id as u64 * PAGE_SIZE as u64;
        if offset >= self.file_length {
            return Err(anyhow::anyhow!("Page ID out of bounds"));
        }

        let mut page = [0u8; PAGE_SIZE];
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.read_exact(&mut page)?;
        Ok(page)
    }

    pub fn write_page(&mut self, page_id: u32, page: &[u8; PAGE_SIZE]) -> Result<()> {
        let offset = page_id as u64 * PAGE_SIZE as u64;
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(page)?;

        if offset >= self.file_length {
            self.file_length = offset + PAGE_SIZE as u64;
        }

        Ok(())
    }

    pub fn num_pages(&self) -> u32 {
        (self.file_length / PAGE_SIZE as u64) as u32
    }

    pub fn sync(&mut self) -> Result<()> {
        self.file.sync_all()?;
        Ok(())
    }
}
