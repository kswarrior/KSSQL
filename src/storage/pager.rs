use std::path::Path;
use anyhow::Result;
use crc32fast::Hasher;
use std::sync::Arc;
use tokio::sync::{oneshot, mpsc};
use std::os::unix::fs::OpenOptionsExt;
use std::alloc::{alloc, dealloc, Layout};
use std::sync::atomic::{AtomicU64, Ordering};

pub const PAGE_SIZE: usize = 4096;

pub enum PagerRequest {
    Read { page_id: u32, resp: oneshot::Sender<Result<[u8; PAGE_SIZE]>> },
    Write { page_id: u32, data: Vec<u8>, resp: oneshot::Sender<Result<()>> },
    Sync { resp: oneshot::Sender<Result<()>> },
}

pub struct Pager {
    tx: mpsc::Sender<PagerRequest>,
    file_length: Arc<AtomicU64>,
}

impl Pager {
    pub async fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_owned = path.as_ref().to_owned();
        let (tx, mut rx) = mpsc::channel::<PagerRequest>(1024);
        let file_length = Arc::new(AtomicU64::new(0));
        let file_length_clone = Arc::clone(&file_length);

        std::thread::spawn(move || {
            tokio_uring::start(async move {
                let mut opts = std::fs::OpenOptions::new();
                opts.read(true).write(true).create(true);
                #[cfg(target_os = "linux")]
                opts.custom_flags(0x4000); // O_DIRECT
                
                let std_file = opts.open(&path_owned).expect("Failed to open pager file (O_DIRECT)");
                let metadata = std_file.metadata().expect("Failed to get metadata");
                file_length_clone.store(metadata.len(), Ordering::SeqCst);
                
                let file = tokio_uring::fs::File::from_std(std_file);

                while let Some(req) = rx.recv().await {
                    match req {
                        PagerRequest::Read { page_id, resp } => {
                            let offset = page_id as u64 * PAGE_SIZE as u64;
                            let buf = AlignedBuf::new(PAGE_SIZE);
                            let (res, buf_ret) = file.read_at(buf.into_inner(), offset).await;
                            
                            if let Ok(n) = res {
                                if n == PAGE_SIZE {
                                    let mut page = [0u8; PAGE_SIZE];
                                    page.copy_from_slice(&buf_ret);
                                    let _ = resp.send(Ok(page));
                                } else if n == 0 {
                                    let _ = resp.send(Ok([0u8; PAGE_SIZE]));
                                } else {
                                    let _ = resp.send(Err(anyhow::anyhow!("Short read")));
                                }
                            } else {
                                let _ = resp.send(Err(anyhow::anyhow!("Read failed: {:?}", res.err())));
                            }
                        }
                        PagerRequest::Write { page_id, data, resp } => {
                            let offset = page_id as u64 * PAGE_SIZE as u64;
                            let mut buf = AlignedBuf::new(PAGE_SIZE);
                            buf.as_mut_slice().copy_from_slice(&data);
                            let (res, _) = file.write_at(buf.into_inner(), offset).await;
                            let _ = resp.send(res.map(|_| ()).map_err(Into::into));
                        }
                        PagerRequest::Sync { resp } => {
                            let res = file.sync_all().await;
                            let _ = resp.send(res.map_err(Into::into));
                        }
                    }
                }
            });
        });

        Ok(Pager {
            tx,
            file_length,
        })
    }

    pub async fn read_page(&self, page_id: u32) -> Result<[u8; PAGE_SIZE]> {
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(PagerRequest::Read { page_id, resp: tx }).await;
        let page = rx.await??;
        
        let stored_checksum = u32::from_le_bytes(page[0..4].try_into()?);
        if stored_checksum != 0 {
            let mut hasher = Hasher::new();
            hasher.update(&page[4..]);
            let actual = hasher.finalize();
            if actual != stored_checksum {
                 return Err(anyhow::anyhow!("Page {} checksum mismatch", page_id));
            }
        }
        Ok(page)
    }

    pub async fn write_page(&self, page_id: u32, page_data: &[u8; PAGE_SIZE]) -> Result<()> {
        let mut page = *page_data;
        let mut hasher = Hasher::new();
        hasher.update(&page[4..]);
        let checksum = hasher.finalize();
        page[0..4].copy_from_slice(&checksum.to_le_bytes());

        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(PagerRequest::Write { page_id, data: page.to_vec(), resp: tx }).await;
        rx.await??;

        let offset = page_id as u64 * PAGE_SIZE as u64;
        let current_len = self.file_length.load(Ordering::SeqCst);
        if offset >= current_len {
            self.file_length.store(offset + PAGE_SIZE as u64, Ordering::SeqCst);
        }

        Ok(())
    }

    pub fn num_pages(&self) -> u32 {
        let len = self.file_length.load(Ordering::SeqCst);
        (len / PAGE_SIZE as u64) as u32
    }
    
    pub async fn sync(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(PagerRequest::Sync { resp: tx }).await;
        rx.await??;
        Ok(())
    }
}

struct AlignedBuf {
    ptr: *mut u8,
    layout: Layout,
    size: usize,
}

impl AlignedBuf {
    fn new(size: usize) -> Self {
        let layout = Layout::from_size_align(size, 4096).unwrap();
        let ptr = unsafe { alloc(layout) };
        Self { ptr, layout, size }
    }
    fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.size) }
    }
    fn into_inner(self) -> Vec<u8> {
        let v = unsafe { Vec::from_raw_parts(self.ptr, self.size, self.size) };
        std::mem::forget(self); 
        v
    }
}

impl Drop for AlignedBuf {
    fn drop(&mut self) {
        unsafe { dealloc(self.ptr, self.layout) };
    }
}
