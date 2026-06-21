use std::path::Path;
use anyhow::Result;
use crc32fast::Hasher;
use std::sync::Arc;
use tokio::sync::{oneshot, mpsc};
use std::alloc::{alloc, dealloc, Layout};
use crate::storage::io::{AsyncIoBackend};

pub const PAGE_SIZE: usize = 4096;

pub enum PagerRequest {
    Read { page_id: u64, resp: oneshot::Sender<Result<[u8; PAGE_SIZE]>> },
    Write { page_id: u64, data: [u8; PAGE_SIZE], resp: oneshot::Sender<Result<()>> },
    Sync { resp: oneshot::Sender<Result<()>> },
    Reload { path: std::path::PathBuf, resp: oneshot::Sender<Result<()>> },
}

pub struct Pager {
    tx: mpsc::Sender<PagerRequest>,
    backend: Arc<dyn AsyncIoBackend>,
}

impl Pager {
    pub async fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_owned = path.as_ref().to_owned();

        // Portability Abstract Layer (PAL): Select backend based on OS
        #[cfg(target_os = "linux")]
        let backend: Arc<dyn AsyncIoBackend> = Arc::new(crate::storage::io::IoUringBackend::open(&path_owned).await?);
        #[cfg(not(target_os = "linux"))]
        let backend: Arc<dyn AsyncIoBackend> = Arc::new(crate::storage::io::StdIoBackend::open(&path_owned)?);

        let (tx, mut rx) = mpsc::channel::<PagerRequest>(1024);
        let backend_clone = Arc::clone(&backend);

        tokio::spawn(async move {
            let mut current_backend = backend_clone;
            while let Some(req) = rx.recv().await {
                match req {
                    PagerRequest::Read { page_id, resp } => {
                        let offset = page_id * PAGE_SIZE as u64;
                        let res = current_backend.read_at(offset, PAGE_SIZE).await;
                        match res {
                            Ok(bytes) => {
                                let mut page = [0u8; PAGE_SIZE];
                                page.copy_from_slice(&bytes);
                                let _ = resp.send(Ok(page));
                            }
                            Err(e) => {
                                let _ = resp.send(Err(e));
                            }
                        }
                    }
                    PagerRequest::Write { page_id, data, resp } => {
                        let offset = page_id * PAGE_SIZE as u64;
                        let _ = resp.send(current_backend.write_at(offset, data.to_vec()).await);
                    }
                    PagerRequest::Sync { resp } => {
                        let _ = resp.send(current_backend.sync_all().await);
                    }
                    PagerRequest::Reload { path, resp } => {
                        #[cfg(target_os = "linux")]
                        let new_backend_res = crate::storage::io::IoUringBackend::open(&path).await.map(|b| Arc::new(b) as Arc<dyn AsyncIoBackend>);
                        #[cfg(not(target_os = "linux"))]
                        let new_backend_res = crate::storage::io::StdIoBackend::open(&path).map(|b| Arc::new(b) as Arc<dyn AsyncIoBackend>);

                        match new_backend_res {
                            Ok(b) => {
                                current_backend = b;
                                let _ = resp.send(Ok(()));
                            }
                            Err(e) => { let _ = resp.send(Err(e)); }
                        }
                    }
                }
            }
        });

        Ok(Pager {
            tx,
            backend,
        })
    }

    pub async fn read_page(&self, page_id: u64) -> Result<[u8; PAGE_SIZE]> {
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

    pub async fn write_page(&self, page_id: u64, page_data: &[u8; PAGE_SIZE]) -> Result<()> {
        let mut page = *page_data;
        let mut hasher = Hasher::new();
        hasher.update(&page[4..]);
        let checksum = hasher.finalize();
        page[0..4].copy_from_slice(&checksum.to_le_bytes());

        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(PagerRequest::Write { page_id, data: page, resp: tx }).await;
        rx.await??;
        Ok(())
    }

    pub fn num_pages(&self) -> u64 {
        let len = self.backend.file_size();
        len / PAGE_SIZE as u64
    }
    
    pub async fn sync(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(PagerRequest::Sync { resp: tx }).await;
        rx.await??;
        Ok(())
    }

    pub async fn reload(&self, path: &Path) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(PagerRequest::Reload { path: path.to_owned(), resp: tx }).await;
        rx.await??;
        Ok(())
    }
}

pub struct AlignedBuf {
    ptr: *mut u8,
    layout: Layout,
    size: usize,
}

impl AlignedBuf {
    pub fn new(size: usize) -> Self {
        let layout = Layout::from_size_align(size, 4096).unwrap();
        let ptr = unsafe { alloc(layout) };
        Self { ptr, layout, size }
    }
    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.size) }
    }
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.size) }
    }
}

unsafe impl tokio_uring::buf::IoBuf for AlignedBuf {
    fn stable_ptr(&self) -> *const u8 { self.ptr }
    fn bytes_init(&self) -> usize { self.size }
    fn bytes_total(&self) -> usize { self.size }
}

unsafe impl tokio_uring::buf::IoBufMut for AlignedBuf {
    fn stable_mut_ptr(&mut self) -> *mut u8 { self.ptr }
    unsafe fn set_init(&mut self, _pos: usize) { }
}

impl Drop for AlignedBuf {
    fn drop(&mut self) {
        unsafe { dealloc(self.ptr, self.layout) };
    }
}
