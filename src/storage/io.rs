use std::path::{Path, PathBuf};
use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::{mpsc, oneshot};

#[async_trait]
pub trait AsyncIoBackend: Send + Sync {
    async fn read_at(&self, offset: u64, size: usize) -> Result<Vec<u8>>;
    async fn write_at(&self, offset: u64, data: Vec<u8>) -> Result<()>;
    async fn sync_all(&self) -> Result<()>;
    fn file_size(&self) -> u64;
}

pub enum IoRequest {
    Read { offset: u64, size: usize, resp: oneshot::Sender<Result<Vec<u8>>> },
    Write { offset: u64, data: Vec<u8>, resp: oneshot::Sender<Result<()>> },
    Sync { resp: oneshot::Sender<Result<()>> },
}

pub struct IoUringBackend {
    tx: mpsc::Sender<IoRequest>,
    size: std::sync::atomic::AtomicU64,
}

impl IoUringBackend {
    pub async fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_owned = path.as_ref().to_owned();
        let (tx, mut rx) = mpsc::channel::<IoRequest>(1024);
        let size = std::sync::atomic::AtomicU64::new(0);

        if path_owned.exists() {
            size.store(std::fs::metadata(&path_owned)?.len(), std::sync::atomic::Ordering::SeqCst);
        }

        #[cfg(target_os = "linux")]
        {
            std::thread::spawn(move || {
                tokio_uring::start(async move {
                    let mut opts = std::fs::OpenOptions::new();
                    opts.read(true).write(true).create(true);
                    {
                        use std::os::unix::fs::OpenOptionsExt;
                        opts.custom_flags(libc::O_DIRECT);
                    }

                    let std_file = opts.open(&path_owned).expect("Failed to open file in io_uring thread");
                    let file = tokio_uring::fs::File::from_std(std_file);

                    while let Some(req) = rx.recv().await {
                        match req {
                            IoRequest::Read { offset, size, resp } => {
                                use crate::storage::pager::AlignedBuf;
                                let buf = AlignedBuf::new(size);
                                let (res, buf_ret) = file.read_at(buf, offset).await;
                                let _ = resp.send(res.map(|_| buf_ret.as_slice().to_vec()).map_err(Into::into));
                            }
                            IoRequest::Write { offset, data, resp } => {
                                use crate::storage::pager::AlignedBuf;
                                let mut buf = AlignedBuf::new(data.len());
                                buf.as_mut_slice().copy_from_slice(&data);
                                let (res, _) = file.write_at(buf, offset).await;
                                let _ = resp.send(res.map(|_| ()).map_err(Into::into));
                            }
                            IoRequest::Sync { resp } => {
                                let _ = resp.send(file.sync_all().await.map_err(Into::into));
                            }
                        }
                    }
                });
            });
        }

        #[cfg(not(target_os = "linux"))]
        {
             let _ = tx;
             let _ = rx;
             return Err(anyhow::anyhow!("io_uring is only available on Linux"));
        }

        #[cfg(target_os = "linux")]
        Ok(Self { tx, size })
    }
}

#[async_trait]
impl AsyncIoBackend for IoUringBackend {
    async fn read_at(&self, offset: u64, size: usize) -> Result<Vec<u8>> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(IoRequest::Read { offset, size, resp: tx }).await?;
        rx.await?
    }

    async fn write_at(&self, offset: u64, data: Vec<u8>) -> Result<()> {
        let data_len = data.len() as u64;
        let (tx, rx) = oneshot::channel();
        self.tx.send(IoRequest::Write { offset, data, resp: tx }).await?;
        let res = rx.await?;
        if res.is_ok() {
            let end = offset + data_len;
            let mut current = self.size.load(std::sync::atomic::Ordering::Relaxed);
            while end > current {
                match self.size.compare_exchange_weak(current, end, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::Relaxed) {
                    Ok(_) => break,
                    Err(actual) => current = actual,
                }
            }
        }
        res
    }

    async fn sync_all(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(IoRequest::Sync { resp: tx }).await?;
        rx.await?
    }

    fn file_size(&self) -> u64 {
        self.size.load(std::sync::atomic::Ordering::Relaxed)
    }
}

pub struct StdIoBackend {
    path: PathBuf,
    size: std::sync::atomic::AtomicU64,
}

impl StdIoBackend {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_buf = path.as_ref().to_path_buf();
        let size = if path_buf.exists() {
            std::fs::metadata(&path_buf)?.len()
        } else {
            std::fs::OpenOptions::new().write(true).create(true).open(&path_buf)?;
            0
        };
        Ok(Self {
            path: path_buf,
            size: std::sync::atomic::AtomicU64::new(size),
        })
    }
}

#[async_trait]
impl AsyncIoBackend for StdIoBackend {
    async fn read_at(&self, offset: u64, size: usize) -> Result<Vec<u8>> {
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || -> Result<Vec<u8>> {
            use std::io::{Read, Seek, SeekFrom};
            let mut file = std::fs::File::open(path)?;
            file.seek(SeekFrom::Start(offset))?;
            let mut buf = vec![0u8; size];
            file.read_exact(&mut buf)?;
            Ok(buf)
        }).await?
    }

    async fn write_at(&self, offset: u64, data: Vec<u8>) -> Result<()> {
        let path = self.path.clone();
        let data_len = data.len() as u64;
        tokio::task::spawn_blocking(move || -> Result<()> {
            use std::io::{Write, Seek, SeekFrom};
            let mut file = std::fs::OpenOptions::new().write(true).create(true).open(path)?;
            file.seek(SeekFrom::Start(offset))?;
            file.write_all(&data)?;
            Ok(())
        }).await??;

        let end = offset + data_len;
        let mut current = self.size.load(std::sync::atomic::Ordering::Relaxed);
        while end > current {
            match self.size.compare_exchange_weak(current, end, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::Relaxed) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
        Ok(())
    }

    async fn sync_all(&self) -> Result<()> {
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let file = std::fs::OpenOptions::new().write(true).open(path)?;
            file.sync_all()?;
            Ok(())
        }).await?
    }

    fn file_size(&self) -> u64 {
        self.size.load(std::sync::atomic::Ordering::Relaxed)
    }
}
