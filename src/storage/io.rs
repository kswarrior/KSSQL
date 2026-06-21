use anyhow::Result;
use std::path::Path;
use async_trait::async_trait;

#[async_trait]
pub trait AsyncIoBackend: Send + Sync {
    async fn read_at(&self, offset: u64, size: usize) -> Result<Vec<u8>>;
    async fn write_at(&self, offset: u64, data: Vec<u8>) -> Result<()>;
    async fn sync_all(&self) -> Result<()>;
    fn file_size(&self) -> u64;
}

pub struct IoUringBackend {
    #[cfg(target_os = "linux")]
    path: std::path::PathBuf,
    size: std::sync::atomic::AtomicU64,
}

#[cfg(target_os = "linux")]
impl IoUringBackend {
    pub async fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_buf = path.as_ref().to_path_buf();
        let metadata = std_file_metadata(&path_buf)?;
        Ok(Self {
            path: path_buf,
            size: std::sync::atomic::AtomicU64::new(metadata.len()),
        })
    }
}

fn std_file_metadata(path: &Path) -> Result<std::fs::Metadata> {
    if !path.exists() {
        std::fs::OpenOptions::new().write(true).create(true).open(path)?;
    }
    Ok(std::fs::metadata(path)?)
}

#[cfg(target_os = "linux")]
#[async_trait]
impl AsyncIoBackend for IoUringBackend {
    async fn read_at(&self, offset: u64, size: usize) -> Result<Vec<u8>> {
        use crate::storage::pager::AlignedBuf;
        let path = self.path.clone();

        let res = tokio::task::spawn_blocking(move || {
            tokio_uring::start(async move {
                let mut opts = std::fs::OpenOptions::new();
                opts.read(true);
                {
                    use std::os::unix::fs::OpenOptionsExt;
                    opts.custom_flags(libc::O_DIRECT);
                }
                let std_file = opts.open(&path).expect("Failed to open");
                let file = tokio_uring::fs::File::from_std(std_file);
                let buf = AlignedBuf::new(size);
                let (res, buf_ret) = file.read_at(buf, offset).await;
                res.map(|_| buf_ret.as_slice().to_vec())
            })
        }).await?;

        res.map_err(Into::into)
    }

    async fn write_at(&self, offset: u64, data: Vec<u8>) -> Result<()> {
        let path = self.path.clone();
        let data_len = data.len() as u64;

        let res = tokio::task::spawn_blocking(move || {
            tokio_uring::start(async move {
                use crate::storage::pager::AlignedBuf;
                let mut opts = std::fs::OpenOptions::new();
                opts.write(true).create(true);
                {
                    use std::os::unix::fs::OpenOptionsExt;
                    opts.custom_flags(libc::O_DIRECT);
                }
                let std_file = opts.open(&path).expect("Failed to open");
                let file = tokio_uring::fs::File::from_std(std_file);
                let mut buf = AlignedBuf::new(data.len());
                buf.as_mut_slice().copy_from_slice(&data);
                let (res, _) = file.write_at(buf, offset).await;
                res
            })
        }).await?;

        if res.is_ok() {
            let end = offset + data_len;
            let current = self.size.load(std::sync::atomic::Ordering::Relaxed);
            if end > current {
                self.size.store(end, std::sync::atomic::Ordering::Relaxed);
            }
        }
        res.map(|_| ()).map_err(Into::into)
    }

    async fn sync_all(&self) -> Result<()> {
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || {
            tokio_uring::start(async move {
                let std_file = std::fs::OpenOptions::new().write(true).open(&path).expect("Failed to open");
                let file = tokio_uring::fs::File::from_std(std_file);
                file.sync_all().await
            })
        }).await?.map_err(Into::into)
    }

    fn file_size(&self) -> u64 {
        self.size.load(std::sync::atomic::Ordering::Relaxed)
    }
}

pub struct StdIoBackend {
    path: std::path::PathBuf,
    size: std::sync::atomic::AtomicU64,
}

impl StdIoBackend {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_buf = path.as_ref().to_path_buf();
        let metadata = std_file_metadata(&path_buf)?;
        Ok(Self {
            path: path_buf,
            size: std::sync::atomic::AtomicU64::new(metadata.len()),
        })
    }
}

#[async_trait]
impl AsyncIoBackend for StdIoBackend {
    async fn read_at(&self, offset: u64, size: usize) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || {
            let mut buf = vec![0u8; size];
            let file = std::fs::File::open(path)?;
            file.read_at(&mut buf, offset)?;
            Ok(buf)
        }).await?
    }

    async fn write_at(&self, offset: u64, data: Vec<u8>) -> Result<()> {
        use std::os::unix::fs::FileExt;
        let path = self.path.clone();
        let data_len = data.len() as u64;
        tokio::task::spawn_blocking(move || -> std::io::Result<()> {
            let file = std::fs::OpenOptions::new().write(true).create(true).open(path)?;
            file.write_at(&data, offset)?;
            Ok(())
        }).await??;

        let end = offset + data_len;
        let current = self.size.load(std::sync::atomic::Ordering::Relaxed);
        if end > current {
            self.size.store(end, std::sync::atomic::Ordering::Relaxed);
        }
        Ok(())
    }

    async fn sync_all(&self) -> Result<()> {
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || {
            let file = std::fs::OpenOptions::new().write(true).open(path)?;
            file.sync_all()?;
            Ok::<(), std::io::Error>(())
        }).await??;
        Ok(())
    }

    fn file_size(&self) -> u64 {
        self.size.load(std::sync::atomic::Ordering::Relaxed)
    }
}
