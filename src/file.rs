use std::{os::fd::AsRawFd, path::Path};

pub struct Metadata {
    pub len: u64,
}

pub trait File: Sized {
    type LockedFile: LockedFile<File = Self>;

    fn create<P: AsRef<Path>>(path: P) -> Result<Self, std::io::Error>;
    fn open<P: AsRef<Path>>(path: P) -> Result<Self, std::io::Error>;
    fn metadata(&self) -> Result<Metadata, std::io::Error>;
    fn set_len(&self, len: u64) -> Result<(), std::io::Error>;
    fn sync_data(&self) -> Result<(), std::io::Error>;
    fn fsync(&self) -> Result<(), std::io::Error>;
}

impl File for std::fs::File {
    type LockedFile = crate::file_lock::LockedFile;

    fn create<P: AsRef<Path>>(path: P) -> Result<Self, std::io::Error> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        Ok(file)
    }

    fn open<P: AsRef<Path>>(path: P) -> Result<Self, std::io::Error> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)?;
        Ok(file)
    }

    fn metadata(&self) -> Result<Metadata, std::io::Error> {
        let m = self.metadata()?;

        Ok(Metadata { len: m.len() })
    }

    fn set_len(&self, len: u64) -> Result<(), std::io::Error> {
        self.set_len(len)
    }

    fn sync_data(&self) -> Result<(), std::io::Error> {
        self.sync_data()
    }

    fn fsync(&self) -> Result<(), std::io::Error> {
        #[cfg(target_os = "macos")]
        {
            let code = unsafe { libc::fcntl(self.as_raw_fd(), libc::F_BARRIERFSYNC) };
            if code == -1 {
                return Err(std::io::Error::last_os_error().into());
            }
        }

        // Currently not implemented on other platforms

        Ok(())
    }
}

#[derive(Debug)]
#[non_exhaustive]
pub enum LockedFileError {
    DatabaseAlreadyOpen,
    Io(std::io::Error),
}

impl From<std::io::Error> for LockedFileError {
    fn from(value: std::io::Error) -> Self {
        LockedFileError::Io(value)
    }
}

impl std::fmt::Display for LockedFileError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl std::error::Error for LockedFileError {}

pub trait LockedFile: Sized {
    type File: File;

    fn new(file: Self::File) -> Result<Self, LockedFileError>;
    fn file(&self) -> &Self::File;
    fn read(&self, offset: u64, len: usize) -> Result<Vec<u8>, LockedFileError>;
    fn write(&self, offset: u64, data: &[u8]) -> Result<(), LockedFileError>;
}

/// In memory representation.
pub struct MemoryFile {
    data: std::sync::Mutex<Vec<u8>>,
}

impl File for MemoryFile {
    type LockedFile = MemoryLockFile;

    fn create<P: AsRef<Path>>(path: P) -> Result<Self, std::io::Error> {
        // TODO: store/ref path?
        Ok(MemoryFile {
            data: Default::default(),
        })
    }

    fn open<P: AsRef<Path>>(path: P) -> Result<Self, std::io::Error> {
        // TODO: store/ref path?
        Ok(MemoryFile {
            data: Default::default(),
        })
    }

    fn metadata(&self) -> Result<Metadata, std::io::Error> {
        Ok(Metadata {
            len: self.data.lock().unwrap().len() as _,
        })
    }

    fn set_len(&self, len: u64) -> Result<(), std::io::Error> {
        self.data
            .lock()
            .unwrap()
            .resize(usize::try_from(len).unwrap(), 0u8);
        Ok(())
    }
    fn sync_data(&self) -> Result<(), std::io::Error> {
        Ok(())
    }
    fn fsync(&self) -> Result<(), std::io::Error> {
        Ok(())
    }
}

pub struct MemoryLockFile(MemoryFile);

impl LockedFile for MemoryLockFile {
    type File = MemoryFile;

    fn new(file: Self::File) -> Result<Self, LockedFileError> {
        Ok(Self(file.into()))
    }

    fn read(&self, offset: u64, len: usize) -> Result<Vec<u8>, LockedFileError> {
        let offset = usize::try_from(offset).unwrap();
        let data = self.0.data.lock().unwrap();
        Ok(data[offset..offset + len].to_vec())
    }

    fn write(&self, offset: u64, new_data: &[u8]) -> Result<(), LockedFileError> {
        let offset = usize::try_from(offset).unwrap();
        let mut data = self.0.data.lock().unwrap();
        if offset + new_data.len() >= data.len() {
            data.resize(offset + new_data.len(), 0u8);
        }
        data[offset..offset + new_data.len()].copy_from_slice(new_data);
        Ok(())
    }

    fn file(&self) -> &Self::File {
        &self.0
    }
}
