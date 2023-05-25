use crate::file::LockedFileError;
use std::fs::File;
use std::io;
use std::os::unix::fs::FileExt;
use std::os::unix::io::AsRawFd;

pub struct LockedFile {
    file: File,
}

impl crate::file::LockedFile for LockedFile {
    type File = std::fs::File;

    fn new(file: Self::File) -> Result<Self, LockedFileError> {
        let fd = file.as_raw_fd();
        let result = unsafe { libc::flock(fd, libc::LOCK_EX | libc::LOCK_NB) };
        if result != 0 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::WouldBlock {
                Err(LockedFileError::DatabaseAlreadyOpen)
            } else {
                Err(LockedFileError::Io(err))
            }
        } else {
            Ok(Self { file })
        }
    }

    fn file(&self) -> &Self::File {
        &self.file
    }

    fn read(&self, offset: u64, len: usize) -> Result<Vec<u8>, LockedFileError> {
        let mut buffer = vec![0; len];
        self.file.read_exact_at(&mut buffer, offset)?;
        Ok(buffer)
    }

    fn write(&self, offset: u64, data: &[u8]) -> Result<(), LockedFileError> {
        self.file.write_all_at(data, offset)?;
        Ok(())
    }
}

impl Drop for LockedFile {
    fn drop(&mut self) {
        unsafe { libc::flock(self.file.as_raw_fd(), libc::LOCK_UN) };
    }
}
