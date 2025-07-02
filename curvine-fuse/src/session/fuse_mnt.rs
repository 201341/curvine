// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(unused)]

use crate::raw::fuse_abi::fuse_args;
use crate::raw::{fuse_mount, fuse_unmount};
use crate::{FuseUtils, RawSession, FUSE_CLONE_FD_MIN_VERSION, UNIX_KERNEL_VERSION};
use log::{error, info};
use orpc::io::IOResult;
use orpc::sys;
use orpc::sys::pipe::{AsyncFd, BorrowedFd, OwnedFd};
use orpc::sys::{CString, RawIO};
use std::sync::{Arc, Mutex};

pub struct FuseMnt {
    pub(crate) path: CString,
    // OwnedFd is not used here, and in some cases fd cannot be turned off by rust.
    pub(crate) fd: RawIO,
    pub(crate) session: Option<RawSession>,
    pub(crate) clone_fds: Mutex<Vec<OwnedFd>>,
}

impl FuseMnt {
    pub fn new(path: &CString, args: *const fuse_args, auto_unmount: bool) -> Self {
        let (fd, session) = fuse_mount(path.as_ptr(), args).unwrap();
        sys::set_pipe_blocking(fd, false).unwrap();
        info!(
            "mount success, path {:?}, fd {}, auto_unmount {}",
            path, fd, auto_unmount
        );

        Self {
            path: path.clone(),
            fd,
            session,
            clone_fds: Mutex::new(vec![]),
        }
    }

    fn crate_task_fd(&self, clone: bool) -> IOResult<BorrowedFd> {
        let clone_fd = if clone && *UNIX_KERNEL_VERSION >= FUSE_CLONE_FD_MIN_VERSION {
            match FuseUtils::fuse_clone_fd(self.fd) {
                Ok(clone_fd) => {
                    info!("Fuse clone fd, {} -> {}", self.fd, clone_fd);
                    clone_fd
                }

                Err(e) => {
                    error!(
                        "clone fd failed, will fall back to shared fd mode; kernel version: {},\
                     source fd {}, cause: {}",
                        *UNIX_KERNEL_VERSION, self.fd, e
                    );
                    sys::dup(self.fd)?
                }
            }
        } else {
            sys::dup(self.fd)?
        };

        let new_fd = OwnedFd::new(clone_fd);
        new_fd.set_blocking(false)?;

        let borrowed = new_fd.as_borrowed();
        // fd is recycled by FuseMnt and saved here.
        self.clone_fds.lock().unwrap().push(new_fd);

        Ok(borrowed)
    }

    // Get 2 fds for asynchronous reading and writing data.
    pub fn create_async_task_fd(&self, clone: bool) -> IOResult<(Arc<AsyncFd>, Arc<AsyncFd>)> {
        let fd = self.crate_task_fd(clone)?;
        let fd = Arc::new(AsyncFd::new(fd)?);
        Ok((fd.clone(), fd))
    }
}

impl Drop for FuseMnt {
    fn drop(&mut self) {
        #[cfg(feature = "fuse2")]
        {
            if let Err(e) = sys::close(self.fd) {
                log::warn!("close fd {}: {}", self.fd, e)
            }
        }
        fuse_unmount(self);
        info!("unmount {:?}", self.path)
    }
}
