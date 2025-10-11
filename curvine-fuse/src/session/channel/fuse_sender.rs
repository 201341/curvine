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

use crate::session::ResponseData;
use orpc::io::IOResult;
use orpc::sync::channel::AsyncReceiver;
use orpc::sys::pipe::{AsyncFd, Pipe2, PipeFd};
use orpc::{err_box, sys};
use std::sync::Arc;

pub struct FuseSender {
    kernel_fd: Arc<AsyncFd>,
    receiver: AsyncReceiver<ResponseData>,
    pipe2: Pipe2,
}

impl FuseSender {
    pub fn new(
        kernel_fd: Arc<AsyncFd>,
        receiver: AsyncReceiver<ResponseData>,
        buf_size: usize,
    ) -> IOResult<Self> {
        let pipe2 = Pipe2::new(PipeFd::new(buf_size, false, false)?)?;
        let fuse_rx = Self {
            kernel_fd,
            receiver,
            pipe2,
        };

        Ok(fuse_rx)
    }

    // Get 1 response data
    pub async fn recv(&mut self) -> Option<ResponseData> {
        self.receiver.recv().await
    }

    // Send response data to fuse.
    pub async fn send(&mut self, rep: ResponseData) -> IOResult<()> {
            self.splice(rep).await
    }

    pub async fn write(&mut self, rep: ResponseData) -> IOResult<()> {
        let (_, iovec) = rep.as_iovec()?;
        self.kernel_fd
            .async_write(|fd| sys::writev(fd.fd(), &iovec))
            .await?;
        Ok(())
    }

    async fn splice(&mut self, rep: ResponseData) -> IOResult<()> {
        let (len, iovec) = rep.as_iovec()?;
        let write_len = self.pipe2.write_iov(&iovec).await?;
        if write_len != len {
            return err_box!("io return value error, res: {}, expect: {}", write_len, len);
        }

        let read_len = self.pipe2.read_io(&self.kernel_fd, len).await?;
        if read_len != len {
            err_box!("io return value error, res: {}, expect: {}", read_len, len)
        } else {
            Ok(())
        }
    }
}
