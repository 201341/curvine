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

use curvine_common::conf::FuseConf;
use log::{error, info};
use nix::unistd::{getgid, getuid};
use orpc::err_io;
use orpc::io::IOResult;
use orpc::sys::RawIO;
use std::ffi::CString;
use std::fs::File;
use std::io::ErrorKind;
use std::path::Path;

use std::os::unix::fs::PermissionsExt;
use std::process::{Command, Stdio};

use orpc::sys::close_raw_io;
use orpc::sys::open;

const FUSERMOUNT_BIN: &str = "fusermount";
const FUSERMOUNT3_BIN: &str = "fusermount3";

pub fn fuse_mount_pure(mnt: &Path, conf: &FuseConf) -> IOResult<RawIO> {
    if conf.auto_umount() {
        // TODO: handle auto umount
    }
    let res = fuse_mount_sys(mnt, conf);
    match res {
        Ok(fd) => Ok(fd),
        Err(e) => {
            error!("fuse mount sys failed; path {:?}, err {:?}", mnt, e);
            err_io!(-1)
        }
    }
}

#[cfg(target_os = "linux")]
fn fuse_mount_sys(mnt: &Path, conf: &FuseConf) -> IOResult<RawIO> {
    let fuse_device_name = "/dev/fuse";
    let mountpoint_mode = File::open(mnt)?.metadata()?.permissions().mode();

    // Auto unmount requests must be sent to fusermount binary
    let path = CString::new(fuse_device_name).unwrap();
    let res = open(&path, libc::O_RDWR | libc::O_CLOEXEC);
    let fd = match res {
        Ok(fd) => fd,
        Err(e) => {
            error!("Open fuse device failed, {}, err {:?}", fuse_device_name, e);
            return Err(std::io::Error::from(ErrorKind::Other).into());
        }
    };
    let mut flags = libc::MS_NOSUID as u64 | libc::MS_NODEV as u64;
    let mut mount_options = format!(
        "fd={},rootmode={:o},user_id={},group_id={}",
        fd,
        mountpoint_mode,
        getuid(),
        getgid()
    );
    let fuse_opts = conf.get_fuse_opts();
    for opt in fuse_opts.iter() {
        match opt.as_str() {
            "nodev" => flags |= libc::MS_NODEV,
            "dev" => flags &= !libc::MS_NODEV,
            "nosuid" => flags |= libc::MS_NOSUID,
            "suid" => flags &= !libc::MS_NOSUID,
            "noexec" => flags |= libc::MS_NOEXEC,
            "exec" => flags &= !libc::MS_NOEXEC,
            _ => {
                mount_options.push(',');
                mount_options.push_str(opt);
            }
        }
    }

    // Default name is "/dev/fuse", then use the subtype, and lastly prefer the name
    let c_source = CString::new("curvinefs").unwrap();
    let c_mountpoint = CString::new(mnt.to_str().unwrap()).unwrap();

    let result = unsafe {
        let c_options = CString::new(mount_options.clone()).unwrap();
        let c_type = CString::new("fuse").unwrap();
        libc::mount(
            c_source.as_ptr(),
            c_mountpoint.as_ptr(),
            c_type.as_ptr(),
            flags,
            c_options.as_ptr() as *const libc::c_void,
        )
    };

    if result != 0 {
        close_raw_io(fd).unwrap();
        error!(
            "Mount fuse failed, {} with result {}",
            mnt.display(),
            result
        );
        return err_io!(-1);
    }
    info!("Mounted at {}", mnt.display());
    Ok(fd)
}

#[cfg(target_os = "macos")]
fn fuse_mount_sys(mnt: &Path, conf: &FuseConf) -> IOResult<RawIO> {}

fn detect_fusermount_bin() -> String {
    for name in [
        FUSERMOUNT3_BIN.to_string(),
        FUSERMOUNT_BIN.to_string(),
        format!("/bin/{FUSERMOUNT3_BIN}"),
        format!("/bin/{FUSERMOUNT_BIN}"),
    ]
    .iter()
    {
        if Command::new(name).arg("-h").output().is_ok() {
            return name.to_string();
        }
    }
    // Default to fusermount3
    FUSERMOUNT3_BIN.to_string()
}

pub fn fuse_umount_pure(mnt: &Path) {
    let c_mountpoint = CString::new(mnt.to_str().unwrap()).unwrap();
    let result = unsafe {
        #[cfg(target_os = "linux")]
        {
            libc::umount2(c_mountpoint.as_ptr(), libc::MNT_DETACH)
        }
        #[cfg(target_os = "macos")]
        {
            libc::umount(c_mountpoint.as_ptr(), libc::MNT_FORCE)
        }
    };

    if result == 0 {
        return;
    }
    let mut builder = Command::new(detect_fusermount_bin());
    builder.stdout(Stdio::piped()).stderr(Stdio::piped());
    builder.arg("-u").arg("-q").arg("-z").arg("--").arg(mnt);

    if let Ok(output) = builder.output() {
        info!("fusermount: {}", String::from_utf8_lossy(&output.stdout));
    }
}
