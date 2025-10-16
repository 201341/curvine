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
use log::{error, info, debug};
use nix::unistd::{getgid, getuid};
use std::ffi::CString;

use std::fs::File;
use std::io::{ErrorKind, Read, BufRead};
use std::path::Path;
use std::os::unix::net::UnixStream;
use std::os::unix::io::AsRawFd;
use std::os::unix::fs::PermissionsExt;
use std::{mem, ptr};
use std::process::{Command, Stdio};

use nix::sys::uio;
use nix::sys::socket::{self, AddressFamily, SockType, SockFlag, ControlMessageOwned, MsgFlags};
use std::io::IoSliceMut;

use orpc::io::IOResult;
use orpc::sys::RawIO;
use orpc::{err_io, err_box};
use orpc::sys::close_raw_io;
use orpc::sys::open;

const FUSERMOUNT_BIN: &str = "fusermount";
const FUSERMOUNT3_BIN: &str = "fusermount3";
const FUSERMOUNT_COMM_ENV: &str = "_FUSE_COMMFD";
const FUSERMOUNT_COMM2_ENV: &str = "_FUSE_COMMFD2";

pub fn fuse_mount_pure(mnt: &Path, conf: &FuseConf) -> IOResult<RawIO> {
    let mut res;
    #[cfg(target_os = "linux")]
    {
        res = fuse_mount_sys(mnt, conf);
    }
    #[cfg(target_os = "macos")]
    {
        res = fuse_mount_darwin(mnt, conf)
    };
    
    match res {
        Ok(fd) => Ok(fd),
        Err(e) => {
            error!("fuse mount sys failed; path {:?}, err {:?}", mnt, e);
            return fuse_mount_fusermount(mnt, conf); 
        }
    }
}

fn fuse_mount_fusermount(mnt: &Path, conf: &FuseConf) -> IOResult<RawIO> {
    let (child_fd, parent_fd) = UnixStream::pair()?;
    unsafe {
        libc::fcntl(child_fd.as_raw_fd(), libc::F_SETFD, 0);
    }
    // move owned copies into the thread so we don't capture non-'static references
    let mnt_owned = mnt.to_path_buf();
    let child_fd_raw = child_fd.as_raw_fd();
    let parent_fd_raw = parent_fd.as_raw_fd();
    std::thread::spawn(move || {
        // use std::process::Command here to avoid requiring a Tokio runtime inside the thread
        let mut builder = std::process::Command::new(detect_fusermount_bin());
        builder
            .arg("--")
            .arg(mnt_owned.to_string_lossy().to_string())
            .env(FUSERMOUNT_COMM_ENV, child_fd_raw.to_string())
            .env(FUSERMOUNT_COMM2_ENV, parent_fd_raw.to_string());
        let child = builder.spawn();
        match child {
            Ok(mut child) => {
                let status = child.wait();
                match status {
                    Ok(status) => {
                        if !status.success() {
                            error!("fusermount exited with status: {}", status);
                        }
                    }
                    Err(e) => {
                        error!("failed to wait on fusermount process: {}", e);
                    }
                }
            }
            Err(e) => {
                error!("failed to spawn fusermount process: {}", e);
            }
        }
    });


    //drop(child_fd);

    let fd =  match get_connection_fd(parent_fd.as_raw_fd()) {
        Ok(fd) => fd,
        Err(e) => {
            error!("get fuse fd from fusermount failed, err {:?}", e);
            return Err(e);
        }
    };
     
// let drop(child_fd);
// });

    let mut parent_fd = Some(parent_fd);
    if !conf.auto_umount() {
        drop(mem::take(&mut parent_fd));
    } 
    
    unsafe {
        libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC);
    }
    Ok(fd)

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
        Err(_) => {
            let e = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
            if e == libc::ENOENT || e == libc::ENODEV {
                error!("FUSE kernel module is not loaded, /dev/fuse does not exist");
                return err_box!("FUSE kernel module is not loaded, /dev/fuse does not exist");
            } else {
                error!("Open fuse device failed, {}, err {:?}", fuse_device_name, e);
                return err_box!("Open fuse device failed, {}, err {:?}", fuse_device_name, e);
            }
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
        return err_io!(result);
    }
    info!("Mounted at {}", mnt.display());
    Ok(fd)
}

#[cfg(target_os = "macos")]
fn fuse_mount_darwin(mnt: &Path, conf: &FuseConf) -> IOResult<RawIO> {

    let (child_fd, parent_fd) = match socket::socketpair(
        AddressFamily::Unix,
        SockType::Stream,
        None,
        SockFlag::empty(),
    ) {
        Err(err) => return err_box!(err.to_string()),

        Ok((sock0, sock1)) => (sock0, sock1),
    };     
    unsafe {
        libc::fcntl(child_fd.as_raw_fd(), libc::F_SETFD, 0);
    }

    let exec_path = match std::env::current_exe() {
        Ok(path) => path,
        Err(err) => return err_box!(err.to_string()),
    };
    info!("Mounting at {}, with opts {:?}", mnt.display(), conf.get_fuse_opts());
    let mnt_owned = mnt.to_path_buf();
    let child_fd_raw = child_fd.as_raw_fd();
    let parent_fd_raw = parent_fd.as_raw_fd();
    std::thread::spawn(move ||{
        let mut builder = Command::new(detect_fusermount_bin());
        let mut mount_args = format!("-ofsname=curvinefs,-odebug");
        builder.stdout(Stdio::piped()).stderr(Stdio::piped());    
        builder
            .env(FUSERMOUNT_COMM_ENV.to_string(), child_fd.as_raw_fd().to_string())
            .env("_FUSE_CALL_BY_LIB".to_string(), "1")
            .env("_FUSE_COMMVERS".to_string(), "2")
            .env("_FUSE_DAEMON_PATH".to_string(), exec_path)
            .args(&mount_args.split(',').collect::<Vec<_>>())
            .arg(mnt_owned.to_string_lossy().to_string());
        info!("Executing mount command: {:?}", builder);
        let child = builder.spawn();
        match child {
             Ok(mut child) => {
                let status = child.wait();
                match status {
                    Ok(status) => {
                        if !status.success() {
                            error!("fusermount exited with status: {}", status);
                        }
                    }
                    Err(e) => {
                        error!("failed to wait on fusermount process: {}", e);
                    }
                }
            }
            Err(e) => {
                error!("failed to spawn fusermount process: {}", e);
            }
        }
    });
    
    //let fuse_opts = conf.get_fuse_opts();
    // for opt in fuse_opts.iter() {
    //     if opt == "allow_root" {
    //         mount_args.push_str(",-oallow_root");
    //     } else if opt == "allow_other" {
    //         //mount_args.push_str(",-oallow_other");
    //     } else if opt == "default_permissions" {
    //         mount_args.push_str(",-odefault_permissions");
    //     }
    // }


    info!("Waiting for connection fd from mount command");
    let fd = get_connection_fd(parent_fd.as_raw_fd())?;
    info!("Received fd {} from mount command", fd);

    unsafe {
        libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC);
    }
    Ok(fd)
}

#[cfg(target_os = "macos")]
fn detect_fusermount_bin() -> String {
    for name in [
        "/Library/Filesystems/macfuse.fs/Contents/Resources/mount_macfuse".to_string(),
        "/Library/Filesystems/osxfuse.fs/Contents/Resources/mount_osxfuse".to_string(),
    ]
    .iter()
    {
        if Command::new(name).arg("-h").output().is_ok() {
            return name.to_string();
        }
    }
    return "none".to_string();
}

#[cfg(target_os = "linux")]
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

fn get_connection_fd(socket_fd: i32) -> IOResult<RawIO> {
    std::thread::sleep(std::time::Duration::from_millis(1000));
    let mut buf = vec![]; // it seems 0 len still works well

    let mut cmsg_buf = nix::cmsg_space!([i32; 1]);

    let mut bufs = [IoSliceMut::new(&mut buf)];

    let msg = match socket::recvmsg::<()>(
        socket_fd,
        &mut bufs[..],
        Some(&mut cmsg_buf),
        MsgFlags::empty(),
    ) {
        Err(err) => return err_box!(err.to_string()),

        Ok(msg) => msg,
    };

    let mut cmsgs = match msg.cmsgs() {
        Err(err) => return err_box!(err.to_string()),
        Ok(cmsgs) => cmsgs,
    };
    let fd = if let Some(ControlMessageOwned::ScmRights(fds)) = cmsgs.next() {
        if fds.is_empty() {
            return err_box!("no fd received");
        }

        fds[0]
    } else {
        return err_box!("get fuse fd failed");
    };

    Ok(fd)


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
            libc::unmount(c_mountpoint.as_ptr(), libc::MNT_FORCE)
        }
    };

    if result == 0 {
        return;
    } else {
        #[cfg(target_os = "linux")]
        {
            let mut builder = Command::new(detect_fusermount_bin());
            builder.stdout(Stdio::piped()).stderr(Stdio::piped());
            builder.arg("-u").arg("-q").arg("-z").arg("--").arg(mnt);

            if let Ok(output) = builder.output() {
                info!("fusermount: {}", String::from_utf8_lossy(&output.stdout));
            }
        }
        #[cfg(target_os = "macos")]
        {
            let mut builder = Command::new("umount");
            builder.stdout(Stdio::piped()).stderr(Stdio::piped());
            builder.arg("-f").arg(mnt);

            if let Ok(output) = builder.output() {
                info!("umount: {}", String::from_utf8_lossy(&output.stdout));
            }
        }
    }

}
