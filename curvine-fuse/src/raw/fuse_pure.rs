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

    let mut builder = Command::new(detect_fusermount_bin());
    builder.stdout(Stdio::piped()).stderr(Stdio::piped());
    builder
        .arg("--")
        .arg(mnt)
        .env(FUSERMOUNT_COMM_ENV, child_fd.as_raw_fd().to_string())
        .env(FUSERMOUNT_COMM2_ENV, parent_fd.as_raw_fd().to_string());


    let fusermount_child = builder.spawn()?;
    drop(child_fd);

    let fd = match get_connection_fd(&parent_fd) {
        Ok(fd) => fd,
        Err(e) => {
            error!("get connection fd failed, err {:?}", e);
            return err_box!("get connection fd failed, err {:?}", e); 
        }
    };
    let mut parent_fd = Some(parent_fd);
    if !conf.auto_umount() {
        drop(mem::take(&mut parent_fd));
        let output = fusermount_child.wait_with_output()?;
        info!("fusermount output: {}", String::from_utf8_lossy(&output.stdout));
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
    let (child_fd, parent_fd) = UnixStream::pair()?;
    unsafe {
        libc::fcntl(child_fd.as_raw_fd(), libc::F_SETFD, 0);
    }

    let exec_path = match std::env::current_exe() {
        Ok(path) => path,
        Err(err) => return err_box!(err.to_string()),
    };
    info!("Mounting at {}, with opts {:?}", mnt.display(), conf.get_fuse_opts());
    let mut env_vars = std::env::vars().collect::<Vec<_>>();
    env_vars.extend([
        ("_FUSE_CALL_BY_LIB".to_string(), "1".to_string()),
        ("_FUSE_DAEMON_PATH".to_string(), exec_path.to_string_lossy().to_string()),
        (FUSERMOUNT_COMM_ENV.to_string(), child_fd.as_raw_fd().to_string()),
        ("_FUSE_COMMVERS".to_string(), "2".to_string()),
        ("MOUNT_OSXFUSE_CALL_BY_LIB".to_string(), "1".to_string()),
        ("MOUNT_OSXFUSE_DAEMON_PATH".to_string(), exec_path.to_string_lossy().to_string()),
    ]);
    
    let mut mount_args = format!("-ofsname=curvinefs,-odebug"); 
    let fuse_opts = conf.get_fuse_opts();
    for opt in fuse_opts.iter() {
        if opt == "allow_root" {
            mount_args.push_str(",-oallow_root");
        } else if opt == "allow_other" {
            mount_args.push_str(",-oallow_other");
        } else if opt == "default_permissions" {
            mount_args.push_str(",-odefault_permissions");
        }
    }

    let mut builder = Command::new(detect_fusermount_bin());
    builder.stdout(Stdio::piped()).stderr(Stdio::piped());    
    builder
        .args(&mount_args.split(',').collect::<Vec<_>>())
        .arg(mnt.to_string_lossy().to_string())
        .envs(env_vars);
    info!("Executing mount command: {:?}", builder);
    let fusermount_child = builder.spawn()?;
    drop(child_fd);

    info!("Waiting for connection fd from mount command");
    let fd = get_connection_fd(&parent_fd)?;
    info!("Received fd {} from mount command", fd);
    let mut parent_fd = Some(parent_fd);
    drop(mem::take(&mut parent_fd));
    let output = fusermount_child.wait_with_output()?;
    info!("fusermount output: {}", String::from_utf8_lossy(&output.stdout));

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

fn get_connection_fd(socket_fd: &UnixStream) -> IOResult<RawIO> {
    let mut io_vec_buf = [0u8];
    let mut io_vec = libc::iovec {
        iov_base: io_vec_buf.as_mut_ptr() as *mut libc::c_void,
        iov_len: io_vec_buf.len(),
    };

    let cmsg_buffer_len = unsafe { libc::CMSG_SPACE(mem::size_of::<libc::c_int>() as libc::c_uint) };
    let mut cmsg_buffer = vec![0u8; cmsg_buffer_len as usize];

    let mut message: libc::msghdr;
    #[cfg(all(target_os = "linux", not(target_env = "musl")))]
    {
        message = libc::msghdr {
            msg_name: ptr::null_mut(),
            msg_namelen: 0,
            msg_iov: &mut io_vec,
            msg_iovlen: 1,
            msg_control: cmsg_buffer.as_mut_ptr() as *mut libc::c_void,
            msg_controllen: cmsg_buffer.len(),
            msg_flags: 0,
        };
    }
    #[cfg(all(target_os = "linux", target_env = "musl"))]
    {
        message = unsafe { std::mem::MaybeUninit::zeroed().assume_init() };
        message.msg_name = ptr::null_mut();
        message.msg_namelen = 0;
        message.msg_iov = &mut io_vec;
        message.msg_iovlen = 1;
        message.msg_control = (&mut cmsg_buffer).as_mut_ptr() as *mut libc::c_void;
        message.msg_controllen = cmsg_buffer.len() as u32;
        message.msg_flags = 0;
    }
    #[cfg(target_os = "macos")]
    {
        message = libc::msghdr {
            msg_name: ptr::null_mut(),
            msg_namelen: 0,
            msg_iov: &mut io_vec,
            msg_iovlen: 1,
            msg_control: (&mut cmsg_buffer).as_mut_ptr() as *mut libc::c_void,
            msg_controllen: cmsg_buffer.len() as u32,
            msg_flags: 0,
        };
    }
    let mut result;
    loop {
        result = unsafe { libc::recvmsg(socket_fd.as_raw_fd(), &mut message, 0) };
        if result != -1 {
            break;
        }
        let err = std::io::Error::last_os_error();
        if err.kind() != ErrorKind::Interrupted {
            error!("recvmsg failed, err {:?}", err);
            return Err(err.into());
        }
        
    }

    if result == 0 {
        return err_box!("Connection closed by peer");
    }

    unsafe {
        let control_msg = libc::CMSG_FIRSTHDR(&message);
        if (*control_msg).cmsg_type != libc::SCM_RIGHTS {
            return err_box!("Unknown control message from fusermount: {}", (*control_msg).cmsg_type);
        }
        let fd_data = libc::CMSG_DATA(control_msg);

        let fd = *(fd_data as *const libc::c_int);
        if fd < 0 {
            return err_box!("Invalid file descriptor received from fusermount: {}", fd);
        } 
        Ok(fd)
    }
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
