///! Just a bunch of useful function for benchmarking networks.
extern crate nix;
#[macro_use]
extern crate log;
extern crate csv;
extern crate ctrlc;

#[macro_use]
extern crate serde_derive;

use std::sync::{Arc, Mutex};

use nix::libc;
use nix::sys::socket;
use nix::sys::time;

use std::os::unix::io::RawFd;

extern "C" {
    /// Given an interface name (i.e., ifconfig name), return its IP address (we implicitly look for an AF_INET; IPv4 address).
    pub fn getifaceaddr(interface: *const libc::c_char) -> socket::sockaddr;
    /// Enable timestamping on the socket and for the given interface.
    pub fn enable_hwtstamp(sock: i32, interface: *const libc::c_char) -> i32;
}

/// A record that logs timestamps for every sent packet.
///
/// We currently take 4 timestamps:
///  *
///  *
///  * Transmit timestamp (immediately before send call)
///  * Transmit timestamp on NIC (HW or SW)
#[derive(Debug, Eq, PartialEq, Serialize)]
pub struct LogRecord {
    /// Timestamp (ns) when packet is recveived at application (immediately after recv call returns)
    pub rx_app: u64,
    /// Timestamp (ns) when packet is recveived on the NIC (either taken by NIC HW or driver SW)
    pub rx_nic: u64,
    /// Timestamp (ns) when packet is sent by the application (immediately before send call)
    pub tx_app: u64,
    /// Timestamp (ns) when packet is sent by the NIC (either taken by NIC HW or driver SW)
    pub tx_nic: u64,
}

/// Transform timespec to nanoseconds.
pub fn timespec_to_ns(ts: libc::timespec) -> u64 {
    (ts.tv_sec as u64) * 1_000_000_000 + (ts.tv_nsec as u64)
}

/// Transform TimeSpec to nanoseconds.
pub fn rstimespec_to_ns(ts: nix::sys::time::TimeSpec) -> u64 {
    (ts.tv_sec() as u64) * 1_000_000_000 + (ts.tv_nsec() as u64)
}

/// Read the TX timestamp from the error queue and return it in ns.
#[cfg(target_os = "linux")]
pub fn retrieve_tx_timestamp(
    sock: RawFd,
    cmsg_space: &mut socket::CmsgSpace<[time::TimeVal; 3]>,
) -> Option<u64> {
    use nix::sys::uio;

    let msg = socket::recvmsg(
        sock,
        &[uio::IoVec::from_mut_slice(&mut [0; 0])],
        Some(cmsg_space),
        socket::MsgFlags::MSG_ERRQUEUE,
    ).expect("Can't receive on error queue");
    read_nic_timestamp(&msg)
}

#[cfg(not(target_os = "linux"))]
pub fn retrieve_tx_timestamp(
    _sock: RawFd,
    _cmsg_space: &mut socket::CmsgSpace<[time::TimeVal; 3]>,
) -> Option<u64> {
    warn!("Can't retrieve timestamp on the current platform.");
    None
}

/// Return the corresponding NIC timestamp (HW or SW) of a given message in ns.
#[cfg(target_os = "linux")]
pub fn read_nic_timestamp(msg: &socket::RecvMsg) -> Option<u64> {
    for cmsg in msg.cmsgs() {
        match cmsg {
            socket::ControlMessage::ScmTimestamping(timestamps) => {
                let ts = if rstimespec_to_ns(timestamps[2]) != 0 {
                    // Try to use the hardware timestamp by default
                    timestamps[2]
                } else {
                    // In case we don't get that log it and return SW timestamp
                    debug!("Didn't receive a hardware NIC timstamp.");
                    timestamps[0]
                };

                let tstp = rstimespec_to_ns(ts);
                assert!(tstp != 0);
                return Some(tstp);
            }
            _ => panic!("Got Unexpected ControlMessage on RX path!"),
        }
    }
    None
}

#[cfg(not(target_os = "linux"))]
pub fn read_nic_timestamp(_msg: &socket::RecvMsg) -> Option<u64> {
    warn!("Can't retrieve timestamp on the current platform.");
    None
}

/// Returns current system time in nanoseconds.
pub fn now() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    unsafe {
        libc::clock_gettime(libc::CLOCK_REALTIME, &mut ts);
    }

    timespec_to_ns(ts)
}

/// Makes a new CSV writer to log the results. We register an abort handler
/// to flush the records out since the csv::Writer has some internal buffering
/// that is not flushed in case we stop the process with SIGINT or SIGTERM.
pub fn create_writer(logfile: String, capacity: usize) -> Arc<Mutex<csv::Writer<std::fs::File>>> {
    // The CSV writer is accessed in the signal handler so we need to wrap it in Arc and Mutex
    let wtr_handle = csv::WriterBuilder::new()
        .buffer_capacity(capacity)
        .from_path(logfile)
        .expect("Can't build CSV writer");
    let wtr = Arc::new(Mutex::new(wtr_handle));

    // Make sure we flush the remaining log records in the CSV writer when we exit the server
    let signal_wtr = Arc::clone(&wtr);
    ctrlc::set_handler(move || {
        debug!("Trying to acquire lock...");
        let mut logfile = signal_wtr.lock().unwrap();
        logfile.flush().expect("Can't flush the csv log");
        std::process::exit(0);
    }).expect("Error setting Ctrl-C handler");

    wtr
}
