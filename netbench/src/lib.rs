///! Just a bunch of useful function for benchmarking networks.
extern crate byteorder;
extern crate nix;
#[macro_use]
extern crate log;
extern crate csv;
extern crate ctrlc;
extern crate mio;

#[macro_use]
extern crate serde_derive;

use std::os::unix::io::RawFd;
use std::sync::{Arc, Mutex};

use nix::libc;
use nix::sys::socket;
use nix::sys::time;

pub enum Connection {
    Datagram(mio::net::UdpSocket),
    Stream(mio::net::TcpStream),
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize)]
pub enum PacketTimestamp {
    None,
    Software,
    Hardware,
}

extern "C" {
    /// Given an interface name (i.e., ifconfig name), return its IP address (we implicitly look for an AF_INET; IPv4 address).
    pub fn getifaceaddr(interface: *const libc::c_char) -> socket::sockaddr;
    /// Enable timestamping on the socket and for the given interface.
    fn enable_hwtstamp(sock: i32, interface: *const libc::c_char, hw: bool) -> i32;
}

pub unsafe fn enable_packet_timestamps(
    sock: i32,
    interface: *const libc::c_char,
    method: PacketTimestamp,
) -> i32 {
    if method == PacketTimestamp::None {
        return 0;
    }
    enable_hwtstamp(sock, interface, method == PacketTimestamp::Hardware)
}

/// A record that logs timestamps for every sent packet.
///
/// We currently take 4 timestamps:
///  *
///  *
///  * Transmit timestamp (immediately before send call)
///  * Transmit timestamp on NIC (HW or SW)
#[derive(Debug, Eq, PartialEq, Serialize, Default)]
pub struct LogRecord {
    /// ID that uniquely identifies the packet (generated at the client, sent with packet)
    pub id: u64,
    /// Timestamp (ns) when packet is recveived at application (immediately after recv call returns)
    pub rx_app: u64,
    /// Timestamp (ns) when packet is recveived on the NIC (either taken by NIC HW or driver SW)
    pub rx_nic: u64,
    /// Timestamp (ns) when packet is sent by the application (immediately before send call)
    pub tx_app: u64,
    /// Timestamp (ns) when packet is sent by the NIC (either taken by NIC HW or driver SW)
    pub tx_nic: u64,
    /// Time received at the HT in case `smt` mode is used in rserver (otherwise 0)
    pub rx_ht: u64,
    /// Packet was sueccessfully handled by the application
    pub completed: bool,
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
    method: PacketTimestamp,
    connection: &Connection,
) -> (u64, u64) {
    use byteorder::{BigEndian, ReadBytesExt};
    use nix::sys::uio;

    // XXX: find a better way to do this
    // 14-byte Ethernet header
    // 20-byte IP header
    // 8-byte UDP header
    const PACKET_HEADERS_UDP: usize = 14 + 20 + 8;
    // or 32-byte TCP header
    const PACKET_HEADERS_TCP: usize = 66;
    let pkt_size = match connection {
        Connection::Datagram(_) => PACKET_HEADERS_UDP + 8,
        Connection::Stream(_) => PACKET_HEADERS_TCP + 8,
    };
    // Ugly allocation here due to not knowing tcp/udp at compile time:
    let mut recv_buf: Vec<u8> = Vec::with_capacity(pkt_size);
    recv_buf.resize(pkt_size, 0);

    let msg = socket::recvmsg(
        sock,
        &[uio::IoVec::from_mut_slice(&mut recv_buf)],
        Some(cmsg_space),
        socket::MsgFlags::MSG_ERRQUEUE,
    ).expect("Can't receive on error queue");

    debug!("msg = {:?}", msg.bytes);
    let payload_id = recv_buf[pkt_size - 8..]
        .as_ref()
        .read_u64::<BigEndian>()
        .expect("Can't read ID?");
    (payload_id, read_nic_timestamp(&msg, method))
}

#[cfg(not(target_os = "linux"))]
pub fn retrieve_tx_timestamp(
    _sock: RawFd,
    _cmsg_space: &mut socket::CmsgSpace<[time::TimeVal; 3]>,
    method: PacketTimestamp,
) -> (u64, u64) {
    debug!("Can't retrieve timestamp on the current platform.");
    assert!(method == PacketTimestamp::None);
    (0, 0)
}

/// Return the corresponding NIC timestamp (HW or SW) of a given message in ns.
#[cfg(target_os = "linux")]
pub fn read_nic_timestamp(msg: &socket::RecvMsg, method: PacketTimestamp) -> u64 {
    match method {
        PacketTimestamp::Hardware | PacketTimestamp::Software => {
            for cmsg in msg.cmsgs() {
                match cmsg {
                    socket::ControlMessage::ScmTimestamping(timestamps) => {
                        let ts = if method == PacketTimestamp::Hardware {
                            timestamps[2]
                        } else {
                            timestamps[0]
                        };

                        let tstp = rstimespec_to_ns(ts);
                        assert!(tstp != 0);
                        return tstp;
                    }
                    _ => panic!("Got Unexpected ControlMessage on RX path!"),
                }
            }
            panic!("No Control Message found");
        }
        PacketTimestamp::None => 0,
    }
}

#[cfg(not(target_os = "linux"))]
pub fn read_nic_timestamp(_msg: &socket::RecvMsg, method: PacketTimestamp) -> u64 {
    debug!("Can't retrieve timestamp on the current platform.");
    assert!(method == PacketTimestamp::None);
    0
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

#[cfg(target_os = "linux")]
pub fn pin_thread(core_ids: Vec<usize>) {
    use nix::sched::{sched_setaffinity, CpuSet};
    use nix::unistd::getpid;

    let pid = getpid();
    let mut affinity_set = CpuSet::new();
    for core_id in core_ids {
        affinity_set.set(core_id).expect("Can't set PU in core set");
    }
    sched_setaffinity(pid, &affinity_set).expect("Can't pin app thread to core");
}

// And this function only gets compiled if the target OS is *not* linux
#[cfg(not(target_os = "linux"))]
pub fn pin_thread(_core_id: Vec<usize>) {
    error!("Pinning threads not supported!");
}
