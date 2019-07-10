///! Just a bunch of useful function for benchmarking networks.
extern crate byteorder;
extern crate nix;
#[macro_use]
extern crate log;
extern crate csv;
extern crate ctrlc;
extern crate mio;
extern crate prctl;
extern crate socket2;

#[macro_use]
extern crate serde_derive;

#[macro_use]
extern crate clap;

use std::net;
use std::os::unix::io::AsRawFd;
use std::os::unix::io::RawFd;
use std::sync::{Arc, Mutex};

use nix::libc;
use nix::sys::socket;
use nix::sys::time;
use socket2::{Domain, Socket, Type};

pub const LOGFILE_SIZE: usize = 256 * 1024 * 1024;

#[derive(Debug)]
pub enum Connection {
    Datagram(net::UdpSocket),
    Stream(net::TcpStream),
}

impl AsRawFd for Connection {
    fn as_raw_fd(&self) -> RawFd {
        match self {
            Connection::Datagram(s) => s.as_raw_fd(),
            Connection::Stream(s) => s.as_raw_fd(),
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize)]
pub enum PacketTimestamp {
    None,
    Software,
    Hardware,
    // Rx path only (for libvma)
    HardwareRx,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize)]
pub enum Transport {
    Udp,
    Tcp,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize)]
pub enum Scheduler {
    Default,
    Fifo,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize)]
pub enum ThreadMapping {
    /// Affinity for each thread to run on all provided CPUs
    All,
    /// Assign threads round-robin to CPUs (may overprovision CPUs in case threads < CPU)
    RoundRobin,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize)]
pub enum SocketMapping {
    All,
    RoundRobin,
}

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub interface: String,
    pub output: String,
    pub core_ids: Vec<usize>,
    pub threads: usize,
    pub sockets: usize,
    pub mapping: ThreadMapping,
    pub socketmapping: SocketMapping,

    pub scheduler: Scheduler,
    pub timestamp: PacketTimestamp,
    pub transport: Transport,

    // Server
    pub port: u16,

    // Client
    pub requests: usize,
    pub destinations: Vec<String>,
    pub rate: Option<u128>,
}

impl AppConfig {
    pub fn parse(matches: &clap::ArgMatches) -> AppConfig {
        let iface = String::from(matches.value_of("iface").unwrap_or("enp216s0f1"));
        let output = value_t!(matches, "output", String).unwrap_or(String::from("none"));
        let core_ids: Vec<usize> = values_t!(matches, "pin", usize).unwrap_or(vec![]);
        let threads = value_t!(matches, "threads", usize).unwrap_or(1);
        let sockets = value_t!(matches, "sockets", usize).unwrap_or(1);

        let socketmapping = match matches.value_of("socketmapping").unwrap_or("all") {
            "all" => SocketMapping::All,
            "roundrobin" => SocketMapping::RoundRobin,
            _ => unreachable!(
                "Invalid CLI argument, may be clap bug if possible_values doesn't work?"
            ),
        };

        let mapping = match matches.value_of("mapping").unwrap_or("all") {
            "all" => ThreadMapping::All,
            "roundrobin" => ThreadMapping::RoundRobin,
            _ => unreachable!(
                "Invalid CLI argument, may be clap bug if possible_values doesn't work?"
            ),
        };

        let timestamp = match matches.value_of("timestamp").unwrap_or("hardware") {
            "hardware" => PacketTimestamp::Hardware,
            "hardwarerx" => PacketTimestamp::HardwareRx,
            "software" => PacketTimestamp::Software,
            "none" => PacketTimestamp::None,
            _ => unreachable!(
                "Invalid CLI argument, may be clap bug if possible_values doesn't work?"
            ),
        };

        let scheduler = match matches.value_of("scheduler").unwrap_or("default") {
            "rt" => Scheduler::Fifo,
            "default" => Scheduler::Default,
            _ => unreachable!(
                "Invalid CLI argument, may be clap bug if possible_values doesn't work?"
            ),
        };

        let transport = match matches.value_of("transport").unwrap_or("udp") {
            "udp" => Transport::Udp,
            "tcp" => Transport::Tcp,
            _ => unreachable!(
                "Invalid CLI argument, may be clap bug if possible_values doesn't work?"
            ),
        };

        let requests = value_t!(matches, "requests", usize).unwrap_or(250000);
        let destinations = values_t!(matches, "destinations", String)
            .unwrap_or(vec![String::from("192.168.0.7:3400")]);
        let rate = value_t!(matches, "rate", u128).ok();
        let port = value_t!(matches, "port", u16).unwrap_or(3400);

        AppConfig {
            interface: iface,
            output: output,
            core_ids: core_ids,
            threads: threads,
            mapping: mapping,
            sockets: sockets,
            socketmapping: socketmapping,

            scheduler: scheduler,
            timestamp: timestamp,
            transport: transport,

            port: port,

            requests: requests,
            destinations: destinations,
            rate: rate,
        }
    }
}

extern "C" {
    /// Given an interface name (i.e., ifconfig name), return its IP address (we implicitly look for an AF_INET; IPv4 address).
    pub fn getifaceaddr(interface: *const libc::c_char) -> socket::sockaddr;
    /// Enable timestamping on the socket and for the given interface.
    fn enable_hwtstamp(sock: i32, interface: *const libc::c_char, hw: bool, rxonly: bool) -> i32;
}

unsafe fn enable_packet_timestamps(
    sock: i32,
    interface: *const libc::c_char,
    method: PacketTimestamp,
) -> i32 {
    if method == PacketTimestamp::None {
        return 0;
    }
    enable_hwtstamp(
        sock,
        interface,
        method == PacketTimestamp::Hardware || method == PacketTimestamp::HardwareRx,
        method == PacketTimestamp::HardwareRx,
    )
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
    /// Packet was successfully handled by the application
    pub completed: bool,
    /// Packet was dropped and we had to send a retransmission at `tx_nic`
    pub retransmission: bool,
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
    config: &AppConfig,
) -> nix::Result<(u64, u64)> {
    use byteorder::{BigEndian, ReadBytesExt};
    use nix::sys::uio;

    // TODO: find a better way to do this
    // 14-byte Ethernet header
    // 20-byte IP header
    // 8-byte UDP header
    const PACKET_HEADERS_UDP: usize = 14 + 20 + 8;
    // or 32-byte TCP header
    const PACKET_HEADERS_TCP: usize = 66;
    let pkt_size = match config.transport {
        Transport::Udp => PACKET_HEADERS_UDP + 8,
        Transport::Tcp => PACKET_HEADERS_TCP + 8,
    };

    // TODO: Ugly allocation here due to not knowing tcp/udp at compile time:
    let mut recv_buf: Vec<u8> = Vec::with_capacity(pkt_size);
    recv_buf.resize(pkt_size, 0);

    let msg = try!(socket::recvmsg(
        sock,
        &[uio::IoVec::from_mut_slice(&mut recv_buf)],
        Some(cmsg_space),
        socket::MsgFlags::MSG_ERRQUEUE,
    ));

    let payload_id = recv_buf[pkt_size - 8..]
        .as_ref()
        .read_u64::<BigEndian>()
        .expect("Can't read ID?");

    Ok((payload_id, read_nic_timestamp(&msg, config.timestamp)))
}

#[cfg(not(target_os = "linux"))]
pub fn retrieve_tx_timestamp(
    _sock: RawFd,
    _cmsg_space: &mut socket::CmsgSpace<[time::TimeVal; 3]>,
    _config: &AppConfig,
) -> nix::Result<(u64, u64)> {
    debug!("Can't retrieve timestamp on the current platform.");
    Ok((0, 0))
}

/// Return the corresponding NIC timestamp (HW or SW) of a given message in ns.
#[cfg(target_os = "linux")]
pub fn read_nic_timestamp(msg: &socket::RecvMsg, method: PacketTimestamp) -> u64 {
    match method {
        PacketTimestamp::Hardware | PacketTimestamp::HardwareRx | PacketTimestamp::Software => {
            for cmsg in msg.cmsgs() {
                match cmsg {
                    socket::ControlMessage::ScmTimestamping(timestamps) => {
                        let ts = if method == PacketTimestamp::Hardware
                            || method == PacketTimestamp::HardwareRx
                        {
                            timestamps[2]
                        } else {
                            timestamps[0]
                        };
                        let tstp = rstimespec_to_ns(ts);
                        assert!(tstp != 0);
                        return tstp;
                    }
                    socket::ControlMessage::Unknown(a) => {
                        error!("Got Unknown ControlMessage on RX path cmsg_len = {:?} cmsg_level = {:?} cmsg_type = {:?}!", a.0.cmsg_len, a.0.cmsg_level, a.0.cmsg_type);
                        for b in a.1 {
                            error!("b = {}", b);
                        }
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
    })
    .expect("Error setting Ctrl-C handler");

    wtr
}

#[cfg(target_os = "linux")]
fn pin_thread(core_ids: &Vec<usize>) {
    use nix::sched::{sched_setaffinity, CpuSet};

    if core_ids.len() > 0 {
        let mut affinity_set = CpuSet::new();
        for core_id in core_ids {
            affinity_set
                .set(*core_id)
                .expect("Can't set PU in core set");
        }

        // pid 0 pins to the current thread
        sched_setaffinity(nix::unistd::Pid::from_raw(0i32), &affinity_set)
            .expect("Can't pin app thread to core");
    }
}

// And this function only gets compiled if the target OS is *not* linux
#[cfg(not(target_os = "linux"))]
fn pin_thread(_core_id: &Vec<usize>) {
    error!("Pinning threads not supported!");
}

#[cfg(target_os = "linux")]
fn set_rt_fifo() {
    unsafe {
        let params = libc::sched_param {
            sched_priority: 1i32,
        };
        let pid = libc::getpid();
        let r = libc::sched_setscheduler(pid, libc::SCHED_FIFO, &params);
        assert_eq!(r, 0, "libc::sched_setscheduler failed");
        debug!("Running thread with real time priority.");
    }
}

#[cfg(not(target_os = "linux"))]
fn set_rt_fifo() {
    error!("set_rt_fifo is not supported in the current platform!");
}

#[cfg(target_os = "linux")]
pub fn set_process_name(name: &str) {
    prctl::set_name(name).expect("Can't set process name");
}

#[cfg(not(target_os = "linux"))]
pub fn set_process_name(name: &str) {
    error!("set_process_name is not supported on the current platform!");
}

pub fn set_thread_affinity(config: &AppConfig, thread_id: usize) -> Vec<usize> {
    let pin_to = match config.mapping {
        ThreadMapping::All => config.core_ids.clone(),
        ThreadMapping::RoundRobin => vec![config.core_ids[thread_id % config.core_ids.len()]],
    };
    pin_thread(&pin_to);
    pin_to
}

pub fn set_scheduling(config: &AppConfig) {
    match config.scheduler {
        Scheduler::Fifo => set_rt_fifo(),
        Scheduler::Default => debug!("Default scheduling"),
    }
}

fn find_my_interface_address(config: &AppConfig) -> net::SocketAddrV4 {
    let interface = std::ffi::CString::new(config.interface.clone()).expect("Can't be null");

    unsafe {
        let interface_addr = getifaceaddr(interface.as_ptr());
        match socket::SockAddr::from_libc_sockaddr(&interface_addr) {
            Some(socket::SockAddr::Inet(s)) => {
                let mut addr = s.to_std();
                addr.set_port(config.port);
                debug!("Found address {} for {}", addr, config.interface);
                if let net::SocketAddr::V4(ip4addr) = addr {
                    ip4addr
                } else {
                    panic!("Got unknown address");
                }
            }
            _ => {
                warn!(
                    "Could not find address for {:?} using 0.0.0.0",
                    config.interface
                );
                net::SocketAddrV4::new(net::Ipv4Addr::new(0, 0, 0, 0), config.port)
            }
        }
    }
}

pub fn parse_args(matches: &clap::ArgMatches) -> (AppConfig, net::SocketAddrV4) {
    let config = AppConfig::parse(matches);
    let address = find_my_interface_address(&config);
    (config, address)
}

/// Create a single, TCP or UDP socket
pub fn make_socket(config: &AppConfig) -> Socket {
    let socket = match config.transport {
        Transport::Tcp => Socket::new(Domain::ipv4(), Type::stream(), None),
        Transport::Udp => Socket::new(Domain::ipv4(), Type::dgram(), None),
    }
    .expect("Can't create socket");

    if config.transport == Transport::Udp && config.sockets > 1 {
        debug!("Set socket reuse_port option");
        socket.set_reuse_port(true).expect("Can't set reuse port");
    }

    socket
        .set_nonblocking(true)
        .expect("Can't set it to blocking mode");

    socket
}

pub fn timestamping_enable(config: &AppConfig, socket: RawFd) {
    let interface = std::ffi::CString::new(config.interface.clone()).expect("Can't be null");

    unsafe {
        let r = enable_packet_timestamps(socket, interface.as_ptr(), config.timestamp);

        if r != 0 {
            panic!(
                "Failed to enable NIC timestamps (ret {}): {}",
                r,
                nix::errno::Errno::last()
            );
        }
    }
}
