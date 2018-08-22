#[macro_use]
extern crate clap;
extern crate csv;
extern crate hwloc;
#[macro_use]
extern crate log;
extern crate byteorder;
extern crate env_logger;
extern crate libc;
extern crate mio;
extern crate netbench;
extern crate nix;
extern crate socket2;

use std::collections::{HashMap, VecDeque};
use std::net;
use std::sync::{Arc, Mutex};

use std::os::unix::io::{AsRawFd, RawFd};

use std::sync::mpsc;
use std::thread;

use nix::sys::socket;
use nix::sys::time;
use nix::sys::uio;

use socket2::{Domain, Socket, Type};

use mio::unix::{EventedFd, UnixReady};
use mio::Ready;

use clap::App;

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use netbench::*;

const PONG: mio::Token = mio::Token(0);

#[derive(Debug)]
struct MessageState {
    sender: Option<socket::SockAddr>,
    log: LogRecord,
}

impl MessageState {
    fn new(
        id: u64,
        sender: Option<socket::SockAddr>,
        rx_app: u64,
        rx_nic: u64,
        rx_ht: u64,
    ) -> MessageState {
        let mut log: LogRecord = Default::default();
        log.id = id;
        log.rx_app = rx_app;
        log.rx_nic = rx_nic;
        log.rx_ht = rx_ht;
        log.completed = false;

        MessageState {
            sender: sender,
            log: log,
        }
    }
}

fn network_loop(
    config: &AppConfig,
    connection: Connection,
    app_channel: Option<(mpsc::Sender<u64>, mpsc::Receiver<(u64, u64)>)>,
    wtr: Arc<Mutex<csv::Writer<std::fs::File>>>,
) {
    let poll = mio::Poll::new().expect("Can't create poll.");

    let raw_fd: std::os::unix::io::RawFd = match connection {
        Connection::Datagram(ref socket) => socket.as_raw_fd(),
        Connection::Stream(ref stream) => stream.as_raw_fd(),
    };

    poll.register(
        &EventedFd(&raw_fd),
        PONG,
        Ready::readable() | UnixReady::error(),
        mio::PollOpt::edge() | mio::PollOpt::oneshot(),
    ).expect("Can't register events.");

    let mut events = mio::Events::with_capacity(10);
    let mut recv_buf: Vec<u8> = Vec::with_capacity(8);
    recv_buf.resize(8, 0);

    let mut packet_count = 0;

    let mut time_rx: socket::CmsgSpace<[time::TimeVal; 3]> = socket::CmsgSpace::new();
    let mut time_tx: socket::CmsgSpace<[time::TimeVal; 3]> = socket::CmsgSpace::new();

    // If this fails think about allocating it on the heap instead of copy?
    assert!(std::mem::size_of::<MessageState>() < 256);
    let mut send_state: VecDeque<MessageState> = VecDeque::with_capacity(1024);
    let mut ts_state: HashMap<u64, MessageState> = HashMap::with_capacity(1024);

    loop {
        poll.poll(&mut events, None).expect("Can't poll channel");
        for event in events.iter() {
            debug!("event = {:?}", event);

            if UnixReady::from(event.readiness()).is_error() {
                // TODO: do until EAGAIN
                loop {
                    let (id, tx_nic) = match retrieve_tx_timestamp(
                        raw_fd,
                        &mut time_tx,
                        config.timestamp,
                        &connection,
                    ) {
                        Ok(data) => data,
                        Err(nix::Error::Sys(nix::errno::Errno::EAGAIN)) => break,
                        Err(e) => panic!("Unexpected error during retrieve_tx_timestamp {:?}", e),
                    };

                    debug!("read from err queue id={} tx_nic={}", id, tx_nic);
                    ts_state.remove(&id).map_or_else(
                        || {
                            panic!("Packet state for id {} not found?", id);
                        },
                        |mut st| {
                            debug!("Reading timestamp");
                            assert!(id == st.log.id);
                            st.log.tx_nic = tx_nic;
                            st.log.completed = true;

                            // Log all the timestamps
                            let mut logfile = wtr.lock().unwrap();
                            logfile.serialize(&st.log).expect("Can't write record.");

                            packet_count = packet_count + 1;
                        },
                    );
                }
            }

            // Receive a packet
            if event.readiness().is_readable() {
                debug!("Read packet");
                // Get the packet
                loop {
                    let msg = match socket::recvmsg(
                        raw_fd,
                        &[uio::IoVec::from_mut_slice(&mut recv_buf)],
                        Some(&mut time_rx),
                        socket::MsgFlags::empty(),
                    ) {
                        Ok(msg) => msg,
                        Err(nix::Error::Sys(nix::errno::Errno::EAGAIN)) => break,
                        Err(e) => panic!("Unexpected error during socket::recvmsg {:?}", e),
                    };

                    if msg.bytes == 0 {
                        // In TCP 0 means sender shut down the connection, we're done here.
                        return;
                    }
                    let rx_app = now();
                    assert_eq!(msg.bytes, 8, "Message payload got {} bytes", msg.bytes);
                    let mut packet_id = recv_buf
                        .as_slice()
                        .read_u64::<BigEndian>()
                        .expect("Can't parse timestamp");
                    debug!("Received packet {} id={}", packet_count, packet_id);

                    let rx_ht = app_channel.as_ref().map_or(0, |(txp, rxp)| {
                        txp.send(packet_id)
                            .expect("Can't forward data to the app thread over channel.");
                        let (payload, tst) = rxp.recv().expect("Can't receive data");
                        packet_id = payload;
                        tst
                    });

                    let rx_nic = read_nic_timestamp(&msg, config.timestamp);
                    debug!("Got rx_nic = {}", rx_nic);

                    if packet_id == 0 {
                        debug!("Client sent 0, flushing logfile.");
                        let mut logfile = wtr.lock().unwrap();

                        // Log packets that probably didn't make it back
                        for packet in &send_state {
                            logfile.serialize(&packet.log).expect("Can't write record.");
                        }
                        for (_id, packet) in &ts_state {
                            logfile.serialize(&packet.log).expect("Can't write record.");
                        }
                        logfile.flush().expect("Can't flush logfile");
                    } else {
                        let mst = MessageState::new(packet_id, msg.address, rx_app, rx_nic, rx_ht);
                        send_state.push_back(mst);
                    }
                }
            }

            // Send a packet
            // TODO: make sure we try to send the first time we receive something again
            if send_state.len() > 0 || event.readiness().is_writable() {
                while send_state.len() > 0 {
                    let mut st = send_state.pop_front().expect("We need an item");
                    st.log.tx_app = now();
                    recv_buf.clear();
                    recv_buf
                        .write_u64::<BigEndian>(st.log.id)
                        .expect("Can't serialize payload");

                    let bytes_sent = if st.sender.is_some() {
                        match socket::sendto(
                            raw_fd,
                            &recv_buf,
                            &st.sender.unwrap(),
                            socket::MsgFlags::empty(),
                        ) {
                            Ok(bytes_sent) => bytes_sent,
                            Err(nix::Error::Sys(nix::errno::Errno::EAGAIN)) => {
                                send_state.push_front(st);
                                break;
                            }
                            Err(e) => panic!("Unexpected error during socket::sendto {:?}", e),
                        }
                    } else {
                        match socket::send(raw_fd, &recv_buf, socket::MsgFlags::empty()) {
                            Ok(bytes_sent) => bytes_sent,
                            Err(nix::Error::Sys(nix::errno::Errno::EAGAIN)) => {
                                send_state.push_front(st);
                                break;
                            }
                            Err(e) => panic!("Unexpected error during socket::send {:?}", e),
                        }
                    };

                    debug!("sent reply");
                    assert_eq!(bytes_sent, 8);
                    ts_state.insert(st.log.id, st);
                }
            }
        }

        // Reregister for events
        let opts = if send_state.len() == 0 {
            Ready::readable() | UnixReady::error()
        } else {
            debug!("Unable to send everything, register for writable");
            Ready::writable() | Ready::readable() | UnixReady::error()
        };

        poll.reregister(
            &EventedFd(&raw_fd),
            PONG,
            opts,
            mio::PollOpt::edge() | mio::PollOpt::oneshot(),
        ).expect("Can't re-register events.");
    }
}

fn _spawn_listen_pair(
    config: AppConfig,
    connection: Connection,
    logger: Arc<Mutex<csv::Writer<std::fs::File>>>,
) -> Vec<(thread::JoinHandle<()>, thread::JoinHandle<()>)> {
    let on_core: Vec<(usize, usize)> = config.core_ids.iter().map(|c: &usize| {
        // Get the SMT threads for the Core
        let topo = hwloc::Topology::new();
        let core_depth = topo
            .depth_or_below_for_type(&hwloc::ObjectType::Core)
            .unwrap();
        let all_cores = topo.objects_at_depth(core_depth);

        for core in all_cores {
            for smt_thread in core.children().iter() {
                if smt_thread.os_index() == *c as u32 {
                    return (
                        smt_thread.os_index() as usize,
                        smt_thread
                            .next_sibling()
                            .expect("CPU doesn't have SMT (check that provided core_id is the min of the pair)?")
                            .os_index() as usize,
                    );
                }
            }
        }
        panic!("Invalid core id");
    }).collect();

    let set_rt: bool = config.scheduler == Scheduler::Fifo;

    let mut handles = Vec::with_capacity(on_core.len());
    let pair = on_core[0];
    let (txa, rxp) = mpsc::channel();
    let (txp, rxa) = mpsc::channel();

    let t_app_name = String::from("rserver/app");
    let t_app = thread::Builder::new()
        .name(t_app_name)
        .stack_size(4096 * 10)
        .spawn(move || loop {
            pin_thread(&vec![pair.0]);
            if set_rt {
                set_rt_fifo();
            }

            let payload = rxa.recv().expect("Can't receive data on app thread.");
            txa.send((payload, now()))
                .expect("Can't send data to network thread.");
        }).expect("Can't spawn application thread");

    let t_poll_name = String::from("rserver/polling");
    let t_poll = thread::Builder::new()
        .name(t_poll_name)
        .stack_size(4096 * 10)
        .spawn(move || {
            pin_thread(&vec![pair.1]);
            if set_rt {
                set_rt_fifo();
            }

            let channel = Some((txp, rxp));
            network_loop(&(config.clone()), connection, channel, logger)
        }).expect("Couldn't spawn a thread");

    handles.push((t_app, t_poll));
    handles
}

/// Create a single, TCP or UDP socket
fn make_socket(config: &AppConfig) -> Socket {
    let socket = match config.transport {
        Transport::Tcp => Socket::new(Domain::ipv4(), Type::stream(), None),
        Transport::Udp => Socket::new(Domain::ipv4(), Type::dgram(), None),
    }.expect("Can't create socket");
    if config.threads > 1 {
        debug!("Set socket reuse_port option");
        socket.set_reuse_port(true).expect("Can't set reuse port");
    }
    socket
}

fn find_incoming_address(config: &AppConfig) -> net::SocketAddrV4 {
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

fn timestamping_enable(config: &AppConfig, socket: RawFd) {
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

fn create_connections(config: &AppConfig, address: net::SocketAddrV4) -> Vec<Connection> {
    let mut connections: Vec<Connection> = Vec::with_capacity(config.threads);

    match config.transport {
        Transport::Tcp => {
            let socket = make_socket(config);
            socket.bind(&address.into()).expect("Can't bind to address");
            let listener = socket.into_tcp_listener();

            for _thread_id in 0..config.threads {
                let (stream, addr) = listener.accept().expect("Waiting for incoming connection");
                let stream = mio::net::TcpStream::from_stream(stream).expect("Make mio stream");
                info!("Incoming connection from {}", addr);
                timestamping_enable(config, stream.as_raw_fd());
                connections.push(Connection::Stream(stream));
            }
        }
        Transport::Udp => {
            let mut sockets = Vec::with_capacity(config.threads);
            // With SO_REUSEPORT we need to set the option before we bind,
            // in the match statement below
            for _thread in 0..config.threads {
                sockets.push(make_socket(config));
            }

            for socket in sockets {
                socket.bind(&address.into()).expect("Can't bind to address");
                timestamping_enable(config, socket.as_raw_fd());

                connections.push(Connection::Datagram(
                    mio::net::UdpSocket::from_socket(socket.into_udp_socket())
                        .expect("Couldn't make mio UDP socket from socket."),
                ));
            }
        }
    }

    connections
}

fn parse_args(matches: &clap::ArgMatches) -> (AppConfig, net::SocketAddrV4) {
    let config = AppConfig::parse(matches);
    let address = find_incoming_address(&config);
    (config, address)
}

fn set_thread_affinity(config: &AppConfig, thread_id: usize) {
    match config.mapping {
        ThreadMapping::All => {
            debug!(
                "Set affinity for thread {} to cpu {:?}",
                thread_id, config.core_ids
            );
            pin_thread(&config.core_ids);
        }
        ThreadMapping::OneToOne => {
            let core = config.core_ids[thread_id % config.core_ids.len()];
            debug!("Set affinity for thread {} to cpu {:?}", thread_id, core);
            pin_thread(&vec![core]);
        }
    }
}

fn set_scheduling(config: &AppConfig) {
    match config.scheduler {
        Scheduler::Fifo => set_rt_fifo(),
        Scheduler::None => debug!("Default scheduling"),
    }
}

fn main() {
    env_logger::init();
    let yaml = load_yaml!("cli.yml");
    let matches = App::from_yaml(yaml).get_matches();
    let (config, address) = parse_args(&matches);

    println!(
        "Listening on {} with process affinity set to {:?}.",
        address, config.core_ids
    );

    /*if let Some(_) = matches.subcommand_matches("smt") {
        let handles = spawn_listen_pair(config, connection, logger);
        for (tapp, tpoll) in handles {
            tapp.join().expect("Can't join app-thread.");
            tpoll.join().expect("Can't join poll-thread.")
        }
    } */

    let mut threads = Vec::with_capacity(config.threads);
    let connections = create_connections(&config, address);
    let logfile = format!("latencies-rserver-{}-{}.csv", address.port(), config.output);
    let logger = create_writer(logfile.clone(), LOGFILE_SIZE);

    if let Some(_) = matches.subcommand_matches("mt") {
        for (idx, connection) in connections.into_iter().enumerate() {
            let config = config.clone();
            let logger = logger.clone();
            let t = thread::Builder::new()
                .name(format!("rserver-{}", idx))
                .stack_size(1024 * 1024 * 2)
                .spawn(move || {
                    set_thread_affinity(&config, idx);
                    set_scheduling(&config);
                    network_loop(&config, connection, None, logger);
                }).expect("Couldn't spawn a thread");
            threads.push(t);
        }
        for thread in threads.into_iter() {
            thread.join().expect("Can't wait for thread?");
        }
    } else if let Some(_) = matches.subcommand_matches("single") {
        assert!(config.threads == 1);
        set_thread_affinity(&config, 0);
        set_scheduling(&config);
        let connection = create_connections(&config, address)
            .into_iter()
            .next()
            .unwrap();
        network_loop(&config, connection, None, logger);
    }
}
