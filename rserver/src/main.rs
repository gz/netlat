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

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use clap::App;
use mio::unix::{EventedFd, UnixReady};
use mio::Ready;
use nix::sys::socket;
use nix::sys::time;
use nix::sys::uio;

use netbench::*;

#[derive(Debug)]
struct MessageState {
    sender: Option<socket::SockAddr>,
    sock: RawFd,
    log: LogRecord,
}

impl MessageState {
    fn new(
        id: u64,
        sender: Option<socket::SockAddr>,
        sock: RawFd,
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
            sock: sock,
            log: log,
        }
    }

    fn linux_rx_latency(&self) -> u64 {
        (self.log.rx_app + 36 * 1_000_000_000) - self.log.rx_nic
    }
}

fn network_loop(
    config: &AppConfig,
    //connections: Vec<Arc<Mutex<RawFd>>>,
    connections: Vec<RawFd>,
    app_channel: Option<(mpsc::Sender<u64>, mpsc::Receiver<(u64, u64)>)>,
    wtr: Arc<Mutex<csv::Writer<std::fs::File>>>,
    send_state: HashMap<RawFd, Arc<Mutex<VecDeque<MessageState>>>>,
    ts_state: Arc<Mutex<HashMap<u64, MessageState>>>,
) {
    let poll = mio::Poll::new().expect("Can't create poll.");

    for (idx, connection) in connections.iter().enumerate() {
        //let connection = connection.lock().unwrap();
        poll.register(
            &EventedFd(&connection),
            mio::Token(idx),
            Ready::readable() | UnixReady::error(),
            mio::PollOpt::edge() | mio::PollOpt::oneshot(),
        ).expect("Can't register events.");
    }

    let mut events = mio::Events::with_capacity(10);
    let mut recv_buf: Vec<u8> = Vec::with_capacity(8);
    recv_buf.resize(8, 0);
    let mut write_saw_egain: bool = false;

    let mut min_seen: u64 = u64::max_value();

    let mut time_rx: socket::CmsgSpace<[time::TimeVal; 3]> = socket::CmsgSpace::new();
    let mut time_tx: socket::CmsgSpace<[time::TimeVal; 3]> = socket::CmsgSpace::new();

    // If this fails think about allocating it on the heap instead of copy?
    assert!(std::mem::size_of::<MessageState>() < 256);

    debug!("Set process name to {}", format!("pkt-{}", 1));
    set_process_name(format!("pkt-{}", 1).as_str());

    loop {
        poll.poll(&mut events, None).expect("Can't poll channel");
        for event in events.iter() {
            debug!("event = {:?}", event);
            //let raw_fd: RawFd = *connections[event.token().0].lock().unwrap();
            let raw_fd: RawFd = connections[event.token().0];

            if UnixReady::from(event.readiness()).is_error() {
                loop {
                    let (id, tx_nic) = match retrieve_tx_timestamp(raw_fd, &mut time_tx, config) {
                        Ok(data) => data,
                        Err(nix::Error::Sys(nix::errno::Errno::EAGAIN)) => break,
                        Err(e) => panic!("Unexpected error during retrieve_tx_timestamp {:?}", e),
                    };

                    debug!("read from err queue id={} tx_nic={}", id, tx_nic);
                    let mut ts_state = ts_state.lock().unwrap();
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
                        },
                    );

                    debug!("Set process name to {}", format!("pkt-{}", id + 1));
                    set_process_name(format!("pkt-{}", id + 1).as_str());
                }
            }

            // Receive a packet
            if event.readiness().is_readable() {
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
                        Err(nix::Error::Sys(nix::errno::Errno::ENOMSG)) => break,
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
                    debug!("Received packet id={}", packet_id);

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
                        let send_state = send_state[&raw_fd].lock().unwrap();
                        let ts_state = ts_state.lock().unwrap();
                        let mut logfile = wtr.lock().unwrap();

                        // Log packets that probably didn't make it back
                        for packet in &*send_state {
                            logfile.serialize(&packet.log).expect("Can't write record.");
                        }
                        for (_id, packet) in &*ts_state {
                            logfile.serialize(&packet.log).expect("Can't write record.");
                        }
                        logfile.flush().expect("Can't flush logfile");
                        println!("Min latency was {}", min_seen);
                    } else {
                        debug!("Storing packet in send_state");
                        let mst = MessageState::new(
                            packet_id,
                            msg.address,
                            raw_fd,
                            rx_app,
                            rx_nic,
                            rx_ht,
                        );
                        if packet_id % 5000 == 0 {
                            println!("packet {} took {} ns", packet_id, mst.linux_rx_latency());
                        }
                        if mst.linux_rx_latency() < min_seen {
                            min_seen = mst.linux_rx_latency();
                        }
                        let mut send_state = send_state[&raw_fd].lock().unwrap();
                        (*send_state).push_back(mst);
                    }
                }
            }

            // Send a packet
            // TODO: make sure we try to send the first time we receive something again
            if event.readiness().is_writable() || !write_saw_egain {
                let mut send_state = send_state[&raw_fd].lock().unwrap();
                while send_state.len() > 0 {
                    let mut st = send_state.pop_front().expect("We need an item");
                    assert!(config.transport != Transport::Tcp); // fix st.sock == raw_fd

                    st.log.tx_app = now();
                    recv_buf.clear();
                    recv_buf
                        .write_u64::<BigEndian>(st.log.id)
                        .expect("Can't serialize payload");

                    let mut ts_state = ts_state.lock().unwrap();

                    let bytes_sent = if st.sender.is_some() {
                        match socket::sendto(
                            raw_fd,
                            &recv_buf,
                            &st.sender.unwrap(),
                            socket::MsgFlags::empty(),
                        ) {
                            Ok(bytes_sent) => bytes_sent,
                            Err(nix::Error::Sys(nix::errno::Errno::EAGAIN)) => {
                                debug!("Got EAGAIN, when trying to reply...");
                                write_saw_egain = true;
                                send_state.push_front(st);
                                break;
                            }
                            Err(nix::Error::Sys(nix::errno::Errno::ENOMSG)) => {
                                debug!("Got ENOMSG, when trying to reply...");
                                write_saw_egain = true;
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

                    debug!("Sent reply for id={}", st.log.id);
                    assert_eq!(bytes_sent, 8);
                    ts_state.insert(st.log.id, st);
                }
            }

            let send_state = send_state[&raw_fd].lock().unwrap();
            // Reregister for event on the FD
            let opts = if send_state.len() == 0 {
                Ready::readable() | UnixReady::error()
            } else {
                debug!("Unable to send everything, register for writable");
                Ready::writable() | Ready::readable() | UnixReady::error()
            };

            debug!("Reregister for {:?} with opts {:?}", raw_fd, opts);
            poll.reregister(
                &EventedFd(&raw_fd),
                mio::Token(event.token().0),
                opts,
                mio::PollOpt::edge() | mio::PollOpt::oneshot(),
            ).expect("Can't re-register events.");
        }
    }
}

fn _spawn_listen_pair(
    config: AppConfig,
    _connections: Vec<RawFd>,
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
    let _pair = on_core[0];
    //let (txa, rxp) = mpsc::channel();
    //let (txp, rxa) = mpsc::channel();

    let t_app_name = String::from("rserver/app");
    let t_app = thread::Builder::new()
        .name(t_app_name)
        .stack_size(4096 * 10)
        .spawn(move || loop {
            //pin_thread(&vec![pair.0]);
            if set_rt {
                //set_rt_fifo();
            }

            //let payload = rxa.recv().expect("Can't receive data on app thread.");
            //txa.send((payload, now()))
            //    .expect("Can't send data to network thread.");
        }).expect("Can't spawn application thread");

    let t_poll_name = String::from("rserver/polling");
    let t_poll = thread::Builder::new()
        .name(t_poll_name)
        .stack_size(4096 * 10)
        .spawn(move || {
            //pin_thread(&vec![pair.1]);
            if set_rt {
                //set_rt_fifo();
            }

            //let channel = Some((txp, rxp));
            //network_loop(&(config.clone()), vec![], channel, logger)
        }).expect("Couldn't spawn a thread");

    handles.push((t_app, t_poll));
    handles
}

fn create_connections(config: &AppConfig, address: net::SocketAddrV4) -> Vec<Connection> {
    let mut connections: Vec<Connection> = Vec::with_capacity(config.sockets);

    match config.transport {
        Transport::Tcp => {
            let socket = make_socket(config);
            socket
                .set_nonblocking(false)
                .expect("Can't unset nonblocking mode for listener");
            socket.bind(&address.into()).expect("Can't bind to address");
            let listener = socket.into_tcp_listener();

            for _sock_id in 0..config.sockets {
                let (stream, addr) = listener.accept().expect("Waiting for incoming connection");
                info!("Incoming connection from {}", addr);
                timestamping_enable(config, stream.as_raw_fd());
                connections.push(Connection::Stream(stream));
            }
        }
        Transport::Udp => {
            let mut sockets = Vec::with_capacity(config.sockets);
            // With SO_REUSEPORT we need to set the option before we bind,
            // in the match statement below
            for _sock_id in 0..config.sockets {
                sockets.push(make_socket(config));
            }

            for socket in sockets {
                socket.bind(&address.into()).expect("Can't bind to address");
                timestamping_enable(config, socket.as_raw_fd());
                connections.push(Connection::Datagram(socket.into_udp_socket()));
            }
        }
    }

    assert!(connections.len() == config.sockets);

    connections
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

    debug!("{:#?}", config);

    /*if let Some(_) = matches.subcommand_matches("smt") {
        let handles = spawn_listen_pair(config, connection, logger);
        for (tapp, tpoll) in handles {
            tapp.join().expect("Can't join app-thread.");
            tpoll.join().expect("Can't join poll-thread.")
        }
    } */

    let mut threads = Vec::with_capacity(config.threads);
    let connections = create_connections(&config, address);
    assert!(connections.len() == config.sockets);
    //let raw_connections: Vec<Arc<Mutex<RawFd>>> = connections
    let raw_connections: Vec<RawFd> = connections
        .iter()
        //.map(|sock| Arc::new(Mutex::new(sock.as_raw_fd())))
        .map(|sock| sock.as_raw_fd())
        .collect();

    let logfile = format!("latencies-rserver-{}.csv", config.output);
    let logger = create_writer(logfile.clone(), LOGFILE_SIZE);

    let mut send_state: HashMap<RawFd, Arc<Mutex<VecDeque<MessageState>>>> =
        HashMap::with_capacity(raw_connections.len());
    for fd in raw_connections.iter() {
        send_state.insert(*fd, Arc::new(Mutex::new(VecDeque::with_capacity(1024))));
    }
    assert!(send_state.len() == raw_connections.len());
    let ts_state: Arc<Mutex<HashMap<u64, MessageState>>> =
        Arc::new(Mutex::new(HashMap::with_capacity(1024)));

    if let Some(_) = matches.subcommand_matches("mt") {
        for idx in 0..config.threads {
            let config = config.clone();
            let logger = logger.clone();
            let send_state = send_state.clone();
            let ts_state = ts_state.clone();
            let connections_to_handle = match config.socketmapping {
                SocketMapping::All => raw_connections.clone(),
                SocketMapping::OneToOne => {
                    // If every core gets just one socket we should have as many sockets as cores
                    assert!(config.threads == config.sockets);
                    vec![raw_connections[idx].clone()]
                }
            };

            let t = thread::Builder::new()
                .name(format!("rserver-{}", idx))
                .stack_size(1024 * 1024 * 2)
                .spawn(move || {
                    let pinned_to = set_thread_affinity(&config, idx);
                    set_scheduling(&config);
                    info!(
                        "Spawning network thread {} on cores {:?} scheduled as {:?} to handle connection(s) = {:?}",
                        idx, pinned_to, config.scheduler, connections_to_handle
                    );
                    network_loop(&config, connections_to_handle, None, logger, send_state, ts_state);
                }).expect("Couldn't spawn a thread");
            threads.push(t);
        }
        for thread in threads.into_iter() {
            thread.join().expect("Can't wait for thread?");
        }
    }

    /*else if let Some(_) = matches.subcommand_matches("single") {
        assert!(config.threads == 1);
        set_thread_affinity(&config, 0);
        set_scheduling(&config);
        network_loop(&config, raw_connections, None, logger);
    }*/
}
