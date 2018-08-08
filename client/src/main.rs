extern crate byteorder;
extern crate csv;
extern crate mio;
#[macro_use]
extern crate log;
extern crate env_logger;
#[macro_use]
extern crate clap;
extern crate netbench;
extern crate nix;
extern crate prctl;

use std::collections::HashMap;
use std::net;
use std::os::unix::io::AsRawFd;
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use clap::App;

use nix::sys::socket;
use nix::sys::time;
use nix::sys::uio;

use netbench::AppConfig;
use netbench::Connection;

const PING: mio::Token = mio::Token(0);

#[derive(Debug, Eq, PartialEq)]
enum HandlerState {
    SendPacket,
    WaitForReply,
    ReadSentTimestamp,
}

#[derive(Debug)]
struct MessageState {
    log: netbench::LogRecord,
    nic_ts_set: usize,
}

impl MessageState {
    fn new(id: u64, tx_app: u64) -> MessageState {
        let mut ms: MessageState = Default::default();
        ms.log.id = id;
        ms.log.tx_app = tx_app;
        ms.log.completed = false;
        ms
    }

    fn set_tx_nic(&mut self, tx_nic: u64) {
        self.log.tx_nic = tx_nic;
        self.nic_ts_set += 1;
    }

    fn set_rx_nic(&mut self, rx_nic: u64) {
        self.log.rx_nic = rx_nic;
        self.nic_ts_set += 1;
    }

    fn complete(&self, timestamp_type: netbench::PacketTimestamp) -> bool {
        let nic_ts_done = timestamp_type == netbench::PacketTimestamp::None || self.nic_ts_set == 2;
        let app_ts_done = self.log.rx_app != 0 && self.log.tx_app != 0;
        return nic_ts_done && app_ts_done;
    }
}

impl Default for MessageState {
    fn default() -> MessageState {
        MessageState {
            log: Default::default(),
            nic_ts_set: 0,
        }
    }
}

fn parse_args(matches: &clap::ArgMatches) -> (Vec<(net::SocketAddr, Connection)>, AppConfig) {
    let config = AppConfig::parse(matches);

    let interface = std::ffi::CString::new(config.interface.clone()).expect("Can't be null");
    let source_address: socket::SockAddr = {
        unsafe {
            let interface_addr = netbench::getifaceaddr(interface.as_ptr());
            socket::SockAddr::from_libc_sockaddr(&interface_addr).expect("Address")
        }
    };

    let start_src_port: u16 = 5000;
    let connections = config
        .destinations
        .iter()
        .enumerate()
        .map(|(i, recipient)| {
            match source_address {
                socket::SockAddr::Inet(s) => {
                    let destination = recipient.parse().expect("Invalid host:port pair");

                    let connection = if config.transport == netbench::Transport::Tcp {
                        debug!("Opening a TCP connection");
                        let stream = mio::net::TcpStream::connect(&destination)
                            .expect("Couldn't connect to TCP stream.");
                        //stream.set_nodelay(true);
                        unsafe {
                            let r = netbench::enable_packet_timestamps(
                                stream.as_raw_fd(),
                                interface.as_ptr(),
                                config.timestamp,
                            );
                            if r != 0 {
                                panic!(
                                    "Failed to enable NIC timestamps (ret {}): {}",
                                    r,
                                    nix::errno::Errno::last()
                                );
                            }
                        }
                        Connection::Stream(stream)
                    } else {
                        debug!("Opening a UDP connection");
                        // Construct socket on correct interface
                        let mut addr = s.to_std();
                        addr.set_port(start_src_port + i as u16);

                        let socket = mio::net::UdpSocket::bind(&addr)
                            .expect("Couldn't bind UDP socket to address.");
                        unsafe {
                            let r = netbench::enable_packet_timestamps(
                                socket.as_raw_fd(),
                                interface.as_ptr(),
                                config.timestamp,
                            );
                            if r != 0 {
                                panic!(
                                    "Failed to enable NIC timestamps (ret {}): {}",
                                    r,
                                    nix::errno::Errno::last()
                                );
                            }
                        }
                        socket
                            .connect(destination)
                            .expect("Can't connect to server");

                        Connection::Datagram(socket)
                    };

                    return (destination, connection);
                }
                _ => panic!("Got invalid address from iface"),
            };
        })
        .collect();

    (connections, config)
}

fn log_packet(
    wtr: &Arc<Mutex<csv::Writer<std::fs::File>>>,
    record: &netbench::LogRecord,
    packets: &mut u64,
) {
    let mut logfile = wtr.lock().unwrap();
    logfile.serialize(&record).expect("Can't write record.");
    *packets = *packets + 1;
}

fn end_network_loop(
    wtr: &Arc<Mutex<csv::Writer<std::fs::File>>>,
    lost_messages: HashMap<u64, MessageState>,
    raw_fd: std::os::unix::io::RawFd,
    destination: &net::SocketAddr,
    _config: &AppConfig,
) {
    let mut logfile = wtr.lock().unwrap();

    // Log the still outstanding messages (these were lost or dropped)
    for (_id, mst) in &lost_messages {
        logfile.serialize(&mst.log).expect("Can't write record.");
    }

    logfile.flush().expect("Can't flush the log");
    debug!("Sender for {} done.", destination);
    // We're done sending requests, stop
    let mut packet_buffer = Vec::with_capacity(8);

    // Send 0 packet to rserver so it flushed the log
    packet_buffer
        .write_u64::<BigEndian>(0)
        .expect("Serialize time");

    socket::send(raw_fd, &packet_buffer, socket::MsgFlags::empty())
        .expect("Sending packet failed.");
    packet_buffer.clear();
}
// We make sure the recvmsg calls don't get inlined here this makes it easier for the tracing infrastructure...
#[inline(never)]
fn recvmsg<'msg>(
    raw_fd: std::os::unix::io::RawFd,
    recv_buf: &'msg mut Vec<u8>,
    time_rx: &'msg mut socket::CmsgSpace<[time::TimeVal; 3]>,
) -> (u64, nix::sys::socket::RecvMsg<'msg>) {
    let msg = socket::recvmsg(
        raw_fd,
        &[uio::IoVec::from_mut_slice(recv_buf)],
        Some(time_rx),
        socket::MsgFlags::empty(),
    ).expect("Can't receive message");
    assert!(msg.bytes == 8);

    let id = recv_buf
        .as_slice()
        .read_u64::<BigEndian>()
        .expect("Can't parse timestamp");
    (id, msg)
}

fn network_loop(
    barrier: std::sync::Arc<std::sync::Barrier>,
    destination: net::SocketAddr,
    connection: Connection,
    config: AppConfig,
) {
    let output = format!(
        "latencies-client-{}-{}.csv",
        destination.port(),
        config.output
    );
    let wtr = netbench::create_writer(output.clone(), 50 * 1024 * 1024);
    if config.scheduler == netbench::Scheduler::Fifo {
        netbench::set_rt_fifo();
    }

    println!(
        "Sending {} requests to {} writing latencies to {} (flood = {})",
        config.requests, destination, output, config.flood
    );

    let poll = mio::Poll::new().expect("Can't create poll.");
    let mut events = mio::Events::with_capacity(1024);

    let raw_fd: std::os::unix::io::RawFd = match connection {
        Connection::Datagram(ref sock) => {
            debug!("Register for UDP send/receive events");
            poll.register(
                sock,
                PING,
                mio::Ready::writable() | mio::Ready::readable() | mio::unix::UnixReady::error(),
                mio::PollOpt::level(),
            ).expect("Can't register send event.");
            sock.as_raw_fd()
        }
        Connection::Stream(ref stream) => {
            debug!("Register for TCP send/receive events");
            poll.register(
                stream,
                PING,
                mio::Ready::writable() | mio::Ready::readable() | mio::unix::UnixReady::error(),
                mio::PollOpt::level(),
            ).expect("Can't register send event.");
            stream.as_raw_fd()
        }
    };

    let mut packet_buffer = Vec::with_capacity(8);
    let mut recv_buf: Vec<u8> = Vec::with_capacity(8);
    recv_buf.resize(8, 0);

    let timeout = Duration::from_millis(1000); // After 1000ms we consider the packet lost
    let mut last_sent = Instant::now();
    let mut packet_count: u64 = 0; // Number of packets completed
    let mut packet_id: u64 = 1; // Unique ID included in every packet (must start at one since ID zero signals end of test on server)

    let mut time_rx: socket::CmsgSpace<[time::TimeVal; 3]> = socket::CmsgSpace::new();
    let mut time_tx: socket::CmsgSpace<[time::TimeVal; 3]> = socket::CmsgSpace::new();
    let mut state_machine = HandlerState::SendPacket;

    // Packets inflight (in case flood is false, this should be size 1)
    let mut message_state: HashMap<u64, MessageState> = HashMap::with_capacity(1024);

    barrier.wait();
    debug!("Start sending...");
    loop {
        poll.poll(&mut events, Some(Duration::from_millis(1000)))
            .expect("Can't poll channel");
        //debug!("events is = {:?}", events);
        for event in events.iter() {
            //debug!("Got event is = {:?}", event);
            // Send a packet
            if (state_machine == HandlerState::SendPacket || config.flood)
                && event.readiness().is_writable()
            {
                // Temporarily change process name (for better traceability with)
                netbench::set_process_name(format!("pkt-{}", packet_id).as_str());
                let tx_app = netbench::now();

                packet_buffer
                    .write_u64::<BigEndian>(packet_id)
                    .expect("Serialize time");

                let bytes_sent = socket::send(raw_fd, &packet_buffer, socket::MsgFlags::empty())
                    .expect("Sending packet failed.");

                assert_eq!(bytes_sent, 8);
                packet_buffer.clear();
                last_sent = Instant::now();

                message_state.insert(packet_id, MessageState::new(packet_id, tx_app));
                packet_id = packet_id + 1;

                state_machine = HandlerState::WaitForReply;
                debug!("Sent packet {}", packet_id - 1);
            }

            if (state_machine == HandlerState::ReadSentTimestamp || config.flood)
                && mio::unix::UnixReady::from(event.readiness()).is_error()
            {
                let (id, tx_nic) = netbench::retrieve_tx_timestamp(
                    raw_fd,
                    &mut time_tx,
                    config.timestamp,
                    &connection,
                );

                debug!("Retrieve tx timestamp {} for id {}", tx_nic, id);

                let completed_record = {
                    let mut mst = message_state
                        .get_mut(&id)
                        .expect(format!("Can't find state for packet {}", id).as_str());
                    mst.set_tx_nic(tx_nic);
                    mst.complete(config.timestamp)
                };

                if completed_record {
                    let mut mst = message_state
                        .remove(&id)
                        .expect("Can't remove completed packet");
                    mst.log.completed = true;
                    log_packet(&wtr, &mst.log, &mut packet_count);
                    if packet_count == config.requests {
                        end_network_loop(&wtr, message_state, raw_fd, &destination, &config);
                        return;
                    } else {
                        // Send another packet
                        state_machine = HandlerState::SendPacket;
                    }
                }
            }

            // Receive a packet
            if (state_machine == HandlerState::WaitForReply || config.flood)
                && event.readiness().is_readable()
            {
                // Get the packet
                let (id, msg) = recvmsg(raw_fd, &mut recv_buf, &mut time_rx);
                debug!("Received packet {}", id);
                let completed_record = {
                    let mut mst = message_state
                        .get_mut(&id)
                        .expect("Can't find state for incoming packet");
                    mst.log.rx_app = netbench::now();
                    netbench::set_process_name("netbench");
                    mst.set_rx_nic(netbench::read_nic_timestamp(&msg, config.timestamp));
                    // Sanity check that we measure the packet we sent...
                    assert!(id == mst.log.id);
                    mst.complete(config.timestamp)
                };

                if completed_record {
                    let mut mst = message_state
                        .remove(&id)
                        .expect("Can't remove complete packet");
                    mst.log.completed = true;
                    log_packet(&wtr, &mst.log, &mut packet_count);
                    if packet_count == config.requests {
                        end_network_loop(&wtr, message_state, raw_fd, &destination, &config);
                        return;
                    } else {
                        debug!("Send another packet");
                        state_machine = HandlerState::SendPacket;
                    }
                } else {
                    debug!("Trying to fetch timestamp for packet");
                    state_machine = HandlerState::ReadSentTimestamp;
                }
            }
        }

        // Report error if we don't see a reply within 500ms and try again with another packet
        if state_machine == HandlerState::WaitForReply && last_sent.elapsed() > timeout {
            error!("Not getting any replies?");
            // Make sure we read the timestamp of the dropped packet so error queue is clear again
            // I guess this is the proper way to do it...
            netbench::retrieve_tx_timestamp(raw_fd, &mut time_tx, config.timestamp, &connection);
            state_machine = HandlerState::SendPacket;
        }
    }
}

fn main() {
    env_logger::init();

    let yaml = load_yaml!("cli.yml");
    let matches = App::from_yaml(yaml).get_matches();
    let (connections, config) = parse_args(&matches);

    debug!("Got {} recipients to send to.", connections.len());

    let barrier = Arc::new(Barrier::new(connections.len()));
    let mut handles = Vec::with_capacity(connections.len());

    // Spawn a new thread for every client
    if connections.len() > 1 {
        println!("making some threads");
        for (destination, socket) in connections {
            let barrier = barrier.clone();
            let config = config.clone();

            handles.push(thread::spawn(move || {
                config.core_id.map(|id: usize| {
                    debug!("Pin to core {}.", id);
                    netbench::pin_thread(vec![id]);
                });

                network_loop(barrier, destination, socket, config)
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }
    } else if connections.len() == 1 {
        // Don't spawn a thread, less of a nightmare with perf...
        config.core_id.map(|id: usize| {
            debug!("Pin to core {}.", id);
            netbench::pin_thread(vec![id]);
        });
        for (destination, socket) in connections {
            let barrier = barrier.clone();
            let config = config.clone();
            network_loop(barrier, destination, socket, config);
        }
    }
}
