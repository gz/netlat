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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use clap::App;
use mio::unix::{EventedFd, UnixReady};
use mio::Ready;
use nix::sys::socket;
use nix::sys::time;
use nix::sys::uio;

use netbench::*;

const PING: mio::Token = mio::Token(0);

#[derive(Debug, Eq, PartialEq)]
enum HandlerState {
    SendPacket,
    WaitForReply,
    ReadSentTimestamp,
}

#[derive(Debug)]
struct MessageState {
    log: LogRecord,
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

    fn complete(&self, timestamp_type: PacketTimestamp) -> bool {
        let nic_ts_done = timestamp_type == PacketTimestamp::None || self.nic_ts_set == 2;
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

fn log_packet(wtr: &Arc<Mutex<csv::Writer<std::fs::File>>>, record: &LogRecord) {
    let mut logfile = wtr.lock().unwrap();
    logfile.serialize(&record).expect("Can't write record.");
}

fn end_network_loop(
    wtr: &Arc<Mutex<csv::Writer<std::fs::File>>>,
    lost_messages: HashMap<u64, MessageState>,
    raw_fd: std::os::unix::io::RawFd,
    _config: &AppConfig,
) {
    let mut logfile = wtr.lock().unwrap();

    // Log the still outstanding messages (these were lost or dropped)
    for (_id, mst) in &lost_messages {
        logfile.serialize(&mst.log).expect("Can't write record.");
    }

    logfile.flush().expect("Can't flush the log");
    debug!("Sender done.");
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
) -> Result<(u64, nix::sys::socket::RecvMsg<'msg>), nix::Error> {
    let msg = try!(socket::recvmsg(
        raw_fd,
        &[uio::IoVec::from_mut_slice(recv_buf)],
        Some(time_rx),
        socket::MsgFlags::empty(),
    ));
    assert!(msg.bytes == 8);

    let id = recv_buf
        .as_slice()
        .read_u64::<BigEndian>()
        .expect("Can't parse timestamp");
    Ok((id, msg))
}

fn network_loop(
    config: AppConfig,
    connection: Connection,
    barrier: std::sync::Arc<std::sync::Barrier>,
    request_id: Arc<AtomicUsize>,
    wtr: Arc<Mutex<csv::Writer<std::fs::File>>>,
) {
    println!(
        "Sending {} requests to {:?} (flood = {})",
        config.requests, connection, config.flood
    );

    let poll = mio::Poll::new().expect("Can't create poll.");
    let mut events = mio::Events::with_capacity(10);

    let raw_fd: std::os::unix::io::RawFd = connection.as_raw_fd();

    poll.register(
        &EventedFd(&raw_fd),
        PING,
        Ready::writable() | Ready::readable() | UnixReady::error(),
        mio::PollOpt::edge() | mio::PollOpt::oneshot(),
    ).expect("Can't register events.");

    let mut packet_buffer = Vec::with_capacity(8);
    let mut recv_buf: Vec<u8> = Vec::with_capacity(8);
    recv_buf.resize(8, 0);

    let timeout = Duration::from_millis(2000); // After 2s we consider the packet lost
    let mut last_sent = Instant::now();

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
            debug!("Got {:?}", event);
            // Send a packet
            if (state_machine == HandlerState::SendPacket || config.flood)
                && event.readiness().is_writable()
            {
                let packet_id = request_id.fetch_add(1, Ordering::SeqCst) as u64;
                if packet_id > config.requests as u64 {
                    end_network_loop(&wtr, message_state, raw_fd, &config);
                    return;
                }

                // Temporarily change process name (for better traceability with perf)
                set_process_name(format!("pkt-{}", packet_id).as_str());
                let tx_app = now();

                packet_buffer
                    .write_u64::<BigEndian>(packet_id)
                    .expect("Serialize time");

                let bytes_sent =
                    match socket::send(raw_fd, &packet_buffer, socket::MsgFlags::empty()) {
                        Ok(bytes_sent) => bytes_sent,
                        //Err(nix::Error::Sys(nix::errno::Errno::EAGAIN)) => break,
                        Err(e) => panic!("Unexpected error during socket::send {:?}", e),
                    };

                assert_eq!(bytes_sent, 8);
                packet_buffer.clear();
                last_sent = Instant::now();

                message_state.insert(packet_id, MessageState::new(packet_id, tx_app));

                state_machine = HandlerState::WaitForReply;
                debug!("Sent packet {}", packet_id);
            }

            if
            //(state_machine == HandlerState::ReadSentTimestamp || config.flood) &&
            mio::unix::UnixReady::from(event.readiness()).is_error() {
                loop {
                    let (id, tx_nic) = match retrieve_tx_timestamp(raw_fd, &mut time_tx, &config) {
                        Ok(data) => data,
                        Err(nix::Error::Sys(nix::errno::Errno::EAGAIN)) => break,
                        Err(e) => panic!("Unexpected error during retrieve_tx_timestamp {:?}", e),
                    };

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
                        log_packet(&wtr, &mst.log);

                        if id == config.requests as u64 {
                            end_network_loop(&wtr, message_state, raw_fd, &config);
                            return;
                        } else {
                            // Send another packet
                            state_machine = HandlerState::SendPacket;
                        }
                    }
                }
            }

            // Receive a packet
            if (state_machine == HandlerState::WaitForReply || config.flood)
                && event.readiness().is_readable()
            {
                loop {
                    // Get the packet
                    let (id, msg) = match recvmsg(raw_fd, &mut recv_buf, &mut time_rx) {
                        Ok(res) => res,
                        Err(nix::Error::Sys(nix::errno::Errno::EAGAIN)) => break,
                        Err(e) => panic!("Unexpected error during socket::recvmsg {:?}", e),
                    };

                    debug!("Received packet {}", id);
                    let completed_record = {
                        let mut mst = message_state
                            .get_mut(&id)
                            .expect("Can't find state for incoming packet");
                        mst.log.rx_app = now();
                        set_process_name("netbench");
                        mst.set_rx_nic(read_nic_timestamp(&msg, config.timestamp));
                        // Sanity check that we measure the packet we sent...
                        assert!(id == mst.log.id);
                        mst.complete(config.timestamp)
                    };

                    if completed_record {
                        let mut mst = message_state
                            .remove(&id)
                            .expect("Can't remove complete packet");
                        mst.log.completed = true;
                        log_packet(&wtr, &mst.log);
                        if id == config.requests as u64 {
                            end_network_loop(&wtr, message_state, raw_fd, &config);
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
        }

        // Report error if we don't see a reply within 500ms and try again with another packet
        if state_machine == HandlerState::WaitForReply && last_sent.elapsed() > timeout {
            error!("Not getting any replies?");
            // Make sure we read the timestamp of the dropped packet so error queue is clear again
            // I guess this is the proper way to do it...
            match retrieve_tx_timestamp(raw_fd, &mut time_tx, &config) {
                Ok(_) => (), // Throw away result
                Err(nix::Error::Sys(nix::errno::Errno::EAGAIN)) => {
                    panic!("We should timestamp by now")
                }
                Err(e) => panic!("Unexpected error during retrieve_tx_timestamp {:?}", e),
            };
            state_machine = HandlerState::SendPacket;
        }

        // Reregister for events
        let opts = if state_machine == HandlerState::SendPacket {
            debug!("need to send packet, register for writing");
            Ready::writable() | Ready::readable() | UnixReady::error()
        } else {
            Ready::readable() | UnixReady::error()
        };

        // One-shot means we re-register every time we got an event.
        poll.reregister(
            &EventedFd(&raw_fd),
            PING,
            opts,
            mio::PollOpt::edge() | mio::PollOpt::oneshot(),
        ).expect("Can't re-register events.");
    }
}

fn create_connections(config: &AppConfig, address: net::SocketAddrV4) -> Vec<Connection> {
    let mut connections: Vec<Connection> = Vec::with_capacity(config.threads);
    let mut sockets = Vec::with_capacity(config.threads);

    match config.transport {
        Transport::Tcp => {
            for _destination in config.destinations.iter() {
                sockets.push(make_socket(config));
            }

            for (i, socket) in sockets.into_iter().enumerate() {
                let destination_address: net::SocketAddrV4 = config.destinations[i]
                    .parse()
                    .expect("Invalid host:port pair");
                socket
                    .connect(&destination_address.into())
                    .expect("Can't connect to address");
                timestamping_enable(&config, socket.as_raw_fd());
                connections.push(Connection::Stream(socket.into_tcp_stream()));
            }
        }
        Transport::Udp => {
            let start_src_port: u16 = 5000;
            for _destination in config.destinations.iter() {
                sockets.push(make_socket(config));
            }

            for (i, socket) in sockets.into_iter().enumerate() {
                let destination_address: net::SocketAddrV4 = config.destinations[i]
                    .parse()
                    .expect("Invalid host:port pair");

                let mut local_address = address.clone();
                local_address.set_port(start_src_port + i as u16);
                socket
                    .bind(&local_address.into())
                    .expect("Can't bind to address");
                socket
                    .connect(&destination_address.into())
                    .expect(format!("Can't connect to {}", destination_address).as_str());

                timestamping_enable(&config, socket.as_raw_fd());

                connections.push(Connection::Datagram(socket.into_udp_socket()));
            }
        }
    }

    connections
}

fn main() {
    env_logger::init();

    let yaml = load_yaml!("cli.yml");
    let matches = App::from_yaml(yaml).get_matches();
    let (config, address) = parse_args(&matches);
    let connections = create_connections(&config, address);

    debug!("Got {} recipients to send to.", connections.len());

    let barrier = Arc::new(Barrier::new(connections.len()));
    let mut handles = Vec::with_capacity(connections.len());

    let output = format!("latencies-client-{}.csv", config.output);
    let wtr = create_writer(output.clone(), LOGFILE_SIZE);
    let request_id = Arc::new(AtomicUsize::new(1));

    // Spawn a new thread for every client
    for (id, connection) in connections.into_iter().enumerate() {
        let barrier = barrier.clone();
        let config = config.clone();
        let wtr = wtr.clone();
        let request_id = request_id.clone();

        handles.push(thread::spawn(move || {
            set_thread_affinity(&config, id);
            set_scheduling(&config);
            network_loop(config, connection, barrier, request_id, wtr)
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    /* else if connections.len() == 1 {
        set_thread_affinity(&config, 0);
        set_scheduling(&config);

        for (destination, socket) in connections {
            let barrier = barrier.clone();
            let config = config.clone();
            network_loop(barrier, destination, socket, config, wtr);
        }
    }*/
}
