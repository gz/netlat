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

use std::net;
use std::os::unix::io::AsRawFd;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use clap::App;

use nix::sys::socket;
use nix::sys::time;
use nix::sys::uio;

const PING: mio::Token = mio::Token(0);

#[derive(Debug, Eq, PartialEq)]
enum HandlerState {
    SendPacket,
    WaitForReply,
    ReadSentTimestamp,
}

fn parse_args(
    matches: &clap::ArgMatches,
) -> (Vec<(net::SocketAddr, net::UdpSocket)>, String, bool, u64) {
    let recipients = values_t!(matches, "destinations", String).unwrap();
    let iface = String::from(matches.value_of("iface").unwrap_or("enp130s0"));
    let interface = std::ffi::CString::new(iface.clone()).expect("Can't be null");
    let suffix = value_t!(matches, "name", String).unwrap_or(String::from("none"));
    let timestamping = matches.is_present("timestamps");
    let requests = value_t!(matches, "requests", u64).unwrap_or(250000);

    let source_address: socket::SockAddr = {
        unsafe {
            let interface_addr = netbench::getifaceaddr(interface.as_ptr());
            socket::SockAddr::from_libc_sockaddr(&interface_addr).expect("Address")
        }
    };

    let start_src_port: u16 = 5000;
    let sockets = recipients
        .iter()
        .enumerate()
        .map(|(i, recipient)| {
            match source_address {
                socket::SockAddr::Inet(s) => {
                    // Construct socket on correct interface
                    let mut addr = s.to_std();
                    addr.set_port(start_src_port + i as u16);
                    let sender = net::UdpSocket::bind(addr).expect("Can't bind");

                    // Enable timestamping for the socket
                    if timestamping {
                        debug!("Enable timestamps for {}", iface);
                        unsafe {
                            let r =
                                netbench::enable_hwtstamp(sender.as_raw_fd(), interface.as_ptr());
                            if r != 0 {
                                panic!(
                                    "HW timstamping enable failed (ret {}): {}",
                                    r,
                                    nix::errno::Errno::last()
                                );
                            }
                        }
                    };

                    // Connect to server
                    let destination = recipient.parse().expect("Invalid host:port pair");
                    sender
                        .connect(destination)
                        .expect("Can't connect to server");

                    return (destination, sender);
                }
                _ => panic!("Got invalid address from iface"),
            };
        })
        .collect();

    (sockets, suffix, timestamping, requests)
}

fn network_loop(
    barrier: std::sync::Arc<std::sync::Barrier>,
    destination: net::SocketAddr,
    socket: net::UdpSocket,
    suffix: String,
    requests: u64,
    nic_timestamps: bool,
) {
    let output = format!("latencies-client-{}-{}.csv", destination.port(), suffix);
    let wtr = netbench::create_writer(output.clone(), 50 * 1024 * 1024);

    println!(
        "Sending {} requests to {} writing latencies to {}",
        requests, destination, output
    );

    let mio_socket = mio::net::UdpSocket::from_socket(socket).expect("Can make socket");
    let poll = mio::Poll::new().expect("Can't create poll.");
    poll.register(
        &mio_socket,
        PING,
        mio::Ready::writable() | mio::Ready::readable() | mio::unix::UnixReady::error(),
        mio::PollOpt::level(),
    ).expect("Can't register send event.");

    let mut packet_buffer = Vec::with_capacity(8);
    let mut events = mio::Events::with_capacity(1024);

    let mut recv_buf: Vec<u8> = Vec::with_capacity(8);
    recv_buf.resize(8, 0);

    let timeout = Duration::from_millis(500); // At that point we consider the UDP packet lost
    let mut last_sent = Instant::now();
    let mut packet_count = 0;

    let mut time_rx: socket::CmsgSpace<[time::TimeVal; 3]> = socket::CmsgSpace::new();
    let mut time_tx: socket::CmsgSpace<[time::TimeVal; 3]> = socket::CmsgSpace::new();

    let mut rx_app = 0;
    let mut rx_nic = 0;
    let mut tx_app = 0;
    let mut state_machine = HandlerState::SendPacket;

    barrier.wait();
    debug!("Start sending...");
    loop {
        poll.poll(&mut events, Some(Duration::from_millis(100)))
            .expect("Can't poll channel");
        for event in events.iter() {
            // Send a packet
            if state_machine == HandlerState::SendPacket && event.readiness().is_writable() {
                tx_app = netbench::now();

                packet_buffer
                    .write_u64::<BigEndian>(tx_app)
                    .expect("Serialize time");

                let bytes_sent = socket::send(
                    mio_socket.as_raw_fd(),
                    &packet_buffer,
                    socket::MsgFlags::empty(),
                ).expect("Sending packet failed.");

                assert_eq!(bytes_sent, 8);
                packet_buffer.clear();
                last_sent = Instant::now();
                state_machine = HandlerState::WaitForReply;
            }

            if state_machine == HandlerState::ReadSentTimestamp
                && mio::unix::UnixReady::from(event.readiness()).is_error()
            {
                debug!("Reading timestamp");
                let tx_nic = if nic_timestamps {
                    netbench::retrieve_tx_timestamp(mio_socket.as_raw_fd(), &mut time_tx)
                        .expect("NIC Timestamps not enabled?")
                } else {
                    0
                };

                // Log all the timestamps
                let mut logfile = wtr.lock().unwrap();
                logfile
                    .serialize(netbench::LogRecord {
                        rx_app: rx_app,
                        rx_nic: rx_nic,
                        tx_app: tx_app,
                        tx_nic: tx_nic,
                    })
                    .expect("Can't write record.");

                packet_count = packet_count + 1;
                if packet_count == requests {
                    logfile.flush().expect("Can't flush the log");
                    debug!("Sender for {} done.", destination);
                    // We're done sending requests, stop
                    return;
                } else {
                    // Send another packet
                    state_machine = HandlerState::SendPacket;
                }
            }

            // Receive a packet
            if state_machine == HandlerState::WaitForReply && event.readiness().is_readable() {
                debug!("Received ts packet");

                // Get the packet
                let msg = socket::recvmsg(
                    mio_socket.as_raw_fd(),
                    &[uio::IoVec::from_mut_slice(&mut recv_buf)],
                    Some(&mut time_rx),
                    socket::MsgFlags::empty(),
                ).expect("Can't receive message");
                rx_app = netbench::now();
                rx_nic = if nic_timestamps {
                    netbench::read_nic_timestamp(&msg).expect("NIC Timestamps not enabled?")
                } else {
                    0
                };
                assert!(msg.bytes == 8);

                // Sanity check that we measure the packet we sent...
                let sent = recv_buf
                    .as_slice()
                    .read_u64::<BigEndian>()
                    .expect("Can't parse timestamp");
                assert!(sent == tx_app);

                debug!("received packet");
                state_machine = HandlerState::ReadSentTimestamp;
            }
        }

        // Report error if we don't see a reply within 500ms and try again with another packet
        if state_machine == HandlerState::WaitForReply && last_sent.elapsed() > timeout {
            error!("Dropped packet?");
            if nic_timestamps {
                // Make sure we read the timestamp of the dropped packet so error queue is clear again
                // I guess this is the proper way to do it...
                netbench::retrieve_tx_timestamp(mio_socket.as_raw_fd(), &mut time_tx)
                    .expect("NIC Timestamps not enabled?");
            }
            state_machine = HandlerState::SendPacket;
        }
    }
}

fn main() {
    env_logger::init();

    let yaml = load_yaml!("cli.yml");
    let matches = App::from_yaml(yaml).get_matches();
    let (sockets, suffix, nic_timestamps, requests) = parse_args(&matches);

    debug!("Got {} recipients to send to.", sockets.len());

    let barrier = Arc::new(Barrier::new(sockets.len()));
    let mut handles = Vec::with_capacity(sockets.len());

    // Spawn a new thread for every client
    for (destination, socket) in sockets {
        let barrier = barrier.clone();
        let suffix = suffix.clone();

        handles.push(thread::spawn(move || {
            network_loop(
                barrier,
                destination,
                socket,
                suffix,
                requests,
                nic_timestamps,
            )
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }
}
