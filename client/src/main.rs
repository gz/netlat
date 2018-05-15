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

use mio::{Events, Poll, PollOpt, Ready, Token};

use nix::sys::socket;
use nix::sys::time;
use nix::sys::uio;

const PING: Token = Token(0);

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
    let wtr = netbench::create_writer(output.clone());

    println!(
        "Sending {} requests to {} writing latencies to {}",
        requests, destination, output
    );

    let mio_socket = mio::net::UdpSocket::from_socket(socket).expect("Can make socket");
    let poll = Poll::new().expect("Can't create poll.");
    poll.register(
        &mio_socket,
        PING,
        Ready::writable() | Ready::readable(),
        PollOpt::level(),
    ).expect("Can't register send event.");

    let mut packet_buffer = Vec::with_capacity(8);
    let mut events = Events::with_capacity(1024);

    let mut recv_buf: Vec<u8> = Vec::with_capacity(8);
    recv_buf.resize(8, 0);

    let mut waiting_for_reply = false;
    let timeout = Duration::from_millis(500); // At that point we consider the UDP packet lost
    let mut last_sent = Instant::now();
    let mut packet_count = 0;
    let mut time_rx: socket::CmsgSpace<[time::TimeVal; 3]> = socket::CmsgSpace::new();
    let mut time_tx: socket::CmsgSpace<[time::TimeVal; 3]> = socket::CmsgSpace::new();

    // XXX: Code currently assumes closed loop system
    let mut tx_app = 0;
    let mut tx_nic = 0;

    barrier.wait();
    debug!("Start sending...");
    loop {
        poll.poll(&mut events, Some(Duration::from_millis(100)))
            .expect("Can't poll channel");
        for event in events.iter() {
            // Send a packet
            if !waiting_for_reply && event.readiness().is_writable() {
                tx_app = netbench::now();

                packet_buffer
                    .write_u64::<BigEndian>(tx_app)
                    .expect("Serialize time");

                let bytes_sent = socket::send(
                    mio_socket.as_raw_fd(),
                    &packet_buffer,
                    socket::MsgFlags::empty(),
                ).expect("Sending reply failed");

                tx_nic = if nic_timestamps {
                    netbench::retrieve_tx_timestamp(mio_socket.as_raw_fd(), &mut time_tx)
                        .expect("NIC Timestamps not enabled?")
                } else {
                    0
                };

                assert_eq!(bytes_sent, 8);
                packet_buffer.clear();
                waiting_for_reply = true;
                last_sent = Instant::now();
            }

            // Receive a packet
            if waiting_for_reply && event.readiness().is_readable() {
                //debug!("Received ts packet");

                // Get the packet
                let msg = socket::recvmsg(
                    mio_socket.as_raw_fd(),
                    &[uio::IoVec::from_mut_slice(&mut recv_buf)],
                    Some(&mut time_rx),
                    socket::MsgFlags::empty(),
                ).expect("Can't receive message");
                let rx_app = netbench::now();
                let rx_nic = if nic_timestamps {
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
                waiting_for_reply = false;
            }
        }

        // Report error if we don't see a reply within 500ms and try again with another packet
        if waiting_for_reply && last_sent.elapsed() > timeout {
            error!("Dropped packet?");
            waiting_for_reply = false;
        }

        // Periodically flush our buffer
        if packet_count % 10000 == 0 {
            wtr.lock()
                .unwrap()
                .flush()
                .expect("Can't flush the csv log");
        }

        // We're done sending requests, stop
        if packet_count == requests {
            debug!("Sender for {} done.", destination);
            break;
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
