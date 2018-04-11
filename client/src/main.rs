extern crate mio;
extern crate time;
extern crate byteorder;
extern crate csv;

#[macro_use]
extern crate serde_derive;


use std::env;
use std::time::Duration;
use csv::Writer;

use mio::net::UdpSocket;
use mio::{Events, Ready, Poll, PollOpt, Token};

use byteorder::{BigEndian, WriteBytesExt, ReadBytesExt};

//use {expect_events, sleep_ms};

#[derive(Serialize)]
struct Row {
    latency_ns: u64,
}

const PING: Token = Token(0);

fn main() {

    let args: Vec<String> = env::args().collect();
    assert!(args.len() >= 3);
    
    println!("Sending {} requests to {}", args[1], args[2]);
    let sender = UdpSocket::bind(&"0.0.0.0:1111".parse().expect("Invalid address.")).expect("Can't bind");
    let requests = args[1].parse::<u64>().expect("First argument should be request count (u64).");

    sender.connect(args[1].parse().expect("Invalid host:port pair")).expect("Can't connect to server");
    let poll = Poll::new().expect("Can't create poll.");

    poll.register(&sender, PING, Ready::writable() | Ready::readable(), PollOpt::level()).expect("Can't register send event.");

    let mut buf = Vec::with_capacity(8);
    let mut events = Events::with_capacity(1024);

    let mut recv_buf: Vec<u8> = Vec::with_capacity(8);
    recv_buf.resize(8, 0);

    let mut wtr = Writer::from_path(args[2].clone()).expect("Can't open log file for writing");
    
    let mut i = 0;
    let mut waiting_for_reply = false;
    loop {
        poll.poll(&mut events, Some(Duration::from_millis(100))).expect("Can't poll channel");
        for event in events.iter() {
 
            if  !waiting_for_reply && event.readiness().is_writable() {
                buf.write_u64::<BigEndian>(time::precise_time_ns()).expect("Serialize time");
                let bytes_sent = sender.send(&buf).expect("Sending failed!");
                assert_eq!(bytes_sent, 8);
                buf.clear();

                waiting_for_reply = true;
            }

            if waiting_for_reply && event.readiness().is_readable() {
                let _ = sender.recv_from(&mut recv_buf).expect("Can't receive timestamp back.");
                let now = time::precise_time_ns();
                let sent = recv_buf.as_slice().read_u64::<BigEndian>().expect("Can't parse timestamp");

                wtr.serialize(Row { latency_ns: now-sent }).expect("Can't write record");
                i = i + 1;

                waiting_for_reply = false;
            }
        }

        if i % 10000 == 0 {
            wtr.flush().expect("Can't flush the csv log");
        }

        if i == requests-1 {
            break;
        }
    }
}