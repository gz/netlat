extern crate mio;
extern crate time;
extern crate byteorder;
extern crate csv;
#[macro_use]
extern crate log;
extern crate env_logger;
#[macro_use]
extern crate clap;

#[macro_use]
extern crate serde_derive;

use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::{Instant, Duration};

use csv::Writer;
use clap::App;
use byteorder::{BigEndian, WriteBytesExt, ReadBytesExt};

use mio::net::UdpSocket;
use mio::{Events, Ready, Poll, PollOpt, Token};

#[derive(Serialize)]
struct Row {
    latency_ns: u64,
}

const PING: Token = Token(0);

fn main() {
    env_logger::init();

    let yaml = load_yaml!("cli.yml");
    let matches = App::from_yaml(yaml).get_matches();
    let requests = value_t!(matches, "requests", u64).unwrap_or(250000);
    let recipients = values_t!(matches, "destinations", String).unwrap_or_else(|e| e.exit());
    let suffix = value_t!(matches, "name", String).unwrap_or(String::from("none"));

    let mut handles = Vec::with_capacity(recipients.len());
    debug!("Got {} recipients to send to.", recipients.len());
    let barrier = Arc::new(Barrier::new(recipients.len()));

    let counter = Arc::new(Mutex::new(0));
    for recipient in recipients {
        let c = barrier.clone();
        let suffix_clone = suffix.clone();
        let counter = Arc::clone(&counter);

        /*
        int flags;
        flags   = SOF_TIMESTAMPING_TX_HARDWARE
                | SOF_TIMESTAMPING_RX_HARDWARE 
                | SOF_TIMESTAMPING_TX_SOFTWARE
                | SOF_TIMESTAMPING_RX_SOFTWARE 
                | SOF_TIMESTAMPING_RAW_HARDWARE;
        if (setsockopt(sd, SOL_SOCKET, SO_TIMESTAMPING, &flags, sizeof(flags)) < 0)
            printf("ERROR: setsockopt SO_TIMESTAMPING\n");
        */

        handles.push(thread::spawn(move|| {
            let dest: Vec<&str> = recipient.split(":").collect();
            let output = format!("latencies-{}-{}-{}.csv", dest[1], requests, suffix_clone);
            
            println!("Sending {} requests to address {} writing latencies to {}", requests, recipient, output);
            let source_address = {
                let mut num = counter.lock().unwrap();
                *num += 1;
                format!("0.0.0.0:{}", num)
            };

            let sender = UdpSocket::bind(&source_address.parse().expect("Invalid address.")).expect("Can't bind");
            sender.connect(recipient.parse().expect("Invalid host:port pair")).expect("Can't connect to server");
            let poll = Poll::new().expect("Can't create poll.");
            poll.register(&sender, PING, Ready::writable() | Ready::readable(), PollOpt::level()).expect("Can't register send event.");

            let mut buf = Vec::with_capacity(8);
            let mut events = Events::with_capacity(1024);
            let mut recv_buf: Vec<u8> = Vec::with_capacity(8);
            recv_buf.resize(8, 0);
            let mut wtr = Writer::from_path(output).expect("Can't open log file for writing");
            let mut received_count = 0;
            let mut waiting_for_reply = false;
            let timeout = Duration::from_millis(500); // At that point we consider the UDP packet lost
            let mut last_sent = Instant::now();

            c.wait();
            debug!("Start sending...");
            loop {
                poll.poll(&mut events, Some(Duration::from_millis(100))).expect("Can't poll channel");
                for event in events.iter() {
                    if  !waiting_for_reply && event.readiness().is_writable() {
                        buf.write_u64::<BigEndian>(time::precise_time_ns()).expect("Serialize time");
                        let bytes_sent = sender.send(&buf).expect("Sending failed!");
                        assert_eq!(bytes_sent, 8);
                        buf.clear();
                        waiting_for_reply = true;
                        last_sent = Instant::now();
                    }

                    if waiting_for_reply && event.readiness().is_readable() {
                        //debug!("Received ts packet");
                        let _ = sender.recv_from(&mut recv_buf).expect("Can't receive timestamp back.");
                        let now = time::precise_time_ns();
                        let sent = recv_buf.as_slice().read_u64::<BigEndian>().expect("Can't parse timestamp");
                        wtr.serialize(Row { latency_ns: now-sent }).expect("Can't write record");
                        received_count = received_count + 1;
                        waiting_for_reply = false;
                    }
                }

                if waiting_for_reply && last_sent.elapsed() > timeout {
                    error!("Dropped packet?");
                    waiting_for_reply = false;
                }

                if received_count % 10000 == 0 {
                    wtr.flush().expect("Can't flush the csv log");
                }

                if received_count == requests {
                    debug!("Sender for {} done.", dest[1]);
                    break;
                }
            }
            
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }
}