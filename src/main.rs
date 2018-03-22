extern crate mio;
use std::env;
use std::time::Duration;

use mio::net::UdpSocket;
use mio::{Events, Ready, Poll, PollOpt, Token};

//use {expect_events, sleep_ms};

const PING: Token = Token(0);
const PONG: Token = Token(1);

fn main() {

    let args: Vec<String> = env::args().collect();
    assert!(args.len() >= 2);
    println!("Connecting to {}", args[1]);

    let sender = UdpSocket::bind(&args[1].parse().unwrap()).unwrap();
    let poll = Poll::new().expect("Can't create poll.");

    poll.register(&sender, PING, Ready::writable(), PollOpt::edge()).expect("Can't register send event.");
    poll.register(&sender, PONG, Ready::readable(), PollOpt::edge()).expect("Can't register receive event.");

    let mut buffer = [1; 9];
    let msg_to_send = [9; 9];

    let mut events = Events::with_capacity(1024);
    loop {
        poll.poll(&mut events, Some(Duration::from_millis(100))).expect("Can't poll channel");
        for event in events.iter() {
            match event.token() {

                // Our SENDER is ready to be written into.
                PING => {
                    let bytes_sent = sender.send(&msg_to_send).expect("Sending failed");
                    assert_eq!(bytes_sent, 9);
                    println!("sent {:?} -> {:?} bytes", msg_to_send, bytes_sent);
                },

                // Our ECHOER is ready to be read from.
                PONG => {
                    let num_recv = sender.recv(&mut buffer).expect("Receiving failed");
                    println!("received {:?} -> {:?}", buffer, num_recv);
                    buffer = [0; 9];
                }

                _ => unreachable!()
            }
        }
    }
}
