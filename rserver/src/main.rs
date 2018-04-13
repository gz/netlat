#[macro_use]
extern crate clap;
extern crate nix;
extern crate hwloc;
#[macro_use]
extern crate log;
extern crate env_logger;

use std::net::UdpSocket;
use std::thread;
use std::sync::mpsc::channel;
use std::ops::Add;
use std::str::FromStr;

use nix::sched::{CpuSet, sched_setaffinity};
use nix::unistd::getpid;

use hwloc::{Topology, ObjectType};

use clap::App;

enum Message {
    Payload([u8; 8]),
    Exit,
}

fn spawn_listen_pair(
    name: String,
    on_core: (usize, usize),
    socket: UdpSocket,
) -> (thread::JoinHandle<()>, thread::JoinHandle<()>) {
    let (txa, rxp) = channel();
    let (txp, rxa) = channel();

    let t_app_name = name.clone().add("/polling");

    let t_app = thread::Builder::new()
        .name(t_app_name)
        .stack_size(4096 * 10)
        .spawn(move || loop {
            let pid = getpid();

            let mut affinity_set = CpuSet::new();
            affinity_set.set(on_core.0).expect("Can't set PU in core set");
            sched_setaffinity(pid, &affinity_set).expect("Can't pin app thread to core");

            match rxa.recv().expect("Can't receive data on app thread") {
                Message::Payload(buf_app) => {
                    if buf_app[0] == '0' as u8 && buf_app[1] == '0' as u8 && buf_app[2] == '0' as u8
                        && buf_app[3] == '0' as u8 && buf_app[4] == '0' as u8
                        && buf_app[5] == '0' as u8 && buf_app[6] == '0' as u8
                        && buf_app[7] == '0' as u8
                    {
                        txa.send(Message::Exit)
                            .expect("Sending exit message to poll thread");
                        break;
                    }
                    txa.send(Message::Payload(buf_app))
                        .expect("Can't send data to network thread.");
                }
                Message::Exit => {
                    println!("Got exit message from poll thread?");
                    break;
                }
            };
        })
        .expect("Can't spawn application thread");

    let t_poll_name = name.clone().add("/polling");
    let t_poll = thread::Builder::new()
        .name(t_poll_name)
        .stack_size(4096 * 10)
        .spawn(move || {
            let pid = getpid();

            let mut affinity_set = CpuSet::new();
            affinity_set.set(on_core.1).expect("Can't set PU in core set");
            sched_setaffinity(pid, &affinity_set).expect("Can't pin poll thread to core");

            loop {
                let mut buf_network: [u8; 8] = [0; 8];
                let (size, addr) = socket
                    .recv_from(&mut buf_network)
                    .expect("Can't receive data");
                assert!(size == 8);

                txp.send(Message::Payload(buf_network))
                    .expect("Can't forward data to the app thread over channel");
                let buf = rxp.recv().expect("Can't receive data");
                match buf {
                    Message::Payload(buf) => {
                        socket.send_to(&buf, addr).expect("Can't send reply back")
                    }
                    Message::Exit => {
                        break;
                    }
                };
            }
        })
        .expect("Couldn't spawn a thread");

    (t_app, t_poll)
}

fn parse_args(matches: &clap::ArgMatches) -> ((usize, usize), String, UdpSocket) {
    let core_id: u32 = u32::from_str(matches.value_of("pin").unwrap_or("0")).unwrap_or(0);
    let port: usize = usize::from_str(matches.value_of("port").unwrap_or("3400")).unwrap_or(3400);
    let address = format!("0.0.0.0:{}", port);
    let socket = std::net::UdpSocket::bind(&address).expect("Couldn't bind to address");

    // Get the SMT threads for the Core
    let topo = Topology::new();
    let core_depth = topo.depth_or_below_for_type(&ObjectType::Core).unwrap();
    let all_cores = topo.objects_at_depth(core_depth);

    let mut pin_to: (usize, usize) = (0, 0);
    for core in all_cores {
        for smt_thread in core.children().iter() {
            if smt_thread.os_index() == core_id {
                pin_to.0 = smt_thread.os_index() as usize;
                pin_to.1 = smt_thread.next_sibling().expect("CPU doesn't have SMT (check that provided core_id is the min of the pair)?").os_index() as usize;
            }
        }
    }

    (pin_to, address, socket)
}

fn main() {
    env_logger::init().expect("Can't initialize logging environment");

    let yaml = load_yaml!("cli.yml");
    let matches = App::from_yaml(yaml).get_matches();

    if let Some(matches) = matches.subcommand_matches("smtconfig") {
        let (pin_to, address, socket) = parse_args(matches);

        println!("Listening on {} with threads spawned on {:?}.", address, pin_to);
        let (tapp, tpoll) = spawn_listen_pair(
            String::from("test"),
            pin_to,
            socket.try_clone().expect("Can't clone this."),
        );

        tapp.join().expect("Can't join app-thread.");
        tpoll.join().expect("Can't join poll-thread.")
    }

    if let Some(matches) = matches.subcommand_matches("single") {
        let (pin_to, address, socket) = parse_args(matches);

        let mut affinity_set = CpuSet::new();
        affinity_set.set(pin_to.0).expect("Can't set PU in core set");
        affinity_set.set(pin_to.1).expect("Can't set PU in core set");
        sched_setaffinity(getpid(), &affinity_set).expect("Can't server to core");

        println!("Listening on {} with process affinity set to {:?}.", address, pin_to);
        loop {
            let mut buf_network: [u8; 8] = [0; 8];
            let (size, addr) = socket
                .recv_from(&mut buf_network)
                .expect("Can't receive data.");
            assert!(size == 8);
            
            socket.send_to(&buf_network, addr).expect("Can't send reply back");
        }
    }
}
