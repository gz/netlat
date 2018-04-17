#[macro_use]
extern crate clap;
extern crate hwloc;
extern crate nix;
#[macro_use]
extern crate log;
extern crate env_logger;

use std::net::UdpSocket;
use std::ops::Add;
use std::str::FromStr;
use std::sync::mpsc::channel;
use std::thread;

use hwloc::{ObjectType, Topology};

use clap::App;

#[cfg(target_os = "linux")]
fn pin_thread(core_id: usize) {
    use nix::unistd::getpid;
    use nix::sched::{sched_setaffinity, CpuSet};

    let pid = getpid();
    let mut affinity_set = CpuSet::new();
    affinity_set.set(core_id).expect("Can't set PU in core set");
    sched_setaffinity(pid, &affinity_set).expect("Can't pin app thread to core");
}

#[cfg(target_os = "linux")]
fn pin_thread2(core_id: usize, core_id2: usize) {
    use nix::unistd::getpid;
    use nix::sched::{sched_setaffinity, CpuSet};

    let pid = getpid();
    let mut affinity_set = CpuSet::new();
    affinity_set.set(core_id).expect("Can't set PU in core set");
    affinity_set.set(core_id2).expect("Can't set PU in core set");
    
    sched_setaffinity(pid, &affinity_set).expect("Can't pin app thread to core");
}

// And this function only gets compiled if the target OS is *not* linux
#[cfg(not(target_os = "linux"))]
fn pin_thread(_core_id: usize) {
    error!("Pinning threads not supported!");
}

// And this function only gets compiled if the target OS is *not* linux
#[cfg(not(target_os = "linux"))]
fn pin_thread2(_core_id: usize, _core_id2: usize) {
    error!("Pinning threads not supported!");
}

enum Message {
    Payload([u8; 8]),
    Exit,
}

fn spawn_listen_pair(
    name: String,
    on_core: Option<(usize, usize)>,
    socket: UdpSocket,
) -> (thread::JoinHandle<()>, thread::JoinHandle<()>) {
    let (txa, rxp) = channel();
    let (txp, rxa) = channel();

    let t_app_name = name.clone().add("/polling");

    let t_app = thread::Builder::new()
        .name(t_app_name)
        .stack_size(4096 * 10)
        .spawn(move || loop {
           
            on_core.map(|ids: (usize, usize)| {
                pin_thread(ids.0);
            });

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

            on_core.map(|ids: (usize, usize)| {
                pin_thread(ids.1);
            });

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

fn parse_args(matches: &clap::ArgMatches) -> (Option<(usize, usize)>, String, UdpSocket) {
    let core_id: Option<u32> = matches
        .value_of("pin")
        .and_then(|c: &str| u32::from_str(c).ok());

    let port: usize = usize::from_str(matches.value_of("port").unwrap_or("3400")).unwrap_or(3400);
    let address = format!("0.0.0.0:{}", port);
    let socket = std::net::UdpSocket::bind(&address).expect("Couldn't bind to address");

    // Get the SMT threads for the Core
    let topo = Topology::new();
    let core_depth = topo.depth_or_below_for_type(&ObjectType::Core).unwrap();
    let all_cores = topo.objects_at_depth(core_depth);

    let pin_to: Option<(usize, usize)> = core_id.and_then(|c: u32| {
        for core in all_cores {
            for smt_thread in core.children().iter() {
                if smt_thread.os_index() == c {
                    return Some((smt_thread.os_index() as usize, smt_thread.next_sibling().expect("CPU doesn't have SMT (check that provided core_id is the min of the pair)?").os_index() as usize));
                }
            }
        }
        None
    });

    (pin_to, address, socket)
}

fn main() {
    env_logger::init().expect("Can't initialize logging environment");

    let yaml = load_yaml!("cli.yml");
    let matches = App::from_yaml(yaml).get_matches();

    if let Some(matches) = matches.subcommand_matches("smtconfig") {
        let (pin_to, address, socket) = parse_args(matches);

        println!(
            "Listening on {} with threads spawned on {:?}.",
            address, pin_to
        );
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

        pin_to.map(|ids: (usize, usize)| {
            pin_thread2(ids.0, ids.1);
        });

        println!(
            "Listening on {} with process affinity set to {:?}.",
            address, pin_to
        );
        loop {
            let mut buf_network: [u8; 8] = [0; 8];
            let (size, addr) = socket
                .recv_from(&mut buf_network)
                .expect("Can't receive data.");
            assert!(size == 8);

            socket
                .send_to(&buf_network, addr)
                .expect("Can't send reply back");
        }
    }
}
