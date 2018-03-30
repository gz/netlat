use std::net::UdpSocket;
use std::thread;
use std::sync::mpsc::channel;
use std::ops::Add;

enum Message {
    Payload([u8; 8]),
    Exit,
}

fn spawn_listen_pair(
    name: String,
    on_core: u64,
    socket: UdpSocket,
) -> (thread::JoinHandle<()>, thread::JoinHandle<()>) {
    assert!(on_core == 0);

    let (txa, rxp) = channel();
    let (txp, rxa) = channel();

    let t_app_name = name.clone().add("/polling");

    let t_app = thread::Builder::new()
        .name(t_app_name)
        .stack_size(4096 * 10)
        .spawn(move || loop {
            match rxa.recv().expect("Can't receive data on app thread") {
                Message::Payload(buf_app) => {
                    //println!("buff app {:?}", buf_app);
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
            println!("Thread spawned!");
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

fn main() {
    let socket = std::net::UdpSocket::bind("0.0.0.0:3400").expect("Couldn't bind to address");
    println!("Listening on 0.0.0.0:3400");

    let (tapp, tpoll) = spawn_listen_pair(
        String::from("test"),
        0,
        socket.try_clone().expect("Can't clone this."),
    );
    tapp.join().expect("Can't join app-thread.");
    tpoll.join().expect("Can't join poll-thread.")
}
