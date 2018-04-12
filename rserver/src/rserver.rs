use std::net::UdpSocket;

fn main() {
    let socket = UdpSocket::bind("0.0.0.0:3400").expect("Couldn't bind to address");
    println!("Listening on 0.0.0.0:3400");

    loop {
        let mut buf_network: [u8; 8] = [0; 8];
        let (size, addr) = socket
            .recv_from(&mut buf_network)
            .expect("Can't receive data");
        assert!(size == 8);
        
        socket.send_to(&buf_network, addr).expect("Can't send reply back");
    }

}
