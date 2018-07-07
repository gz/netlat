use std::net::Ipv4Addr;

use pnet::packet::ethernet::{EtherTypes, MutableEthernetPacket};
use pnet::packet::ip::IpNextHeaderProtocols;
use pnet::packet::ipv4::{self, MutableIpv4Packet};
use pnet::packet::udp::{self, MutableUdpPacket};
use pnet::packet::MutablePacket;
use pnet::util::MacAddr;

static IPV4_HEADER_LEN: usize = 20;
static UDP_HEADER_LEN: usize = 8;
static TEST_DATA_LEN: usize = 8;

fn build_ipv4_header(from: Ipv4Addr, to: Ipv4Addr, packet: &mut [u8]) -> MutableIpv4Packet {
    let mut ip_header = MutableIpv4Packet::new(packet).unwrap();

    let total_len = (IPV4_HEADER_LEN + UDP_HEADER_LEN + TEST_DATA_LEN) as u16;

    ip_header.set_version(4);
    ip_header.set_header_length(5);
    ip_header.set_total_length(total_len);
    ip_header.set_ttl(4);
    ip_header.set_next_level_protocol(IpNextHeaderProtocols::Udp);
    ip_header.set_source(from);
    ip_header.set_destination(to);

    let checksum = ipv4::checksum(&ip_header.to_immutable());
    ip_header.set_checksum(checksum);

    ip_header
}

fn build_udp_header(from_port: u16, to_port: u16, packet: &mut [u8]) -> MutableUdpPacket {
    let mut udp_header = MutableUdpPacket::new(packet).unwrap();

    udp_header.set_source(from_port);
    udp_header.set_destination(to_port);
    udp_header.set_length((UDP_HEADER_LEN + TEST_DATA_LEN) as u16);

    udp_header
}

fn build_udp4_packet(
    from: Ipv4Addr,
    from_port: u16,
    to: Ipv4Addr,
    to_port: u16,
    packet: &mut [u8],
    payload: [u8; 8],
) {
    let mut ip_header = build_ipv4_header(from, to, packet);
    let source = ip_header.get_source();
    let destination = ip_header.get_destination();
    let mut udp_header = build_udp_header(from_port, to_port, ip_header.payload_mut());

    {
        let data = udp_header.payload_mut();
        data[0] = payload[0];
        data[1] = payload[1];
        data[2] = payload[2];
        data[3] = payload[3];
        data[4] = payload[4];
        data[5] = payload[5];
        data[6] = payload[6];
        data[7] = payload[7];
    }

    let checksum = udp::ipv4_checksum(&udp_header.to_immutable(), &source, &destination);
    //udp_header.set_checksum(checksum);
}

pub fn udp_packet_raw(
    from_mac: MacAddr,
    to_mac: MacAddr,
    from: (Ipv4Addr, u16),
    to: (Ipv4Addr, u16),
    payload: [u8; 8],
) -> [u8; 64] {
    let mut buffer = [0u8; 64];

    {
        let mut mut_ethernet_header = MutableEthernetPacket::new(&mut buffer[..]).unwrap();
        mut_ethernet_header.set_destination(to_mac);
        mut_ethernet_header.set_source(from_mac);
        mut_ethernet_header.set_ethertype(EtherTypes::Ipv4);
        build_udp4_packet(
            from.0,
            from.1,
            to.0,
            to.1,
            mut_ethernet_header.payload_mut(),
            payload,
        );
    }

    buffer
}
