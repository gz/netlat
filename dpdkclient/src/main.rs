extern crate dpdk;
extern crate getopts;
extern crate pnet;

#[macro_use]
extern crate lazy_static;

use dpdk::ffi;
use getopts::Options;
use std::net::Ipv4Addr;
use std::os::raw::c_void;
use std::ptr;
use std::sync::Mutex;
use std::vec::Vec;

mod packet;

static mut FORCE_QUIT: bool = false;
static mut DUMP_FLAG: bool = false;

const MAX_PKT_BURST: u16 = 32;

lazy_static! {
    static ref PORTS: Mutex<Vec<dpdk::eth::Port>> = Mutex::new(vec![]);
}

fn dump_packet_type(ptype: u32) {
    match ptype & ffi::RTE_PTYPE_L2_MASK {
        ffi::RTE_PTYPE_L2_ETHER => print!("Ether,"),
        ffi::RTE_PTYPE_L2_ETHER_VLAN => print!("Ether+VLAN,"),
        ffi::RTE_PTYPE_L2_ETHER_QINQ => print!("Ether+QinQ,"),
        ffi::RTE_PTYPE_L2_ETHER_ARP => print!("Ether+ARP,"),
        _ => print!("Other L2 ({}),", ptype & ffi::RTE_PTYPE_L2_MASK),
    }
    match ptype & ffi::RTE_PTYPE_L3_MASK {
        ffi::RTE_PTYPE_L3_IPV4 => print!("IPv4,"),
        ffi::RTE_PTYPE_L3_IPV4_EXT => print!("IPv4-Ext,"),
        ffi::RTE_PTYPE_L3_IPV6 => print!("IPv6,"),
        ffi::RTE_PTYPE_L3_IPV6_EXT => print!("IPv6-Ext,"),
        _ => print!("Other L3 ({}),", ptype & ffi::RTE_PTYPE_L3_MASK),
    }
    match ptype & ffi::RTE_PTYPE_L4_MASK {
        ffi::RTE_PTYPE_L4_UDP => println!("UDP"),
        ffi::RTE_PTYPE_L4_TCP => println!("TCP"),
        ffi::RTE_PTYPE_L4_SCTP => println!("SCTP"),
        ffi::RTE_PTYPE_L4_ICMP => println!("ICMP"),
        ffi::RTE_PTYPE_L4_FRAG => println!("Fragment"),
        _ => println!("Other L4 ({})", ptype & ffi::RTE_PTYPE_L4_MASK),
    }
}

unsafe fn dump_mbuf(m: &ffi::rte_mbuf) {
    let mut hdr_lens: ffi::rte_net_hdr_lens = std::mem::zeroed();
    let ptype = dpdk::net::get_ptype(m, &mut hdr_lens, ffi::RTE_PTYPE_ALL_MASK);
    dump_packet_type(ptype);
    print!("l2_len {},", hdr_lens.l2_len);
    print!("l3_len {},", hdr_lens.l3_len);
    println!("l4_len {}", hdr_lens.l4_len);
}

unsafe fn pktmbuf_as<T>(mesg: &ffi::rte_mbuf, offset: isize) -> &T {
    //std::mem::transmute::<*mut std::os::raw::c_void, &T>(mesg.buf_addr.offset(offset))
    &mut *(mesg.buf_addr.offset(mesg.data_off as isize + offset) as *mut T)
}

unsafe extern "C" fn l2fwd_main_loop(arg: *mut c_void) -> i32 {
    if ffi::rte_eth_dev_socket_id(arg as u16) > 0
        && ffi::rte_eth_dev_socket_id(arg as u16) != ffi::rte_socket_id() as i32
    {
        println!(
            "Warning program is running on remote NUMA node. We don't have optimal performance."
        );
    }

    let lcore_id = dpdk::lcore::id();
    let mut pkts: [&mut ffi::rte_mbuf; MAX_PKT_BURST as usize] = std::mem::zeroed();
    let mut tx_buffer: dpdk::eth::tx_buffer = std::mem::zeroed();
    tx_buffer.init(MAX_PKT_BURST);

    let port = dpdk::eth::Port {
        port_id: arg as u16,
    };

    let txpool = dpdk::pktmbuf::pool_create(
        "tx",
        128 - 1,
        64,
        0,
        ffi::RTE_MBUF_DEFAULT_BUF_SIZE as u16,
        dpdk::socket::id(),
    );

    let udp_packet: [u8; 64] = packet::udp_packet_raw(
        pnet::util::MacAddr::new(0x50, 0x6b, 0x4b, 0x23, 0xa9, 0x05), // 50:6b:4b:23:a9:05
        pnet::util::MacAddr::new(0x50, 0x6b, 0x4b, 0x23, 0xa8, 0x25), // 50:6b:4b:23:a8:25
        (Ipv4Addr::new(192, 168, 0, 34), 9999),
        (Ipv4Addr::new(192, 168, 0, 7), 3400),
        [0, 0, 0, 0, 0, 0, 0, 1],
    );
    let send_packet: *mut ffi::rte_mbuf = dpdk::ffi::rte_pktmbuf_alloc1(txpool);
    let pptr: *mut i8 = dpdk::ffi::rte_pktmbuf_append1(send_packet, 64);
    ptr::copy(udp_packet.as_ptr() as *mut i8, pptr, 64);

    println!("lcore{}: loop start", lcore_id);
    let mut send = true;
    while FORCE_QUIT != true {
        if send {
            let sent = tx_buffer.tx(&port, 1, send_packet);
            let flush = tx_buffer.flush(&port, 0);
            send = false;
            println!("sent packet = {} flush = {}", sent, flush);
        }

        if !send {
            let nb_rx = port.rx_burst(0, pkts.as_mut_ptr(), MAX_PKT_BURST);
            if nb_rx == 0 {
                continue;
            }
            for i in 0..nb_rx as usize {
                let mut hdr_lens: ffi::rte_net_hdr_lens = std::mem::zeroed();
                let ptype = dpdk::net::get_ptype(pkts[i], &mut hdr_lens, ffi::RTE_PTYPE_ALL_MASK);
                let is_ether = ptype & ffi::RTE_PTYPE_L2_MASK == ffi::RTE_PTYPE_L2_ETHER;
                let is_ip4 = ptype & ffi::RTE_PTYPE_L3_MASK == ffi::RTE_PTYPE_L3_IPV4;
                let is_udp = ptype & ffi::RTE_PTYPE_L4_UDP == ffi::RTE_PTYPE_L4_UDP;
                assert!(pkts[i].nb_segs == 1, "rte_pktmbuf_is_contiguous");

                if is_ether && is_ip4 && is_udp {
                    println!("Probably got response");
                    println!(
                        "buf_len = {} pkt_len = {} data_len = {} timestamp = {}",
                        pkts[i].buf_len, pkts[i].pkt_len, pkts[i].data_len, pkts[i].timestamp
                    );
                    let ether_hdr: &ffi::ether_hdr = pktmbuf_as(pkts[i], 0);
                    println!("ether_hdr = {:?}", ether_hdr);

                    let ipv4_hdr: &ffi::ipv4_hdr =
                        pktmbuf_as(pkts[i], std::mem::size_of::<ffi::ether_hdr>() as isize);
                    println!("ipv4_hdr = {:?}", ipv4_hdr);

                    let udp_hdr: &ffi::udp_hdr = pktmbuf_as(
                        pkts[i],
                        (std::mem::size_of::<ffi::ether_hdr>()
                            + std::mem::size_of::<ffi::ipv4_hdr>())
                            as isize,
                    );
                    println!("udp_hdr = {:?}", udp_hdr);

                    let ip = (ipv4_hdr.src_addr as u32).to_be();
                    let a = (ip >> 24) as u8;
                    let b = (ip >> 16) as u8;
                    let c = (ip >> 8) as u8;
                    let d = (ip >> 0) as u8;
                    let sender_ip = Ipv4Addr::new(a, b, c, d);

                    if sender_ip == Ipv4Addr::new(192, 168, 0, 7)
                        && u16::from_be(udp_hdr.src_port) == 3400
                        && u16::from_be(udp_hdr.dst_port) == 9999
                    {
                        println!("Really packet we want!");
                        let data_len = u16::from_be(udp_hdr.dgram_len)
                            - std::mem::size_of::<ffi::udp_hdr>() as u16;
                        assert!(data_len == 8);
                        let tstamp: &u64 = pktmbuf_as(
                            pkts[i],
                            (std::mem::size_of::<ffi::ether_hdr>()
                                + std::mem::size_of::<ffi::ipv4_hdr>()
                                + std::mem::size_of::<ffi::udp_hdr>())
                                as isize,
                        );
                        println!("payload is = {:?}", tstamp);
                    }

                    send = true;
                    loop {}
                } else {
                    println!("Got unexpected packet");
                    dump_mbuf(pkts[i]);
                }
            }

            for pkt in 0..nb_rx {
                dpdk::pktmbuf::free(pkts[pkt as usize]);
            }
        }
    }
    0
}

fn main() {
    unsafe {
        let pool: *mut ffi::rte_mempool;
        let mut opts = Options::new();
        opts.optopt("p", "portmap", "set port bitmap", "PORTMAP");
        opts.optflag("d", "dump", "show packet mbuf dump");
        opts.optflag("v", "version", "show version and exit");

        let exargs = dpdk::eal::init(std::env::args());
        if exargs.is_none() == true {
            println!("parameter required.");
            return;
        }
        let matches = match opts.parse(exargs.unwrap()) {
            Ok(m) => m,
            Err(f) => panic!(f.to_string()),
        };
        if matches.opt_present("v") {
            println!(
                "dpdk-netlat {} with {}",
                env!("CARGO_PKG_VERSION"),
                dpdk::version::string()
            );
            return;
        }
        if matches.opt_present("d") {
            println!("Dumping active");
            DUMP_FLAG = true;
        }
        let mut portmap = matches.opt_str("p").unwrap().parse::<u32>().unwrap();
        // lcore and port assignment
        let mut lcores: Vec<u32> = Vec::new();
        let mut n = 0u16;
        let mut lc = dpdk::lcore::get_first(true);
        while portmap > 0 {
            if portmap & 1 != 0 {
                if lc == ffi::RTE_MAX_LCORE {
                    panic!("Not enough logical core.");
                }
                /*
                    Device (d8:00.1):
                    d8:00.1 Ethernet controller: Mellanox Technologies MT27800 Family [ConnectX-5]
                    Link Width: x16
                    PCI Link Speed: 8GT/s
                    enp216s0f1 Link encap:Ethernet  HWaddr 50:6b:4b:23:a9:05
                    inet addr:192.168.0.34  Bcast:192.168.0.255  Mask:255.255.255.0
                */
                println!("portid {}: lcore {}", n, lc);
                PORTS.lock().unwrap().push(dpdk::eth::Port { port_id: n });
                lcores.push(lc);
                lc = dpdk::lcore::get_next(lc, false, false);
            }
            portmap /= 2;
            n += 1;
        }

        pool = dpdk::pktmbuf::pool_create(
            "mbp",
            128 - 1,
            64,
            0,
            ffi::RTE_MBUF_DEFAULT_BUF_SIZE as u16,
            dpdk::socket::id(),
        );
        assert!(pool.is_null() == false);
        let mut port_conf: ffi::rte_eth_conf = std::mem::zeroed();
        port_conf.rxmode.set_hw_strip_crc(1);

        for port in PORTS.lock().unwrap().clone() {
            let mut info: ffi::rte_eth_dev_info = std::mem::zeroed();
            port.info(&mut info);
            let device = port.devices();
            println!("Initializing port {}: name {}", port.port_id, device.name());
            if device.is_intr_lsc_enable() == true {
                port_conf.intr_conf.set_lsc(1);
            } else {
                port_conf.intr_conf.set_lsc(0);
            }
            let rv = port.configure(1, 1, &port_conf);
            assert!(
                rv == 0,
                "configure failed: portid {}, rv: {}",
                port.port_id,
                rv
            );
            let nb_rxd = port.adjust_rx_desc(128);
            let nb_txd = port.adjust_tx_desc(512);
            let rv =
                port.rx_queue_setup(0, nb_rxd, port.socket_id(), &mut info.default_rxconf, pool);
            assert!(
                rv == 0,
                "rx queue setup failed: portid {}, rv: {}",
                port.port_id,
                rv
            );
            let rv = port.tx_queue_setup(0, nb_txd, port.socket_id(), &mut info.default_txconf);
            assert!(
                rv == 0,
                "tx queue setup failed: portid {}, rv: {}",
                port.port_id,
                rv
            );
            let rv = port.start();
            assert!(
                rv == 0,
                "ethernet devvice not started: portid {}, rv: {}",
                port.port_id,
                rv
            );
            port.promiscuous_set(true);
        }
        let callback = l2fwd_main_loop;
        for n in 0..lcores.len() {
            let callback_arg = PORTS.lock().unwrap()[n].port_id as *mut c_void;
            dpdk::eal::remote_launch(callback, callback_arg, lcores[n]);
        }
        dpdk::eal::mp_wait_lcore();
    }
}
