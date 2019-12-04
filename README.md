## Pre-requisites

1. You need to have libhwloc installed on your system: 
`sudo apt install libhwloc-dev`

2. You need to have rust installed on your system:
`curl https://sh.rustup.rs -sSf | sh`

## Install the program

1. Get the code
`git clone https://github.com/gz/netlat.git`

2. Build the project
`cargo build --release`

## Running the rserver

```bash
cd rserver

RUST_LOG='netlat=debug' cargo run --release --bin rserver -- --iface enp94s0f0 --output test --pin 2 --scheduler default --sockets 1 --threads 1 --timestamp hardware --transport udp single
```

Use `cargo run --release -- --help` for more information.

## Running the netlat client

```bash
cd client

RUST_LOG='netlat=debug' cargo run --release -- --iface enp94s0f0 --output test --pin 2 --scheduler default --timestamp hardware --transport udp 10000 192.168.100.117:3400
```

Use `cargo run --release -- --help` for more information.
