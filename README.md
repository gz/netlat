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


```
cd rserver

# Debug
RUST_BACKTRACE=1 RUST_LOG='netbench=debug,rserver=debug' cargo run -- --iface enp216s0f1 --mapping all --output test --pin 14 15 16 17 18 19 20 21 22 23 24 25 26 27 --port 3400 --scheduler none --threads 4 --timestamp hardware --transport udp mt

# No debug
cargo run --release -- --iface enp216s0f1 --mapping all --output test --pin 14 15 16 17 18 19 20 21 22 23 24 25 26 27 --port 3400 --scheduler none --threads 4 --timestamp hardware --transport udp mt
```

Use `cargo run --release -- --help` for more information.

## Running the netlat client

```
cd client;

RUST_LOG='netlat=debug' cargo run -- --iface enp216s0f1 --output test --pin 14 15 16 17 18 19 20 21 22 23 24 25 26 27 --scheduler none --timestamp hardware --transport udp 10 192.168.0.34:3400 192.168.0.34:3400 192.168.0.34:3400
```

Use `cargo run --release -- --help` for more information.
