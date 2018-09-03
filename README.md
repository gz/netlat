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

RUST_LOG='rserver=debug' cargo run -- --iface enp216s0f1 --output test --pin 1 2 3 4 --scheduler default --timestamp hardware --transport udp --port 3400 --threads 4 --mapping all --sockets 1 --socketmapping all mt


# No debug

cargo run --release -- --iface enp216s0f1 --output test --pin none --scheduler default --timestamp hardware --transport udp --port 3400 --threads 1 --mapping all --sockets 1 --socketmapping all mt

cargo run --release -- --iface enp216s0f1 --output test --pin 37 --scheduler default --timestamp hardware --transport udp --port 3400 --threads 1 --mapping all --sockets 1 --socketmapping all mt



cargo run --release -- --iface enp216s0f1 --output test --pin 14 15 16 17 18 19 20 21 22 23 24 25 26 27 42 43 44 45 46 47 48 49 50 51 52 53 54 55 --scheduler default --timestamp hardware --transport udp --port 3400 --threads 1 --mapping all --sockets 1 --socketmapping all mt

cargo run --release -- --iface enp216s0f1 --output test --pin 1 2 3 4 --scheduler default --timestamp hardware --transport udp --port 3400 --threads 4 --mapping all --sockets 1 --socketmapping all mt


```

Use `cargo run --release -- --help` for more information.

## Running the netlat client

```
cd client;

RUST_LOG='netlat=debug' cargo run -- --iface enp216s0f1 --output test --pin 14 15 16 17 18 19 20 21 22 23 24 25 26 27 --scheduler default --timestamp hardware --transport udp 10 192.168.0.34:3400 192.168.0.34:3400 192.168.0.34:3400

cargo run --release -- --iface enp216s0f1 --output test --pin 14 15 16 17 18 19 20 21 22 23 24 25 26 27 --scheduler default --timestamp hardware --transport udp 100000 192.168.0.34:3400 192.168.0.34:3400 192.168.0.34:3400

# Debug


RUST_LOG='netlat=debug' cargo run -- --iface enp216s0f1 --output test --pin 14 15 16 17 18 19 20 21 22 23 24 25 26 27 --scheduler default --timestamp hardware --transport udp 10 192.168.0.34:3400 192.168.0.34:3400 192.168.0.34:3400

cargo run --release -- --iface enp216s0f1 --output test --pin 14 15 16 17 18 19 20 21 22 23 24 25 26 27 --scheduler default --timestamp hardware --transport udp 1000000 192.168.0.34:3400 192.168.0.34:3400 192.168.0.34:3400



RUST_LOG='netlat=debug' cargo run -- --iface enp216s0f1 --output test --pin 14 15 16 17 18 19 20 21 22 23 24 25 26 27 --rate 1000 --scheduler default --timestamp hardware --transport udp 10 192.168.0.34:3400

cargo run --release -- --iface enp216s0f1 --output test --pin 14 15 16 17 18 19 20 21 22 23 24 25 26 27 --rate 1000 --scheduler default --timestamp hardware --transport udp 10 192.168.0.34:3400


```

Use `cargo run --release -- --help` for more information.
