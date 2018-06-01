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

Run a server on interface `enp216s0f1` port 3400, with hardware timestamps, store results in `latencies-rserver-3400-test.csv`:

```
cd rserver; cargo run --release -- --iface enp216s0f1 -t hardware -s test 3400 single
```

Use `cargo run --release -- --help` for more information.

## Running the netlat client

Send 100000 requests to the server listening on `192.168.1.40:3400` over our local interface `enp216s0f1`, store result in `latencies-client-3400-test.csv`:

```
cd client; cargo run -- --iface enp216s0f1 -t hardware -r 100000 -p 192.168.1.40:3400 -s test
```

Use `cargo run --release -- --help` for more information.
