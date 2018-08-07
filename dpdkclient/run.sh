#!/usr/bin/env bash
set -x

RUSTFLAGS='-lnuma' LD_LIBRARY_PATH=/usr/local/lib/ RUST_BACKTRACE=1 cargo +nightly run -- -w 'd8:00.1' -cf -n1 -d /usr/local/lib/librte_mempool_ring.so -d /usr/local/lib/librte_pmd_mlx5.so -d /usr/local/lib/librte_pmd_mlx5_glue.so.18.02.0  --  -p1 -d