#!/usr/bin/env bash
workdir=/home/jw2282/storm-testbed
log_file=/tmp/rdma-prio/rank0_nf-server101.log
mkdir -p "$(dirname "$log_file")"
cd "$workdir"
./build/rdma_ring --rank 0 --ranks 2 --next-host nf-server102 --oob-port 18515 --rdma-device mlx5_0 --rdma-port 1 --chunk-bytes 16777216 --chunk-count 7 --compute-delay-us 1 --gid-index 3 2>&1 | tee -a "$log_file"
