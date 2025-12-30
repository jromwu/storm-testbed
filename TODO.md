# RDMA priority ring testbed TODO

## Context / goal
- Build a ring-based RDMA microbenchmark to show how prioritizing different RC QPs affects collective-like traffic. One rank per host; 7 priority levels (0–6) connecting each host to its next neighbor. For each chunk: RDMA WRITE on the chosen priority QP, followed by WRITE_WITH_IMM to notify; receiver waits a configurable compute delay after the immediate before allowing the next chunk; measure total and per-chunk timing.

## Open questions (please confirm)
- [x] OK to implement in C++17 with rdma-core (libibverbs + CMake); avoid rdma-cm and instead use verbs + OOB TCP control to exchange QP/rkey info.
- [ ] Default RNIC + port (targeting mlx5_0 / port 1 unless hosts differ; need `ibv_devinfo -l` to confirm on each host).
- [x] Two hosts with passwordless SSH (sudoer) available; need ssh-agent guidance for password-protected keys.
- [x] Defaults: chunk_count = 7, chunk_bytes = 128MB, compute_delay_us = 1.

## Milestones / tasks (incremental, testable)
- [ ] Repo scaffolding
  - [ ] Decide language/build system; set up `src/` + CMake; add minimal logging and cli parsing.
  - [ ] Add config template `config/hosts.yaml` (hosts with ssh_user/host/port, rdma_device/port, workdir, priority_levels=7, chunk_bytes, chunk_count, compute_delay_us, iterations, results_dir, optional tunings).
- [ ] RDMA utilities
  - [ ] Memory registration + buffer layout for send/recv per priority (pinned; optional hugepages).
  - [ ] QP factory: rdma_cm to resolve neighbor; create 7 RC QPs with priority mapping (SL/traffic class per priority); exchange QP nums, rkeys, and remote addrs via CM private data/control socket.
  - [ ] CQ/comp channel setup; helpers to post WRITE and WRITE_WITH_IMM; polling and error handling.
- [ ] Per-node ring worker
  - [ ] CLI args: rank id, total ranks, neighbor hosts, chunk_bytes, chunk_count, compute_delay_us, priority cycle strategy.
  - [ ] Startup barrier (orchestrator-coordinated) before posting buffers; initialize per-priority send targets.
  - [ ] Chunk loop: for chunk i, choose QP priority i % 7 (first chunk uses highest priority); post RDMA_WRITE then WRITE_WITH_IMM (immediate = chunk id). Poll CQ for incoming immediates from previous rank; after receipt, sleep compute delay, then allow next chunk. Track per-chunk timestamps.
  - [ ] Graceful teardown, stats emission (per-chunk latency, total runtime).
- [ ] Orchestration / automation
  - [ ] Python `scripts/run_ring.py`: read config, build binary, rsync to hosts, start ranks via SSH, collect logs/metrics; optional sudo steps for sysctls/ethtool/DSCP map.
  - [ ] Simple log parser to collate per-run timing into CSV/JSON.
- [ ] Validation plan
  - [ ] Local loopback smoke test (single host talking to itself) to verify verbs flow and completion handling.
  - [ ] 2-host ring to confirm WRITE+WRITE_IMM ordering and compute-delay gating.
  - [ ] Full N-host ring per config; multiple iterations to compare priority ordering vs baseline (single priority); capture counters if available.

## Config sketch (to be added in `config/hosts.yaml`)
```
hosts:
  - name: host0
    ssh_host: host0
    ssh_user: ubuntu
    ssh_port: 22
    rdma_device: mlx5_0
    rdma_port: 1
    workdir: /home/ubuntu/storm-testbed
run:
  total_ranks: 4
  priority_levels: 7
  chunk_bytes: 1048576
  chunk_count: 7
  compute_delay_us: 500
  iterations: 3
  results_dir: /tmp/rdma-prio
```
- Rank i sends to (i+1) % N and receives from (i-1+N) % N.
- Priority mapping: assume priority 0 is highest; map to SL/traffic class = priority index (confirm hardware expectations).

## Notes
- Timing: measure start after all ranks signal ready; end after last immediate processed. Emit per-chunk latency and total runtime for aggregation.
- Use WRITE_WITH_IMM for notifications; no RNR-driven RECV postings needed on receiver since data lands via WRITE and the immediate drives completion.
- Orchestrator should support staged testing (local -> 2-host -> full ring) and allow toggling compute_delay_us and chunk sizing to study sensitivity.
