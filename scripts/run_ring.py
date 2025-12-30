#!/usr/bin/env python3
"""
Launch the RDMA priority ring benchmark across hosts defined in config/hosts.yaml.
Requires passwordless SSH (ssh-agent recommended) and rsync. Assumes rdma_ring is built locally.
Generates a tiny per-rank remote wrapper script and triggers it with a simple ssh command.
"""
import argparse
import pathlib
import shlex
import subprocess
import sys
import tempfile
from typing import Dict, List

try:
    import yaml
except ImportError:
    print("PyYAML is required: pip install pyyaml", file=sys.stderr)
    sys.exit(1)


def sh(cmd: List[str], check: bool = True, capture: bool = False):
    return subprocess.run(
        cmd,
        check=check,
        text=True,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.STDOUT if capture else None,
    )


def load_config(path: pathlib.Path) -> Dict:
    with path.open() as f:
        cfg = yaml.safe_load(f)
    if "hosts" not in cfg or "run" not in cfg:
        raise ValueError("config must contain hosts and run sections")
    return cfg


def build_local():
    sh(["cmake", "-S", ".", "-B", "build", "-DCMAKE_BUILD_TYPE=Release"])
    sh(["cmake", "--build", "build", "-j"])


def sync_binary_to_host(local_bin: pathlib.Path, cfg: Dict):
    dest = f"{cfg['ssh_user']}@{cfg['ssh_host']}:{cfg['workdir']}/build/"
    sh(["ssh", f"{cfg['ssh_user']}@{cfg['ssh_host']}", "mkdir", "-p", f"{cfg['workdir']}/build"])
    sh(["rsync", "-av", local_bin.as_posix(), dest])


def write_remote_script(path: pathlib.Path, workdir: str, log_file: str, args: List[str]) -> None:
    quoted_args = " ".join(shlex.quote(a) for a in args)
    script = (
        "#!/usr/bin/env bash\n"
        f"workdir={shlex.quote(workdir)}\n"
        f"log_file={shlex.quote(log_file)}\n"
        "mkdir -p \"$(dirname \"$log_file\")\"\n"
        "cd \"$workdir\"\n"
        f"./build/rdma_ring {quoted_args} 2>&1 | tee -a \"$log_file\"\n"
    )
    path.write_text(script, encoding="utf-8")


def launch_rank(
    cfg: Dict,
    rank: int,
    ranks: int,
    next_host: str,
    run_cfg: Dict,
    script_dir: pathlib.Path,
) -> subprocess.Popen:
    log_dir = str(run_cfg.get("results_dir", "/tmp/rdma-prio"))
    remote_log = f"{log_dir}/rank{rank}_{cfg['name']}.log"
    ssh_dest = f"{cfg['ssh_user']}@{cfg['ssh_host']}"
    args = [
        "--rank",
        str(rank),
        "--ranks",
        str(ranks),
        "--next-host",
        next_host,
        "--oob-port",
        str(run_cfg.get("oob_port", 18515)),
        "--rdma-device",
        str(cfg.get("rdma_device", "mlx5_0")),
        "--rdma-port",
        str(cfg.get("rdma_port", 1)),
        "--chunk-bytes",
        str(run_cfg.get("chunk_bytes", 134217728)),
        "--chunk-count",
        str(run_cfg.get("chunk_count", 7)),
        "--compute-delay-us",
        str(run_cfg.get("compute_delay_us", 1)),
    ]
    if run_cfg.get("uniform_priority"):
        args.append("--uniform-priority")
    if "gid_index" in run_cfg:
        args.extend(["--gid-index", str(run_cfg["gid_index"])])
    local_script = script_dir / f"rdma_ring_remote_{cfg['name']}_rank{rank}.sh"
    write_remote_script(local_script, cfg["workdir"], remote_log, args)
    helper_dest = f"{cfg['ssh_user']}@{cfg['ssh_host']}:{cfg['workdir']}/rdma_ring_remote.sh"
    sh(["rsync", "-av", local_script.as_posix(), helper_dest])
    sh(["ssh", f"{cfg['ssh_user']}@{cfg['ssh_host']}", "chmod", "+x", f"{cfg['workdir']}/rdma_ring_remote.sh"])
    cmd = ["ssh", ssh_dest, "bash", f"{cfg['workdir']}/rdma_ring_remote.sh"]
    printable = " ".join(shlex.quote(x) for x in cmd)
    print(f"[launch rank {rank} -> {cfg['name']}] {printable}")
    return subprocess.Popen(cmd)


def main():
    parser = argparse.ArgumentParser(description="Run RDMA ring across hosts.")
    parser.add_argument("--config", default="config/hosts.yaml", help="Path to hosts config")
    parser.add_argument("--skip-build", action="store_true", help="Skip local cmake build")
    parser.add_argument("--uniform-priority", action="store_true", help="Use default priority on all QPs")
    parser.add_argument("--gid-index", type=int, help="Override gid_index from config")
    args = parser.parse_args()

    cfg_path = pathlib.Path(args.config)
    cfg = load_config(cfg_path)
    hosts = cfg["hosts"]
    run_cfg = cfg["run"]
    run_cfg["uniform_priority"] = args.uniform_priority or run_cfg.get("uniform_priority", False)
    if args.gid_index is not None:
        run_cfg["gid_index"] = args.gid_index
    ranks = len(hosts)
    if ranks < 2:
        print("Need at least 2 hosts for the ring.", file=sys.stderr)
        sys.exit(1);

    local_bin = pathlib.Path("build/rdma_ring")
    if not args.skip_build:
        print("Building locally...")
        build_local()
    if not local_bin.exists():
        print("Binary not found at build/rdma_ring", file=sys.stderr)
        sys.exit(1)

    print("Syncing binary to hosts...")
    for h in hosts:
        sync_binary_to_host(local_bin, h)

    print("Launching ranks...")
    procs = []
    with tempfile.TemporaryDirectory(prefix="rdma-ring-") as tmpdir:
        script_dir = pathlib.Path(tmpdir)
        for idx, h in enumerate(hosts):
            next_h = hosts[(idx + 1) % ranks]
            p = launch_rank(h, idx, ranks, next_h["ssh_host"], run_cfg, script_dir)
            procs.append((h["name"], p))

    # Wait for completion and stream output if needed.
    rc = 0
    for name, p in procs:
        ret = p.wait()
        if ret != 0:
            print(f"{name}: exited with {ret}", file=sys.stderr)
            rc = ret
    if rc == 0:
        print("All ranks completed. Check logs under results_dir on each host.")
    sys.exit(rc)


if __name__ == "__main__":
    main()
