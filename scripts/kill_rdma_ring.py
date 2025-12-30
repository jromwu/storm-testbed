#!/usr/bin/env python3
"""
Kill rdma_ring processes on all hosts in config/hosts.yaml.
"""
import argparse
import pathlib
import subprocess
import sys
from typing import Dict, List

try:
    import yaml
except ImportError:
    print("PyYAML is required: pip install pyyaml", file=sys.stderr)
    sys.exit(1)


def sh(cmd: List[str], check: bool = True):
    return subprocess.run(cmd, check=check)


def load_config(path: pathlib.Path) -> Dict:
    with path.open() as f:
        cfg = yaml.safe_load(f)
    if "hosts" not in cfg:
        raise ValueError("config must contain hosts")
    return cfg


def main() -> int:
    parser = argparse.ArgumentParser(description="Kill rdma_ring on all hosts.")
    parser.add_argument("--config", default="config/hosts.yaml", help="Path to hosts config")
    args = parser.parse_args()

    cfg = load_config(pathlib.Path(args.config))
    hosts = cfg["hosts"]
    if not hosts:
        print("No hosts in config.", file=sys.stderr)
        return 1

    for host in hosts:
        dest = f"{host['ssh_user']}@{host['ssh_host']}"
        cmd = [
            "ssh",
            dest,
            "pkill",
            "rdma_ring",
        ]
        print(f"[kill] {dest}: {' '.join(cmd)}")
        sh(cmd, check=False)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
