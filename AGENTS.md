# Repository Guidelines

## Project Structure & Module Organization
- `TODO.md`: roadmap for the RDMA priority-ring testbed.
- `config/`: host and run configuration templates (e.g., `hosts.yaml`).
- `src/`: RDMA benchmark code (planned: C++17 with rdma-core and CMake).
- `scripts/`: orchestration helpers for multi-host runs, log collection, and parsing.
- `build/`: local CMake build output (not checked in).

## Build, Test, and Development Commands
- Configure: `cmake -S . -B build -DCMAKE_BUILD_TYPE=Release` (set up out-of-tree build).
- Build: `cmake --build build --config Release -j$(nproc)` (compile binaries).
- Run single-host smoke test (when available): `./build/rdma_ring --rank 0 --ranks 1 --config config/hosts.yaml`.
- Format (if `clang-format` is added): `clang-format -i $(find src -name '*.cc' -o -name '*.h')`.

## Coding Style & Naming Conventions
- C++17, 2- or 4-space indentation; prefer consistent brace-on-same-line for control blocks.
- Use `snake_case` for functions/variables, `PascalCase` for types, `kCamelCase` for constants.
- Keep headers minimal; favor RAII wrappers for RDMA resources and error-check all returns.
- Log with concise, actionable messages; include host/rank context for distributed flows.

## Testing Guidelines
- Add lightweight smoke tests first (loopback or 2-host) before scaling full ring.
- Name tests after behavior (e.g., `RingWriteImmOrdersChunks`); group by component.
- Prefer deterministic parameters (fixed chunk size/delay) for repeatability.
- Record runtime metrics to files under `results/` or a run-specific subdir for later parsing.

## Commit & Pull Request Guidelines
- Commit messages: short imperative subject, optional bullet body for key changes/risks.
- Keep commits focused (config, transport logic, orchestration separated when possible).
- PRs should summarize behavior changes, mention test scope/hosts used, and note any required privileges (e.g., sudo for RDMA tuning).

## Security & Configuration Tips
- Store host lists and SSH users in `config/hosts.yaml`; avoid committing secrets or private keys.
- Validate RDMA device/port and priority mapping (SL/DSCP) per host; prefer per-run overrides instead of code edits.
