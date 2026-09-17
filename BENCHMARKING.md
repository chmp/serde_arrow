# Benchmarking and profiling

Run the full serialization suite with:

```bash
cargo bench -p serde_arrow_bench --bench serde_arrow_bench
```

The suite uses 1,000-record primitive and nested workloads. It reports:

- `serde_arrow_marrow`: serialization into marrow arrays only;
- `serde_arrow_marrow_push`: the reserved record-at-a-time API;
- `serde_arrow_marrow_to_arrow`: marrow serialization plus Arrow conversion;
- `serde_arrow_arrow`: the public `to_arrow` API; and
- `arrow_builder` and `arrow`: direct Arrow baselines.

Benchmark binaries retain debug line tables through `[profile.bench] debug = 1`
in the workspace manifest. This makes optimized CPU profiles attributable to
Rust source without materially changing execution performance.

## CPU profile on Linux

Enter the project development shell (`nix develop`) to obtain `perf`, Heaptrack,
and the other benchmark tools. Build the benchmark before recording so
compilation is not included:

```bash
cargo bench -p serde_arrow_bench --bench serde_arrow_bench --no-run
BENCH=$(find target/release/deps -maxdepth 1 -type f -executable -name 'serde_arrow_bench-*' | head -1)
perf record -F 999 -g --call-graph dwarf -o perf.data \
  "$BENCH" --bench --profile-time 20 complex_1000/serde_arrow_arrow
perf report --stdio --no-children
```

Replace the final filter with `primitives_1000/serde_arrow_marrow`,
`complex_1000/serde_arrow_marrow_push`, or another Criterion benchmark name.
If `perf` denies access, an administrator can temporarily set
`kernel.perf_event_paranoid=-1`.

## Allocation profile

Use Heaptrack for allocation hot spots:

```bash
heaptrack "$BENCH" --bench --profile-time 20 complex_1000/serde_arrow_arrow
heaptrack_gui heaptrack.*.gz
```
