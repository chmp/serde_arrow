# Deferred serialization optimization ideas

These ideas preserve the public API and avoid new unsafe code, but are not part
of the current implementation batch.

1. Reserve the `BytesViewArray` backing byte buffer for known-length binary
   sequences. Benchmark a BinaryView `Vec<u8>` workload and check allocation
   counts before choosing a policy; do not use a fixed byte estimate.
2. Avoid temporary heap `String` values when serializing scalar values into
   UTF-8 arrays: literals for booleans, a stack buffer for `char`, and `itoa` /
   `ryu` buffers for numeric values. Add formatting compatibility tests.
3. Preallocate field and child-data vectors in marrow-to-Arrow struct and union
   conversion. This is expected to be a small conversion-only improvement.
4. Add an isolated marrow-to-Arrow conversion Criterion benchmark with setup
   outside the timed loop, plus a builder-construction benchmark.
5. Consider parallel top-level conversion only after isolated benchmarks show a
   wide, conversion-heavy workload where thread overhead is amortized.
6. For repeated calls, document/benchmark reuse through the existing
   `ArrayBuilder::from_arrow` path; stateless `to_arrow` cannot retain owned
   schema state internally.

Not pursued: bypassing Arrow validation (would weaken safety guarantees),
larger fixed string-capacity estimates (measured regression), and public typed
or batched serialization APIs (outside the requested API constraints).
