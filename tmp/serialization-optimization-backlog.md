# Deferred serialization optimization ideas

These ideas preserve the public API and avoid new unsafe code, but need focused
benchmarks and/or allocation profiles before implementation.

1. Cache static reordered `SerializeStruct` field names in `StructBuilder`.
   The canonical-layout path already handles derived structs, but repeated,
   stable reordered layouts still linearly scan fields. Only enable a static
   name-to-index cache for schemas with unique field names.
2. Avoid temporary heap `String` values when serializing scalar values into
   UTF-8 arrays: literals for booleans, a stack buffer for `char`, and `itoa` /
   `ryu` buffers for numeric values. Add exact formatting tests for NaN,
   infinity, negative zero, and exponent formatting.
3. Avoid recursive Arrow-field conversion during Arrow deserializer setup when
   only top-level field names and strategy metadata are required. Preserve
   metadata validation and decide whether unsupported-schema rejection remains
   part of the setup contract before changing this path.
4. Share immutable builder configuration (field names and metadata) across
   `take`/reset cycles. This may help repeated small flushes on deep schemas,
   but requires carefully keeping emitted `FieldMeta` independently owned.
5. Benchmark an isolated marrow-to-Arrow conversion path with setup outside
   the timed loop, plus builder-construction and wide-schema deserializer setup
   benchmarks. Use heaptrack or dhat to distinguish allocation reductions from
   host timing noise.
6. Consider reserving dictionary indices and values for known outer lengths.
   Measure 1%, 50%, and 100% cardinality first: high-cardinality data benefits,
   while duplicate-heavy dictionaries can waste substantial memory.
7. Avoid temporary external-buffer writes for known BinaryView sequences of at
   most 12 bytes by accumulating them inline. Test unknown-length sequences and
   error paths as well as `[u8; N]` inputs.
8. Specialize already-validated `&[u8]` serialization for fixed-size lists to
   skip generic per-element length bookkeeping. Preserve generic checks for
   arbitrary Serde sequences and tuples.
9. Replace the quadratic unique-field-name construction scan only if profiling
   shows wide-schema setup is hot. A `HashSet` avoids O(n²) comparisons but
   reintroduces allocation and hashing for the usual small schemas.
10. Preallocate dictionary/union and wide-schema test cases before measuring
    micro-optimizations; ordinary primitive and complex benchmarks do not
    expose their relevant allocation patterns.

Not pursued: bypassing Arrow validation (would weaken safety guarantees),
larger fixed string-capacity estimates (measured regression), parallel
top-level conversion before wide-schema evidence, and public typed or batched
serialization APIs (outside the requested API constraints).
