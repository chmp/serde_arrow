# Example how to convert arrow2 arrays to arrow arrays using the FFI interface

The [example](src/main.rs) demonstrates

- how to build an arrow array from arrow2 arrays in `example_array`
- how to build an arrow record batch from arrow2 arrays in
  `example_record_batch`

Relevant docs:

- https://docs.rs/arrow/latest/arrow/ffi/index.html
- https://docs.rs/arrow2/latest/arrow2/ffi/fn.export_array_to_c.html
- https://docs.rs/arrow2/latest/arrow2/ffi/fn.export_field_to_c.html
