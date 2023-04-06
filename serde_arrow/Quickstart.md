# Quickstart guide

**Contents**

1. [Working with date time objects](#working-with-date-time-objects)
2. [Dictionary encoding for strings](#dictionary-encoding-for-strings)
3. [Convert from arrow2 to arrow arrays](#convert-from-arrow2-to-arrow-arrays)

## Working with date time objects

When using `chrono`'s `DateTime<Utc>` or  `NaiveDateTime`, the values are per
default encoded as strings. To stores them as  `Date64` columns, the data type
has to be modified.

For example

```rust
#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Record {
    val: NaiveDateTime,
}

let records: &[Record] = &[
    Record {
        val: NaiveDateTime::from_timestamp(12 * 60 * 60 * 24, 0),
    },
    Record {
        val: NaiveDateTime::from_timestamp(9 * 60 * 60 * 24, 0),
    },
];

let mut fields = serialize_into_fields(records, Default::default()).unwrap();
```

The traced field `val` will be of type `Utf8`. To store it as `Date64` field,
modify the data type as in

```rust
*find_field_mut(&mut fields, "val").unwrap() = Field::new(
    "val", DataType::Date64, false,
).with_metadata(Strategy::NaiveStrAsDate64.into());
```

Integer fields containing timestamps in milliseconds since the epoch can be
directly stored as `Date64`:

```rust
#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Record {
    timestamp: i64,
}

let records: &[Record] = &[
    Record { timestamp: 12 * 60 * 60 * 24 * 1000 },
    Record { timestamp: 9 * 60 * 60 * 24 * 1000 },
];

let mut fields = serialize_into_fields(records, Default::default()).unwrap();
find_field_mut(&mut fields, "timestmap").unwrap() = Field::new(
    "timestamp", DataType::Date64, false,
);
```

## Dictionary encoding for strings

To encode strings with repeated values via a dictionary, the data type of the
corresponding field must be changed from `Utf8` or `LargeUtf8` to `Dictionary`.

For an existing field this can be done via:

```rust
field.data_type = DataType::Dictionary(
    // the integer type used for the keys
    IntegerType::UInt32,
    // the data type of the values
    Box::new(DataType::Utf8),
    // serde_arrow does not support sorted generating sorted dictionaries
    false,
);
```

To dictionary encode all string fields, set the `string_dictionary_encoding` of
`TracingOptions`, when tracing the fields:

```rust
let fields = serialize_into_fields(
    &items,
    TracingOptions::default().string_dictionary_encoding(true),
)?;
```

## Convert from arrow2 to arrow arrays

Both `arrow` and `arrow2` use the Arrow memory format. Thanks to this fact, it
is possible to convert arrays between both packages with minimal work using
their respective FFI interfaces:

- [arrow2::ffi::export_field_to_c](https://docs.rs/arrow2/latest/arrow2/ffi/fn.export_field_to_c.html)
- [arrow2::ffi_export_array_to_ce](https://docs.rs/arrow2/latest/arrow2/ffi/fn.export_array_to_c.html)
- [arrow::ffi::ArrowArray::new](https://docs.rs/arrow/latest/arrow/ffi/struct.ArrowArray.html#method.new)

A fully worked example can be found in the [arrow2-to-arrow][] example of the
`serde_arrow` repository.

[arrow2-to-arrow]: https://github.com/chmp/serde_arrow/tree/main/arrow2-to-arrow
