# Change log

## 0.3.0-rc.1

Breaking changes:

- Bumped the MSRV to Rust 1.83.
- Drop `arrow2` support.
- Drop `arrow<53` support.

## 0.2.7

- Add `arrow=59` support

## 0.2.6

- Add `arrow=58` support

## 0.2.5

- Add `arrow=57` support

Thanks [@lonless9](https://github.com/lonless9) for the [contribution](https://github.com/chmp/marrow/pull/33)

## 0.2.4

- Add `arrow=56` support.

Thanks [@jpopesculian](https://github.com/jpopesculian) for the
[contribution](https://github.com/chmp/marrow/pull/30).

## 0.2.3

- Add `arrow=55` support

## 0.2.2

- Add helpers to work with bit arrays

## 0.2.1

- Add `arrow=54` support
- Add support for `BinaryView` and `Utf8View` for `arrow>=53`

## 0.2.0

Breaking changes:

- Rework map arrays to use explicit keys and values array to simplify interaction the underlying
  arrays
- Rename `DenseUnion` to `Union` and change offsets to be `Option<Vec<i32>>`, implement sparse
  unions
- Rename `Dictionary::indices` to `Dictionary::keys`
- Remove the sorted flag from the dictionary `DataType` it is not supported by `arrow`
- Rework `StructArray` and `UnionArray`: place metadata in front of arrays in `StructArray::fields`,
  `UnionArray::fields`

New features

- Add `Interval` arrays and the `Interval` data type
- Add `RunEndEncoded` arrays
- Add `Array::data_type()` and `View::data_type()`
- Add `MarrowError::new` and `MarrowError::with_cause`
- Add `as_view` for `Array` and the array structs
- Implement `PartialEq` for `Array` and `View`, and `FieldMeta`
- Implement `Default` for `Field` and `FieldMeta`

## 0.1.0

Initial release to publish the arrow interop functionality of
[`serde_arrow`](https://github.com/chmp/serde_arrow) as a separate crate.
