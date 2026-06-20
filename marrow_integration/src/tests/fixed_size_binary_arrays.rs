use marrow::array::{Array, FixedSizeBinaryArray};

use super::utils::{as_array_ref, assert_arrays_eq, PanicOnError};

mod fixed_size_binary {
    use super::*;

    #[test]
    fn not_nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            as_array_ref::<arrow_array::FixedSizeBinaryArray>(vec![
                b"foo" as &[u8],
                b"bar",
                b"baz",
            ]),
            Array::FixedSizeBinary(FixedSizeBinaryArray {
                validity: None,
                n: 3,
                data: b"foobarbaz".to_vec(),
            }),
        )
    }

    #[test]
    fn nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            as_array_ref::<arrow_array::FixedSizeBinaryArray>(vec![
                Some(b"foo" as &[u8]),
                Some(b"bar"),
                None,
                None,
            ]),
            Array::FixedSizeBinary(FixedSizeBinaryArray {
                validity: Some(marrow::bit_vec![true, true, false, false]),
                n: 3,
                data: b"foobar\0\0\0\0\0\0".to_vec(),
            }),
        )
    }
}
