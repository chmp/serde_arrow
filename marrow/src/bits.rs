//! Helpers to work with bit vectors
//!
//! Bit vectors encode eight booleans into the bits of a byte. The Arrow format uses them to encode
//! validity information of arrays, but also for boolean arrays.
//!
//! To construct bit vectors as arrays or vectors see [`marrow::bit_array!`][crate::bit_array] and
//! [`marrow::bit_vec!`][crate::bit_vec].

/// Build a fixed-size bit array from a sequence of booleans
///
/// ```rust
/// assert_eq!(marrow::bit_array![true],  [0b_1]);
/// assert_eq!(marrow::bit_array![true, true],  [0b_11]);
/// assert_eq!(marrow::bit_array![true, true, false],  [0b_011]);
/// assert_eq!(marrow::bit_array![true, true, false, true],  [0b_1011]);
///
/// assert_eq!(
///     marrow::bit_array![
///         // first byte
///         true, true, false, false, true, false, true, false,
///         // second byte
///         true, true, true, false, true,
///     ],
///     [0b_01010011, 0b_10111],
/// );
/// ```
///
/// If all items are const expressions, `bit_array` can be used in const contexts, e.g.,
///
/// ```rust
/// const fn func() -> bool {
///     3 % 2 == 0
/// }
///
/// const { marrow::bit_array![true, false, func()] }
/// # ;
/// ```
///
/// Rules starting with `@num_bits` and `@num_bytes` are internal and are not subject to
/// compatibility guarantees.
///
/// See also [`marrow::bits`][crate::bits].
#[macro_export]
macro_rules! bit_array {
    ($($items:expr),* $(,)?) => {
        {
            const N: usize = $crate::bit_array![@num_bits, $($items),*];
            const M: usize = $crate::bit_array![@num_bytes, $($items),*];

            let items: [bool; N] = [$($items),*];
            let mut res: [u8; M] = [0; M];

            // use while loop to be const-compatible
            let mut idx = 0;
            while idx < items.len() {
                if items[idx] {
                    res[idx / 8] |= 1 << (idx % 8);
                }
                idx += 1;
            }
            res
        }
    };
    (@num_bits $(,)?) => { const { 0_usize } };
    (@num_bits, $head:expr $(, $tail:expr)* $(,)?) => {
        const { 1_usize + $crate::bit_array!(@num_bits, $($tail),*) }
    };
    (@num_bytes $(, $items:expr)* $(,)?) => {
        const {
            const N: usize = $crate::bit_array!(@num_bits $(, $items)*);
            const EXTRA_BYTE: usize = if (N % 8) != 0 { 1 } else { 0 };
            N / 8 + EXTRA_BYTE
        }
    }
}

const _: () = const {
    assert!(crate::bit_array![@num_bits, ] == 0);
    assert!(crate::bit_array![@num_bits] == 0);
    assert!(crate::bit_array![@num_bits, 1, 2, 3] == 3);
    assert!(crate::bit_array![@num_bits, 1, 2, 3, ] == 3);

    assert!(crate::bit_array![@num_bytes, ] == 0);
    assert!(crate::bit_array![@num_bytes] == 0);
    assert!(crate::bit_array![@num_bytes, 1, 2, 3] == 1);
    assert!(crate::bit_array![@num_bytes, 1, 2, 3, ] == 1);

    assert!(crate::bit_array![@num_bytes, 1, 2, 3, 4, 5, 6, 7, 8, 9] == 2);
    assert!(crate::bit_array![@num_bytes, 1, 2, 3, 4, 5, 6, 7, 8, 9, ] == 2);
};

/// Construct a bit vector (`Vec<u8>`) from a sequence of booleans
///
/// ```rust
/// assert_eq!(marrow::bit_vec![true, true, false, true],  vec![0b_1011]);
/// ```
///
/// Equivalent to `marrow::bit_array![..].to_vec()`. See [`marrow::bit_array`][crate::bit_array] for
/// more details. See also [`marrow::bits`][crate::bits].
///
#[macro_export]
macro_rules! bit_vec {
    ($($items:expr),* $(,)?) => { $crate::bit_array![$($items),*].to_vec() }
}

#[test]
fn test_bit_array() {
    // force non-const'ness
    fn bool(val: bool) -> bool {
        val
    }

    assert_eq!(
        crate::bit_array![bool(true), bool(false), bool(true), bool(true)],
        [0b_1101]
    );

    const ARRAY: [u8; 1] = const { crate::bit_array![true, false, true, true] };
    assert_eq!(ARRAY, [0b_1101]);
}

/// Get a bit of a bit vector
///
/// ```rust
/// # use marrow::bits;
/// #
/// let bit_vec = &[0b_01010011, 0b_10111];
///
/// assert_eq!(bits::get(bit_vec, 0), true);
/// assert_eq!(bits::get(bit_vec, 1), true);
/// assert_eq!(bits::get(bit_vec, 2), false);
/// assert_eq!(bits::get(bit_vec, 3), false);
/// assert_eq!(bits::get(bit_vec, 4), true);
/// assert_eq!(bits::get(bit_vec, 5), false);
/// assert_eq!(bits::get(bit_vec, 6), true);
/// assert_eq!(bits::get(bit_vec, 7), false);
///
/// assert_eq!(bits::get(bit_vec, 8), true);
/// assert_eq!(bits::get(bit_vec, 9), true);
/// assert_eq!(bits::get(bit_vec, 10), true);
/// assert_eq!(bits::get(bit_vec, 11), false);
/// assert_eq!(bits::get(bit_vec, 12), true);
/// ```
pub const fn get(bit_vec: &[u8], idx: usize) -> bool {
    let mask = 1 << (idx % 8);
    bit_vec[idx / 8] & mask == mask
}

/// Set a bit of a bit vector
///
/// ```rust
/// # use marrow::bits;
/// #
/// let mut bit_vec = [0; 2];
///
/// // update bits in random order
/// bits::set(&mut bit_vec, 9, true);
/// bits::set(&mut bit_vec, 4, true);
/// bits::set(&mut bit_vec, 11, false);
/// bits::set(&mut bit_vec, 2, false);
/// bits::set(&mut bit_vec, 7, false);
/// bits::set(&mut bit_vec, 1, true);
/// bits::set(&mut bit_vec, 12, true);
/// bits::set(&mut bit_vec, 0, true);
/// bits::set(&mut bit_vec, 5, false);
/// bits::set(&mut bit_vec, 8, true);
/// bits::set(&mut bit_vec, 3, false);
/// bits::set(&mut bit_vec, 10, true);
/// bits::set(&mut bit_vec, 6, true);
///
/// assert_eq!(&bit_vec, &[0b_01010011, 0b_10111]);
///
/// ```
pub const fn set(bit_vec: &mut [u8], idx: usize, value: bool) {
    let mask = 1 << (idx % 8);
    if value {
        bit_vec[idx / 8] |= mask;
    } else {
        bit_vec[idx / 8] &= !mask;
    }
}

/// Push a new bit into a bit vector with `len` items
///
///
/// Usage:
///
/// ```rust
/// # use marrow::bits;
/// #
/// let mut vec = Vec::new();
/// let mut len = 0;
///
/// bits::push(&mut vec, &mut len, true);
/// assert_eq!(&vec, &[0b_1]);
/// assert_eq!(len, 1);
///
/// bits::push(&mut vec, &mut len, true);
/// assert_eq!(&vec, &[0b_11]);
/// assert_eq!(len, 2);
///
/// bits::push(&mut vec, &mut len, false);
/// assert_eq!(&vec, &[0b_011]);
/// assert_eq!(len, 3);
///
/// bits::push(&mut vec, &mut len, true);
/// assert_eq!(&vec, &[0b_1011]);
/// assert_eq!(len, 4);
///
/// bits::push(&mut vec, &mut len, false);
/// assert_eq!(&vec, &[0b_01011]);
/// assert_eq!(len, 5);
///
/// bits::push(&mut vec, &mut len, false);
/// assert_eq!(&vec, &[0b_001011]);
/// assert_eq!(len, 6);
///
/// bits::push(&mut vec, &mut len, true);
/// assert_eq!(&vec, &[0b_1001011]);
/// assert_eq!(len, 7);
///
/// bits::push(&mut vec, &mut len, true);
/// assert_eq!(&vec, &[0b_11001011]);
/// assert_eq!(len, 8);
///
/// bits::push(&mut vec, &mut len, true);
/// assert_eq!(&vec, &[0b_11001011, 0b_1]);
/// assert_eq!(len, 9);
/// ```
///
pub fn push(bit_vec: &mut Vec<u8>, len: &mut usize, value: bool) {
    // custom impl to keep MSRV
    fn div_ceil(a: usize, b: usize) -> usize {
        (a / b) + if (a % b) != 0 { 1 } else { 0 }
    }

    assert_eq!(
        div_ceil(*len, 8),
        bit_vec.len(),
        "len and bit_vec incompatible"
    );

    if *len == 8 * bit_vec.len() {
        bit_vec.push(0);
    }

    set(bit_vec, *len, value);
    // NOTE: needs to be last
    *len += 1;
}
