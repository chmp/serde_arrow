//! Extension of the array types

use marrow::array::{BytesArray, BytesViewArray, PrimitiveArray};

use crate::internal::{
    error::{fail, Error, Result},
    utils::Offset,
};

const ASSUMED_BYTES_PER_ELEMENT: usize = 8;

pub trait ArrayExt: Sized + 'static {
    fn new(is_nullable: bool) -> Self;
    fn take(&mut self) -> Self;
    fn is_nullable(&self) -> bool;
    fn reserve(&mut self, additional: usize);
}

pub trait ScalarArrayExt<'value>: ArrayExt {
    type Value: 'value;

    fn push_scalar_default(&mut self) -> Result<()>;
    fn push_scalar_none(&mut self) -> Result<()>;
    fn push_scalar_value(&mut self, value: Self::Value) -> Result<()>;
}

/// An array that models a sequence
///
/// As some sequence arrays, e.g., `ListArrays`, can contain arbitrarily nested subarrays, the
/// element itself is not modelled.
pub trait SeqArrayExt: ArrayExt {
    fn push_seq_default(&mut self) -> Result<()>;
    fn push_seq_none(&mut self) -> Result<()>;
    fn start_seq(&mut self) -> Result<()>;
    fn push_seq_elements(&mut self, n: usize) -> Result<()>;
    fn end_seq(&mut self) -> Result<()>;
}

impl<T: Default + 'static> ArrayExt for PrimitiveArray<T> {
    fn new(is_nullable: bool) -> Self {
        PrimitiveArray {
            validity: is_nullable.then(Vec::new),
            values: Vec::new(),
        }
    }

    fn is_nullable(&self) -> bool {
        self.validity.is_some()
    }

    fn take(&mut self) -> Self {
        Self {
            validity: self.validity.as_mut().map(std::mem::take),
            values: std::mem::take(&mut self.values),
        }
    }

    fn reserve(&mut self, additional: usize) {
        if let Some(validity) = &mut self.validity {
            reserve_bits(validity, self.values.len(), additional);
        }
        self.values.reserve(additional);
    }
}

impl<T: Default + 'static> ScalarArrayExt<'static> for PrimitiveArray<T> {
    type Value = T;

    fn push_scalar_default(&mut self) -> Result<()> {
        set_validity_default(self.validity.as_mut(), self.values.len());
        self.values.push(T::default());
        Ok(())
    }

    fn push_scalar_none(&mut self) -> Result<()> {
        set_validity(self.validity.as_mut(), self.values.len(), false)?;
        self.values.push(T::default());
        Ok(())
    }

    fn push_scalar_value(&mut self, value: Self::Value) -> Result<()> {
        set_validity(self.validity.as_mut(), self.values.len(), true)?;
        self.values.push(value);
        Ok(())
    }
}

impl<O: Offset> ArrayExt for BytesArray<O> {
    fn new(is_nullable: bool) -> Self {
        BytesArray {
            validity: is_nullable.then(Vec::new),
            offsets: vec![O::default()],
            data: Vec::new(),
        }
    }

    fn is_nullable(&self) -> bool {
        self.validity.is_some()
    }

    fn take(&mut self) -> Self {
        Self {
            validity: self.validity.as_mut().map(std::mem::take),
            data: std::mem::take(&mut self.data),
            offsets: std::mem::replace(&mut self.offsets, vec![O::default()]),
        }
    }

    fn reserve(&mut self, additional: usize) {
        if let Some(validity) = &mut self.validity {
            reserve_bits(validity, self.offsets.len().saturating_sub(1), additional);
        }
        self.offsets.reserve(additional);
        reserve_to_new_capacity(&mut self.data, additional * ASSUMED_BYTES_PER_ELEMENT);
    }
}

impl<O: Offset> SeqArrayExt for BytesArray<O> {
    fn push_seq_default(&mut self) -> Result<()> {
        self.push_scalar_default()
    }

    fn push_seq_none(&mut self) -> Result<()> {
        self.push_scalar_none()
    }

    fn start_seq(&mut self) -> Result<()> {
        set_validity(
            self.validity.as_mut(),
            self.offsets.len().saturating_sub(1),
            true,
        )?;
        duplicate_last(&mut self.offsets)?;
        Ok(())
    }

    fn push_seq_elements(&mut self, n: usize) -> Result<()> {
        increment_last(&mut self.offsets, n)?;
        Ok(())
    }

    fn end_seq(&mut self) -> Result<()> {
        Ok(())
    }
}

impl<'s, O: Offset> ScalarArrayExt<'s> for BytesArray<O> {
    type Value = &'s [u8];

    fn push_scalar_default(&mut self) -> Result<()> {
        set_validity_default(self.validity.as_mut(), self.offsets.len().saturating_sub(1));
        duplicate_last(&mut self.offsets)?;
        Ok(())
    }

    fn push_scalar_none(&mut self) -> Result<()> {
        set_validity(
            self.validity.as_mut(),
            self.offsets.len().saturating_sub(1),
            false,
        )?;
        duplicate_last(&mut self.offsets)?;
        Ok(())
    }

    fn push_scalar_value(&mut self, value: Self::Value) -> Result<()> {
        set_validity(
            self.validity.as_mut(),
            self.offsets.len().saturating_sub(1),
            true,
        )?;
        duplicate_last(&mut self.offsets)?;
        increment_last(&mut self.offsets, value.len())?;
        self.data.extend(value);
        Ok(())
    }
}

impl ArrayExt for BytesViewArray {
    fn new(is_nullable: bool) -> Self {
        BytesViewArray {
            validity: is_nullable.then(Vec::new),
            data: Vec::new(),
            buffers: vec![vec![]],
        }
    }

    fn is_nullable(&self) -> bool {
        self.validity.is_some()
    }

    fn take(&mut self) -> Self {
        Self {
            buffers: std::mem::replace(&mut self.buffers, vec![vec![]]),
            data: std::mem::take(&mut self.data),
            validity: self.validity.as_mut().map(std::mem::take),
        }
    }

    fn reserve(&mut self, additional: usize) {
        if let Some(validity) = &mut self.validity {
            reserve_bits(validity, self.data.len(), additional);
        }
        self.data.reserve(additional);
    }
}

impl SeqArrayExt for BytesViewArray {
    fn push_seq_default(&mut self) -> Result<()> {
        self.push_scalar_default()
    }

    fn push_seq_none(&mut self) -> Result<()> {
        self.push_scalar_none()
    }

    fn start_seq(&mut self) -> Result<()> {
        set_validity(self.validity.as_mut(), self.data.len(), true)?;
        self.data.push(bytes_view::pack_len(0));
        Ok(())
    }

    fn push_seq_elements(&mut self, n: usize) -> Result<()> {
        let Some(curr) = self.data.last_mut() else {
            fail!("push_seq_elements must be called after start_seq");
        };
        *curr = bytes_view::pack_len(bytes_view::get_len(*curr) + n);
        Ok(())
    }

    fn end_seq(&mut self) -> Result<()> {
        let Some(curr) = self.data.last_mut() else {
            fail!("end_seq must be called after start_seq");
        };

        let n = bytes_view::get_len(*curr);
        if self.buffers[0].len() < n {
            fail!("Inconsistent length and data in BytesViewArray");
        }
        let start = self.buffers[0].len() - n;
        let data = &self.buffers[0][start..];

        if data.len() <= 12 {
            *curr = bytes_view::pack_inline(data);
            self.buffers[0].truncate(start);
        } else {
            *curr = bytes_view::pack_extern(data, 0, start);
        }
        Ok(())
    }
}

impl<'s> ScalarArrayExt<'s> for BytesViewArray {
    type Value = &'s [u8];

    fn push_scalar_default(&mut self) -> Result<()> {
        set_validity_default(self.validity.as_mut(), self.data.len());
        self.data.push(bytes_view::pack_inline(&[]));
        Ok(())
    }

    fn push_scalar_none(&mut self) -> Result<()> {
        set_validity(self.validity.as_mut(), self.data.len(), false)?;
        self.data.push(bytes_view::pack_inline(&[]));
        Ok(())
    }

    fn push_scalar_value(&mut self, value: Self::Value) -> Result<()> {
        set_validity(self.validity.as_mut(), self.data.len(), true)?;
        if value.len() <= 12 {
            self.data.push(bytes_view::pack_inline(value));
        } else {
            assert!(!self.buffers.is_empty());
            self.data
                .push(bytes_view::pack_extern(value, 0, self.buffers[0].len()));
            self.buffers[0].extend(value);
        }
        Ok(())
    }
}

pub mod bytes_view {
    pub fn get_len(packed: u128) -> usize {
        // NOTE: first truncate to only select the first 4 bytes
        (packed as u32) as usize
    }

    pub fn pack_len(len: usize) -> u128 {
        assert!(len <= i32::MAX as usize);
        len as u128
    }

    pub fn pack_inline(data: &[u8]) -> u128 {
        assert!(data.len() <= 12);
        let mut result = data.len() as u128;
        for (i, b) in data.iter().enumerate() {
            result |= (*b as u128) << (8 * (4 + i));
        }

        result
    }

    pub fn pack_extern(data: &[u8], buffer: usize, offset: usize) -> u128 {
        assert!(data.len() >= 4);
        assert!(data.len() <= i32::MAX as usize);
        assert!(buffer <= i32::MAX as usize);
        assert!(offset <= i32::MAX as usize);

        let len_bytes = (data.len() as u32) as u128;
        let prefix = (data[0] as u128)
            | ((data[1] as u128) << 8)
            | ((data[2] as u128) << 16)
            | ((data[3] as u128) << 24);
        let buffer_bytes = (buffer as u32) as u128;
        let offset_bytes = (offset as u32) as u128;

        len_bytes | (prefix << 32) | (buffer_bytes << 64) | (offset_bytes << 96)
    }
}

#[derive(Debug, Clone)]
pub struct OffsetsArray<O> {
    pub validity: Option<Vec<u8>>,
    pub offsets: Vec<O>,
}

impl<O: Offset> ArrayExt for OffsetsArray<O> {
    fn new(is_nullable: bool) -> Self {
        Self {
            validity: is_nullable.then(Vec::new),
            offsets: vec![O::default()],
        }
    }

    fn is_nullable(&self) -> bool {
        self.validity.is_some()
    }

    fn take(&mut self) -> Self {
        Self {
            validity: self.validity.as_mut().map(std::mem::take),
            offsets: std::mem::replace(&mut self.offsets, vec![O::default()]),
        }
    }

    fn reserve(&mut self, additional: usize) {
        if let Some(validity) = &mut self.validity {
            reserve_bits(validity, self.offsets.len().saturating_sub(1), additional);
        }
        self.offsets.reserve(additional);
    }
}

impl<O: Offset> SeqArrayExt for OffsetsArray<O> {
    fn push_seq_default(&mut self) -> Result<()> {
        set_validity_default(self.validity.as_mut(), self.offsets.len().saturating_sub(1));
        duplicate_last(&mut self.offsets)?;
        Ok(())
    }

    fn push_seq_none(&mut self) -> Result<()> {
        set_validity(
            self.validity.as_mut(),
            self.offsets.len().saturating_sub(1),
            false,
        )?;
        duplicate_last(&mut self.offsets)?;
        Ok(())
    }

    fn start_seq(&mut self) -> Result<()> {
        set_validity(
            self.validity.as_mut(),
            self.offsets.len().saturating_sub(1),
            true,
        )?;
        duplicate_last(&mut self.offsets)?;
        Ok(())
    }

    fn push_seq_elements(&mut self, n: usize) -> Result<()> {
        increment_last(&mut self.offsets, n)?;
        Ok(())
    }

    fn end_seq(&mut self) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct CountArray {
    pub len: usize,
    pub validity: Option<Vec<u8>>,
}

impl ArrayExt for CountArray {
    fn new(is_nullable: bool) -> Self {
        Self {
            len: 0,
            validity: is_nullable.then(Vec::new),
        }
    }

    fn is_nullable(&self) -> bool {
        self.validity.is_some()
    }

    fn take(&mut self) -> Self {
        Self {
            len: std::mem::take(&mut self.len),
            validity: self.validity.as_mut().map(std::mem::take),
        }
    }

    fn reserve(&mut self, additional: usize) {
        if let Some(validity) = &mut self.validity {
            reserve_bits(validity, self.len, additional);
        }
    }
}

impl SeqArrayExt for CountArray {
    fn push_seq_default(&mut self) -> Result<()> {
        set_validity_default(self.validity.as_mut(), self.len);
        self.len += 1;
        Ok(())
    }

    fn push_seq_none(&mut self) -> Result<()> {
        set_validity(self.validity.as_mut(), self.len, false)?;
        self.len += 1;
        Ok(())
    }

    fn start_seq(&mut self) -> Result<()> {
        set_validity(self.validity.as_mut(), self.len, true)?;
        self.len += 1;
        Ok(())
    }

    fn push_seq_elements(&mut self, _n: usize) -> Result<()> {
        Ok(())
    }

    fn end_seq(&mut self) -> Result<()> {
        Ok(())
    }
}

pub fn duplicate_last<T: Clone>(vec: &mut Vec<T>) -> Result<()> {
    let Some(last) = vec.last() else {
        fail!("Invalid offset array: expected at least a single element")
    };
    vec.push(last.clone());
    Ok(())
}

pub fn increment_last<O: Offset>(vec: &mut [O], inc: usize) -> Result<()> {
    let Some(last) = vec.last_mut() else {
        fail!("Invalid offset array: expected at least a single element")
    };
    *last = *last + O::try_form_usize(inc)?;
    Ok(())
}

pub fn reserve_bits(bits: &mut Vec<u8>, len: usize, additional: usize) {
    bits.reserve(calculate_bytes_to_reserve(bits.capacity(), len, additional));
}

fn calculate_bytes_to_reserve(capacity: usize, len: usize, additional: usize) -> usize {
    let target_bits_capacity = len + additional;
    let target_byte_capacity = if target_bits_capacity % 8 == 0 {
        target_bits_capacity / 8
    } else {
        target_bits_capacity / 8 + 1
    };
    target_byte_capacity.saturating_sub(capacity)
}

#[test]
fn test_calculate_bytes_to_reserve() {
    assert_eq!(calculate_bytes_to_reserve(0, 0, 0), 0);
    assert_eq!(calculate_bytes_to_reserve(2, 8, 8), 0);
    assert_eq!(calculate_bytes_to_reserve(2, 8, 9), 1);
    assert_eq!(calculate_bytes_to_reserve(2, 9, 8), 1);
    assert_eq!(calculate_bytes_to_reserve(2, 8, 16), 1);
    assert_eq!(calculate_bytes_to_reserve(2, 8, 17), 2);
}

pub fn reserve_to_new_capacity<T>(vec: &mut Vec<T>, additional: usize) {
    vec.reserve((vec.len() + additional).saturating_sub(vec.capacity()));
}

pub fn set_validity(buffer: Option<&mut Vec<u8>>, idx: usize, value: bool) -> Result<()> {
    if let Some(buffer) = buffer {
        set_bit_buffer(buffer, idx, value);
        Ok(())
    } else if value {
        Ok(())
    } else {
        Err(Error::nullability_violation(None))
    }
}

/// In contrast to `set_validity` nulls for non-nullable fields are not an error
pub fn set_validity_default(buffer: Option<&mut Vec<u8>>, idx: usize) {
    if let Some(buffer) = buffer {
        set_bit_buffer(buffer, idx, false);
    }
}

pub fn set_bit_buffer(buffer: &mut Vec<u8>, idx: usize, value: bool) {
    while idx / 8 >= buffer.len() {
        buffer.push(0);
    }

    let bit_mask: u8 = 1 << (idx % 8);
    if value {
        buffer[idx / 8] |= bit_mask;
    } else {
        buffer[idx / 8] &= !bit_mask;
    }
}

pub fn get_bit_buffer(data: &[u8], offset: usize, idx: usize) -> Result<bool> {
    let flag = 1 << ((idx + offset) % 8);
    let Some(byte) = data.get((idx + offset) / 8) else {
        fail!("Invalid access in bitset");
    };
    Ok(byte & flag == flag)
}

/// True if all bits in the `start_bit..end_bit` range are set.
pub fn all_set_buffer(data: &[u8], start_bit: usize, end_bit: usize) -> Result<bool> {
    if end_bit > data.len() * 8 {
        fail!("Invalid access in bitset");
    }

    let mut current = start_bit;

    while current < end_bit && (current % 8) != 0 {
        if !get_bit_buffer(data, 0, current)? {
            return Ok(false);
        }
        current += 1;
    }

    while current.saturating_add(8) < end_bit {
        if data[current / 8] != 0xFF {
            return Ok(false);
        }
        current += 8;
    }

    while current < end_bit {
        if !get_bit_buffer(data, 0, current)? {
            return Ok(false);
        }
        current += 1;
    }

    Ok(true)
}

#[test]
fn test_set_bit_buffer() {
    let mut buffer = vec![];

    set_bit_buffer(&mut buffer, 0, true);
    set_bit_buffer(&mut buffer, 1, false);
    set_bit_buffer(&mut buffer, 2, false);
    set_bit_buffer(&mut buffer, 3, false);
    set_bit_buffer(&mut buffer, 4, true);
    set_bit_buffer(&mut buffer, 5, true);

    assert_eq!(buffer, vec![0b_0011_0001]);

    set_bit_buffer(&mut buffer, 16 + 2, true);
    set_bit_buffer(&mut buffer, 16 + 4, false);

    assert_eq!(buffer, vec![0b_0011_0001, 0b_0000_0000, 0b_0000_0100]);

    set_bit_buffer(&mut buffer, 4, false);
    assert_eq!(buffer, vec![0b_0010_0001, 0b_0000_0000, 0b_0000_0100]);
}

#[test]
fn test_all_set_buffer() {
    assert!(all_set_buffer(&[0b_0000_0001], 0, 1).unwrap());
    assert!(!all_set_buffer(&[0b_0000_0001], 0, 2).unwrap());
    assert!(all_set_buffer(&[0b_1000_0000], 7, 8).unwrap());
    assert!(!all_set_buffer(&[0b_1000_0000], 6, 8).unwrap());

    assert!(all_set_buffer(&[0b_1111_1111], 0, 8).unwrap());
    assert!(!all_set_buffer(&[0b_1110_1111], 0, 8).unwrap());
    assert!(all_set_buffer(&[0b_1110_1111], 0, 4).unwrap());
    assert!(all_set_buffer(&[0b_1110_1111], 5, 8).unwrap());
    assert!(!all_set_buffer(&[0b_1110_1111], 4, 5).unwrap());

    assert!(all_set_buffer(&[0, 0b_1111_1111], 8, 16).unwrap());
    assert!(!all_set_buffer(&[0, 0b_1110_1111], 8, 16).unwrap());
    assert!(all_set_buffer(&[0, 0b_1110_1111], 8, 12).unwrap());
    assert!(all_set_buffer(&[0, 0b_1110_1111], 13, 16).unwrap());
    assert!(!all_set_buffer(&[0, 0b_1110_1111], 12, 13).unwrap());

    assert!(all_set_buffer(&[0, 0b_1111_1111, 0], 8, 16).unwrap());
    assert!(all_set_buffer(&[0, 0b_1111_1111, 0b_1111_1111], 8, 24).unwrap());
    assert!(!all_set_buffer(&[0, 0b_1111_1111, 0b_1111_1111], 7, 24).unwrap());
    assert!(!all_set_buffer(&[0, 0b_1111_1111, 0b_0111_1111], 8, 24).unwrap());
    assert!(!all_set_buffer(&[0, 0b_1111_1111, 0b_1111_1110], 8, 24).unwrap());
    assert!(!all_set_buffer(&[0, 0b_1111_0111, 0b_1111_1111], 8, 24).unwrap());
    assert!(!all_set_buffer(&[0, 0b_1111_1111, 0b_0111_1111], 8, 24).unwrap());
    assert!(all_set_buffer(&[0, 0b_1111_1111, 0b_0111_1111], 8, 23).unwrap());

    assert!(!all_set_buffer(&[0, 0b_1111_1111, 0b_0111_1111], 23, 24).unwrap());
    assert!(all_set_buffer(&[0, 0b_1111_1111, 0b_0111_1111], 22, 23).unwrap());

    assert!(all_set_buffer(&[0, 0b_1111_1111, 0b_1111_1111, 0b_1111_1111], 8, 32).unwrap());
    assert!(!all_set_buffer(&[0, 0b_0111_1111, 0b_1111_1111, 0b_1111_1111], 8, 32).unwrap());
    assert!(!all_set_buffer(&[0, 0b_1111_1111, 0b_1111_1111, 0b_1111_1110], 8, 32).unwrap());
    assert!(all_set_buffer(&[0, 0b_1111_1111, 0b_1111_1111, 0b_1111_1110], 8, 24).unwrap());
    assert!(!all_set_buffer(&[0, 0b_1111_1111, 0b_1111_1111, 0b_0111_1111], 8, 32).unwrap());
    assert!(all_set_buffer(&[0, 0b_1111_1111, 0b_1111_1111, 0b_0111_1111], 8, 31).unwrap());
    assert!(!all_set_buffer(&[0, 0b_1111_1111, 0b_1110_1111, 0b_1111_1111], 8, 32).unwrap());
    assert!(all_set_buffer(&[0, 0b_1111_1111, 0b_1110_1111, 0b_1111_1111], 8, 20).unwrap());
    assert!(all_set_buffer(&[0, 0b_1111_1111, 0b_1110_1111, 0b_1111_1111], 21, 32).unwrap());
    assert!(!all_set_buffer(&[0, 0b_1111_1111, 0b_1110_1111, 0b_1111_1111], 20, 21).unwrap());
}
