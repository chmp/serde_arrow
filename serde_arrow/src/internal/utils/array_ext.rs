//! Extension of the array types

use marrow::array::{BytesArray, BytesViewArray, PrimitiveArray};

use crate::internal::{
    error::{fail, Error, ErrorKind, Result},
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

        let Some(first_buffer) = self.buffers.first_mut() else {
            fail!("sequence operations without underlying buffer");
        };
        let Some(start) = first_buffer.len().checked_sub(n) else {
            fail!("inconsistent length and data in BytesViewArray");
        };
        let data = first_buffer
            .get(start..)
            .unwrap_or_else(|| unreachable!("checked length before hand"));

        if data.len() <= 12 {
            *curr = bytes_view::pack_inline(data);
            first_buffer.truncate(start);
        } else {
            *curr = bytes_view::pack_extern(data, 0, start)?;
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
            let Some(first_buffer) = self.buffers.first_mut() else {
                fail!(
                    "cannot append value with length greater than 12 without an underlying buffer"
                );
            };
            self.data
                .push(bytes_view::pack_extern(value, 0, first_buffer.len())?);
            first_buffer.extend(value);
        }
        Ok(())
    }
}

pub mod bytes_view {
    use crate::internal::{
        error::{fail, Result},
        utils::truncating_cast::TruncatingCast,
    };

    pub fn get_len(packed: u128) -> usize {
        packed.truncating_cast::<u32>("first u32 is the length") as usize
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

    pub fn pack_extern(data: &[u8], buffer: usize, offset: usize) -> Result<u128> {
        if i32::try_from(data.len()).is_err() {
            fail!("data too large for string view type");
        };
        let len_bytes = u128::from(u32::try_from(data.len())?);

        let prefix = u128::from(data.first().copied().unwrap_or_default())
            | (u128::from(data.get(1).copied().unwrap_or_default()) << 8)
            | (u128::from(data.get(2).copied().unwrap_or_default()) << 16)
            | (u128::from(data.get(3).copied().unwrap_or_default()) << 24);

        if i32::try_from(buffer).is_err() {
            fail!("too large buffer index for view type");
        };
        let buffer_bytes = u128::from(u32::try_from(buffer)?);

        if i32::try_from(offset).is_err() {
            fail!("offset too large for view type");
        }
        let offset_bytes = u128::from(u32::try_from(offset)?);

        Ok(len_bytes | (prefix << 32) | (buffer_bytes << 64) | (offset_bytes << 96))
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
        fail!("offset array must contain at least one element")
    };
    vec.push(last.clone());
    Ok(())
}

pub fn increment_last<O: Offset>(vec: &mut [O], inc: usize) -> Result<()> {
    let Some(last) = vec.last_mut() else {
        fail!("offset array must contain at least one element")
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
        Err(Error::new(
            ErrorKind::NullabilityViolation { field: None },
            "cannot serialize null into non-nullable array".into(),
        ))
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
    let dest = buffer
        .get_mut(idx / 8)
        .unwrap_or_else(|| unreachable!("ensured enough bytes are available"));

    let bit_mask: u8 = 1 << (idx % 8);
    if value {
        *dest |= bit_mask;
    } else {
        *dest &= !bit_mask;
    }
}

pub fn get_bit_buffer(data: &[u8], offset: usize, idx: usize) -> Result<bool> {
    let flag = 1 << ((idx + offset) % 8);
    let Some(byte) = data.get((idx + offset) / 8) else {
        fail!("invalid access in bitset");
    };
    Ok(byte & flag == flag)
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
