//! Extension of the array types

use crate::internal::{
    arrow::{BytesArray, PrimitiveArray},
    error::{fail, Result},
    utils::Offset,
};

pub trait ArrayExt: Sized + 'static {
    fn take(&mut self) -> Self;
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

pub fn new_primitive_array<T>(is_nullable: bool) -> PrimitiveArray<T> {
    PrimitiveArray {
        validity: is_nullable.then(Vec::new),
        values: Vec::new(),
    }
}

impl<T: Default + 'static> ArrayExt for PrimitiveArray<T> {
    fn take(&mut self) -> Self {
        Self {
            validity: self.validity.as_mut().map(std::mem::take),
            values: std::mem::take(&mut self.values),
        }
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

pub fn new_bytes_array<O: Offset>(is_nullable: bool) -> BytesArray<O> {
    BytesArray {
        validity: is_nullable.then(Vec::new),
        offsets: vec![O::default()],
        data: Vec::new(),
    }
}

impl<O: Offset> ArrayExt for BytesArray<O> {
    fn take(&mut self) -> Self {
        Self {
            validity: self.validity.as_mut().map(std::mem::take),
            data: std::mem::take(&mut self.data),
            offsets: std::mem::replace(&mut self.offsets, vec![O::default()]),
        }
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

#[derive(Debug, Clone)]
pub struct OffsetsArray<O> {
    pub validity: Option<Vec<u8>>,
    pub offsets: Vec<O>,
}

impl<O: Offset> OffsetsArray<O> {
    pub fn new(is_nullable: bool) -> Self {
        Self {
            validity: is_nullable.then(Vec::new),
            offsets: vec![O::default()],
        }
    }
}

impl<O: Offset> ArrayExt for OffsetsArray<O> {
    fn take(&mut self) -> Self {
        Self {
            validity: self.validity.as_mut().map(std::mem::take),
            offsets: std::mem::replace(&mut self.offsets, vec![O::default()]),
        }
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

impl CountArray {
    pub fn new(is_nullable: bool) -> Self {
        Self {
            len: 0,
            validity: is_nullable.then(Vec::new),
        }
    }
}

impl ArrayExt for CountArray {
    fn take(&mut self) -> Self {
        Self {
            len: std::mem::take(&mut self.len),
            validity: self.validity.as_mut().map(std::mem::take),
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

pub fn set_validity(buffer: Option<&mut Vec<u8>>, idx: usize, value: bool) -> Result<()> {
    if let Some(buffer) = buffer {
        set_bit_buffer(buffer, idx, value);
        Ok(())
    } else if value {
        Ok(())
    } else {
        fail!("Cannot push null for non-nullable array");
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
