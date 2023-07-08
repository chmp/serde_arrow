use std::sync::Arc;

use bytemuck::NoUninit;

use super::array_mapping::ArrayMapping;
use crate::internal::{error::Result, schema::GenericField};

pub trait BufferExtract {
    fn len(&self) -> usize;
    fn extract_buffers<'a>(
        &'a self,
        field: &GenericField,
        buffers: &mut Buffers<'a>,
    ) -> Result<ArrayMapping>;
}

impl<T: BufferExtract + ?Sized> BufferExtract for Box<T> {
    fn len(&self) -> usize {
        self.as_ref().len()
    }

    fn extract_buffers<'a>(
        &'a self,
        field: &GenericField,
        buffers: &mut Buffers<'a>,
    ) -> Result<ArrayMapping> {
        self.as_ref().extract_buffers(field, buffers)
    }
}

impl<T: BufferExtract + ?Sized> BufferExtract for Arc<T> {
    fn len(&self) -> usize {
        self.as_ref().len()
    }

    fn extract_buffers<'a>(
        &'a self,
        field: &GenericField,
        buffers: &mut Buffers<'a>,
    ) -> Result<ArrayMapping> {
        self.as_ref().extract_buffers(field, buffers)
    }
}

/// Readonly buffers
#[derive(Default)]
pub struct Buffers<'a> {
    pub u0: Vec<usize>,
    pub u1: Vec<BitBuffer<'a>>,
    pub u8: Vec<&'a [u8]>,
    pub u16: Vec<&'a [u16]>,
    pub u32: Vec<&'a [u32]>,
    pub u64: Vec<&'a [u64]>,
}

impl<'a> Buffers<'a> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push_u0(&mut self, val: usize) -> usize {
        self.u0.push(val);
        self.u0.len() - 1
    }

    pub fn push_u1(&mut self, val: BitBuffer<'a>) -> usize {
        self.u1.push(val);
        self.u1.len() - 1
    }

    pub fn push_u8(&mut self, val: &'a [u8]) -> usize {
        self.u8.push(val);
        self.u8.len() - 1
    }

    pub fn push_u16(&mut self, val: &'a [u16]) -> usize {
        self.u16.push(val);
        self.u16.len() - 1
    }

    pub fn push_u32(&mut self, val: &'a [u32]) -> usize {
        self.u32.push(val);
        self.u32.len() - 1
    }

    pub fn push_u64(&mut self, val: &'a [u64]) -> usize {
        self.u64.push(val);
        self.u64.len() - 1
    }
}

impl<'a> Buffers<'a> {
    pub fn push_u8_cast<T: NoUninit>(&mut self, val: &'a [T]) -> Result<usize> {
        Ok(self.push_u8(bytemuck::try_cast_slice::<T, u8>(val)?))
    }

    pub fn push_u16_cast<T: NoUninit>(&mut self, val: &'a [T]) -> Result<usize> {
        Ok(self.push_u16(bytemuck::try_cast_slice::<T, u16>(val)?))
    }

    pub fn push_u32_cast<T: NoUninit>(&mut self, val: &'a [T]) -> Result<usize> {
        Ok(self.push_u32(bytemuck::try_cast_slice::<T, u32>(val)?))
    }

    pub fn push_u64_cast<T: NoUninit>(&mut self, val: &'a [T]) -> Result<usize> {
        Ok(self.push_u64(bytemuck::try_cast_slice::<T, u64>(val)?))
    }
}

impl<'a> Buffers<'a> {
    pub fn get_u8(&self, idx: usize) -> &'a [u8] {
        self.u8[idx]
    }

    pub fn get_u16(&self, idx: usize) -> &'a [u16] {
        self.u16[idx]
    }

    pub fn get_u32(&self, idx: usize) -> &'a [u32] {
        self.u32[idx]
    }

    pub fn get_u64(&self, idx: usize) -> &'a [u64] {
        self.u64[idx]
    }

    pub fn get_i8(&self, idx: usize) -> &'a [i8] {
        bytemuck::cast_slice(self.u8[idx])
    }

    pub fn get_i16(&self, idx: usize) -> &'a [i16] {
        bytemuck::cast_slice(self.u16[idx])
    }

    pub fn get_i32(&self, idx: usize) -> &'a [i32] {
        bytemuck::cast_slice(self.u32[idx])
    }

    pub fn get_i64(&self, idx: usize) -> &'a [i64] {
        bytemuck::cast_slice(self.u64[idx])
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub struct BitBuffer<'a> {
    pub data: &'a [u8],
    pub offset: usize,
    pub number_of_bits: usize,
}

impl<'a> BitBuffer<'a> {
    pub fn is_set(&self, idx: usize) -> bool {
        let flag = 1 << ((idx + self.offset) % 8);
        let byte = self.data[(idx + self.offset) / 8];
        byte & flag == flag
    }

    pub fn len(&self) -> usize {
        self.number_of_bits
    }
}

#[derive(Debug, Default, PartialEq, Eq, Clone)]
pub struct MutableBitBuffer {
    pub(crate) buffer: Vec<u8>,
    pub(crate) len: usize,
    pub(crate) capacity: usize,
}

impl MutableBitBuffer {
    #[allow(unused)]
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn push(&mut self, value: bool) {
        while self.len >= self.capacity {
            for _ in 0..64 {
                self.buffer.push(0);
                self.capacity += 8;
            }
        }

        if value {
            self.buffer[self.len / 8] |= 1 << (self.len % 8);
        }
        self.len += 1;
    }

    pub fn clear(&mut self) {
        *self = Self::default();
    }
}

#[derive(Debug, Default, Clone)]
pub struct MutableCountBuffer {
    len: usize,
}

impl MutableCountBuffer {
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn push(&mut self, _: ()) {
        self.len += 1;
    }

    pub fn clear(&mut self) {
        *self = Self::default();
    }
}

pub trait Offset: std::ops::Add<Self, Output = Self> + Clone + Default {
    fn try_form_usize(val: usize) -> Result<Self>;
}

impl Offset for i32 {
    fn try_form_usize(val: usize) -> Result<Self> {
        Ok(i32::try_from(val)?)
    }
}

impl Offset for i64 {
    fn try_form_usize(val: usize) -> Result<Self> {
        Ok(i64::try_from(val)?)
    }
}

#[derive(Debug, Clone)]
pub struct MutableOffsetBuffer<O> {
    pub(crate) offsets: Vec<O>,
    pub(crate) current_items: O,
}

impl<O: Offset> std::default::Default for MutableOffsetBuffer<O> {
    fn default() -> Self {
        Self {
            offsets: vec![O::default()],
            current_items: O::default(),
        }
    }
}

impl<O: Offset> MutableOffsetBuffer<O> {
    /// The number of items pushed (one less than the number of offsets)
    #[allow(unused)]
    pub fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    // push a new item with the given number of children
    pub fn push(&mut self, num_children: usize) -> Result<()> {
        self.current_items = self.current_items.clone() + O::try_form_usize(num_children)?;
        self.offsets.push(self.current_items.clone());

        Ok(())
    }

    pub fn push_current_items(&mut self) {
        self.offsets.push(self.current_items.clone());
    }

    pub fn inc_current_items(&mut self) -> Result<()> {
        self.current_items = self.current_items.clone() + O::try_form_usize(1)?;
        Ok(())
    }

    pub fn clear(&mut self) {
        *self = Self::default();
    }
}
