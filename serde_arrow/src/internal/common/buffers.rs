use crate::internal::error::Result;

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
    pub fn as_bool(&self) -> Vec<bool> {
        (0..self.len())
            .map(|i| {
                let flag = 1 << i;
                (self.buffer[i / 8] & flag) == flag
            })
            .collect()
    }

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

    pub fn reserve(&mut self, num_elements: usize) {
        self.buffer.reserve(num_elements / 8);
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

    pub fn reserve(&mut self, num_elements: usize) {
        self.offsets.reserve(num_elements);
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
