use crate::internal::error::Result;

/// Readonly buffers
#[derive(Default)]
pub struct Buffers<'a> {
    pub u0: Vec<usize>,
    pub u8: Vec<&'a [u8]>,
    pub u16: Vec<&'a [u16]>,
    pub u32: Vec<&'a [u32]>,
}

impl<'a> Buffers<'a> {
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Debug, Default, PartialEq, Eq, Clone)]
pub struct BitBuffer {
    pub(crate) buffer: Vec<u8>,
    pub(crate) len: usize,
    pub(crate) capacity: usize,
}

impl BitBuffer {
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
pub struct NullBuffer {
    len: usize,
}

impl NullBuffer {
    #[allow(unused)]
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
pub struct OffsetBuilder<O> {
    pub(crate) offsets: Vec<O>,
    pub(crate) current_items: O,
}

impl<O: Offset> std::default::Default for OffsetBuilder<O> {
    fn default() -> Self {
        Self {
            offsets: vec![O::default()],
            current_items: O::default(),
        }
    }
}

impl<O: Offset> OffsetBuilder<O> {
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
