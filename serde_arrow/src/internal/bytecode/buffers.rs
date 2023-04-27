use crate::internal::error::{error, Result};

#[derive(Debug, Default, PartialEq, Eq, Clone)]
pub struct BoolBuffer {
    pub(crate) buffer: Vec<u8>,
    pub(crate) len: usize,
    pub(crate) capacity: usize,
}

impl BoolBuffer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&mut self, value: bool) -> Result<()> {
        while self.len >= self.capacity {
            self.buffer.push(0);
            self.capacity += 8;
        }

        if value {
            self.buffer[self.len / 8] |= 1 << (self.len % 8);
        }
        self.len += 1;
        Ok(())
    }
}

impl<const N: usize> From<[bool; N]> for BoolBuffer {
    fn from(values: [bool; N]) -> Self {
        let mut res = BoolBuffer::new();
        for value in values {
            res.push(value).unwrap();
        }
        res
    }
}

#[cfg(test)]
mod test_validity_bitmap {
    use super::BoolBuffer;

    #[test]
    fn empty() {
        let bitmap = BoolBuffer::from([]);
        assert_eq!(bitmap.buffer, Vec::<u8>::new());
        assert_eq!(bitmap.len, 0);
        assert_eq!(bitmap.capacity, 0);
    }

    #[test]
    fn len2() {
        let bitmap = BoolBuffer::from([true, false]);
        assert_eq!(bitmap.buffer, vec![0b_0000_0001]);
        assert_eq!(bitmap.len, 2);
        assert_eq!(bitmap.capacity, 8);
    }

    #[test]
    fn len5() {
        let bitmap = BoolBuffer::from([true, false, false, true, true]);
        assert_eq!(bitmap.buffer, vec![0b_0001_1001]);
        assert_eq!(bitmap.len, 5);
        assert_eq!(bitmap.capacity, 8);
    }

    #[test]
    fn len10() {
        let bitmap = BoolBuffer::from([
            true, false, false, true, true, true, false, false, true, true,
        ]);
        assert_eq!(bitmap.buffer, vec![0b_0011_1001, 0b_0000_0011]);
        assert_eq!(bitmap.len, 10);
        assert_eq!(bitmap.capacity, 16);
    }

    #[test]
    fn len24() {
        let bitmap = BoolBuffer::from([
            true, false, false, true, true, true, false, false, true, true, false, false, false,
            false, false, false, true, true, true, true, true, false, true, true,
        ]);
        assert_eq!(
            bitmap.buffer,
            vec![0b_0011_1001, 0b_0000_0011, 0b_1101_1111]
        );
        assert_eq!(bitmap.len, 24);
        assert_eq!(bitmap.capacity, 24);
    }
}

#[derive(Debug, Clone)]
pub struct PrimitiveBuffer<T> {
    pub(crate) data: Vec<T>,
    pub(crate) validity: BoolBuffer,
}

impl<T> std::default::Default for PrimitiveBuffer<T> {
    fn default() -> Self {
        Self {
            data: Vec::new(),
            validity: BoolBuffer::new(),
        }
    }
}

impl<T> PrimitiveBuffer<T> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&mut self, val: T) -> Result<()> {
        self.data.push(val);
        self.validity.push(true)?;
        Ok(())
    }
}

impl<T: Default> PrimitiveBuffer<T> {
    pub fn push_null(&mut self) -> Result<()> {
        self.data.push(Default::default());
        self.validity.push(false)?;
        Ok(())
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
}

impl<O: Offset> std::default::Default for OffsetBuilder<O> {
    fn default() -> Self {
        Self {
            offsets: vec![O::default()],
        }
    }
}

impl<O: Offset> OffsetBuilder<O> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&mut self, num_items: usize) -> Result<()> {
        let last_offset = self
            .offsets
            .last()
            .ok_or_else(|| error!("internal error: no existing offset in string builder"))?
            .clone();
        let num_items = O::try_form_usize(num_items)?;

        self.offsets.push(last_offset + num_items);

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct StringBuffer<O> {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: OffsetBuilder<O>,
    pub(crate) validity: BoolBuffer,
}

impl<O: Offset> std::default::Default for StringBuffer<O> {
    fn default() -> Self {
        Self {
            offsets: Default::default(),
            data: Default::default(),
            validity: Default::default(),
        }
    }
}

impl<O: Offset> StringBuffer<O> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&mut self, val: &str) -> Result<()> {
        self.data.extend(val.as_bytes().iter().copied());
        self.offsets.push(val.len())?;
        self.validity.push(true)?;

        Ok(())
    }

    pub fn push_null(&mut self) -> Result<()> {
        self.offsets.push(0)?;
        self.validity.push(false)?;

        Ok(())
    }
}
