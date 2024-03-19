use crate::internal::{
    common::BitBuffer,
    error::{error, fail, Result},
};

pub struct ArrayBufferIterator<'a, T: Copy> {
    pub buffer: &'a [T],
    pub validity: Option<BitBuffer<'a>>,
    pub next: usize,
}

impl<'a, T: Copy> ArrayBufferIterator<'a, T> {
    pub fn new(buffer: &'a [T], validity: Option<BitBuffer<'a>>) -> Self {
        Self {
            buffer,
            validity,
            next: 0,
        }
    }

    pub fn next(&mut self) -> Result<Option<T>> {
        if self.next > self.buffer.len() {
            fail!("Tried to deserialize a value from an exhausted FloatDeserializer");
        }

        if let Some(validity) = &self.validity {
            if !validity.is_set(self.next) {
                return Ok(None);
            }
        }
        let val = self.buffer[self.next];
        self.next += 1;

        Ok(Some(val))
    }

    pub fn next_required(&mut self) -> Result<T> {
        self.next()?.ok_or_else(|| error!("missing value"))
    }

    pub fn peek_next(&self) -> Result<bool> {
        if self.next > self.buffer.len() {
            fail!("Tried to deserialize a value from an exhausted StringDeserializer");
        }

        if let Some(validity) = &self.validity {
            if !validity.is_set(self.next) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub fn consume_next(&mut self) {
        self.next += 1;
    }
}
