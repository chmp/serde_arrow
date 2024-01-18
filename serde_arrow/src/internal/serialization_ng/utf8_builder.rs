use crate::{
    internal::common::{MutableBitBuffer, MutableOffsetBuffer, Offset},
    Result,
};

use super::utils::SimpleSerializer;

#[derive(Debug, Clone)]
pub struct Utf8Builder<O> {
    pub validity: Option<MutableBitBuffer>,
    pub offsets: MutableOffsetBuffer<O>,
    pub buffer: Vec<u8>,
}

impl<O: Offset> Utf8Builder<O> {
    pub fn new(is_nullable: bool) -> Self {
        Self {
            validity: is_nullable.then(MutableBitBuffer::default),
            offsets: MutableOffsetBuffer::default(),
            buffer: Vec::new(),
        }
    }
}

impl<O: Offset> SimpleSerializer for Utf8Builder<O> {
    fn name(&self) -> &str {
        "Utf8Builder"
    }

    fn serialize_str(&mut self, v: &str) -> Result<()> {
        self.offsets.push(v.len())?;
        self.buffer.extend(v.as_bytes());

        Ok(())
    }
}
