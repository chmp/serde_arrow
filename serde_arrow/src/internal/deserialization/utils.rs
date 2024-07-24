use crate::internal::{
    error::{error, fail, Result},
    utils::Offset,
};

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

/// Check that the list layout given in terms of validity and offsets is
/// supported by serde_arrow
///
/// While the [arrow format spec][] explicitly allows null values in lists that
/// correspond to non-empty segments, this is currently not supported in arrow
/// deserialization. The spec says "a null value may correspond to a
/// **non-empty** segment in the child array."
///
/// [arrow format spec]: https://arrow.apache.org/docs/format/Columnar.html#variable-size-list-layout
pub fn check_supported_list_layout<'a, O: Offset>(
    validity: Option<BitBuffer<'a>>,
    offsets: &'a [O],
) -> Result<()> {
    let Some(validity) = validity else {
        return Ok(());
    };

    if offsets.len() != validity.len() + 1 {
        fail!(
            "validity length {val} and offsets length {off} do not match (expected {val}, {exp})",
            val = validity.len(),
            off = offsets.len(),
            exp = validity.len() + 1,
        );
    }
    for i in 0..validity.len() {
        let curr = offsets[i].try_into_usize()?;
        let next = offsets[i + 1].try_into_usize()?;
        if !validity.is_set(i) && (next - curr) != 0 {
            fail!("lists with data in null values are currently not supported in deserialization");
        }
    }

    Ok(())
}
