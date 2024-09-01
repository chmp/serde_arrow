use crate::internal::{
    arrow::BitsWithOffset,
    error::{fail, Result},
    utils::Offset,
};

pub fn bitset_is_set(set: &BitsWithOffset<'_>, idx: usize) -> Result<bool> {
    let flag = 1 << ((idx + set.offset) % 8);
    let Some(byte) = set.data.get((idx + set.offset) / 8) else {
        fail!("invalid access in bitset");
    };
    Ok(byte & flag == flag)
}

pub struct ArrayBufferIterator<'a, T: Copy> {
    pub buffer: &'a [T],
    pub validity: Option<BitsWithOffset<'a>>,
    pub next: usize,
}

impl<'a, T: Copy> ArrayBufferIterator<'a, T> {
    pub fn new(buffer: &'a [T], validity: Option<BitsWithOffset<'a>>) -> Self {
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
            if !bitset_is_set(validity, self.next)? {
                return Ok(None);
            }
        }
        let val = self.buffer[self.next];
        self.next += 1;

        Ok(Some(val))
    }

    pub fn next_required(&mut self) -> Result<T> {
        let Some(next) = self.next()? else {
            fail!("missing value");
        };
        Ok(next)
    }

    pub fn peek_next(&self) -> Result<bool> {
        if self.next > self.buffer.len() {
            fail!("Tried to deserialize a value from an exhausted StringDeserializer");
        }

        if let Some(validity) = &self.validity {
            if !bitset_is_set(validity, self.next)? {
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
    validity: Option<BitsWithOffset<'a>>,
    offsets: &'a [O],
) -> Result<()> {
    let Some(validity) = validity else {
        return Ok(());
    };

    if offsets.is_empty() {
        fail!("list offsets must be non empty");
    }

    for i in 0..offsets.len().saturating_sub(1) {
        let curr = offsets[i].try_into_usize()?;
        let next = offsets[i + 1].try_into_usize()?;
        if !bitset_is_set(&validity, i)? && (next - curr) != 0 {
            fail!("lists with data in null values are currently not supported in deserialization");
        }
    }

    Ok(())
}
