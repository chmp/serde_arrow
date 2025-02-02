use marrow::view::{BitsWithOffset, BytesView, BytesViewView, PrimitiveView};
use serde::de::{SeqAccess, Visitor};

use crate::internal::{
    error::{fail, Context, Error, Result},
    utils::{array_ext::get_bit_buffer, Mut, Offset},
};

use super::simple_deserializer::SimpleDeserializer;

pub fn bitset_is_set(set: &BitsWithOffset<'_>, idx: usize) -> Result<bool> {
    get_bit_buffer(set.data, set.offset, idx)
}

// TODO: remove
pub struct ArrayBufferIterator<'a, T: Copy> {
    pub array: PrimitiveView<'a, T>,
    pub next: usize,
}

impl<'a, T: Copy> std::ops::Deref for ArrayBufferIterator<'a, T> {
    type Target = PrimitiveView<'a, T>;

    fn deref(&self) -> &Self::Target {
        &self.array
    }
}

impl<'a, T: Copy> ArrayBufferIterator<'a, T> {
    pub fn new(values: &'a [T], validity: Option<BitsWithOffset<'a>>) -> Self {
        Self {
            array: PrimitiveView { validity, values },
            next: 0,
        }
    }

    pub fn next(&mut self) -> Result<Option<T>> {
        if self.next > self.array.values.len() {
            fail!("Exhausted deserializer");
        }

        if let Some(validity) = &self.array.validity {
            if !bitset_is_set(validity, self.next)? {
                return Ok(None);
            }
        }
        let val = self.array.values[self.next];
        self.next += 1;

        Ok(Some(val))
    }

    pub fn next_required(&mut self) -> Result<T> {
        let Some(next) = self.next()? else {
            fail!("Exhausted deserializer");
        };
        Ok(next)
    }

    pub fn peek_next(&self) -> Result<bool> {
        if self.next > self.array.values.len() {
            fail!("Exhausted deserializer");
        }

        if let Some(validity) = &self.array.validity {
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
    if offsets.is_empty() {
        fail!("Unsupported: list offsets must be non empty");
    }

    for i in 0..offsets.len().saturating_sub(1) {
        let curr = offsets[i].try_into_usize()?;
        let next = offsets[i + 1].try_into_usize()?;

        if next < curr {
            fail!("Unsupported: list offsets are assumed to be monotonically increasing");
        }
        if let Some(validity) = validity.as_ref() {
            if !bitset_is_set(validity, i)? && (next - curr) != 0 {
                fail!("Unsupported: lists with data in null values are currently not supported in deserialization");
            }
        }
    }

    Ok(())
}

#[test]
fn test_check_supported_list_layout() {
    use crate::internal::testing::assert_error_contains;

    assert_error_contains(&check_supported_list_layout::<i32>(None, &[]), "non empty");
    assert_error_contains(
        &check_supported_list_layout::<i32>(None, &[0, 1, 0]),
        "monotonically increasing",
    );
    assert_error_contains(
        &check_supported_list_layout::<i32>(
            Some(BitsWithOffset {
                offset: 0,
                data: &[0b_101],
            }),
            &[0, 5, 10, 15],
        ),
        "data in null values",
    );
}

pub trait BytesAccess<'a> {
    fn get_bytes(&self, idx: usize) -> Result<Option<&'a [u8]>>;
}

impl<'a, O: Offset> BytesAccess<'a> for BytesView<'a, O> {
    fn get_bytes(&self, idx: usize) -> Result<Option<&'a [u8]>> {
        if idx + 1 > self.offsets.len() {
            fail!(
                "Invalid access: tried to get element {idx} of array with {len} elements",
                len = self.offsets.len().saturating_sub(1)
            );
        }

        if let Some(validity) = &self.validity {
            if !bitset_is_set(validity, idx)? {
                return Ok(None);
            }
        }

        let start = self.offsets[idx].try_into_usize()?;
        let end = self.offsets[idx + 1].try_into_usize()?;
        Ok(Some(&self.data[start..end]))
    }
}

impl<'a> BytesAccess<'a> for BytesViewView<'a> {
    fn get_bytes(&self, idx: usize) -> Result<Option<&'a [u8]>> {
        let Some(desc) = self.data.get(idx) else {
            fail!(
                "Invalid access: tried to get element {idx} of array with {len} elements",
                len = self.data.len()
            );
        };

        if let Some(validity) = &self.validity {
            if !bitset_is_set(validity, idx)? {
                return Ok(None);
            }
        }

        let len = (*desc as u32) as usize;
        let res = || -> Option<&'a [u8]> {
            if len <= 12 {
                let bytes: &[u8] = bytemuck::try_cast_slice(std::slice::from_ref(desc)).ok()?;
                bytes.get(4..4 + len)
            } else {
                let buf_idx = ((*desc >> 64) as u32) as usize;
                let offset = ((*desc >> 96) as u32) as usize;
                self.buffers.get(buf_idx)?.get(offset..offset + len)
            }
        }();

        if res.is_none() {
            fail!("invalid state in bytes deserialization");
        }
        Ok(res)
    }
}

pub struct U8Deserializer(pub u8);

impl Context for U8Deserializer {
    fn annotate(&self, _: &mut std::collections::BTreeMap<String, String>) {}
}

impl<'de> SimpleDeserializer<'de> for U8Deserializer {
    fn deserialize_u8<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_u8(self.0)
    }

    fn deserialize_u16<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_u16(self.0.into())
    }

    fn deserialize_u32<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_u32(self.0.into())
    }

    fn deserialize_u64<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_u64(self.0.into())
    }

    fn deserialize_i8<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_i8(self.0.try_into()?)
    }

    fn deserialize_i16<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_i16(self.0.into())
    }

    fn deserialize_i32<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_i32(self.0.into())
    }

    fn deserialize_i64<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_i64(self.0.into())
    }
}

pub struct U8SliceDeserializer<'a>(&'a [u8], usize);

impl<'a> U8SliceDeserializer<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self(bytes, 0)
    }
}

impl<'de> SeqAccess<'de> for U8SliceDeserializer<'de> {
    type Error = Error;

    fn size_hint(&self) -> Option<usize> {
        Some(self.0.len())
    }

    fn next_element_seed<T: serde::de::DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> Result<Option<T::Value>> {
        let U8SliceDeserializer(bytes, idx) = *self;
        if idx >= bytes.len() {
            return Ok(None);
        }

        let mut item_deserializer = U8Deserializer(bytes[idx]);
        let item = seed.deserialize(Mut(&mut item_deserializer))?;

        self.1 = idx + 1;

        Ok(Some(item))
    }
}
