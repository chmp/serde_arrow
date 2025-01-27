use marrow::view::{BytesView, BytesViewView};
use serde::de::{SeqAccess, Visitor};

use crate::internal::{
    error::{fail, set_default, try_, Context, ContextSupport, Error, Result},
    utils::Mut,
};

use super::{simple_deserializer::SimpleDeserializer, string_deserializer::BytesAccess};

trait BinaryDeserializerDataType {
    const DATA_TYPE_NAME: &'static str;
}

impl BinaryDeserializerDataType for BytesView<'_, i32> {
    const DATA_TYPE_NAME: &'static str = "Binary";
}

impl BinaryDeserializerDataType for BytesView<'_, i64> {
    const DATA_TYPE_NAME: &'static str = "LargeBinary";
}

impl BinaryDeserializerDataType for BytesViewView<'_> {
    const DATA_TYPE_NAME: &'static str = "BinaryView";
}

pub struct BinaryDeserializer<V> {
    pub path: String,
    pub view: V,
    pub next: (usize, usize),
}

impl<'a, V: BytesAccess<'a>> BinaryDeserializer<V> {
    pub fn new(path: String, view: V) -> Self {
        Self {
            path,
            view,
            next: (0, 0),
        }
    }

    pub fn peek_next_slice(&self) -> Result<Option<&'a [u8]>> {
        self.view.get_bytes(self.next.0)
    }

    pub fn consume_next(&mut self) {
        self.next = (self.next.0 + 1, 0);
    }

    pub fn next_slice(&mut self) -> Result<&'a [u8]> {
        let Some(slice) = self.view.get_bytes(self.next.0)? else {
            fail!("Trying to deserialize from exhausted deserializer");
        };
        self.next = (self.next.0 + 1, 0);
        Ok(slice)
    }
}

impl<V: BinaryDeserializerDataType> Context for BinaryDeserializer<V> {
    fn annotate(&self, annotations: &mut std::collections::BTreeMap<String, String>) {
        set_default(annotations, "field", &self.path);
        set_default(annotations, "data_type", V::DATA_TYPE_NAME);
    }
}

impl<'a, VV: BytesAccess<'a> + BinaryDeserializerDataType> SimpleDeserializer<'a>
    for BinaryDeserializer<VV>
{
    fn deserialize_any<V: Visitor<'a>>(&mut self, visitor: V) -> Result<V::Value> {
        try_(|| {
            if self.peek_next_slice().ctx(self)?.is_some() {
                self.deserialize_bytes(visitor).ctx(self)
            } else {
                self.consume_next();
                visitor.visit_none::<Error>().ctx(self)
            }
        })
        .ctx(self)
    }

    fn deserialize_option<V: Visitor<'a>>(&mut self, visitor: V) -> Result<V::Value> {
        try_(|| {
            if self.peek_next_slice().ctx(self)?.is_some() {
                visitor.visit_some(Mut(self)).ctx(self)
            } else {
                self.consume_next();
                visitor.visit_none::<Error>().ctx(self)
            }
        })
        .ctx(self)
    }

    fn deserialize_seq<V: Visitor<'a>>(&mut self, visitor: V) -> Result<V::Value> {
        try_(|| visitor.visit_seq(&mut *self)).ctx(self)
    }

    fn deserialize_bytes<V: Visitor<'a>>(&mut self, visitor: V) -> Result<V::Value> {
        try_(|| visitor.visit_borrowed_bytes::<Error>(self.next_slice()?)).ctx(self)
    }

    fn deserialize_byte_buf<V: Visitor<'a>>(&mut self, visitor: V) -> Result<V::Value> {
        try_(|| visitor.visit_borrowed_bytes::<Error>(self.next_slice()?)).ctx(self)
    }
}

impl<'de, VV: BytesAccess<'de> + BinaryDeserializerDataType> SeqAccess<'de>
    for BinaryDeserializer<VV>
{
    type Error = Error;

    fn next_element_seed<T: serde::de::DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> Result<Option<T::Value>> {
        let (item, offset) = self.next;
        let Some(s) = self.peek_next_slice()? else {
            fail!("Trying to deserialize from an exhausted deserializer");
        };

        if offset >= s.len() {
            self.next = (item + 1, 0);
            return Ok(None);
        }
        self.next = (item, offset + 1);

        let mut item_deserializer = U8Deserializer(s[offset]);
        let item = seed.deserialize(Mut(&mut item_deserializer))?;
        Ok(Some(item))
    }
}

struct U8Deserializer(u8);

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
