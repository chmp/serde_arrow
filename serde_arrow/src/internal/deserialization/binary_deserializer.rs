use marrow::view::{BytesView, BytesViewView};
use serde::de::{SeqAccess, Visitor};

use crate::internal::{
    error::{fail, set_default, try_, Context, ContextSupport, Error, Result},
    utils::{array_view_ext::ViewAccess, Mut},
};

use super::{
    random_access_deserializer::RandomAccessDeserializer,
    simple_deserializer::SimpleDeserializer,
    utils::{BytesAccess, U8Deserializer, U8SliceDeserializer},
};

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
            fail!("Invalid access: required item is not defined");
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

impl<'de, VV> RandomAccessDeserializer<'de> for BinaryDeserializer<VV>
where
    VV: ViewAccess<'de, [u8]> + BinaryDeserializerDataType + 'static,
{
    fn is_some(&self, idx: usize) -> Result<bool> {
        self.view.is_some(idx)
    }

    fn deserialize_any_some<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        self.deserialize_bytes(visitor, idx)
    }

    fn deserialize_any<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        try_(|| {
            if self.view.is_some(idx)? {
                self.deserialize_bytes(visitor, idx)
            } else {
                visitor.visit_none()
            }
        })
        .ctx(self)
    }

    fn deserialize_option<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        try_(|| {
            if self.view.is_some(idx)? {
                visitor.visit_some(self.at(idx))
            } else {
                visitor.visit_none()
            }
        })
        .ctx(self)
    }

    fn deserialize_seq<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        try_(|| {
            let bytes = self.view.get_required(idx)?;
            visitor.visit_seq(U8SliceDeserializer::new(bytes))
        })
        .ctx(self)
    }

    fn deserialize_bytes<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        try_(|| visitor.visit_borrowed_bytes::<Error>(self.view.get_required(idx)?)).ctx(self)
    }

    fn deserialize_byte_buf<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        try_(|| visitor.visit_borrowed_bytes::<Error>(self.view.get_required(idx)?)).ctx(self)
    }
}
