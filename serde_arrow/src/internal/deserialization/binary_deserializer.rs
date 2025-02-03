use marrow::view::{BytesView, BytesViewView};
use serde::de::Visitor;

use crate::internal::{
    error::{set_default, try_, Context, ContextSupport, Error, Result},
    utils::array_view_ext::ViewAccess,
};

use super::{
    random_access_deserializer::RandomAccessDeserializer, simple_deserializer::SimpleDeserializer,
    utils::U8SliceDeserializer,
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
}

impl<'a, V> BinaryDeserializer<V> {
    pub fn new(path: String, view: V) -> Self {
        Self { path, view }
    }
}

impl<V: BinaryDeserializerDataType> Context for BinaryDeserializer<V> {
    fn annotate(&self, annotations: &mut std::collections::BTreeMap<String, String>) {
        set_default(annotations, "field", &self.path);
        set_default(annotations, "data_type", V::DATA_TYPE_NAME);
    }
}

impl<'a, VV: BinaryDeserializerDataType> SimpleDeserializer<'a> for BinaryDeserializer<VV> {}

impl<'de, VV> RandomAccessDeserializer<'de> for BinaryDeserializer<VV>
where
    VV: ViewAccess<'de, [u8]> + BinaryDeserializerDataType + 'de,
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
