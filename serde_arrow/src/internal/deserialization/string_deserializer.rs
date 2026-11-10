use marrow::view::{BytesView, BytesViewView};

use crate::internal::{
    error::{set_default, try_, Context, ContextSupport, Result},
    utils::array_view_ext::{Utf8Check, ViewAccess},
};

use super::{
    enums_as_string_impl::EnumAccess, random_access_deserializer::RandomAccessDeserializer,
};

pub trait StringDeserializerDataType {
    const DATA_TYPE_NAME: &'static str;
}

impl StringDeserializerDataType for BytesView<'_, i32> {
    const DATA_TYPE_NAME: &'static str = "Utf8";
}

impl StringDeserializerDataType for BytesView<'_, i64> {
    const DATA_TYPE_NAME: &'static str = "LargeUtf8";
}

impl StringDeserializerDataType for BytesViewView<'_> {
    const DATA_TYPE_NAME: &'static str = "Utf8View";
}

pub struct StringDeserializer<V> {
    pub path: String,
    pub view: V,
    pub utf8: Utf8Check,
}

impl<V> StringDeserializer<V> {
    pub fn new(path: String, view: V, utf8: Utf8Check) -> Self {
        Self { path, view, utf8 }
    }
}

impl<V: StringDeserializerDataType> Context for StringDeserializer<V> {
    fn annotate(&self, annotations: &mut std::collections::BTreeMap<String, String>) {
        set_default(annotations, "field", &self.path);
        set_default(annotations, "data_type", V::DATA_TYPE_NAME);
    }
}

impl<'a, VV> RandomAccessDeserializer<'a> for StringDeserializer<VV>
where
    VV: ViewAccess<'a, [u8]> + StringDeserializerDataType + 'a,
{
    fn is_some(&self, idx: usize) -> Result<bool> {
        ViewAccess::<str>::is_some(&self.view, idx)
    }

    fn deserialize_any_some<V: serde::de::Visitor<'a>>(
        &self,
        visitor: V,
        idx: usize,
    ) -> Result<V::Value> {
        self.deserialize_str(visitor, idx)
    }

    fn deserialize_str<V: serde::de::Visitor<'a>>(
        &self,
        visitor: V,
        idx: usize,
    ) -> Result<V::Value> {
        try_(|| visitor.visit_borrowed_str(self.utf8.get_required(&self.view, idx)?)).ctx(self)
    }

    fn deserialize_string<V: serde::de::Visitor<'a>>(
        &self,
        visitor: V,
        idx: usize,
    ) -> Result<V::Value> {
        try_(|| visitor.visit_string(self.utf8.get_required(&self.view, idx)?.to_owned())).ctx(self)
    }

    fn deserialize_bytes<V: serde::de::Visitor<'a>>(
        &self,
        visitor: V,
        idx: usize,
    ) -> Result<V::Value> {
        // Bytes need no UTF-8 check.
        try_(|| visitor.visit_bytes(ViewAccess::<[u8]>::get_required(&self.view, idx)?)).ctx(self)
    }

    fn deserialize_byte_buf<V: serde::de::Visitor<'a>>(
        &self,
        visitor: V,
        idx: usize,
    ) -> Result<V::Value> {
        try_(|| {
            visitor.visit_byte_buf(ViewAccess::<[u8]>::get_required(&self.view, idx)?.to_owned())
        })
        .ctx(self)
    }

    fn deserialize_enum<V: serde::de::Visitor<'a>>(
        &self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
        idx: usize,
    ) -> Result<V::Value> {
        try_(|| {
            let variant = self.utf8.get_required(&self.view, idx)?;
            visitor.visit_enum(EnumAccess(variant))
        })
        .ctx(self)
    }
}
