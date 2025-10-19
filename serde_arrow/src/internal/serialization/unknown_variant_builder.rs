use std::collections::BTreeMap;

use marrow::array::{Array, NullArray};

use crate::internal::{
    error::{fail, set_default, Context, Result},
    serialization::utils::impl_serializer,
};

use super::array_builder::ArrayBuilder;

#[derive(Debug, Clone)]
pub struct UnknownVariantBuilder {
    path: String,
}

impl UnknownVariantBuilder {
    pub fn new(path: String) -> Self {
        UnknownVariantBuilder { path }
    }

    pub fn take(&mut self) -> ArrayBuilder {
        ArrayBuilder::UnknownVariant(UnknownVariantBuilder {
            path: self.path.clone(),
        })
    }

    pub fn is_nullable(&self) -> bool {
        false
    }

    pub fn into_array(self) -> Result<Array> {
        Ok(Array::Null(NullArray { len: 0 }))
    }

    pub fn reserve(&mut self, _additional: usize) {}

    pub fn serialize_default_value(&mut self) -> Result<()> {
        fail!(in self, "Unknown variant does not support serialize_default")
    }
}

impl Context for UnknownVariantBuilder {
    fn annotate(&self, annotations: &mut BTreeMap<String, String>) {
        set_default(annotations, "field", &self.path);
        set_default(annotations, "data_type", "<unknown variant>");
    }
}

impl<'a> serde::Serializer for &'a mut UnknownVariantBuilder {
    impl_serializer!('a, UnknownVariantBuilder;);
}
