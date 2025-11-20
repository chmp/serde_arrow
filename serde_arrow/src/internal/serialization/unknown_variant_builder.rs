use std::collections::{BTreeMap, HashMap};

use marrow::{
    array::{Array, NullArray},
    datatypes::FieldMeta,
};

use crate::internal::{
    error::{fail, set_default, Context, Result},
    serialization::utils::impl_serializer,
};

use super::array_builder::ArrayBuilder;

#[derive(Debug, Clone)]
pub struct UnknownVariantBuilder {
    name: String,
    metadata: HashMap<String, String>,
}

impl UnknownVariantBuilder {
    pub fn new(name: String, metadata: HashMap<String, String>) -> Self {
        UnknownVariantBuilder { name, metadata }
    }

    pub fn take(&mut self) -> ArrayBuilder {
        ArrayBuilder::UnknownVariant(UnknownVariantBuilder {
            name: self.name.clone(),
            metadata: self.metadata.clone(),
        })
    }

    // TODO: is this correct?
    pub fn is_nullable(&self) -> bool {
        false
    }

    pub fn into_array(self) -> Result<Array> {
        Ok(Array::Null(NullArray { len: 0 }))
    }

    pub fn into_array_and_field_meta(self) -> Result<(Array, FieldMeta)> {
        let meta = FieldMeta {
            name: self.name,
            metadata: self.metadata,
            nullable: false,
        };
        let array = Array::Null(NullArray { len: 0 });
        Ok((array, meta))
    }

    pub fn reserve(&mut self, _additional: usize) {}

    pub fn serialize_default_value(&mut self) -> Result<()> {
        fail!(in self, "Unknown variant does not support serialize_default")
    }
}

impl Context for UnknownVariantBuilder {
    fn annotate(&self, annotations: &mut BTreeMap<String, String>) {
        set_default(annotations, "field", &self.name);
        set_default(annotations, "data_type", "<unknown variant>");
    }
}

impl<'a> serde::Serializer for &'a mut UnknownVariantBuilder {
    impl_serializer!('a, UnknownVariantBuilder;);
}
