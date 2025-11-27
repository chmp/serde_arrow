use std::collections::{BTreeMap, HashMap};

use marrow::{
    array::{Array, NullArray},
    datatypes::FieldMeta,
};
use serde::{Serialize, Serializer};

use crate::internal::{
    error::{set_default, Context, ContextSupport, Result},
    serialization::utils::impl_serializer,
};

use super::array_builder::ArrayBuilder;

#[derive(Debug, Clone)]
pub struct NullBuilder {
    pub name: String,
    pub metadata: HashMap<String, String>,
    pub count: usize,
}

impl NullBuilder {
    pub fn new(name: String, metadata: HashMap<String, String>) -> Self {
        Self {
            name,
            metadata,
            count: 0,
        }
    }

    pub fn take(&mut self) -> ArrayBuilder {
        ArrayBuilder::Null(Self {
            name: self.name.clone(),
            metadata: self.metadata.clone(),
            count: std::mem::take(&mut self.count),
        })
    }

    pub fn is_nullable(&self) -> bool {
        true
    }

    pub fn into_array_and_field_meta(self) -> Result<(Array, FieldMeta)> {
        let meta = FieldMeta {
            name: self.name,
            metadata: self.metadata,
            nullable: true,
        };
        let array = Array::Null(NullArray { len: self.count });
        Ok((array, meta))
    }

    pub fn reserve(&mut self, _additional: usize) {}

    pub fn serialize_default_value(&mut self) -> Result<()> {
        self.count += 1;
        Ok(())
    }

    pub fn serialize_value<V: Serialize>(&mut self, value: V) -> Result<()> {
        value.serialize(&mut *self).ctx(self)
    }
}

impl Context for NullBuilder {
    fn annotate(&self, annotations: &mut BTreeMap<String, String>) {
        set_default(annotations, "field", &self.name);
        set_default(annotations, "data_type", "Null");
    }
}

impl<'a> Serializer for &'a mut NullBuilder {
    impl_serializer!(
        'a, NullBuilder;
        override serialize_none,
        override serialize_unit_struct,
    );

    fn serialize_none(self) -> Result<()> {
        self.count += 1;
        Ok(())
    }

    fn serialize_unit_struct(self, _: &'static str) -> Result<()> {
        self.count += 1;
        Ok(())
    }
}
