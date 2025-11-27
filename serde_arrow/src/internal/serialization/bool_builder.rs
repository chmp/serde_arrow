use std::collections::{BTreeMap, HashMap};

use marrow::{
    array::{Array, BooleanArray},
    datatypes::FieldMeta,
};
use serde::{Serialize, Serializer};

use crate::internal::{
    error::{set_default, try_, Context, ContextSupport, Result},
    serialization::utils::impl_serializer,
    utils::array_ext::{set_bit_buffer, set_validity, set_validity_default},
};

use super::array_builder::ArrayBuilder;

#[derive(Debug, Clone)]
pub struct BoolBuilder {
    pub name: String,
    array: BooleanArray,
    metadata: HashMap<String, String>,
}

impl BoolBuilder {
    pub fn new(name: String, is_nullable: bool, metadata: HashMap<String, String>) -> Self {
        Self {
            name,
            array: BooleanArray {
                len: 0,
                validity: is_nullable.then(Vec::new),
                values: Vec::new(),
            },
            metadata,
        }
    }

    pub fn take(&mut self) -> ArrayBuilder {
        ArrayBuilder::Bool(Self {
            name: self.name.clone(),
            metadata: self.metadata.clone(),
            array: BooleanArray {
                len: std::mem::take(&mut self.array.len),
                validity: self.array.validity.as_mut().map(std::mem::take),
                values: std::mem::take(&mut self.array.values),
            },
        })
    }

    pub fn is_nullable(&self) -> bool {
        self.array.validity.is_some()
    }

    #[inline]
    pub fn into_array_and_field_meta(self) -> Result<(Array, FieldMeta)> {
        let meta = FieldMeta {
            name: self.name,
            nullable: self.array.validity.is_some(),
            metadata: self.metadata,
        };
        let array = Array::Boolean(self.array);
        Ok((array, meta))
    }

    pub fn reserve(&mut self, additional: usize) {
        if let Some(validity) = &mut self.array.validity {
            validity.reserve(additional / 8 + 1);
        }
        self.array.values.reserve(additional / 8 + 1);
    }

    pub fn serialize_default_value(&mut self) -> Result<()> {
        try_(|| {
            set_validity_default(self.array.validity.as_mut(), self.array.len);
            set_bit_buffer(&mut self.array.values, self.array.len, false);
            self.array.len += 1;
            Ok(())
        })
        .ctx(self)
    }

    pub fn serialize_value<V: Serialize>(&mut self, value: V) -> Result<()> {
        value.serialize(&mut *self).ctx(self)
    }
}

impl Context for BoolBuilder {
    fn annotate(&self, annotations: &mut BTreeMap<String, String>) {
        set_default(annotations, "field", &self.name);
        set_default(annotations, "data_type", "Boolean");
    }
}

impl<'a> Serializer for &'a mut BoolBuilder {
    impl_serializer!(
        'a, BoolBuilder;
        override serialize_none,
        override serialize_bool,
    );

    fn serialize_none(self) -> Result<()> {
        set_validity(self.array.validity.as_mut(), self.array.len, false)?;
        set_bit_buffer(&mut self.array.values, self.array.len, false);
        self.array.len += 1;
        Ok(())
    }

    fn serialize_bool(self, v: bool) -> Result<()> {
        set_validity(self.array.validity.as_mut(), self.array.len, true)?;
        set_bit_buffer(&mut self.array.values, self.array.len, v);
        self.array.len += 1;
        Ok(())
    }
}
