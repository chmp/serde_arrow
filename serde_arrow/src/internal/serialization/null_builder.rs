use std::collections::BTreeMap;

use marrow::array::{Array, NullArray};

use crate::internal::{
    error::{set_default, Context, Result},
    serialization::utils::impl_serializer,
};

use super::{array_builder::ArrayBuilder, simple_serializer::SimpleSerializer};

#[derive(Debug, Clone)]
pub struct NullBuilder {
    pub path: String,
    pub count: usize,
}

impl NullBuilder {
    pub fn new(path: String) -> Self {
        Self { path, count: 0 }
    }

    pub fn take(&mut self) -> ArrayBuilder {
        ArrayBuilder::Null(Self {
            path: self.path.clone(),
            count: std::mem::take(&mut self.count),
        })
    }

    pub fn is_nullable(&self) -> bool {
        true
    }

    pub fn into_array(self) -> Result<Array> {
        Ok(Array::Null(NullArray { len: self.count }))
    }

    pub fn reserve(&mut self, _additional: usize) {}

    pub fn serialize_default_value(&mut self) -> Result<()> {
        self.count += 1;
        Ok(())
    }
}

impl Context for NullBuilder {
    fn annotate(&self, annotations: &mut BTreeMap<String, String>) {
        set_default(annotations, "field", &self.path);
        set_default(annotations, "data_type", "Null");
    }
}

impl SimpleSerializer for NullBuilder {
    fn serialize_default(&mut self) -> Result<()> {
        self.serialize_default_value()
    }

    fn serialize_none(&mut self) -> Result<()> {
        self.count += 1;
        Ok(())
    }

    fn serialize_unit_struct(&mut self, _: &'static str) -> Result<()> {
        self.count += 1;
        Ok(())
    }
}

impl<'a> serde::Serializer for &'a mut NullBuilder {
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
