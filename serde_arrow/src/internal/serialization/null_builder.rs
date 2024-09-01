use crate::internal::{
    arrow::{Array, NullArray},
    error::{Error, Result},
};

use super::simple_serializer::SimpleSerializer;

#[derive(Debug, Clone)]
pub struct NullBuilder {
    pub path: String,
    pub count: usize,
}

impl NullBuilder {
    pub fn new(path: String) -> Self {
        Self { path, count: 0 }
    }

    pub fn take(&mut self) -> Self {
        Self {
            path: self.path.clone(),
            count: std::mem::take(&mut self.count),
        }
    }

    pub fn is_nullable(&self) -> bool {
        true
    }

    pub fn into_array(self) -> Result<Array> {
        Ok(Array::Null(NullArray { len: self.count }))
    }
}

impl SimpleSerializer for NullBuilder {
    fn name(&self) -> &str {
        "NullBuilder"
    }

    fn annotate_error(&self, err: Error) -> Error {
        err.annotate_unannotated(|annotations| {
            annotations.insert(String::from("field"), self.path.clone());
        })
    }

    fn serialize_default(&mut self) -> Result<()> {
        self.count += 1;
        Ok(())
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
