use crate::Result;

use super::utils::{Mut, SimpleSerializer};

#[derive(Debug, Clone)]
pub struct NullBuilder {
    pub count: usize,
}

impl NullBuilder {
    pub fn new() -> Self {
        Self { count: 0 }
    }

    pub fn take(&mut self) -> Self {
        Self {
            count: std::mem::take(&mut self.count),
        }
    }
}

impl SimpleSerializer for NullBuilder {
    fn name(&self) -> &str {
        "NullBuilder"
    }

    fn serialize_default(&mut self) -> Result<()> {
        self.count += 1;
        Ok(())
    }

    fn serialize_none(&mut self) -> Result<()> {
        self.count += 1;
        Ok(())
    }

    fn serialize_some<V: serde::Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        value.serialize(Mut(self))
    }

    fn serialize_unit(&mut self) -> Result<()> {
        self.count += 1;
        Ok(())
    }

    fn serialize_unit_struct(&mut self, _: &'static str) -> Result<()> {
        self.count += 1;
        Ok(())
    }
}
