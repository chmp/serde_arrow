use crate::{internal::common::MutableBitBuffer, Result};

use super::utils::{push_null, SimpleSerializer};

#[derive(Debug, Clone, Default)]
pub struct I8Builder {
    pub validity: Option<MutableBitBuffer>,
    pub buffer: Vec<i8>,
}

impl I8Builder {
    pub fn serialize_default(&mut self) -> Result<()> {
        self.buffer.push(0);
        Ok(())
    }
}

impl SimpleSerializer for I8Builder {
    fn name(&self) -> &str {
        "I8Builder"
    }

    fn serialize_i8(&mut self, v: i8) -> Result<()> {
        push_null(&mut self.validity, true)?;
        self.buffer.push(v);
        Ok(())
    }
}
