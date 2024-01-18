use serde::Serialize;

use crate::{
    internal::{
        common::{MutableBitBuffer, MutableOffsetBuffer},
        error::fail,
    },
    Result,
};

use super::{
    array_builder::ArrayBuilder,
    utils::{Mut, SimpleSerializer},
};

#[derive(Debug, Clone)]

pub struct ListBuilder {
    pub validity: Option<MutableBitBuffer>,
    pub offsets: MutableOffsetBuffer<i32>,
    pub element: Box<ArrayBuilder>,
}

impl ListBuilder {
    pub fn new(element: ArrayBuilder, is_nullable: bool) -> Self {
        Self {
            validity: is_nullable.then(MutableBitBuffer::default),
            offsets: Default::default(),
            element: Box::new(element),
        }
    }
}

impl ListBuilder {
    pub fn serialize_default(&mut self) -> Result<()> {
        fail!("not implemented");
    }
}

impl SimpleSerializer for ListBuilder {
    fn name(&self) -> &str {
        "ListBuilder"
    }

    fn serialize_seq_start(&mut self, len: Option<usize>) -> Result<()> {
        Ok(())
    }

    fn serialize_seq_element<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        self.offsets.inc_current_items()?;
        value.serialize(Mut(self.element.as_mut()))?;
        Ok(())
    }

    fn serialize_seq_end(&mut self) -> Result<()> {
        self.offsets.push_current_items();
        Ok(())
    }
}
