use serde::Serialize;

use crate::{
    internal::common::{MutableBitBuffer, MutableOffsetBuffer, Offset},
    Result,
};

use super::{
    array_builder::ArrayBuilder,
    utils::{push_validity, push_validity_default, Mut, SimpleSerializer},
};

#[derive(Debug, Clone)]

pub struct ListBuilder<O> {
    pub validity: Option<MutableBitBuffer>,
    pub offsets: MutableOffsetBuffer<O>,
    pub element: Box<ArrayBuilder>,
}

impl<O: Offset> ListBuilder<O> {
    pub fn new(element: ArrayBuilder, is_nullable: bool) -> Self {
        Self {
            validity: is_nullable.then(MutableBitBuffer::default),
            offsets: Default::default(),
            element: Box::new(element),
        }
    }
}

impl<O: Offset> SimpleSerializer for ListBuilder<O> {
    fn name(&self) -> &str {
        "ListBuilder"
    }

    fn serialize_default(&mut self) -> Result<()> {
        push_validity_default(&mut self.validity);
        self.offsets.push_current_items();
        Ok(())
    }

    fn serialize_none(&mut self) -> Result<()> {
        self.serialize_default()?;
        push_validity(&mut self.validity, false)
    }

    fn serialize_some<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        value.serialize(Mut(self))
    }

    fn serialize_seq_start(&mut self, _: Option<usize>) -> Result<()> {
        push_validity(&mut self.validity, true)
    }

    fn serialize_seq_element<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        self.offsets.inc_current_items()?;
        value.serialize(Mut(self.element.as_mut()))
    }

    fn serialize_seq_end(&mut self) -> Result<()> {
        self.offsets.push_current_items();
        Ok(())
    }
}
