use serde::Serialize;

use crate::internal::common::{MutableBitBuffer, MutableOffsetBuffer};

use super::{
    array_builder::ArrayBuilder,
    utils::{Mut, SimpleSerializer},
};

#[derive(Debug, Clone)]
pub struct MapBuilder {
    pub validity: Option<MutableBitBuffer>,
    pub offsets: MutableOffsetBuffer<i32>,
    pub key: Box<ArrayBuilder>,
    pub value: Box<ArrayBuilder>,
}

impl MapBuilder {
    pub fn new(key: ArrayBuilder, value: ArrayBuilder, is_nullable: bool) -> Self {
        Self {
            validity: is_nullable.then(MutableBitBuffer::default),
            offsets: MutableOffsetBuffer::default(),
            key: Box::new(key),
            value: Box::new(value),
        }
    }
}

impl SimpleSerializer for MapBuilder {
    fn name(&self) -> &str {
        "MapBuilder"
    }

    fn serialize_map_start(&mut self, _: Option<usize>) -> crate::Result<()> {
        Ok(())
    }

    fn serialize_map_key<V: Serialize + ?Sized>(&mut self, key: &V) -> crate::Result<()> {
        self.offsets.inc_current_items()?;
        key.serialize(Mut(self.key.as_mut()))?;
        Ok(())
    }

    fn serialize_map_value<V: Serialize + ?Sized>(&mut self, value: &V) -> crate::Result<()> {
        value.serialize(Mut(self.value.as_mut()))
    }

    fn serialize_map_end(&mut self) -> crate::Result<()> {
        self.offsets.push_current_items();
        Ok(())
    }
}
