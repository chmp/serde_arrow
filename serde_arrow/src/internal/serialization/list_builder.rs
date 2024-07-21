use serde::Serialize;

use crate::internal::{
    arrow::{Array, ListArray},
    error::Result,
    schema::GenericField,
    utils::{Mut, Offset},
};

use super::{
    array_builder::ArrayBuilder,
    utils::{
        meta_from_field, push_validity, push_validity_default, MutableBitBuffer,
        MutableOffsetBuffer, SimpleSerializer,
    },
};

#[derive(Debug, Clone)]

pub struct ListBuilder<O> {
    pub field: GenericField,
    pub validity: Option<MutableBitBuffer>,
    pub offsets: MutableOffsetBuffer<O>,
    pub element: Box<ArrayBuilder>,
}

impl<O: Offset> ListBuilder<O> {
    pub fn new(field: GenericField, element: ArrayBuilder, is_nullable: bool) -> Self {
        Self {
            field,
            validity: is_nullable.then(MutableBitBuffer::default),
            offsets: Default::default(),
            element: Box::new(element),
        }
    }

    pub fn take(&mut self) -> Self {
        Self {
            field: self.field.clone(),
            validity: self.validity.as_mut().map(std::mem::take),
            offsets: std::mem::take(&mut self.offsets),
            element: Box::new(self.element.take()),
        }
    }

    pub fn is_nullable(&self) -> bool {
        self.validity.is_some()
    }
}

impl ListBuilder<i32> {
    pub fn into_array(self) -> Result<Array> {
        Ok(Array::List(ListArray {
            validity: self.validity.map(|b| b.buffer),
            offsets: self.offsets.offsets,
            element: Box::new(self.element.into_array()?),
            meta: meta_from_field(self.field)?,
        }))
    }
}

impl ListBuilder<i64> {
    pub fn into_array(self) -> Result<Array> {
        Ok(Array::LargeList(ListArray {
            validity: self.validity.map(|b| b.buffer),
            offsets: self.offsets.offsets,
            element: Box::new(self.element.into_array()?),
            meta: meta_from_field(self.field)?,
        }))
    }
}

impl<O: Offset> ListBuilder<O> {
    fn start(&mut self) -> Result<()> {
        push_validity(&mut self.validity, true)
    }

    fn element<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        self.offsets.inc_current_items()?;
        value.serialize(Mut(self.element.as_mut()))
    }

    fn end(&mut self) -> Result<()> {
        self.offsets.push_current_items();
        Ok(())
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
        self.offsets.push_current_items();
        push_validity(&mut self.validity, false)
    }

    fn serialize_seq_start(&mut self, _: Option<usize>) -> Result<()> {
        self.start()
    }

    fn serialize_seq_element<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        self.element(value)
    }

    fn serialize_seq_end(&mut self) -> Result<()> {
        self.end()
    }

    fn serialize_tuple_start(&mut self, _: usize) -> Result<()> {
        self.start()
    }

    fn serialize_tuple_element<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        self.element(value)
    }

    fn serialize_tuple_end(&mut self) -> Result<()> {
        self.end()
    }

    fn serialize_tuple_struct_start(&mut self, _: &'static str, _: usize) -> Result<()> {
        self.start()
    }

    fn serialize_tuple_struct_field<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        self.element(value)
    }

    fn serialize_tuple_struct_end(&mut self) -> Result<()> {
        self.end()
    }

    fn serialize_bytes(&mut self, v: &[u8]) -> Result<()> {
        self.start()?;
        for item in v {
            self.element(item)?;
        }
        self.end()
    }
}
