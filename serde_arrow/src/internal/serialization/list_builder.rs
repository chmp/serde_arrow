use serde::Serialize;

use crate::internal::{
    arrow::{Array, FieldMeta, ListArray},
    error::Result,
    utils::{Mut, Offset},
};

use super::{
    array_builder::ArrayBuilder,
    array_ext::{ArrayExt, OffsetsArray, SeqArrayExt},
    simple_serializer::SimpleSerializer,
};

#[derive(Debug, Clone)]

pub struct ListBuilder<O> {
    pub meta: FieldMeta,
    pub element: Box<ArrayBuilder>,
    pub offsets: OffsetsArray<O>,
}

impl<O: Offset> ListBuilder<O> {
    pub fn new(meta: FieldMeta, element: ArrayBuilder, is_nullable: bool) -> Result<Self> {
        Ok(Self {
            meta,
            element: Box::new(element),
            offsets: OffsetsArray::new(is_nullable),
        })
    }

    pub fn take(&mut self) -> Self {
        Self {
            meta: self.meta.clone(),
            offsets: self.offsets.take(),
            element: Box::new(self.element.take()),
        }
    }

    pub fn is_nullable(&self) -> bool {
        self.offsets.validity.is_some()
    }
}

impl ListBuilder<i32> {
    pub fn into_array(self) -> Result<Array> {
        Ok(Array::List(ListArray {
            validity: self.offsets.validity,
            offsets: self.offsets.offsets,
            element: Box::new(self.element.into_array()?),
            meta: self.meta,
        }))
    }
}

impl ListBuilder<i64> {
    pub fn into_array(self) -> Result<Array> {
        Ok(Array::LargeList(ListArray {
            validity: self.offsets.validity,
            offsets: self.offsets.offsets,
            element: Box::new(self.element.into_array()?),
            meta: self.meta,
        }))
    }
}

impl<O: Offset> ListBuilder<O> {
    fn start(&mut self) -> Result<()> {
        self.offsets.start_seq()
    }

    fn element<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        self.offsets.push_seq_elements(1)?;
        value.serialize(Mut(self.element.as_mut()))
    }

    fn end(&mut self) -> Result<()> {
        self.offsets.end_seq()
    }
}

impl<O: Offset> SimpleSerializer for ListBuilder<O> {
    fn name(&self) -> &str {
        "ListBuilder"
    }

    fn serialize_default(&mut self) -> Result<()> {
        self.offsets.push_seq_default()
    }

    fn serialize_none(&mut self) -> Result<()> {
        self.offsets.push_seq_none()
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
