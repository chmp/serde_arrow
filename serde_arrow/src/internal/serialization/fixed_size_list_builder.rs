use serde::Serialize;

use crate::internal::{
    arrow::{Array, FixedSizeListArray},
    error::{fail, Result},
    schema::GenericField,
    utils::Mut,
};

use super::{
    array_builder::ArrayBuilder,
    array_ext::{ArrayExt, CountArray, SeqArrayExt},
    utils::{meta_from_field, SimpleSerializer},
};

#[derive(Debug, Clone)]

pub struct FixedSizeListBuilder {
    pub seq: CountArray,
    pub field: GenericField,
    pub n: usize,
    pub current_count: usize,
    pub element: Box<ArrayBuilder>,
}

impl FixedSizeListBuilder {
    pub fn new(field: GenericField, element: ArrayBuilder, n: usize, is_nullable: bool) -> Self {
        Self {
            seq: CountArray::new(is_nullable),
            field,
            n,
            current_count: 0,
            element: Box::new(element),
        }
    }

    pub fn take(&mut self) -> Self {
        Self {
            seq: self.seq.take(),
            field: self.field.clone(),
            n: self.n,
            current_count: std::mem::take(&mut self.current_count),
            element: Box::new(self.element.take()),
        }
    }

    pub fn is_nullable(&self) -> bool {
        self.seq.validity.is_some()
    }

    pub fn into_array(self) -> Result<Array> {
        Ok(Array::FixedSizeList(FixedSizeListArray {
            len: self.seq.len,
            validity: self.seq.validity,
            n: self.n.try_into()?,
            meta: meta_from_field(self.field)?,
            element: Box::new((*self.element).into_array()?),
        }))
    }
}

impl FixedSizeListBuilder {
    fn start(&mut self) -> Result<()> {
        self.current_count = 0;
        self.seq.start_seq()
    }

    fn element<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        self.current_count += 1;
        self.seq.push_seq_elements(1)?;
        value.serialize(Mut(self.element.as_mut()))
    }

    fn end(&mut self) -> Result<()> {
        // TODO: fill with default values? would simplify using the builder
        if self.current_count != self.n {
            fail!(
                "Invalid number of elements for FixedSizedList({n}). Expected {n}, got {actual}",
                n = self.n,
                actual = self.current_count
            );
        }
        self.seq.end_seq()
    }
}

impl SimpleSerializer for FixedSizeListBuilder {
    fn name(&self) -> &str {
        "FixedSizeListBuilder"
    }

    fn serialize_default(&mut self) -> Result<()> {
        self.seq.push_seq_default()?;
        for _ in 0..self.n {
            self.element.serialize_default()?;
        }
        Ok(())
    }

    fn serialize_none(&mut self) -> Result<()> {
        self.seq.push_seq_none()?;
        for _ in 0..self.n {
            self.element.serialize_default()?;
        }
        Ok(())
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
}
