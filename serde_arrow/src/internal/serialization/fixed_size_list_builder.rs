use serde::Serialize;

use crate::internal::{
    error::{fail, Result},
    schema::GenericField,
    utils::Mut,
};

use super::{
    array_builder::ArrayBuilder,
    utils::{push_validity, push_validity_default, MutableBitBuffer, SimpleSerializer},
};

#[derive(Debug, Clone)]

pub struct FixedSizeListBuilder {
    pub field: GenericField,
    pub n: usize,
    pub current_count: usize,
    pub num_elements: usize,
    pub validity: Option<MutableBitBuffer>,
    pub element: Box<ArrayBuilder>,
}

impl FixedSizeListBuilder {
    pub fn new(field: GenericField, element: ArrayBuilder, n: usize, is_nullable: bool) -> Self {
        Self {
            field,
            n,
            current_count: 0,
            num_elements: 0,
            validity: is_nullable.then(MutableBitBuffer::default),
            element: Box::new(element),
        }
    }

    pub fn take(&mut self) -> Self {
        Self {
            field: self.field.clone(),
            n: self.n,
            current_count: std::mem::take(&mut self.current_count),
            num_elements: std::mem::take(&mut self.num_elements),
            validity: self.validity.as_mut().map(std::mem::take),
            element: Box::new(self.element.take()),
        }
    }

    pub fn is_nullable(&self) -> bool {
        self.validity.is_some()
    }
}

impl FixedSizeListBuilder {
    fn start(&mut self) -> Result<()> {
        self.current_count = 0;
        push_validity(&mut self.validity, true)
    }

    fn element<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        self.current_count += 1;
        value.serialize(Mut(self.element.as_mut()))
    }

    fn end(&mut self) -> Result<()> {
        if self.current_count != self.n {
            fail!(
                "Invalid number of elements for FixedSizedList({n}). Expected {n}, got {actual}",
                n = self.n,
                actual = self.current_count
            );
        }
        self.num_elements += 1;
        Ok(())
    }
}

impl SimpleSerializer for FixedSizeListBuilder {
    fn name(&self) -> &str {
        "FixedSizeListBuilder"
    }

    fn serialize_default(&mut self) -> Result<()> {
        push_validity_default(&mut self.validity);
        for _ in 0..self.n {
            self.element.serialize_default()?;
        }
        self.num_elements += 1;
        Ok(())
    }

    fn serialize_none(&mut self) -> Result<()> {
        push_validity(&mut self.validity, false)?;
        for _ in 0..self.n {
            self.element.serialize_default()?;
        }
        self.num_elements += 1;
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
