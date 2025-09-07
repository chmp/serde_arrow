use std::collections::BTreeMap;

use marrow::{
    array::{Array, FixedSizeListArray},
    datatypes::FieldMeta,
};
use serde::Serialize;

use crate::internal::{
    error::{fail, set_default, try_, Context, ContextSupport, Result},
    utils::{
        array_ext::{ArrayExt, CountArray, SeqArrayExt},
        Mut,
    },
};

use super::{array_builder::ArrayBuilder, simple_serializer::SimpleSerializer};

#[derive(Debug, Clone)]

pub struct FixedSizeListBuilder {
    pub path: String,
    pub seq: CountArray,
    pub meta: FieldMeta,
    pub n: usize,
    pub current_count: usize,
    pub elements: Box<ArrayBuilder>,
}

impl FixedSizeListBuilder {
    pub fn new(
        path: String,
        meta: FieldMeta,
        element: ArrayBuilder,
        n: usize,
        is_nullable: bool,
    ) -> Self {
        Self {
            path,
            seq: CountArray::new(is_nullable),
            meta,
            n,
            current_count: 0,
            elements: Box::new(element),
        }
    }

    pub fn take(&mut self) -> ArrayBuilder {
        ArrayBuilder::FixedSizedList(Self {
            path: self.path.clone(),
            seq: self.seq.take(),
            meta: self.meta.clone(),
            n: self.n,
            current_count: std::mem::take(&mut self.current_count),
            elements: Box::new(self.elements.take()),
        })
    }

    pub fn is_nullable(&self) -> bool {
        self.seq.validity.is_some()
    }

    pub fn into_array(self) -> Result<Array> {
        Ok(Array::FixedSizeList(FixedSizeListArray {
            len: self.seq.len,
            validity: self.seq.validity,
            n: self.n.try_into()?,
            meta: self.meta,
            elements: Box::new((*self.elements).into_array()?),
        }))
    }

    pub fn reserve(&mut self, additional: usize) {
        self.elements.reserve(additional * self.n);
        self.seq.reserve(additional);
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
        value.serialize(Mut(self.elements.as_mut()))
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

impl Context for FixedSizeListBuilder {
    fn annotate(&self, annotations: &mut BTreeMap<String, String>) {
        set_default(annotations, "field", &self.path);
        set_default(annotations, "data_type", "FixedSizeList(..)");
    }
}

impl SimpleSerializer for FixedSizeListBuilder {
    fn serialize_default(&mut self) -> Result<()> {
        try_(|| {
            self.seq.push_seq_default()?;
            for _ in 0..self.n {
                self.elements.serialize_default()?;
            }
            Ok(())
        })
        .ctx(self)
    }

    fn serialize_none(&mut self) -> Result<()> {
        try_(|| {
            self.seq.push_seq_none()?;
            for _ in 0..self.n {
                self.elements.serialize_default()?;
            }
            Ok(())
        })
        .ctx(self)
    }

    fn serialize_seq_start(&mut self, _: Option<usize>) -> Result<()> {
        try_(|| self.start()).ctx(self)
    }

    fn serialize_seq_element<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        try_(|| self.element(value)).ctx(self)
    }

    fn serialize_seq_end(&mut self) -> Result<()> {
        try_(|| self.end()).ctx(self)
    }

    fn serialize_tuple_start(&mut self, _: usize) -> Result<()> {
        try_(|| self.start()).ctx(self)
    }

    fn serialize_tuple_element<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        try_(|| self.element(value)).ctx(self)
    }

    fn serialize_tuple_end(&mut self) -> Result<()> {
        try_(|| self.end()).ctx(self)
    }

    fn serialize_tuple_struct_start(&mut self, _: &'static str, _: usize) -> Result<()> {
        try_(|| self.start()).ctx(self)
    }

    fn serialize_tuple_struct_field<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        try_(|| self.element(value)).ctx(self)
    }

    fn serialize_tuple_struct_end(&mut self) -> Result<()> {
        try_(|| self.end()).ctx(self)
    }
}
