use std::collections::{BTreeMap, HashMap};

use marrow::{
    array::{Array, FixedSizeListArray},
    datatypes::FieldMeta,
};
use serde::Serialize;

use crate::internal::{
    error::{fail, set_default, try_, Context, ContextSupport, Error, Result},
    serialization::utils::impl_serializer,
    utils::array_ext::{ArrayExt, CountArray, SeqArrayExt},
};

use super::array_builder::ArrayBuilder;

#[derive(Debug, Clone)]

pub struct FixedSizeListBuilder {
    pub name: String,
    pub seq: CountArray,
    pub n: usize,
    pub current_count: usize,
    pub elements: Box<ArrayBuilder>,
    pub metadata: HashMap<String, String>,
}

impl FixedSizeListBuilder {
    pub fn new(
        name: String,
        element: ArrayBuilder,
        n: usize,
        is_nullable: bool,
        metadata: HashMap<String, String>,
    ) -> Self {
        Self {
            name,
            seq: CountArray::new(is_nullable),
            n,
            current_count: 0,
            elements: Box::new(element),
            metadata,
        }
    }

    pub fn take(&mut self) -> ArrayBuilder {
        ArrayBuilder::FixedSizedList(Self {
            name: self.name.clone(),
            metadata: self.metadata.clone(),
            seq: self.seq.take(),
            n: self.n,
            current_count: std::mem::take(&mut self.current_count),
            elements: Box::new(self.elements.take()),
        })
    }

    pub fn is_nullable(&self) -> bool {
        self.seq.validity.is_some()
    }

    pub fn into_array_and_field_meta(self) -> Result<(Array, FieldMeta)> {
        let meta = FieldMeta {
            name: self.name,
            metadata: self.metadata,
            nullable: self.seq.validity.is_some(),
        };
        let (child_array, child_meta) = (*self.elements).into_array_and_field_meta()?;
        let array = Array::FixedSizeList(FixedSizeListArray {
            len: self.seq.len,
            validity: self.seq.validity,
            n: self.n.try_into()?,
            meta: child_meta,
            elements: Box::new(child_array),
        });
        Ok((array, meta))
    }

    pub fn reserve(&mut self, additional: usize) {
        self.elements.reserve(additional * self.n);
        self.seq.reserve(additional);
    }

    pub fn serialize_default_value(&mut self) -> Result<()> {
        try_(|| {
            self.seq.push_seq_default()?;
            for _ in 0..self.n {
                self.elements.serialize_default_value()?;
            }
            Ok(())
        })
        .ctx(self)
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
        value.serialize(self.elements.as_mut())
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
        set_default(annotations, "field", &self.name);
        set_default(annotations, "data_type", "FixedSizeList(..)");
    }
}

impl<'a> serde::Serializer for &'a mut FixedSizeListBuilder {
    impl_serializer!(
        'a, FixedSizeListBuilder;
        override serialize_none,
        override serialize_seq,
    );

    fn serialize_none(self) -> Result<()> {
        try_(|| {
            self.seq.push_seq_none()?;
            for _ in 0..self.n {
                self.elements.serialize_default_value()?;
            }
            Ok(())
        })
        .ctx(self)
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq> {
        if let Some(len) = len {
            self.reserve(len);
        }
        self.start().ctx(self)?;
        Ok(super::utils::SerializeSeq::FixedSizeList(self))
    }
}

impl serde::ser::SerializeSeq for &mut FixedSizeListBuilder {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<()> {
        self.element(value).ctx(*self)
    }

    fn end(self) -> std::result::Result<Self::Ok, Self::Error> {
        FixedSizeListBuilder::end(&mut *self).ctx(self)
    }
}
