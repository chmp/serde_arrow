use std::collections::BTreeMap;

use marrow::{
    array::{Array, ListArray},
    datatypes::FieldMeta,
};
use serde::Serialize;

use crate::internal::{
    error::{set_default, try_, Context, ContextSupport, Result},
    serialization::utils::impl_serializer,
    utils::{
        array_ext::{ArrayExt, OffsetsArray, SeqArrayExt},
        Mut, NamedType, Offset,
    },
};

use super::{array_builder::ArrayBuilder, simple_serializer::SimpleSerializer};

#[derive(Debug, Clone)]

pub struct ListBuilder<O> {
    pub path: String,
    pub meta: FieldMeta,
    pub elements: Box<ArrayBuilder>,
    pub offsets: OffsetsArray<O>,
}

impl<O: Offset + NamedType> ListBuilder<O> {
    pub fn new(path: String, meta: FieldMeta, element: ArrayBuilder, is_nullable: bool) -> Self {
        Self {
            path,
            meta,
            elements: Box::new(element),
            offsets: OffsetsArray::new(is_nullable),
        }
    }

    pub fn take_self(&mut self) -> Self {
        Self {
            path: self.path.clone(),
            meta: self.meta.clone(),
            offsets: self.offsets.take(),
            elements: Box::new(self.elements.take()),
        }
    }

    pub fn is_nullable(&self) -> bool {
        self.offsets.validity.is_some()
    }

    pub fn reserve(&mut self, additional: usize) {
        self.offsets.reserve(additional);
    }

    pub fn serialize_default_value(&mut self) -> Result<()> {
        try_(|| self.offsets.push_seq_default()).ctx(self)
    }
}

impl ListBuilder<i32> {
    pub fn take(&mut self) -> ArrayBuilder {
        ArrayBuilder::List(self.take_self())
    }

    pub fn into_array(self) -> Result<Array> {
        Ok(Array::List(ListArray {
            validity: self.offsets.validity,
            offsets: self.offsets.offsets,
            elements: Box::new(self.elements.into_array()?),
            meta: self.meta,
        }))
    }
}

impl ListBuilder<i64> {
    pub fn take(&mut self) -> ArrayBuilder {
        ArrayBuilder::LargeList(self.take_self())
    }

    pub fn into_array(self) -> Result<Array> {
        Ok(Array::LargeList(ListArray {
            validity: self.offsets.validity,
            offsets: self.offsets.offsets,
            elements: Box::new(self.elements.into_array()?),
            meta: self.meta,
        }))
    }
}

impl<O: NamedType + Offset> ListBuilder<O> {
    fn start(&mut self) -> Result<()> {
        self.offsets.start_seq()
    }

    fn element<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        self.offsets.push_seq_elements(1)?;
        value.serialize(Mut(self.elements.as_mut()))
    }

    fn end(&mut self) -> Result<()> {
        self.offsets.end_seq()
    }
}

impl<O: NamedType> Context for ListBuilder<O> {
    fn annotate(&self, annotations: &mut BTreeMap<String, String>) {
        set_default(annotations, "field", &self.path);
        set_default(
            annotations,
            "data_type",
            if O::NAME == "i32" {
                "List"
            } else {
                "LargeList"
            },
        );
    }
}

impl<O: NamedType + Offset> SimpleSerializer for ListBuilder<O> {
    fn serialize_default(&mut self) -> Result<()> {
        self.serialize_default_value()
    }

    fn serialize_none(&mut self) -> Result<()> {
        try_(|| self.offsets.push_seq_none()).ctx(self)
    }

    fn serialize_seq_start(&mut self, len: Option<usize>) -> Result<()> {
        try_(|| {
            if let Some(len) = len {
                self.elements.reserve(len);
            }
            self.start()
        })
        .ctx(self)
    }

    fn serialize_seq_element<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        try_(|| self.element(value)).ctx(self)
    }

    fn serialize_seq_end(&mut self) -> Result<()> {
        try_(|| self.end()).ctx(self)
    }

    fn serialize_tuple_start(&mut self, len: usize) -> Result<()> {
        try_(|| {
            self.elements.reserve(len);
            self.start()
        })
        .ctx(self)
    }

    fn serialize_tuple_element<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        try_(|| self.element(value)).ctx(self)
    }

    fn serialize_tuple_end(&mut self) -> Result<()> {
        try_(|| self.end()).ctx(self)
    }

    fn serialize_tuple_struct_start(&mut self, _: &'static str, len: usize) -> Result<()> {
        try_(|| {
            self.elements.reserve(len);
            self.start()
        })
        .ctx(self)
    }

    fn serialize_tuple_struct_field<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        try_(|| self.element(value)).ctx(self)
    }

    fn serialize_tuple_struct_end(&mut self) -> Result<()> {
        try_(|| self.end()).ctx(self)
    }

    fn serialize_bytes(&mut self, v: &[u8]) -> Result<()> {
        try_(|| {
            self.elements.reserve(v.len());
            self.start()?;
            for item in v {
                self.element(item)?;
            }
            self.end()
        })
        .ctx(self)
    }
}

impl<'a, O: NamedType + Offset> serde::Serializer for &'a mut ListBuilder<O> {
    impl_serializer!('a, ListBuilder;);
}
