use std::collections::BTreeMap;

use serde::Serialize;

use crate::internal::{
    arrow::{Array, FieldMeta, ListArray},
    error::{set_default, Context, ContextSupport, Result},
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
    pub element: Box<ArrayBuilder>,
    pub offsets: OffsetsArray<O>,
}

impl<O: Offset> ListBuilder<O> {
    pub fn new(path: String, meta: FieldMeta, element: ArrayBuilder, is_nullable: bool) -> Self {
        Self {
            path,
            meta,
            element: Box::new(element),
            offsets: OffsetsArray::new(is_nullable),
        }
    }

    pub fn take_self(&mut self) -> Self {
        Self {
            path: self.path.clone(),
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
    pub fn take(&mut self) -> ArrayBuilder {
        ArrayBuilder::List(self.take_self())
    }

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
    pub fn take(&mut self) -> ArrayBuilder {
        ArrayBuilder::LargeList(self.take_self())
    }

    pub fn into_array(self) -> Result<Array> {
        Ok(Array::LargeList(ListArray {
            validity: self.offsets.validity,
            offsets: self.offsets.offsets,
            element: Box::new(self.element.into_array()?),
            meta: self.meta,
        }))
    }
}

impl<O: NamedType + Offset> ListBuilder<O> {
    fn start(&mut self) -> Result<()> {
        self.offsets.start_seq()
    }

    fn element<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        self.offsets.push_seq_elements(1).ctx(self)?;
        value.serialize(Mut(self.element.as_mut()))
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
        self.offsets.push_seq_default().ctx(self)
    }

    fn serialize_none(&mut self) -> Result<()> {
        self.offsets.push_seq_none().ctx(self)
    }

    fn serialize_seq_start(&mut self, _: Option<usize>) -> Result<()> {
        self.start().ctx(self)
    }

    fn serialize_seq_element<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        self.element(value)
    }

    fn serialize_seq_end(&mut self) -> Result<()> {
        self.end().ctx(self)
    }

    fn serialize_tuple_start(&mut self, _: usize) -> Result<()> {
        self.start().ctx(self)
    }

    fn serialize_tuple_element<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        self.element(value)
    }

    fn serialize_tuple_end(&mut self) -> Result<()> {
        self.end().ctx(self)
    }

    fn serialize_tuple_struct_start(&mut self, _: &'static str, _: usize) -> Result<()> {
        self.start().ctx(self)
    }

    fn serialize_tuple_struct_field<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        self.element(value)
    }

    fn serialize_tuple_struct_end(&mut self) -> Result<()> {
        self.end().ctx(self)
    }

    fn serialize_bytes(&mut self, v: &[u8]) -> Result<()> {
        self.start().ctx(self)?;
        for item in v {
            self.element(item)?;
        }
        self.end().ctx(self)
    }
}
