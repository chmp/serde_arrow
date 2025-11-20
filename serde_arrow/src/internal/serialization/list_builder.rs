use std::collections::{BTreeMap, HashMap};

use marrow::{
    array::{Array, ListArray},
    datatypes::FieldMeta,
};
use serde::Serialize;

use crate::internal::{
    error::{set_default, try_, Context, ContextSupport, Error, Result},
    serialization::utils::impl_serializer,
    utils::{
        array_ext::{ArrayExt, OffsetsArray, SeqArrayExt},
        NamedType, Offset,
    },
};

use super::array_builder::ArrayBuilder;

#[derive(Debug, Clone)]

pub struct ListBuilder<O> {
    pub name: String,
    pub meta: FieldMeta,
    pub elements: Box<ArrayBuilder>,
    pub offsets: OffsetsArray<O>,
    pub metadata: HashMap<String, String>,
}

impl<O: Offset + NamedType> ListBuilder<O> {
    pub fn new(
        name: String,
        meta: FieldMeta,
        element: ArrayBuilder,
        is_nullable: bool,
        metadata: HashMap<String, String>,
    ) -> Self {
        Self {
            name,
            meta,
            elements: Box::new(element),
            offsets: OffsetsArray::new(is_nullable),
            metadata,
        }
    }

    pub fn take_self(&mut self) -> Self {
        Self {
            name: self.name.clone(),
            metadata: self.metadata.clone(),
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

    pub fn into_array_and_field_meta(self) -> Result<(Array, FieldMeta)> {
        let meta = FieldMeta {
            name: self.name,
            metadata: self.metadata,
            nullable: self.offsets.validity.is_some(),
        };
        let array = Array::List(ListArray {
            validity: self.offsets.validity,
            offsets: self.offsets.offsets,
            elements: Box::new(self.elements.into_array()?),
            meta: self.meta,
        });
        Ok((array, meta))
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

    pub fn into_array_and_field_meta(self) -> Result<(Array, FieldMeta)> {
        let meta = FieldMeta {
            name: self.name,
            metadata: self.metadata,
            nullable: self.offsets.validity.is_some(),
        };
        let array = Array::LargeList(ListArray {
            validity: self.offsets.validity,
            offsets: self.offsets.offsets,
            elements: Box::new(self.elements.into_array()?),
            meta: self.meta,
        });
        Ok((array, meta))
    }
}

impl<O: NamedType + Offset> ListBuilder<O> {
    fn start(&mut self) -> Result<()> {
        self.offsets.start_seq()
    }

    fn element<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        self.offsets.push_seq_elements(1)?;
        value.serialize(self.elements.as_mut())
    }

    fn end(&mut self) -> Result<()> {
        self.offsets.end_seq()
    }
}

impl<O: NamedType> Context for ListBuilder<O> {
    fn annotate(&self, annotations: &mut BTreeMap<String, String>) {
        set_default(annotations, "field", &self.name);
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

trait ListOffset: NamedType + Offset {
    fn as_serialize_seq(builder: &mut ListBuilder<Self>) -> super::utils::SerializeSeq<'_>;
    fn as_serialize_tuple(builder: &mut ListBuilder<Self>) -> super::utils::SerializeTuple<'_>;
}

impl ListOffset for i32 {
    fn as_serialize_seq(builder: &mut ListBuilder<Self>) -> super::utils::SerializeSeq<'_> {
        super::utils::SerializeSeq::List(builder)
    }

    fn as_serialize_tuple(builder: &mut ListBuilder<Self>) -> super::utils::SerializeTuple<'_> {
        super::utils::SerializeTuple::List(builder)
    }
}

impl ListOffset for i64 {
    fn as_serialize_seq(builder: &mut ListBuilder<Self>) -> super::utils::SerializeSeq<'_> {
        super::utils::SerializeSeq::LargeList(builder)
    }

    fn as_serialize_tuple(builder: &mut ListBuilder<Self>) -> super::utils::SerializeTuple<'_> {
        super::utils::SerializeTuple::LargeList(builder)
    }
}

impl<'a, O: ListOffset> serde::Serializer for &'a mut ListBuilder<O> {
    impl_serializer!(
        'a, ListBuilder;
        override serialize_none,
        override serialize_seq,
        override serialize_tuple,
        override serialize_bytes,
    );

    fn serialize_none(self) -> Result<()> {
        self.offsets.push_seq_none().ctx(self)
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq> {
        try_(|| {
            if let Some(len) = len {
                self.elements.reserve(len);
            }
            self.start()
        })
        .ctx(self)?;
        Ok(O::as_serialize_seq(self))
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple> {
        try_(|| {
            self.elements.reserve(len);
            self.start()
        })
        .ctx(self)?;
        Ok(O::as_serialize_tuple(self))
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<()> {
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

impl<O: ListOffset> serde::ser::SerializeSeq for &mut ListBuilder<O> {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<()> {
        try_(|| self.element(value)).ctx(*self)
    }

    fn end(self) -> Result<()> {
        try_(|| ListBuilder::end(self)).ctx(self)
    }
}

impl<O: ListOffset> serde::ser::SerializeTuple for &mut ListBuilder<O> {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<()> {
        try_(|| self.element(value)).ctx(*self)
    }

    fn end(self) -> Result<()> {
        try_(|| ListBuilder::end(self)).ctx(self)
    }
}
