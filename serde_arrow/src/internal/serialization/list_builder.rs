use std::collections::{BTreeMap, HashMap};

use marrow::{
    array::{Array, ListArray},
    datatypes::FieldMeta,
};
use serde::Serialize;

use crate::internal::{
    error::{prepend, set_default, try_, Context, ContextSupport, Error, Result},
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
    pub elements: Box<ArrayBuilder>,
    pub offsets: OffsetsArray<O>,
    pub metadata: HashMap<String, String>,
}

impl<O: Offset + NamedType> ListBuilder<O> {
    pub fn new(
        name: String,
        element: ArrayBuilder,
        is_nullable: bool,
        metadata: HashMap<String, String>,
    ) -> Self {
        Self {
            name,
            elements: Box::new(element),
            offsets: OffsetsArray::new(is_nullable),
            metadata,
        }
    }

    pub fn take_self(&mut self) -> Self {
        Self {
            name: self.name.clone(),
            metadata: self.metadata.clone(),
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

    pub fn into_array_and_field_meta(self) -> Result<(Array, FieldMeta)> {
        let meta = FieldMeta {
            name: self.name,
            metadata: self.metadata,
            nullable: self.offsets.validity.is_some(),
        };
        let (child_array, child_meta) = self.elements.into_array_and_field_meta()?;
        let array = Array::List(ListArray {
            validity: self.offsets.validity,
            offsets: self.offsets.offsets,
            elements: Box::new(child_array),
            meta: child_meta,
        });
        Ok((array, meta))
    }
}

impl ListBuilder<i64> {
    pub fn take(&mut self) -> ArrayBuilder {
        ArrayBuilder::LargeList(self.take_self())
    }

    pub fn into_array_and_field_meta(self) -> Result<(Array, FieldMeta)> {
        let meta = FieldMeta {
            name: self.name,
            metadata: self.metadata,
            nullable: self.offsets.validity.is_some(),
        };
        let (child_array, child_meta) = self.elements.into_array_and_field_meta()?;
        let array = Array::LargeList(ListArray {
            validity: self.offsets.validity,
            offsets: self.offsets.offsets,
            elements: Box::new(child_array),
            meta: child_meta,
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
        prepend(annotations, "field", &self.name);
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
        if let Some(len) = len {
            self.elements.reserve(len);
        }
        self.start().ctx(self)?;
        Ok(O::as_serialize_seq(self))
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple> {
        self.elements.reserve(len);
        self.start().ctx(self)?;
        Ok(O::as_serialize_tuple(self))
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<()> {
        self.elements.reserve(v.len());
        self.start().ctx(self)?;
        for item in v {
            self.element(item).ctx(self)?;
        }
        self.end().ctx(self)
    }
}

impl<O: ListOffset> serde::ser::SerializeSeq for &mut ListBuilder<O> {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<()> {
        self.element(value).ctx(*self)
    }

    fn end(self) -> Result<()> {
        ListBuilder::end(self).ctx(self)
    }
}

impl<O: ListOffset> serde::ser::SerializeTuple for &mut ListBuilder<O> {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<()> {
        self.element(value).ctx(*self)
    }

    fn end(self) -> Result<()> {
        ListBuilder::end(self).ctx(self)
    }
}
