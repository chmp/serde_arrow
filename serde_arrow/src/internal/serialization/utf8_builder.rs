use std::collections::BTreeMap;

use marrow::array::{Array, BytesArray, BytesViewArray};

use crate::internal::{
    error::{fail, set_default, try_, Context, ContextSupport, Result},
    utils::array_ext::{new_bytes_array, ArrayExt, ScalarArrayExt},
};

use super::{array_builder::ArrayBuilder, simple_serializer::SimpleSerializer};

pub trait Utf8BuilderArray:
    ArrayExt + for<'s> ScalarArrayExt<'s, Value = &'s [u8]> + Sized
{
    const DATA_TYPE_NAME: &'static str;

    fn new(is_nullable: bool) -> Self;
    fn is_nullable(&self) -> bool;
}

impl Utf8BuilderArray for BytesArray<i32> {
    const DATA_TYPE_NAME: &'static str = "Utf8";

    fn new(is_nullable: bool) -> Self {
        new_bytes_array(is_nullable)
    }

    fn is_nullable(&self) -> bool {
        self.validity.is_some()
    }
}

impl Utf8BuilderArray for BytesArray<i64> {
    const DATA_TYPE_NAME: &'static str = "LargeUtf8";

    fn new(is_nullable: bool) -> Self {
        new_bytes_array(is_nullable)
    }

    fn is_nullable(&self) -> bool {
        self.validity.is_some()
    }
}

impl Utf8BuilderArray for BytesViewArray {
    const DATA_TYPE_NAME: &'static str = "Utf8View";

    fn new(is_nullable: bool) -> Self {
        BytesViewArray {
            validity: is_nullable.then(Vec::new),
            data: Vec::new(),
            buffers: vec![vec![]],
        }
    }

    fn is_nullable(&self) -> bool {
        self.validity.is_some()
    }
}

#[derive(Debug, Clone)]
pub struct Utf8Builder<A> {
    path: String,
    array: A,
}

impl<A: Utf8BuilderArray> Utf8Builder<A> {
    pub fn new(path: String, is_nullable: bool) -> Self {
        Self {
            path,
            array: A::new(is_nullable),
        }
    }

    pub fn take_self(&mut self) -> Self {
        Self {
            path: self.path.clone(),
            array: self.array.take(),
        }
    }

    pub fn is_nullable(&self) -> bool {
        self.array.is_nullable()
    }
}

impl Utf8Builder<BytesArray<i32>> {
    pub fn take(&mut self) -> ArrayBuilder {
        ArrayBuilder::Utf8(self.take_self())
    }

    pub fn into_array(self) -> Result<Array> {
        Ok(Array::Utf8(self.array))
    }
}

impl Utf8Builder<BytesArray<i64>> {
    pub fn take(&mut self) -> ArrayBuilder {
        ArrayBuilder::LargeUtf8(self.take_self())
    }

    pub fn into_array(self) -> Result<Array> {
        Ok(Array::LargeUtf8(self.array))
    }
}

impl Utf8Builder<BytesViewArray> {
    pub fn take(&mut self) -> ArrayBuilder {
        ArrayBuilder::Utf8View(self.take_self())
    }

    pub fn into_array(self) -> Result<Array> {
        Ok(Array::Utf8View(self.array))
    }
}

impl<A: Utf8BuilderArray> Context for Utf8Builder<A> {
    fn annotate(&self, annotations: &mut BTreeMap<String, String>) {
        set_default(annotations, "field", &self.path);
        set_default(annotations, "data_type", A::DATA_TYPE_NAME);
    }
}

impl<A: Utf8BuilderArray> SimpleSerializer for Utf8Builder<A> {
    fn serialize_default(&mut self) -> Result<()> {
        try_(|| self.array.push_scalar_default()).ctx(self)
    }

    fn serialize_none(&mut self) -> Result<()> {
        try_(|| self.array.push_scalar_none()).ctx(self)
    }

    fn serialize_str(&mut self, v: &str) -> Result<()> {
        try_(|| self.array.push_scalar_value(v.as_bytes())).ctx(self)
    }

    fn serialize_unit_variant(
        &mut self,
        _: &'static str,
        _: u32,
        variant: &'static str,
    ) -> Result<()> {
        try_(|| self.array.push_scalar_value(variant.as_bytes())).ctx(self)
    }

    fn serialize_tuple_variant_start<'this>(
        &'this mut self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<&'this mut super::ArrayBuilder> {
        fail!(in self, "Cannot serialize enum with data as string");
    }

    fn serialize_struct_variant_start<'this>(
        &'this mut self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<&'this mut super::ArrayBuilder> {
        fail!(in self, "Cannot serialize enum with data as string");
    }

    fn serialize_newtype_variant<V: serde::Serialize + ?Sized>(
        &mut self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: &V,
    ) -> Result<()> {
        fail!(in self, "Cannot serialize enum with data as string");
    }
}
