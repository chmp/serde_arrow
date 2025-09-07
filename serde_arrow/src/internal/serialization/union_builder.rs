use std::collections::BTreeMap;

use marrow::{
    array::{Array, UnionArray},
    datatypes::FieldMeta,
};

use crate::internal::{
    error::{fail, set_default, try_, Context, ContextSupport, Result},
    utils::Mut,
};

use super::{array_builder::ArrayBuilder, simple_serializer::SimpleSerializer};

#[derive(Debug, Clone)]
pub struct UnionBuilder {
    pub path: String,
    pub fields: Vec<(ArrayBuilder, FieldMeta)>,
    pub types: Vec<i8>,
    pub offsets: Vec<i32>,
    pub current_offset: Vec<i32>,
}

impl UnionBuilder {
    pub fn new(path: String, fields: Vec<(ArrayBuilder, FieldMeta)>) -> Self {
        Self {
            path,
            current_offset: vec![0; fields.len()],
            types: Vec::new(),
            offsets: Vec::new(),
            fields,
        }
    }

    pub fn take(&mut self) -> ArrayBuilder {
        ArrayBuilder::Union(Self {
            path: self.path.clone(),
            fields: self
                .fields
                .iter_mut()
                .map(|(field, meta)| (field.take(), meta.clone()))
                .collect(),
            types: std::mem::take(&mut self.types),
            offsets: std::mem::take(&mut self.offsets),
            current_offset: std::mem::replace(&mut self.current_offset, vec![0; self.fields.len()]),
        })
    }

    pub fn is_nullable(&self) -> bool {
        false
    }

    pub fn into_array(self) -> Result<Array> {
        let mut fields = Vec::new();
        for (idx, (builder, meta)) in self.fields.into_iter().enumerate() {
            fields.push((idx.try_into()?, meta, builder.into_array()?));
        }

        Ok(Array::Union(UnionArray {
            types: self.types,
            offsets: Some(self.offsets),
            fields,
        }))
    }

    pub fn reserve(&mut self, _additional: usize) {
        // TODO: figure out what to do here
    }
}

impl UnionBuilder {
    pub fn serialize_variant(&mut self, variant_index: u32) -> Result<&mut ArrayBuilder> {
        let variant_index = variant_index as usize;
        let Some((variant_builder, _)) = self.fields.get_mut(variant_index) else {
            fail!("Could not find variant {variant_index} in Union");
        };

        self.offsets.push(self.current_offset[variant_index]);
        self.types.push(i8::try_from(variant_index)?);
        self.current_offset[variant_index] += 1;

        Ok(variant_builder)
    }
}

impl Context for UnionBuilder {
    fn annotate(&self, annotations: &mut BTreeMap<String, String>) {
        set_default(annotations, "field", &self.path);
        set_default(annotations, "data_type", "Union(..)");
    }
}

impl SimpleSerializer for UnionBuilder {
    fn serialize_default(&mut self) -> Result<()> {
        let mut ctx = BTreeMap::new();
        self.annotate(&mut ctx);

        try_(|| self.serialize_variant(0)?.serialize_default()).ctx(&ctx)
    }

    fn serialize_unit_variant(
        &mut self,
        _: &'static str,
        variant_index: u32,
        _: &'static str,
    ) -> Result<()> {
        let mut ctx = BTreeMap::new();
        self.annotate(&mut ctx);

        try_(|| self.serialize_variant(variant_index)?.serialize_unit()).ctx(&ctx)
    }

    fn serialize_newtype_variant<V: serde::Serialize + ?Sized>(
        &mut self,
        _: &'static str,
        variant_index: u32,
        _: &'static str,
        value: &V,
    ) -> Result<()> {
        let mut ctx = BTreeMap::new();
        self.annotate(&mut ctx);

        try_(|| {
            let variant_builder = self.serialize_variant(variant_index)?;
            value.serialize(Mut(variant_builder))
        })
        .ctx(&ctx)
    }

    fn serialize_struct_variant_start<'this>(
        &'this mut self,
        _: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<&'this mut ArrayBuilder> {
        let mut ctx = BTreeMap::new();
        self.annotate(&mut ctx);

        try_(|| {
            let variant_builder = self.serialize_variant(variant_index)?;
            variant_builder.serialize_struct_start(variant, len)?;
            Ok(variant_builder)
        })
        .ctx(&ctx)
    }

    fn serialize_tuple_variant_start<'this>(
        &'this mut self,
        _: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<&'this mut ArrayBuilder> {
        let mut ctx = BTreeMap::new();
        self.annotate(&mut ctx);

        try_(|| {
            let variant_builder = self.serialize_variant(variant_index)?;
            variant_builder.serialize_tuple_struct_start(variant, len)?;
            Ok(variant_builder)
        })
        .ctx(&ctx)
    }
}
