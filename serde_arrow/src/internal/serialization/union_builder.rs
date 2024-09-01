use std::collections::BTreeMap;

use crate::internal::{
    arrow::{Array, DenseUnionArray, FieldMeta},
    error::{fail, Context, ContextSupport, Result},
    utils::{btree_map, Mut},
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
            fields.push((idx.try_into()?, builder.into_array()?, meta));
        }

        Ok(Array::DenseUnion(DenseUnionArray {
            types: self.types,
            offsets: self.offsets,
            fields,
        }))
    }
}

impl UnionBuilder {
    pub fn serialize_variant(&mut self, variant_index: u32) -> Result<&mut ArrayBuilder> {
        let variant_index = variant_index as usize;
        let Some((variant_builder, _)) = self.fields.get_mut(variant_index) else {
            fail!("Unknown variant {variant_index}");
        };

        self.offsets.push(self.current_offset[variant_index]);
        self.types.push(i8::try_from(variant_index)?);
        self.current_offset[variant_index] += 1;

        Ok(variant_builder)
    }
}

impl Context for UnionBuilder {
    fn annotations(&self) -> BTreeMap<String, String> {
        btree_map!("field" => self.path.clone())
    }
}

impl SimpleSerializer for UnionBuilder {
    fn serialize_unit_variant(
        &mut self,
        _: &'static str,
        variant_index: u32,
        _: &'static str,
    ) -> Result<()> {
        let ctx = self.annotations();
        self.serialize_variant(variant_index)
            .ctx(&ctx)?
            .serialize_unit()
    }

    fn serialize_newtype_variant<V: serde::Serialize + ?Sized>(
        &mut self,
        _: &'static str,
        variant_index: u32,
        _: &'static str,
        value: &V,
    ) -> Result<()> {
        let ctx = self.annotations();
        let variant_builder = self.serialize_variant(variant_index).ctx(&ctx)?;
        value.serialize(Mut(variant_builder))
    }

    fn serialize_struct_variant_start<'this>(
        &'this mut self,
        _: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<&'this mut ArrayBuilder> {
        let ctx = self.annotations();
        let variant_builder = self.serialize_variant(variant_index).ctx(&ctx)?;
        variant_builder.serialize_struct_start(variant, len)?;
        Ok(variant_builder)
    }

    fn serialize_tuple_variant_start<'this>(
        &'this mut self,
        _: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<&'this mut ArrayBuilder> {
        let ctx = self.annotations();
        let variant_builder = self.serialize_variant(variant_index).ctx(&ctx)?;
        variant_builder.serialize_tuple_struct_start(variant, len)?;
        Ok(variant_builder)
    }
}
