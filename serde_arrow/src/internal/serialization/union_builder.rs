use crate::internal::{
    arrow::Array,
    error::{fail, Result},
    schema::GenericField,
    utils::Mut,
};

use super::{utils::SimpleSerializer, ArrayBuilder};

#[derive(Debug, Clone)]
pub struct UnionBuilder {
    pub field: GenericField,
    pub fields: Vec<ArrayBuilder>,
    pub types: Vec<i8>,
    pub offsets: Vec<i32>,
    pub current_offset: Vec<i32>,
}

impl UnionBuilder {
    pub fn new(field: GenericField, fields: Vec<ArrayBuilder>) -> Result<Self> {
        Ok(Self {
            field,
            current_offset: vec![0; fields.len()],
            types: Vec::new(),
            offsets: Vec::new(),
            fields,
        })
    }

    pub fn take(&mut self) -> Self {
        Self {
            field: self.field.clone(),
            fields: self.fields.iter_mut().map(|field| field.take()).collect(),
            types: std::mem::take(&mut self.types),
            offsets: std::mem::take(&mut self.offsets),
            current_offset: std::mem::replace(&mut self.current_offset, vec![0; self.fields.len()]),
        }
    }

    pub fn is_nullable(&self) -> bool {
        false
    }

    pub fn into_array(self) -> Array {
        unimplemented!()
    }
}

impl UnionBuilder {
    pub fn serialize_variant(&mut self, variant_index: u32) -> Result<&mut ArrayBuilder> {
        let variant_index = variant_index as usize;
        let Some(variant_builder) = self.fields.get_mut(variant_index) else {
            fail!("Unknown variant {variant_index}");
        };

        self.offsets.push(self.current_offset[variant_index]);
        self.types.push(i8::try_from(variant_index)?);
        self.current_offset[variant_index] += 1;

        Ok(variant_builder)
    }
}

impl SimpleSerializer for UnionBuilder {
    fn name(&self) -> &str {
        "UnionBuilder"
    }

    fn serialize_unit_variant(
        &mut self,
        _: &'static str,
        variant_index: u32,
        _: &'static str,
    ) -> Result<()> {
        self.serialize_variant(variant_index)?.serialize_unit()
    }

    fn serialize_newtype_variant<V: serde::Serialize + ?Sized>(
        &mut self,
        _: &'static str,
        variant_index: u32,
        _: &'static str,
        value: &V,
    ) -> Result<()> {
        let variant_builder = self.serialize_variant(variant_index)?;
        value.serialize(Mut(variant_builder))
    }

    fn serialize_struct_variant_start<'this>(
        &'this mut self,
        _: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<&'this mut ArrayBuilder> {
        let variant_builder = self.serialize_variant(variant_index)?;
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
        let variant_builder = self.serialize_variant(variant_index)?;
        variant_builder.serialize_tuple_struct_start(variant, len)?;
        Ok(variant_builder)
    }
}
