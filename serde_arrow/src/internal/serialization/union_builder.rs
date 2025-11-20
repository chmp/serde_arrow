use std::collections::{BTreeMap, HashMap};

use marrow::{
    array::{Array, UnionArray},
    datatypes::FieldMeta,
};

use crate::internal::{
    error::{fail, set_default, try_, Context, ContextSupport, Result},
    serialization::utils::impl_serializer,
};

use super::array_builder::ArrayBuilder;

#[derive(Debug, Clone)]
pub struct UnionBuilder {
    pub name: String,
    pub fields: Vec<ArrayBuilder>,
    pub types: Vec<i8>,
    pub offsets: Vec<i32>,
    pub current_offset: Vec<i32>,
    metadata: HashMap<String, String>,
}

impl UnionBuilder {
    pub fn new(name: String, fields: Vec<ArrayBuilder>, metadata: HashMap<String, String>) -> Self {
        Self {
            name,
            current_offset: vec![0; fields.len()],
            types: Vec::new(),
            offsets: Vec::new(),
            fields,
            metadata,
        }
    }

    pub fn take(&mut self) -> ArrayBuilder {
        ArrayBuilder::Union(Self {
            name: self.name.clone(),
            metadata: self.metadata.clone(),
            fields: self.fields.iter_mut().map(|field| field.take()).collect(),
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
        for (idx, builder) in self.fields.into_iter().enumerate() {
            let (child_array, child_meta) = builder.into_array_and_field_meta()?;
            fields.push((idx.try_into()?, child_meta, child_array));
        }

        Ok(Array::Union(UnionArray {
            types: self.types,
            offsets: Some(self.offsets),
            fields,
        }))
    }

    pub fn into_array_and_field_meta(self) -> Result<(Array, FieldMeta)> {
        let meta = FieldMeta {
            name: self.name,
            metadata: self.metadata,
            nullable: false,
        };

        let mut fields = Vec::new();
        for (idx, builder) in self.fields.into_iter().enumerate() {
            let (child_array, child_meta) = builder.into_array_and_field_meta()?;
            fields.push((idx.try_into()?, child_meta, child_array));
        }

        let array = Array::Union(UnionArray {
            types: self.types,
            offsets: Some(self.offsets),
            fields,
        });

        Ok((array, meta))
    }

    pub fn reserve(&mut self, _additional: usize) {
        // TODO: figure out what to do here
    }

    pub fn serialize_default_value(&mut self) -> Result<()> {
        let mut ctx = BTreeMap::new();
        self.annotate(&mut ctx);

        try_(|| self.serialize_variant(0)?.serialize_default_value()).ctx(&ctx)
    }
}

impl UnionBuilder {
    pub fn serialize_variant(&mut self, variant_index: u32) -> Result<&mut ArrayBuilder> {
        let variant_index = variant_index as usize;
        let Some(variant_builder) = self.fields.get_mut(variant_index) else {
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
        set_default(annotations, "field", &self.name);
        set_default(annotations, "data_type", "Union(..)");
    }
}

impl<'a> serde::Serializer for &'a mut UnionBuilder {
    impl_serializer!(
        'a, UnionBuilder;
        override serialize_unit_variant,
        override serialize_newtype_variant,
        override serialize_struct_variant,
        override serialize_tuple_variant,
    );

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        variant_index: u32,
        _variant: &'static str,
    ) -> Result<()> {
        let mut ctx = BTreeMap::new();
        self.annotate(&mut ctx);

        try_(|| serde::Serializer::serialize_unit(self.serialize_variant(variant_index)?)).ctx(&ctx)
    }

    fn serialize_newtype_variant<T: ?Sized + serde::Serialize>(
        self,
        _name: &'static str,
        variant_index: u32,
        _variant: &'static str,
        value: &T,
    ) -> Result<()> {
        let mut ctx = BTreeMap::new();
        self.annotate(&mut ctx);

        try_(|| {
            let variant_builder = self.serialize_variant(variant_index)?;
            value.serialize(variant_builder)
        })
        .ctx(&ctx)
    }

    fn serialize_struct_variant(
        self,
        name: &'static str,
        variant_index: u32,
        _variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        self.serialize_variant(variant_index)?
            .serialize_struct(name, len)
    }

    fn serialize_tuple_variant(
        self,
        name: &'static str,
        variant_index: u32,
        _variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        self.serialize_variant(variant_index)?
            .serialize_tuple_struct(name, len)
    }
}
