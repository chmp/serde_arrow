use std::collections::BTreeMap;

use crate::internal::{
    arrow::{Array, FieldMeta, StructArray},
    error::{fail, set_default, try_, Context, ContextSupport, Result},
};

use super::{array_builder::ArrayBuilder, simple_serializer::SimpleSerializer};

#[derive(Debug, Clone)]
pub struct FlattenedUnionBuilder {
    pub path: String,
    pub fields: Vec<(ArrayBuilder, FieldMeta)>,
}

impl FlattenedUnionBuilder {
    pub fn new(path: String, fields: Vec<(ArrayBuilder, FieldMeta)>) -> Self {
        Self { path, fields }
    }

    pub fn take(&mut self) -> ArrayBuilder {
        ArrayBuilder::FlattenedUnion(Self {
            path: self.path.clone(),
            fields: self
                .fields
                .iter_mut()
                .map(|(field, meta)| (field.take(), meta.clone()))
                .collect(),
        })
    }

    pub fn is_nullable(&self) -> bool {
        false
    }

    pub fn into_array(self) -> Result<Array> {
        let mut fields = Vec::new();
        let mut num_elements = 0;

        for (builder, meta) in self.fields.into_iter() {
            let ArrayBuilder::Struct(builder) = builder else {
                fail!("enum variant not built as a struct") // TODO: better failure message
            };

            for (sub_builder, mut sub_meta) in builder.fields.into_iter() {
                num_elements += 1;
                // TODO: this mirrors the field name structure in the tracer but represents
                // implementation details crossing boundaries. Is there another way?
                // Currently necessary to allow struct field lookup to work correctly.
                sub_meta.name = format!("{}::{}", meta.name, sub_meta.name);
                fields.push((sub_builder.into_array()?, sub_meta));
            }
        }

        Ok(Array::Struct(StructArray {
            len: num_elements,
            validity: None, // TODO: is this ok?
            fields,
        }))
    }
}

impl FlattenedUnionBuilder {
    pub fn serialize_variant(&mut self, variant_index: u32) -> Result<&mut ArrayBuilder> {
        let variant_index = variant_index as usize;

        // don't serialize any variant not selected
        for (idx, (builder, _meta)) in self.fields.iter_mut().enumerate() {
            if idx != variant_index {
                builder.serialize_none()?;
            }
        }

        let Some((variant_builder, _variant_meta)) = self.fields.get_mut(variant_index) else {
            fail!("Could not find variant {variant_index} in Union");
        };

        Ok(variant_builder)
    }
}

impl Context for FlattenedUnionBuilder {
    fn annotate(&self, annotations: &mut BTreeMap<String, String>) {
        set_default(annotations, "field", &self.path);
        set_default(annotations, "data_type", "Struct(..)");
    }
}

impl SimpleSerializer for FlattenedUnionBuilder {
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
}

// TODO: add tests

// #[cfg(test)]
// mod tests {
//     fn test_serialize_union() {
//         #[derive(Serialize, Deserialize)]
//         enum Number {
//             Real { value: f32 },
//             Complex { i: f32, j: f32 },
//         }

//         let numbers = vec![];
//     }
// }
