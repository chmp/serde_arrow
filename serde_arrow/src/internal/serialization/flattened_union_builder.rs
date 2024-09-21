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
        let num_fields = self.fields.len();

        for (builder, meta) in self.fields.into_iter() {
            let ArrayBuilder::Struct(builder) = builder else {
                fail!("enum variant not built as a struct") // TODO: better failure message
            };

            for (sub_builder, mut sub_meta) in builder.fields.into_iter() {
                // TODO: this mirrors the field name structure in the tracer but represents
                // implementation details crossing boundaries. Is there another way?
                // Name change is currently needed for struct field lookup to work correctly.

                sub_meta.name = format!("{}::{}", meta.name, sub_meta.name);
                fields.push((sub_builder.into_array()?, sub_meta));
            }
        }

        Ok(Array::Struct(StructArray {
            len: num_fields,
            // TODO: is this ok to hardcode?
            // assuming so because when testing manually,
            // validity of struct with nullable fields was None
            validity: None,
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::{
        internal::{
            array_builder::ArrayBuilder,
            arrow::{DataType, Field},
            serialization::{self, outer_sequence_builder::build_builder},
        },
        schema::SerdeArrowSchema,
        Serializer,
    };
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize)]
    struct Number {
        v: Value,
    }

    #[derive(Serialize, Deserialize)]
    enum Value {
        Real { value: f32 },
        Complex { i: f32, j: f32 },
        Whole { value: usize },
    }

    fn number_field() -> Field {
        Field {
            name: "v".to_string(),
            data_type: DataType::Struct(vec![
                Field {
                    name: "Complex::i".to_string(),
                    data_type: DataType::Float32,
                    nullable: true,
                    metadata: HashMap::new(),
                },
                Field {
                    name: "Complex::j".to_string(),
                    data_type: DataType::Float32,
                    nullable: true,
                    metadata: HashMap::new(),
                },
                Field {
                    name: "Real::value".to_string(),
                    data_type: DataType::Float32,
                    nullable: true,
                    metadata: HashMap::new(),
                },
                Field {
                    name: "Whole::value".to_string(),
                    data_type: DataType::UInt64,
                    nullable: true,
                    metadata: HashMap::new(),
                },
            ]),
            nullable: false,
            metadata: HashMap::from([(
                "SERDE_ARROW:strategy".to_string(),
                "EnumsWithNamedFieldsAsStructs".to_string(),
            )]),
        }
    }

    fn number_data() -> Vec<Number> {
        vec![
            Number {
                v: Value::Real { value: 0.0 },
            },
            Number {
                v: Value::Complex { i: 0.5, j: 0.5 },
            },
            Number {
                v: Value::Whole { value: 5 },
            },
        ]
    }

    #[test]
    fn test_build_flattened_union_builder() {
        let field = number_field();

        let array_builder =
            build_builder("$".to_string(), &field).expect("failed to build builder");

        let serialization::ArrayBuilder::FlattenedUnion(builder) = array_builder else {
            panic!("did not create correct builder");
        };

        // Should be 3 struct builders: one for Real, one for Complex, one for Whole
        assert_eq!(
            builder.fields.len(),
            3,
            "contained {} builder fields",
            builder.fields.len()
        );
        assert!(
            builder
                .fields
                .iter()
                .all(|(inner, _)| matches!(inner, serialization::ArrayBuilder::Struct(_))),
            "some inner builders were not Struct builders"
        );
    }

    #[test]
    fn test_serialize_flattened_union_builder() {
        let field = number_field();
        let data = number_data();
        let schema = SerdeArrowSchema {
            fields: vec![field],
        };

        let api_builder = ArrayBuilder::new(schema).expect("failed to create api array builder");
        let serializer = Serializer::new(api_builder);
        data.serialize(serializer)
            .expect("failed to serialize")
            .into_inner()
            .to_arrow()
            .expect("failed to serialize to arrow");
    }

    #[test]
    fn test_record_batch_flattened_union_builder() {
        let field = number_field();
        let data = number_data();
        let schema = SerdeArrowSchema {
            fields: vec![field],
        };

        let api_builder = ArrayBuilder::new(schema).expect("failed to create api array builder");
        let serializer = Serializer::new(api_builder);
        data.serialize(serializer)
            .expect("failed to serialize")
            .into_inner()
            .to_record_batch()
            .expect("failed to create record batch");
    }
}
