use std::collections::{BTreeMap, HashMap};

use serde::Serialize;

use crate::internal::{
    arrow::{DataType, Field, TimeUnit},
    error::{fail, Context, ContextSupport, Result},
    schema::{get_strategy_from_metadata, SerdeArrowSchema, Strategy},
    serialization::{
        binary_builder::BinaryBuilder, duration_builder::DurationBuilder,
        fixed_size_binary_builder::FixedSizeBinaryBuilder,
        fixed_size_list_builder::FixedSizeListBuilder,
    },
    utils::{btree_map, meta_from_field, ChildName, Mut},
};

use super::{
    bool_builder::BoolBuilder, date32_builder::Date32Builder, date64_builder::Date64Builder,
    decimal_builder::DecimalBuilder, dictionary_utf8_builder::DictionaryUtf8Builder,
    float_builder::FloatBuilder, int_builder::IntBuilder, list_builder::ListBuilder,
    map_builder::MapBuilder, null_builder::NullBuilder, simple_serializer::SimpleSerializer,
    struct_builder::StructBuilder, time_builder::TimeBuilder, union_builder::UnionBuilder,
    unknown_variant_builder::UnknownVariantBuilder, utf8_builder::Utf8Builder, ArrayBuilder,
};

#[derive(Debug, Clone)]
pub struct OuterSequenceBuilder(StructBuilder);

impl OuterSequenceBuilder {
    pub fn new(schema: &SerdeArrowSchema) -> Result<Self> {
        Ok(Self(build_struct(
            String::from("$"),
            &schema.fields,
            false,
        )?))
    }

    /// Extract the contained struct fields
    pub fn take_records(&mut self) -> Result<Vec<ArrayBuilder>> {
        let mut result = Vec::new();
        for (builder, _) in self.0.take_self().fields {
            result.push(builder);
        }
        Ok(result)
    }

    /// Extend the builder with a sequence of items
    pub fn extend<T: Serialize>(&mut self, value: T) -> Result<()> {
        value.serialize(Mut(self))
    }

    /// Push a single item into the builder
    pub fn push<T: Serialize>(&mut self, value: T) -> Result<()> {
        self.element(&value)
    }
}

impl OuterSequenceBuilder {
    fn element<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        value.serialize(Mut(&mut self.0))
    }
}

impl Context for OuterSequenceBuilder {
    fn annotate(&self, annotations: &mut BTreeMap<String, String>) {
        self.0.annotate(annotations)
    }
}

impl SimpleSerializer for OuterSequenceBuilder {
    fn serialize_none(&mut self) -> Result<()> {
        self.0.serialize_none()
    }

    fn serialize_seq_start(&mut self, _: Option<usize>) -> Result<()> {
        Ok(())
    }

    fn serialize_seq_element<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        self.element(value)
    }

    fn serialize_seq_end(&mut self) -> Result<()> {
        Ok(())
    }

    fn serialize_tuple_start(&mut self, _: usize) -> Result<()> {
        Ok(())
    }

    fn serialize_tuple_element<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        self.element(value)
    }

    fn serialize_tuple_end(&mut self) -> Result<()> {
        Ok(())
    }

    fn serialize_tuple_struct_start(&mut self, _: &'static str, _: usize) -> Result<()> {
        Ok(())
    }

    fn serialize_tuple_struct_field<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        self.element(value)
    }

    fn serialize_tuple_struct_end(&mut self) -> Result<()> {
        Ok(())
    }
}

fn build_struct(path: String, struct_fields: &[Field], nullable: bool) -> Result<StructBuilder> {
    let mut fields = Vec::new();
    for field in struct_fields {
        let field_path = format!("{path}.{field_name}", field_name = field.name);
        fields.push((
            build_builder(field_path, field)?,
            meta_from_field(field.clone()),
        ));
    }
    StructBuilder::new(path, fields, nullable)
}

fn build_builder(path: String, field: &Field) -> Result<ArrayBuilder> {
    use {ArrayBuilder as A, DataType as T};
    let ctx: BTreeMap<String, String> = btree_map!("field" => path.clone());

    let builder = match &field.data_type {
        T::Null => match get_strategy_from_metadata(&field.metadata)? {
            Some(Strategy::UnknownVariant) => A::UnknownVariant(UnknownVariantBuilder::new(path)),
            _ => A::Null(NullBuilder::new(path)),
        },
        T::Boolean => A::Bool(BoolBuilder::new(path, field.nullable)),
        T::Int8 => A::I8(IntBuilder::new(path, field.nullable)),
        T::Int16 => A::I16(IntBuilder::new(path, field.nullable)),
        T::Int32 => A::I32(IntBuilder::new(path, field.nullable)),
        T::Int64 => A::I64(IntBuilder::new(path, field.nullable)),
        T::UInt8 => A::U8(IntBuilder::new(path, field.nullable)),
        T::UInt16 => A::U16(IntBuilder::new(path, field.nullable)),
        T::UInt32 => A::U32(IntBuilder::new(path, field.nullable)),
        T::UInt64 => A::U64(IntBuilder::new(path, field.nullable)),
        T::Float16 => A::F16(FloatBuilder::new(path, field.nullable)),
        T::Float32 => A::F32(FloatBuilder::new(path, field.nullable)),
        T::Float64 => A::F64(FloatBuilder::new(path, field.nullable)),
        T::Date32 => A::Date32(Date32Builder::new(path, field.nullable)),
        T::Date64 => A::Date64(Date64Builder::new(path, None, false, field.nullable)),
        T::Timestamp(unit, tz) => A::Date64(Date64Builder::new(
            path,
            Some((*unit, tz.clone())),
            is_utc_tz(tz.as_deref()).ctx(&ctx)?,
            field.nullable,
        )),
        T::Time32(unit) => {
            if !matches!(unit, TimeUnit::Second | TimeUnit::Millisecond) {
                fail!(in ctx, "Time32 only supports second or millisecond resolutions");
            }
            A::Time32(TimeBuilder::new(path, *unit, field.nullable))
        }
        T::Time64(unit) => {
            if !matches!(unit, TimeUnit::Nanosecond | TimeUnit::Microsecond) {
                fail!(in ctx, "Time64 only supports nanosecond or microsecond resolutions");
            }
            A::Time64(TimeBuilder::new(path, *unit, field.nullable))
        }
        T::Duration(unit) => A::Duration(DurationBuilder::new(path, *unit, field.nullable)),
        T::Decimal128(precision, scale) => A::Decimal128(DecimalBuilder::new(
            path,
            *precision,
            *scale,
            field.nullable,
        )),
        T::Utf8 => A::Utf8(Utf8Builder::new(path, field.nullable)),
        T::LargeUtf8 => A::LargeUtf8(Utf8Builder::new(path, field.nullable)),
        T::List(child) => {
            let child_path = format!("{path}.{child_name}", child_name = ChildName(&child.name));
            A::List(ListBuilder::new(
                path,
                meta_from_field(*child.clone()),
                build_builder(child_path, child.as_ref())?,
                field.nullable,
            ))
        }
        T::LargeList(child) => {
            let child_path = format!("{path}.{child_name}", child_name = ChildName(&child.name));
            A::LargeList(ListBuilder::new(
                path,
                meta_from_field(*child.clone()),
                build_builder(child_path, child.as_ref())?,
                field.nullable,
            ))
        }
        T::FixedSizeList(child, n) => {
            let child_path = format!("{path}.{child_name}", child_name = ChildName(&child.name));
            let n = usize::try_from(*n).ctx(&ctx)?;
            A::FixedSizedList(FixedSizeListBuilder::new(
                path,
                meta_from_field(*child.clone()),
                build_builder(child_path, child.as_ref())?,
                n,
                field.nullable,
            ))
        }
        T::Binary => A::Binary(BinaryBuilder::new(path, field.nullable)),
        T::LargeBinary => A::LargeBinary(BinaryBuilder::new(path, field.nullable)),
        T::FixedSizeBinary(n) => {
            let n = usize::try_from(*n).ctx(&ctx)?;
            A::FixedSizeBinary(FixedSizeBinaryBuilder::new(path, n, field.nullable))
        }
        T::Map(entry_field, _) => {
            let child_path = format!(
                "{path}.{child_name}",
                child_name = ChildName(&entry_field.name)
            );
            A::Map(
                MapBuilder::new(
                    path,
                    meta_from_field(*entry_field.clone()),
                    build_builder(child_path, entry_field.as_ref())?,
                    field.nullable,
                )
                .ctx(&ctx)?,
            )
        }
        T::Struct(children) => A::Struct(build_struct(path, children, field.nullable)?),
        T::Dictionary(key, value, _) => {
            let key_path = format!("{path}.key");
            let key_field = Field {
                name: "key".to_string(),
                data_type: *key.clone(),
                nullable: field.nullable,
                metadata: HashMap::new(),
            };

            let value_path = format!("{path}.value");
            let value_field = Field {
                name: "value".to_string(),
                data_type: *value.clone(),
                nullable: false,
                metadata: HashMap::new(),
            };

            A::DictionaryUtf8(DictionaryUtf8Builder::new(
                path,
                build_builder(key_path, &key_field)?,
                build_builder(value_path, &value_field)?,
            ))
        }
        T::Union(union_fields, _) => {
            let mut fields = Vec::new();
            for (idx, (type_id, field)) in union_fields.iter().enumerate() {
                if usize::try_from(*type_id) != Ok(idx) {
                    fail!(in ctx, "Union with non consecutive type ids are not supported");
                }
                let field_path =
                    format!("{path}.{field_name}", field_name = ChildName(&field.name));
                fields.push((
                    build_builder(field_path, field)?,
                    meta_from_field(field.clone()),
                ));
            }

            A::Union(UnionBuilder::new(path, fields))
        }
    };
    Ok(builder)
}

fn is_utc_tz(tz: Option<&str>) -> Result<bool> {
    match tz {
        None => Ok(false),
        Some(tz) if tz.to_uppercase() == "UTC" => Ok(true),
        Some(tz) => fail!("Timezone {tz} is not supported"),
    }
}
