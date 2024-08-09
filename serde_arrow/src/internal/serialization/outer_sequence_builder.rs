use std::collections::HashMap;

use serde::Serialize;

use crate::internal::{
    arrow::{DataType, Field, TimeUnit},
    error::{fail, Result},
    schema::{get_strategy_from_metadata, SerdeArrowSchema, Strategy},
    serialization::{
        binary_builder::BinaryBuilder, duration_builder::DurationBuilder,
        fixed_size_binary_builder::FixedSizeBinaryBuilder,
        fixed_size_list_builder::FixedSizeListBuilder,
    },
    utils::{meta_from_field, Mut},
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
        return Ok(Self(build_struct(&schema.fields, false)?));

        fn build_struct(struct_fields: &[Field], nullable: bool) -> Result<StructBuilder> {
            let mut fields = Vec::new();
            for field in struct_fields {
                fields.push((build_builder(field)?, meta_from_field(field.clone())?));
            }
            StructBuilder::new(fields, nullable)
        }

        fn build_builder(field: &Field) -> Result<ArrayBuilder> {
            use {ArrayBuilder as A, DataType as T};

            let builder = match &field.data_type {
                T::Null => match get_strategy_from_metadata(&field.metadata)? {
                    Some(Strategy::UnknownVariant) => A::UnknownVariant(UnknownVariantBuilder),
                    _ => A::Null(NullBuilder::new()),
                },
                T::Boolean => A::Bool(BoolBuilder::new(field.nullable)),
                T::Int8 => A::I8(IntBuilder::new(field.nullable)),
                T::Int16 => A::I16(IntBuilder::new(field.nullable)),
                T::Int32 => A::I32(IntBuilder::new(field.nullable)),
                T::Int64 => A::I64(IntBuilder::new(field.nullable)),
                T::UInt8 => A::U8(IntBuilder::new(field.nullable)),
                T::UInt16 => A::U16(IntBuilder::new(field.nullable)),
                T::UInt32 => A::U32(IntBuilder::new(field.nullable)),
                T::UInt64 => A::U64(IntBuilder::new(field.nullable)),
                T::Float16 => A::F16(FloatBuilder::new(field.nullable)),
                T::Float32 => A::F32(FloatBuilder::new(field.nullable)),
                T::Float64 => A::F64(FloatBuilder::new(field.nullable)),
                T::Date32 => A::Date32(Date32Builder::new(field.nullable)),
                T::Date64 => A::Date64(Date64Builder::new(
                    None,
                    is_utc_strategy(get_strategy_from_metadata(&field.metadata)?.as_ref())?,
                    field.nullable,
                )),
                T::Timestamp(unit, tz) => A::Date64(Date64Builder::new(
                    Some((*unit, tz.clone())),
                    is_utc_tz(tz.as_deref())?,
                    field.nullable,
                )),
                T::Time32(unit) => {
                    if !matches!(unit, TimeUnit::Second | TimeUnit::Millisecond) {
                        fail!("Only timestamps with second or millisecond unit are supported");
                    }
                    A::Time32(TimeBuilder::new(*unit, field.nullable))
                }
                T::Time64(unit) => {
                    if !matches!(unit, TimeUnit::Nanosecond | TimeUnit::Microsecond) {
                        fail!("Only timestamps with nanosecond or microsecond unit are supported");
                    }
                    A::Time64(TimeBuilder::new(*unit, field.nullable))
                }
                T::Duration(unit) => A::Duration(DurationBuilder::new(*unit, field.nullable)),
                T::Decimal128(precision, scale) => {
                    A::Decimal128(DecimalBuilder::new(*precision, *scale, field.nullable))
                }
                T::Utf8 => A::Utf8(Utf8Builder::new(field.nullable)),
                T::LargeUtf8 => A::LargeUtf8(Utf8Builder::new(field.nullable)),
                T::List(child) => A::List(ListBuilder::new(
                    meta_from_field(*child.clone())?,
                    build_builder(child.as_ref())?,
                    field.nullable,
                )?),
                T::LargeList(child) => A::LargeList(ListBuilder::new(
                    meta_from_field(*child.clone())?,
                    build_builder(child.as_ref())?,
                    field.nullable,
                )?),
                T::FixedSizeList(child, n) => A::FixedSizedList(FixedSizeListBuilder::new(
                    meta_from_field(*child.clone())?,
                    build_builder(child.as_ref())?,
                    (*n).try_into()?,
                    field.nullable,
                )),
                T::Binary => A::Binary(BinaryBuilder::new(field.nullable)),
                T::LargeBinary => A::LargeBinary(BinaryBuilder::new(field.nullable)),
                T::FixedSizeBinary(n) => A::FixedSizeBinary(FixedSizeBinaryBuilder::new(
                    (*n).try_into()?,
                    field.nullable,
                )),
                T::Map(entry_field, _) => A::Map(MapBuilder::new(
                    meta_from_field(*entry_field.clone())?,
                    build_builder(entry_field.as_ref())?,
                    field.nullable,
                )?),
                T::Struct(children) => A::Struct(build_struct(children, field.nullable)?),
                T::Dictionary(key, value, _) => {
                    let key_field = Field {
                        name: "key".to_string(),
                        data_type: *key.clone(),
                        nullable: field.nullable,
                        metadata: HashMap::new(),
                    };
                    let value_field = Field {
                        name: "value".to_string(),
                        data_type: *value.clone(),
                        nullable: false,
                        metadata: HashMap::new(),
                    };

                    A::DictionaryUtf8(DictionaryUtf8Builder::new(
                        build_builder(&key_field)?,
                        build_builder(&value_field)?,
                    ))
                }
                T::Union(union_fields, _) => {
                    let mut fields = Vec::new();
                    for (idx, (type_id, field)) in union_fields.iter().enumerate() {
                        if usize::try_from(*type_id) != Ok(idx) {
                            fail!("non consecutive type ids are not supported");
                        }
                        fields.push((build_builder(field)?, meta_from_field(field.clone())?));
                    }

                    A::Union(UnionBuilder::new(fields))
                }
            };
            Ok(builder)
        }
    }

    /// Extract the contained struct fields
    pub fn take_records(&mut self) -> Result<Vec<ArrayBuilder>> {
        let mut result = Vec::new();
        for (builder, _) in self.0.take().fields {
            result.push(builder);
        }
        Ok(result)
    }

    /// Extend the builder with a sequence of items
    pub fn extend<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        value.serialize(Mut(self))
    }

    /// Push a single item into the builder
    pub fn push<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        self.element(value)
    }
}

impl OuterSequenceBuilder {
    fn element<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        value.serialize(Mut(&mut self.0))
    }
}

impl SimpleSerializer for OuterSequenceBuilder {
    fn name(&self) -> &str {
        "OuterSequenceBuilder"
    }

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

fn is_utc_tz(tz: Option<&str>) -> Result<bool> {
    match tz {
        None => Ok(false),
        Some(tz) if tz.to_uppercase() == "UTC" => Ok(true),
        Some(tz) => fail!("Timezone {tz} is not supported"),
    }
}

fn is_utc_strategy(strategy: Option<&Strategy>) -> Result<bool> {
    match strategy {
        Some(Strategy::UtcStrAsDate64) | None => Ok(true),
        Some(Strategy::NaiveStrAsDate64) => Ok(false),
        Some(st) => fail!("Cannot builder Date64 builder with strategy {st}"),
    }
}
