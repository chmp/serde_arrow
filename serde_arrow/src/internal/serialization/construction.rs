use std::collections::HashMap;

use marrow::datatypes::{DataType, Field, TimeUnit};

use crate::internal::{
    error::{fail, Result},
    schema::{get_strategy_from_metadata, Strategy},
    serialization::{
        binary_builder::BinaryBuilder, duration_builder::DurationBuilder,
        fixed_size_binary_builder::FixedSizeBinaryBuilder,
        fixed_size_list_builder::FixedSizeListBuilder,
    },
};

use super::{
    bool_builder::BoolBuilder, date_builder::DateBuilder, decimal_builder::DecimalBuilder,
    dictionary_utf8_builder::DictionaryUtf8Builder, float_builder::FloatBuilder,
    int_builder::IntBuilder, list_builder::ListBuilder, map_builder::MapBuilder,
    null_builder::NullBuilder, struct_builder::StructBuilder, time_builder::TimeBuilder,
    timestamp_builder::TimestampBuilder, union_builder::UnionBuilder,
    unknown_variant_builder::UnknownVariantBuilder, utf8_builder::Utf8Builder, ArrayBuilder,
};

pub fn build_struct(
    path: String,
    struct_fields: Vec<Field>,
    nullable: bool,
    metadata: HashMap<String, String>,
) -> Result<StructBuilder> {
    let mut fields = Vec::new();
    for field in struct_fields {
        fields.push(build_builder(
            field.name,
            field.data_type,
            field.nullable,
            field.metadata,
        )?);
    }
    StructBuilder::new(path, fields, nullable, metadata)
}

fn build_builder(
    name: String,
    data_type: DataType,
    nullable: bool,
    metadata: HashMap<String, String>,
) -> Result<ArrayBuilder> {
    use {ArrayBuilder as A, DataType as T};

    let builder = match data_type {
        T::Null => match get_strategy_from_metadata(&metadata)? {
            Some(Strategy::UnknownVariant) => {
                A::UnknownVariant(UnknownVariantBuilder::new(name, metadata))
            }
            _ => A::Null(NullBuilder::new(name, metadata)),
        },
        T::Boolean => A::Bool(BoolBuilder::new(name, nullable, metadata)),
        T::Int8 => A::I8(IntBuilder::new(name, nullable, metadata)),
        T::Int16 => A::I16(IntBuilder::new(name, nullable, metadata)),
        T::Int32 => A::I32(IntBuilder::new(name, nullable, metadata)),
        T::Int64 => A::I64(IntBuilder::new(name, nullable, metadata)),
        T::UInt8 => A::U8(IntBuilder::new(name, nullable, metadata)),
        T::UInt16 => A::U16(IntBuilder::new(name, nullable, metadata)),
        T::UInt32 => A::U32(IntBuilder::new(name, nullable, metadata)),
        T::UInt64 => A::U64(IntBuilder::new(name, nullable, metadata)),
        T::Float16 => A::F16(FloatBuilder::new(name, nullable, metadata)),
        T::Float32 => A::F32(FloatBuilder::new(name, nullable, metadata)),
        T::Float64 => A::F64(FloatBuilder::new(name, nullable, metadata)),
        T::Date32 => A::Date32(DateBuilder::new(name, nullable, metadata)),
        T::Date64 => A::Date64(DateBuilder::new(name, nullable, metadata)),
        T::Timestamp(unit, tz) => {
            A::Timestamp(TimestampBuilder::new(name, unit, tz, nullable, metadata)?)
        }
        T::Time32(unit) => {
            if !matches!(unit, TimeUnit::Second | TimeUnit::Millisecond) {
                fail!("Time32 only supports second or millisecond resolutions");
            }
            A::Time32(TimeBuilder::new(name, unit, nullable, metadata))
        }
        T::Time64(unit) => {
            if !matches!(unit, TimeUnit::Nanosecond | TimeUnit::Microsecond) {
                fail!("Time64 only supports nanosecond or microsecond resolutions");
            }
            A::Time64(TimeBuilder::new(name, unit, nullable, metadata))
        }
        T::Duration(unit) => A::Duration(DurationBuilder::new(name, unit, nullable, metadata)),
        T::Decimal128(precision, scale) => A::Decimal128(DecimalBuilder::new(
            name, precision, scale, nullable, metadata,
        )),
        T::Utf8 => A::Utf8(Utf8Builder::new(name, nullable, metadata)),
        T::LargeUtf8 => A::LargeUtf8(Utf8Builder::new(name, nullable, metadata)),
        T::Utf8View => A::Utf8View(Utf8Builder::new(name, nullable, metadata)),
        T::List(child) => A::List(ListBuilder::new(
            name,
            build_builder(child.name, child.data_type, child.nullable, child.metadata)?,
            nullable,
            metadata,
        )),
        T::LargeList(child) => A::LargeList(ListBuilder::new(
            name,
            build_builder(child.name, child.data_type, child.nullable, child.metadata)?,
            nullable,
            metadata,
        )),
        T::FixedSizeList(child, n) => {
            let n = usize::try_from(n)?;
            A::FixedSizedList(FixedSizeListBuilder::new(
                name,
                build_builder(child.name, child.data_type, child.nullable, child.metadata)?,
                n,
                nullable,
                metadata,
            ))
        }
        T::Binary => A::Binary(BinaryBuilder::new(name, nullable, metadata)),
        T::LargeBinary => A::LargeBinary(BinaryBuilder::new(name, nullable, metadata)),
        T::BinaryView => A::BinaryView(BinaryBuilder::new(name, nullable, metadata)),
        T::FixedSizeBinary(n) => {
            let n = usize::try_from(n)?;
            A::FixedSizeBinary(FixedSizeBinaryBuilder::new(name, n, nullable, metadata))
        }
        T::Map(entry_field, sorted) => {
            let DataType::Struct(entries_field) = entry_field.data_type else {
                fail!(
                    "Unexpected data type for map array: Expected Struct, got {:?}",
                    entry_field.data_type
                );
            };
            let Ok([keys_field, values_field]) = <[Field; 2]>::try_from(entries_field) else {
                fail!("A map field must be a struct with exactly two fields");
            };
            if sorted {
                fail!("Sorted maps are not supported");
            }

            A::Map(MapBuilder::new(
                name,
                entry_field.name,
                sorted,
                build_builder(
                    keys_field.name,
                    keys_field.data_type,
                    keys_field.nullable,
                    keys_field.metadata,
                )?,
                build_builder(
                    values_field.name,
                    values_field.data_type,
                    values_field.nullable,
                    values_field.metadata,
                )?,
                nullable,
                metadata,
            )?)
        }
        T::Struct(children) => A::Struct(build_struct(name, children, nullable, metadata)?),
        T::Dictionary(key, value) => A::DictionaryUtf8(DictionaryUtf8Builder::new(
            name,
            build_builder(String::from("key"), *key, nullable, Default::default())?,
            build_builder(String::from("value"), *value, false, Default::default())?,
            metadata,
        )),
        T::Union(union_fields, _) => {
            let mut fields = Vec::new();
            for (idx, (type_id, field)) in union_fields.into_iter().enumerate() {
                if usize::try_from(type_id) != Ok(idx) {
                    fail!("Union with non consecutive type ids are not supported");
                }
                fields.push(build_builder(
                    field.name,
                    field.data_type,
                    field.nullable,
                    field.metadata,
                )?);
            }

            A::Union(UnionBuilder::new(name, fields, metadata))
        }
        dt => fail!("Cannot build ArrayBuilder for data type {dt:?}"),
    };
    Ok(builder)
}
