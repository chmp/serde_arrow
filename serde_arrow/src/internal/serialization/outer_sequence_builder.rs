use std::collections::{BTreeMap, HashMap};

use marrow::datatypes::{DataType, Field, FieldMeta, MapMeta, TimeUnit};
use serde::Serialize;

use crate::internal::{
    error::{fail, Context, Error, Result},
    schema::{get_strategy_from_metadata, Strategy},
    serialization::{
        binary_builder::BinaryBuilder, duration_builder::DurationBuilder,
        fixed_size_binary_builder::FixedSizeBinaryBuilder,
        fixed_size_list_builder::FixedSizeListBuilder, utils::impl_serializer,
    },
    utils::{meta_from_field, ChildName},
};

use super::{
    bool_builder::BoolBuilder, date_builder::DateBuilder, decimal_builder::DecimalBuilder,
    dictionary_utf8_builder::DictionaryUtf8Builder, float_builder::FloatBuilder,
    int_builder::IntBuilder, list_builder::ListBuilder, map_builder::MapBuilder,
    null_builder::NullBuilder, struct_builder::StructBuilder, time_builder::TimeBuilder,
    timestamp_builder::TimestampBuilder, union_builder::UnionBuilder,
    unknown_variant_builder::UnknownVariantBuilder, utf8_builder::Utf8Builder, ArrayBuilder,
};

#[derive(Debug, Clone)]
pub struct OuterSequenceBuilder(pub StructBuilder);

impl OuterSequenceBuilder {
    pub fn new(fields: Vec<Field>) -> Result<Self> {
        Ok(Self(build_struct(String::from("$"), fields, false)?))
    }

    /// Extend the builder with a sequence of items
    pub fn extend<T: Serialize>(&mut self, value: T) -> Result<()> {
        value.serialize(self)
    }

    /// Push a single item into the builder
    pub fn push<T: Serialize>(&mut self, value: T) -> Result<()> {
        self.element(&value)
    }

    pub fn num_fields(&self) -> usize {
        self.0.fields.len()
    }

    pub fn reserve(&mut self, additional: usize) {
        self.0.reserve(additional);
    }
}

impl OuterSequenceBuilder {
    fn element<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        value.serialize(&mut self.0)
    }
}

impl Context for OuterSequenceBuilder {
    fn annotate(&self, annotations: &mut BTreeMap<String, String>) {
        self.0.annotate(annotations)
    }
}

impl<'a> serde::Serializer for &'a mut OuterSequenceBuilder {
    impl_serializer!(
        'a, OuterSequenceBuilder;
        override SerializeSeq,
        override SerializeTuple,
        override SerializeTupleStruct,
        override serialize_none,
        override serialize_seq,
        override serialize_tuple,
        override serialize_tuple_struct,
    );

    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;

    fn serialize_none(self) -> Result<()> {
        serde::Serializer::serialize_none(&mut self.0)
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq> {
        if let Some(len) = len {
            self.0.reserve(len);
        }
        Ok(self)
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple> {
        self.0.reserve(len);
        Ok(self)
    }

    fn serialize_tuple_struct(
        self,
        _: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        self.0.reserve(len);
        Ok(self)
    }
}

impl serde::ser::SerializeSeq for &mut OuterSequenceBuilder {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<()> {
        value.serialize(&mut self.0)
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl serde::ser::SerializeTuple for &mut OuterSequenceBuilder {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<()> {
        value.serialize(&mut self.0)
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl serde::ser::SerializeTupleStruct for &mut OuterSequenceBuilder {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<()> {
        value.serialize(&mut self.0)
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

fn build_struct(path: String, struct_fields: Vec<Field>, nullable: bool) -> Result<StructBuilder> {
    let mut fields = Vec::new();
    for field in struct_fields {
        let Field {
            name,
            data_type,
            nullable,
            metadata,
        } = field;
        let field_path = format!("{path}.{name}", name = name);

        fields.push((
            build_builder(field_path, data_type, nullable, &metadata)?,
            FieldMeta {
                name,
                nullable,
                metadata,
            },
        ));
    }
    StructBuilder::new(path, fields, nullable)
}

fn build_builder(
    path: String,
    data_type: DataType,
    nullable: bool,
    metadata: &HashMap<String, String>,
) -> Result<ArrayBuilder> {
    use {ArrayBuilder as A, DataType as T};

    let builder = match data_type {
        T::Null => match get_strategy_from_metadata(metadata)? {
            Some(Strategy::UnknownVariant) => A::UnknownVariant(UnknownVariantBuilder::new(path)),
            _ => A::Null(NullBuilder::new(path)),
        },
        T::Boolean => A::Bool(BoolBuilder::new(path, nullable)),
        T::Int8 => A::I8(IntBuilder::new(path, nullable)),
        T::Int16 => A::I16(IntBuilder::new(path, nullable)),
        T::Int32 => A::I32(IntBuilder::new(path, nullable)),
        T::Int64 => A::I64(IntBuilder::new(path, nullable)),
        T::UInt8 => A::U8(IntBuilder::new(path, nullable)),
        T::UInt16 => A::U16(IntBuilder::new(path, nullable)),
        T::UInt32 => A::U32(IntBuilder::new(path, nullable)),
        T::UInt64 => A::U64(IntBuilder::new(path, nullable)),
        T::Float16 => A::F16(FloatBuilder::new(path, nullable)),
        T::Float32 => A::F32(FloatBuilder::new(path, nullable)),
        T::Float64 => A::F64(FloatBuilder::new(path, nullable)),
        T::Date32 => A::Date32(DateBuilder::new(path, nullable)),
        T::Date64 => A::Date64(DateBuilder::new(path, nullable)),
        T::Timestamp(unit, tz) => A::Timestamp(TimestampBuilder::new(path, unit, tz, nullable)?),
        T::Time32(unit) => {
            if !matches!(unit, TimeUnit::Second | TimeUnit::Millisecond) {
                fail!("Time32 only supports second or millisecond resolutions");
            }
            A::Time32(TimeBuilder::new(path, unit, nullable))
        }
        T::Time64(unit) => {
            if !matches!(unit, TimeUnit::Nanosecond | TimeUnit::Microsecond) {
                fail!("Time64 only supports nanosecond or microsecond resolutions");
            }
            A::Time64(TimeBuilder::new(path, unit, nullable))
        }
        T::Duration(unit) => A::Duration(DurationBuilder::new(path, unit, nullable)),
        T::Decimal128(precision, scale) => {
            A::Decimal128(DecimalBuilder::new(path, precision, scale, nullable))
        }
        T::Utf8 => A::Utf8(Utf8Builder::new(path, nullable)),
        T::LargeUtf8 => A::LargeUtf8(Utf8Builder::new(path, nullable)),
        T::Utf8View => A::Utf8View(Utf8Builder::new(path, nullable)),
        T::List(child) => {
            let child_path = format!("{path}.{child_name}", child_name = ChildName(&child.name));
            A::List(ListBuilder::new(
                path,
                meta_from_field(*child.clone()),
                build_builder(child_path, child.data_type, child.nullable, &child.metadata)?,
                nullable,
            ))
        }
        T::LargeList(child) => {
            let child_path = format!("{path}.{child_name}", child_name = ChildName(&child.name));
            A::LargeList(ListBuilder::new(
                path,
                meta_from_field(*child.clone()),
                build_builder(child_path, child.data_type, child.nullable, &child.metadata)?,
                nullable,
            ))
        }
        T::FixedSizeList(child, n) => {
            let child_path = format!("{path}.{child_name}", child_name = ChildName(&child.name));
            let n = usize::try_from(n)?;
            A::FixedSizedList(FixedSizeListBuilder::new(
                path,
                meta_from_field(*child.clone()),
                build_builder(child_path, child.data_type, child.nullable, &child.metadata)?,
                n,
                nullable,
            ))
        }
        T::Binary => A::Binary(BinaryBuilder::new(path, nullable)),
        T::LargeBinary => A::LargeBinary(BinaryBuilder::new(path, nullable)),
        T::BinaryView => A::BinaryView(BinaryBuilder::new(path, nullable)),
        T::FixedSizeBinary(n) => {
            let n = usize::try_from(n)?;
            A::FixedSizeBinary(FixedSizeBinaryBuilder::new(path, n, nullable))
        }
        T::Map(entry_field, sorted) => {
            let DataType::Struct(entries_field) = entry_field.data_type else {
                fail!(
                    "unexpected data type for map array: {:?}",
                    entry_field.data_type
                );
            };
            let Ok([keys_field, values_field]) = <[Field; 2]>::try_from(entries_field) else {
                fail!("A map field must be a struct with exactly two fields");
            };

            let keys_path = format!(
                "{path}.{entries_name}.{keys__name}",
                entries_name = ChildName(&entry_field.name),
                keys__name = ChildName(&keys_field.name),
            );
            let values_path = format!(
                "{path}.{entries_name}.{values_name}",
                entries_name = ChildName(&entry_field.name),
                values_name = ChildName(&values_field.name),
            );

            let meta = MapMeta {
                sorted,
                entries_name: entry_field.name.clone(),
                keys: meta_from_field(keys_field.clone()),
                values: meta_from_field(values_field.clone()),
            };

            A::Map(MapBuilder::new(
                path.clone(),
                meta,
                build_builder(
                    keys_path,
                    keys_field.data_type,
                    keys_field.nullable,
                    &keys_field.metadata,
                )?,
                build_builder(
                    values_path,
                    values_field.data_type,
                    values_field.nullable,
                    &values_field.metadata,
                )?,
                nullable,
            )?)
        }
        T::Struct(children) => A::Struct(build_struct(path, children, nullable)?),
        T::Dictionary(key, value) => {
            let key_path = format!("{path}.key");
            let value_path = format!("{path}.value");
            let empty_metadata = HashMap::new();
            A::DictionaryUtf8(DictionaryUtf8Builder::new(
                path,
                build_builder(key_path, *key, nullable, &empty_metadata)?,
                build_builder(value_path, *value, false, &empty_metadata)?,
            ))
        }
        T::Union(union_fields, _) => {
            let mut fields = Vec::new();
            for (idx, (type_id, field)) in union_fields.into_iter().enumerate() {
                if usize::try_from(type_id) != Ok(idx) {
                    fail!("Union with non consecutive type ids are not supported");
                }
                let field_path =
                    format!("{path}.{field_name}", field_name = ChildName(&field.name));
                fields.push((
                    build_builder(field_path, field.data_type, field.nullable, &field.metadata)?,
                    FieldMeta {
                        name: field.name,
                        nullable: field.nullable,
                        metadata: field.metadata,
                    },
                ));
            }

            A::Union(UnionBuilder::new(path, fields))
        }
        dt => fail!("Cannot build ArrayBuilder for data type {dt:?}"),
    };
    Ok(builder)
}
