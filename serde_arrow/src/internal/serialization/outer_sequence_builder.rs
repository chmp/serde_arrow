use serde::Serialize;

use crate::internal::{
    error::{fail, Result},
    schema::{GenericDataType, GenericField, GenericTimeUnit, SerdeArrowSchema, Strategy},
    serialization::{
        binary_builder::BinaryBuilder, duration_builder::DurationBuilder,
        fixed_size_list_builder::FixedSizeListBuilder,
    },
    utils::Mut,
};

use super::{
    bool_builder::BoolBuilder, date32_builder::Date32Builder, date64_builder::Date64Builder,
    decimal_builder::DecimalBuilder, dictionary_utf8_builder::DictionaryUtf8Builder,
    float_builder::FloatBuilder, int_builder::IntBuilder, list_builder::ListBuilder,
    map_builder::MapBuilder, null_builder::NullBuilder, struct_builder::StructBuilder,
    time_builder::TimeBuilder, union_builder::UnionBuilder,
    unknown_variant_builder::UnknownVariantBuilder, utf8_builder::Utf8Builder,
    utils::SimpleSerializer, ArrayBuilder,
};

#[derive(Debug, Clone)]
pub struct OuterSequenceBuilder(StructBuilder);

impl OuterSequenceBuilder {
    pub fn new(schema: &SerdeArrowSchema) -> Result<Self> {
        return Ok(Self(build_struct(&schema.fields, false)?));

        fn build_struct(fields: &[GenericField], nullable: bool) -> Result<StructBuilder> {
            let mut named_fields = Vec::new();
            for field in fields {
                let builder = build_builder(field)?;
                named_fields.push((field.name.to_owned(), builder));
            }

            StructBuilder::new(fields.to_vec(), named_fields, nullable)
        }

        fn build_builder(field: &GenericField) -> Result<ArrayBuilder> {
            use {ArrayBuilder as A, GenericDataType as T};

            let builder = match &field.data_type {
                T::Null => {
                    if matches!(&field.strategy, Some(Strategy::UnknownVariant)) {
                        A::UnknownVariant(UnknownVariantBuilder)
                    } else {
                        A::Null(NullBuilder::new())
                    }
                }
                T::Bool => A::Bool(BoolBuilder::new(field.nullable)),
                T::I8 => A::I8(IntBuilder::new(field.nullable)),
                T::I16 => A::I16(IntBuilder::new(field.nullable)),
                T::I32 => A::I32(IntBuilder::new(field.nullable)),
                T::I64 => A::I64(IntBuilder::new(field.nullable)),
                T::U8 => A::U8(IntBuilder::new(field.nullable)),
                T::U16 => A::U16(IntBuilder::new(field.nullable)),
                T::U32 => A::U32(IntBuilder::new(field.nullable)),
                T::U64 => A::U64(IntBuilder::new(field.nullable)),
                T::F16 => A::F16(FloatBuilder::new(field.nullable)),
                T::F32 => A::F32(FloatBuilder::new(field.nullable)),
                T::F64 => A::F64(FloatBuilder::new(field.nullable)),
                T::Date32 => A::Date32(Date32Builder::new(field.clone(), field.nullable)),
                T::Date64 => {
                    let is_utc = match field.strategy.as_ref() {
                        Some(Strategy::UtcStrAsDate64) | None => true,
                        Some(Strategy::NaiveStrAsDate64) => false,
                        Some(st) => fail!("Cannot builder Date64 builder with strategy {st}"),
                    };
                    A::Date64(Date64Builder::new(field.clone(), is_utc, field.nullable))
                }
                T::Timestamp(_, tz) => match tz.as_deref() {
                    None => A::Date64(Date64Builder::new(field.clone(), false, field.nullable)),
                    Some(tz) if tz.to_uppercase() == "UTC" => {
                        A::Date64(Date64Builder::new(field.clone(), true, field.nullable))
                    }
                    Some(tz) => fail!("Timezone {tz} is not supported"),
                },
                T::Time32(unit) => {
                    if !matches!(unit, GenericTimeUnit::Second | GenericTimeUnit::Millisecond) {
                        fail!("Only timestamps with second or millisecond unit are supported");
                    }
                    A::Time32(TimeBuilder::new(field.clone(), field.nullable, *unit))
                }
                T::Time64(unit) => {
                    if !matches!(
                        unit,
                        GenericTimeUnit::Nanosecond | GenericTimeUnit::Microsecond
                    ) {
                        fail!("Only timestamps with nanosecond or microsecond unit are supported");
                    }
                    A::Time64(TimeBuilder::new(field.clone(), field.nullable, *unit))
                }
                T::Duration(unit) => A::Duration(DurationBuilder::new(*unit, field.nullable)),
                T::Decimal128(precision, scale) => {
                    A::Decimal128(DecimalBuilder::new(*precision, *scale, field.nullable))
                }
                T::Utf8 => A::Utf8(Utf8Builder::new(field.nullable)),
                T::LargeUtf8 => A::LargeUtf8(Utf8Builder::new(field.nullable)),
                T::List => {
                    let Some(child) = field.children.first() else {
                        fail!("cannot build a list without an element field");
                    };
                    A::List(ListBuilder::new(
                        child.clone(),
                        build_builder(child)?,
                        field.nullable,
                    ))
                }
                T::LargeList => {
                    let Some(child) = field.children.first() else {
                        fail!("cannot build list without an element field");
                    };
                    A::LargeList(ListBuilder::new(
                        child.clone(),
                        build_builder(child)?,
                        field.nullable,
                    ))
                }
                T::FixedSizeList(n) => {
                    let Some(child) = field.children.first() else {
                        fail!("cannot build list without an element field");
                    };
                    A::FixedSizedList(FixedSizeListBuilder::new(
                        child.clone(),
                        build_builder(child)?,
                        (*n).try_into()?,
                        field.nullable,
                    ))
                }
                T::Binary => A::Binary(BinaryBuilder::new(field.nullable)),
                T::LargeBinary => A::LargeBinary(BinaryBuilder::new(field.nullable)),
                T::FixedSizeBinary(_) => todo!(),
                T::Map => {
                    let Some(entry_field) = field.children.first() else {
                        fail!("Cannot build a map with an entry field");
                    };
                    if entry_field.data_type != T::Struct && entry_field.children.len() != 2 {
                        fail!("Invalid child field for map: {entry_field:?}")
                    }
                    A::Map(MapBuilder::new(
                        entry_field.clone(),
                        build_builder(entry_field)?,
                        field.nullable,
                    ))
                }
                T::Struct => A::Struct(build_struct(&field.children, field.nullable)?),
                T::Dictionary => {
                    let Some(indices) = field.children.first() else {
                        fail!("Cannot build a dictionary without index field");
                    };
                    let Some(values) = field.children.get(1) else {
                        fail!("Cannot build a dictionary without values field");
                    };
                    if !matches!(values.data_type, T::Utf8 | T::LargeUtf8) {
                        fail!("At the moment only string dictionaries are supported");
                    }
                    // TODO: figure out how arrow encodes nullability and fix this
                    let mut indices = indices.clone();
                    indices.nullable = field.nullable;

                    A::DictionaryUtf8(DictionaryUtf8Builder::new(
                        field.clone(),
                        build_builder(&indices)?,
                        build_builder(values)?,
                    ))
                }
                T::Union => {
                    let mut fields = Vec::new();
                    for field in &field.children {
                        fields.push(build_builder(field)?);
                    }

                    A::Union(UnionBuilder::new(field.clone(), fields)?)
                }
            };
            Ok(builder)
        }
    }

    /// Extract the contained struct fields
    pub fn take_records(&mut self) -> Result<Vec<ArrayBuilder>> {
        let builder = self.0.take();

        let mut result = Vec::new();
        for (_, field) in builder.named_fields {
            result.push(field);
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
