use half::f16;
use serde::de::{Deserialize, DeserializeSeed, VariantAccess, Visitor};

use crate::internal::{
    arrow::{ArrayView, BitsWithOffset, BytesArrayView, FieldMeta, PrimitiveArrayView, TimeUnit},
    error::{fail, Error, Result},
    schema::{Strategy, STRATEGY_KEY},
    utils::{Mut, Offset},
};

use super::{
    binary_deserializer::BinaryDeserializer,
    bool_deserializer::BoolDeserializer,
    date32_deserializer::Date32Deserializer,
    date64_deserializer::Date64Deserializer,
    decimal_deserializer::DecimalDeserializer,
    dictionary_deserializer::DictionaryDeserializer,
    enum_deserializer::EnumDeserializer,
    fixed_size_binary_deserializer::FixedSizeBinaryDeserializer,
    fixed_size_list_deserializer::FixedSizeListDeserializer,
    float_deserializer::FloatDeserializer,
    integer_deserializer::{Integer, IntegerDeserializer},
    list_deserializer::ListDeserializer,
    map_deserializer::MapDeserializer,
    null_deserializer::NullDeserializer,
    simple_deserializer::SimpleDeserializer,
    string_deserializer::StringDeserializer,
    struct_deserializer::StructDeserializer,
    time_deserializer::TimeDeserializer,
    utils::BitBuffer,
};

pub enum ArrayDeserializer<'a> {
    Null(NullDeserializer),
    Bool(BoolDeserializer<'a>),
    U8(IntegerDeserializer<'a, u8>),
    U16(IntegerDeserializer<'a, u16>),
    U32(IntegerDeserializer<'a, u32>),
    U64(IntegerDeserializer<'a, u64>),
    I8(IntegerDeserializer<'a, i8>),
    I16(IntegerDeserializer<'a, i16>),
    I32(IntegerDeserializer<'a, i32>),
    I64(IntegerDeserializer<'a, i64>),
    F16(FloatDeserializer<'a, f16>),
    F32(FloatDeserializer<'a, f32>),
    F64(FloatDeserializer<'a, f64>),
    Decimal128(DecimalDeserializer<'a>),
    Date32(Date32Deserializer<'a>),
    Date64(Date64Deserializer<'a>),
    Time32(TimeDeserializer<'a, i32>),
    Time64(TimeDeserializer<'a, i64>),
    Utf8(StringDeserializer<'a, i32>),
    LargeUtf8(StringDeserializer<'a, i64>),
    DictionaryU8I32(DictionaryDeserializer<'a, u8, i32>),
    DictionaryU16I32(DictionaryDeserializer<'a, u16, i32>),
    DictionaryU32I32(DictionaryDeserializer<'a, u32, i32>),
    DictionaryU64I32(DictionaryDeserializer<'a, u64, i32>),
    DictionaryI8I32(DictionaryDeserializer<'a, i8, i32>),
    DictionaryI16I32(DictionaryDeserializer<'a, i16, i32>),
    DictionaryI32I32(DictionaryDeserializer<'a, i32, i32>),
    DictionaryI64I32(DictionaryDeserializer<'a, i64, i32>),
    DictionaryU8I64(DictionaryDeserializer<'a, u8, i64>),
    DictionaryU16I64(DictionaryDeserializer<'a, u16, i64>),
    DictionaryU32I64(DictionaryDeserializer<'a, u32, i64>),
    DictionaryU64I64(DictionaryDeserializer<'a, u64, i64>),
    DictionaryI8I64(DictionaryDeserializer<'a, i8, i64>),
    DictionaryI16I64(DictionaryDeserializer<'a, i16, i64>),
    DictionaryI32I64(DictionaryDeserializer<'a, i32, i64>),
    DictionaryI64I64(DictionaryDeserializer<'a, i64, i64>),
    Struct(StructDeserializer<'a>),
    List(ListDeserializer<'a, i32>),
    LargeList(ListDeserializer<'a, i64>),
    FixedSizeList(FixedSizeListDeserializer<'a>),
    Binary(BinaryDeserializer<'a, i32>),
    LargeBinary(BinaryDeserializer<'a, i64>),
    FixedSizeBinary(FixedSizeBinaryDeserializer<'a>),
    Map(MapDeserializer<'a>),
    Enum(EnumDeserializer<'a>),
}

impl<'a> ArrayDeserializer<'a> {
    pub fn new(strategy: Option<&Strategy>, array: ArrayView<'a>) -> Result<Self> {
        match array {
            ArrayView::Null(_) => Ok(Self::Null(NullDeserializer {})),
            ArrayView::Boolean(view) => Ok(Self::Bool(BoolDeserializer::new(
                buffer_from_bits_with_offset(view.values, view.len),
                buffer_from_bits_with_offset_opt(view.validity, view.len),
            ))),
            ArrayView::Int8(view) => Ok(Self::I8(IntegerDeserializer::new(
                view.values,
                buffer_from_bits_with_offset_opt(view.validity, view.values.len()),
            ))),
            ArrayView::Int16(view) => Ok(Self::I16(IntegerDeserializer::new(
                view.values,
                buffer_from_bits_with_offset_opt(view.validity, view.values.len()),
            ))),
            ArrayView::Int32(view) => Ok(Self::I32(IntegerDeserializer::new(
                view.values,
                buffer_from_bits_with_offset_opt(view.validity, view.values.len()),
            ))),
            ArrayView::Int64(view) => Ok(Self::I64(IntegerDeserializer::new(
                view.values,
                buffer_from_bits_with_offset_opt(view.validity, view.values.len()),
            ))),
            ArrayView::UInt8(view) => Ok(Self::U8(IntegerDeserializer::new(
                view.values,
                buffer_from_bits_with_offset_opt(view.validity, view.values.len()),
            ))),
            ArrayView::UInt16(view) => Ok(Self::U16(IntegerDeserializer::new(
                view.values,
                buffer_from_bits_with_offset_opt(view.validity, view.values.len()),
            ))),
            ArrayView::UInt32(view) => Ok(Self::U32(IntegerDeserializer::new(
                view.values,
                buffer_from_bits_with_offset_opt(view.validity, view.values.len()),
            ))),
            ArrayView::UInt64(view) => Ok(Self::U64(IntegerDeserializer::new(
                view.values,
                buffer_from_bits_with_offset_opt(view.validity, view.values.len()),
            ))),
            ArrayView::Float16(view) => Ok(Self::F16(FloatDeserializer::new(
                view.values,
                buffer_from_bits_with_offset_opt(view.validity, view.values.len()),
            ))),
            ArrayView::Float32(view) => Ok(Self::F32(FloatDeserializer::new(
                view.values,
                buffer_from_bits_with_offset_opt(view.validity, view.values.len()),
            ))),
            ArrayView::Float64(view) => Ok(Self::F64(FloatDeserializer::new(
                view.values,
                buffer_from_bits_with_offset_opt(view.validity, view.values.len()),
            ))),
            ArrayView::Decimal128(view) => Ok(Self::Decimal128(DecimalDeserializer::new(
                view.values,
                buffer_from_bits_with_offset_opt(view.validity, view.values.len()),
                view.scale,
            ))),
            ArrayView::Date32(view) => Ok(Self::Date32(Date32Deserializer::new(
                view.values,
                buffer_from_bits_with_offset_opt(view.validity, view.values.len()),
            ))),
            ArrayView::Date64(view) => Ok(Self::Date64(Date64Deserializer::new(
                view.values,
                buffer_from_bits_with_offset_opt(view.validity, view.values.len()),
                TimeUnit::Millisecond,
                is_utc_date64(strategy)?,
            ))),
            ArrayView::Time32(view) => Ok(Self::Time32(TimeDeserializer::new(
                view.values,
                buffer_from_bits_with_offset_opt(view.validity, view.values.len()),
                view.unit,
            ))),
            ArrayView::Time64(view) => Ok(Self::Time64(TimeDeserializer::new(
                view.values,
                buffer_from_bits_with_offset_opt(view.validity, view.values.len()),
                view.unit,
            ))),
            ArrayView::Timestamp(view) => match strategy {
                Some(Strategy::NaiveStrAsDate64 | Strategy::UtcStrAsDate64) => {
                    Ok(Self::Date64(Date64Deserializer::new(
                        view.values,
                        buffer_from_bits_with_offset_opt(view.validity, view.values.len()),
                        view.unit,
                        is_utc_timestamp(view.timezone.as_deref())?,
                    )))
                }
                Some(strategy) => fail!("invalid strategy {strategy} for timestamp field"),
                None => Ok(Date64Deserializer::new(
                    view.values,
                    buffer_from_bits_with_offset_opt(view.validity, view.values.len()),
                    view.unit,
                    is_utc_timestamp(view.timezone.as_deref())?,
                )
                .into()),
            },
            ArrayView::Duration(view) => Ok(Self::I64(IntegerDeserializer::new(
                view.values,
                buffer_from_bits_with_offset_opt(view.validity, view.values.len()),
            ))),
            ArrayView::Utf8(view) => Ok(Self::Utf8(StringDeserializer::new(
                view.data,
                view.offsets,
                buffer_from_bits_with_offset_opt(
                    view.validity,
                    view.offsets.len().saturating_sub(1),
                ),
            ))),
            ArrayView::LargeUtf8(view) => Ok(Self::LargeUtf8(StringDeserializer::new(
                view.data,
                view.offsets,
                buffer_from_bits_with_offset_opt(
                    view.validity,
                    view.offsets.len().saturating_sub(1),
                ),
            ))),
            ArrayView::Binary(view) => Ok(Self::Binary(BinaryDeserializer::new(
                view.data,
                view.offsets,
                buffer_from_bits_with_offset_opt(
                    view.validity,
                    view.offsets.len().saturating_sub(1),
                ),
            ))),
            ArrayView::LargeBinary(view) => Ok(Self::LargeBinary(BinaryDeserializer::new(
                view.data,
                view.offsets,
                buffer_from_bits_with_offset_opt(
                    view.validity,
                    view.offsets.len().saturating_sub(1),
                ),
            ))),
            ArrayView::List(view) => Ok(Self::List(ListDeserializer::new(
                ArrayDeserializer::new(get_strategy(&view.meta)?.as_ref(), *view.element)?,
                view.offsets,
                buffer_from_bits_with_offset_opt(
                    view.validity,
                    view.offsets.len().saturating_sub(1),
                ),
            )?)),
            ArrayView::LargeList(view) => Ok(Self::LargeList(ListDeserializer::new(
                ArrayDeserializer::new(get_strategy(&view.meta)?.as_ref(), *view.element)?,
                view.offsets,
                buffer_from_bits_with_offset_opt(
                    view.validity,
                    view.offsets.len().saturating_sub(1),
                ),
            )?)),
            ArrayView::FixedSizeList(view) => {
                Ok(Self::FixedSizeList(FixedSizeListDeserializer::new(
                    ArrayDeserializer::new(get_strategy(&view.meta)?.as_ref(), *view.element)?,
                    buffer_from_bits_with_offset_opt(view.validity, view.len),
                    view.n.try_into()?,
                    view.len,
                )))
            }
            ArrayView::Struct(view) => {
                let mut fields = Vec::new();
                for (field_view, field_meta) in view.fields {
                    let field_deserializer =
                        ArrayDeserializer::new(get_strategy(&field_meta)?.as_ref(), field_view)?;
                    let field_name = field_meta.name;

                    fields.push((field_name, field_deserializer));
                }

                Ok(Self::Struct(StructDeserializer::new(
                    fields,
                    buffer_from_bits_with_offset_opt(view.validity, view.len),
                    view.len,
                )))
            }
            ArrayView::Map(view) => {
                let ArrayView::Struct(entries_view) = *view.element else {
                    fail!("invalid entries field in map array");
                };
                let Ok(entries_fields) = <[_; 2]>::try_from(entries_view.fields) else {
                    fail!("invalid entries field in map array")
                };
                let [(keys_view, keys_meta), (values_view, values_meta)] = entries_fields;
                let keys = ArrayDeserializer::new(get_strategy(&keys_meta)?.as_ref(), keys_view)?;
                let values =
                    ArrayDeserializer::new(get_strategy(&values_meta)?.as_ref(), values_view)?;

                Ok(Self::Map(MapDeserializer::new(
                    keys,
                    values,
                    view.offsets,
                    buffer_from_bits_with_offset_opt(
                        view.validity,
                        view.offsets.len().saturating_sub(1),
                    ),
                )?))
            }
            ArrayView::Dictionary(view) => match (*view.indices, *view.values) {
                (ArrayView::Int8(keys), ArrayView::Utf8(values)) => {
                    Ok(Self::DictionaryI8I32(build_dictionary_array(keys, values)?))
                }
                (ArrayView::Int16(keys), ArrayView::Utf8(values)) => Ok(Self::DictionaryI16I32(
                    build_dictionary_array(keys, values)?,
                )),
                (ArrayView::Int32(keys), ArrayView::Utf8(values)) => Ok(Self::DictionaryI32I32(
                    build_dictionary_array(keys, values)?,
                )),
                (ArrayView::Int64(keys), ArrayView::Utf8(values)) => Ok(Self::DictionaryI64I32(
                    build_dictionary_array(keys, values)?,
                )),
                (ArrayView::UInt8(keys), ArrayView::Utf8(values)) => {
                    Ok(Self::DictionaryU8I32(build_dictionary_array(keys, values)?))
                }
                (ArrayView::UInt16(keys), ArrayView::Utf8(values)) => Ok(Self::DictionaryU16I32(
                    build_dictionary_array(keys, values)?,
                )),
                (ArrayView::UInt32(keys), ArrayView::Utf8(values)) => Ok(Self::DictionaryU32I32(
                    build_dictionary_array(keys, values)?,
                )),
                (ArrayView::UInt64(keys), ArrayView::Utf8(values)) => Ok(Self::DictionaryU64I32(
                    build_dictionary_array(keys, values)?,
                )),
                (ArrayView::Int8(keys), ArrayView::LargeUtf8(values)) => {
                    Ok(Self::DictionaryI8I64(build_dictionary_array(keys, values)?))
                }
                (ArrayView::Int16(keys), ArrayView::LargeUtf8(values)) => Ok(
                    Self::DictionaryI16I64(build_dictionary_array(keys, values)?),
                ),
                (ArrayView::Int32(keys), ArrayView::LargeUtf8(values)) => Ok(
                    Self::DictionaryI32I64(build_dictionary_array(keys, values)?),
                ),
                (ArrayView::Int64(keys), ArrayView::LargeUtf8(values)) => Ok(
                    Self::DictionaryI64I64(build_dictionary_array(keys, values)?),
                ),
                (ArrayView::UInt8(keys), ArrayView::LargeUtf8(values)) => {
                    Ok(Self::DictionaryU8I64(build_dictionary_array(keys, values)?))
                }
                (ArrayView::UInt16(keys), ArrayView::LargeUtf8(values)) => Ok(
                    Self::DictionaryU16I64(build_dictionary_array(keys, values)?),
                ),
                (ArrayView::UInt32(keys), ArrayView::LargeUtf8(values)) => Ok(
                    Self::DictionaryU32I64(build_dictionary_array(keys, values)?),
                ),
                (ArrayView::UInt64(keys), ArrayView::LargeUtf8(values)) => Ok(
                    Self::DictionaryU64I64(build_dictionary_array(keys, values)?),
                ),
                _ => fail!("unsupported dictionary array"),
            },
            _ => unimplemented!(),
        }
    }
}

fn build_dictionary_array<'a, K: Integer, V: Offset>(
    keys: PrimitiveArrayView<'a, K>,
    values: BytesArrayView<'a, V>,
) -> Result<DictionaryDeserializer<'a, K, V>> {
    if values.validity.is_some() {
        // TODO: check whether all values are defined?
        fail!("dictionaries with nullable values are not supported");
    }
    Ok(DictionaryDeserializer::new(
        keys.values,
        buffer_from_bits_with_offset_opt(keys.validity, keys.values.len()),
        values.data,
        values.offsets,
    ))
}

fn is_utc_timestamp(timezone: Option<&str>) -> Result<bool> {
    match timezone {
        Some(tz) if tz.to_lowercase() == "utc" => Ok(true),
        Some(tz) => fail!("unsupported timezone {}", tz),
        None => Ok(false),
    }
}

fn is_utc_date64(strategy: Option<&Strategy>) -> Result<bool> {
    match strategy {
        None | Some(Strategy::UtcStrAsDate64) => Ok(true),
        Some(Strategy::NaiveStrAsDate64) => Ok(false),
        Some(strategy) => fail!("invalid strategy for date64 deserializer: {strategy}"),
    }
}

fn get_strategy(meta: &FieldMeta) -> Result<Option<Strategy>> {
    let Some(strategy) = meta.metadata.get(STRATEGY_KEY) else {
        return Ok(None);
    };
    Ok(Some(strategy.parse()?))
}

fn buffer_from_bits_with_offset(bits: BitsWithOffset, len: usize) -> BitBuffer {
    BitBuffer {
        data: bits.data,
        offset: bits.offset,
        number_of_bits: len,
    }
}

fn buffer_from_bits_with_offset_opt(bits: Option<BitsWithOffset>, len: usize) -> Option<BitBuffer> {
    Some(buffer_from_bits_with_offset(bits?, len))
}

impl<'a> From<IntegerDeserializer<'a, i8>> for ArrayDeserializer<'a> {
    fn from(value: IntegerDeserializer<'a, i8>) -> Self {
        Self::I8(value)
    }
}

impl<'a> From<IntegerDeserializer<'a, i16>> for ArrayDeserializer<'a> {
    fn from(value: IntegerDeserializer<'a, i16>) -> Self {
        Self::I16(value)
    }
}

impl<'a> From<IntegerDeserializer<'a, i32>> for ArrayDeserializer<'a> {
    fn from(value: IntegerDeserializer<'a, i32>) -> Self {
        Self::I32(value)
    }
}

impl<'a> From<IntegerDeserializer<'a, i64>> for ArrayDeserializer<'a> {
    fn from(value: IntegerDeserializer<'a, i64>) -> Self {
        Self::I64(value)
    }
}

impl<'a> From<IntegerDeserializer<'a, u8>> for ArrayDeserializer<'a> {
    fn from(value: IntegerDeserializer<'a, u8>) -> Self {
        Self::U8(value)
    }
}

impl<'a> From<IntegerDeserializer<'a, u16>> for ArrayDeserializer<'a> {
    fn from(value: IntegerDeserializer<'a, u16>) -> Self {
        Self::U16(value)
    }
}

impl<'a> From<IntegerDeserializer<'a, u32>> for ArrayDeserializer<'a> {
    fn from(value: IntegerDeserializer<'a, u32>) -> Self {
        Self::U32(value)
    }
}

impl<'a> From<IntegerDeserializer<'a, u64>> for ArrayDeserializer<'a> {
    fn from(value: IntegerDeserializer<'a, u64>) -> Self {
        Self::U64(value)
    }
}

impl<'a> From<FloatDeserializer<'a, f16>> for ArrayDeserializer<'a> {
    fn from(value: FloatDeserializer<'a, f16>) -> Self {
        Self::F16(value)
    }
}

impl<'a> From<FloatDeserializer<'a, f32>> for ArrayDeserializer<'a> {
    fn from(value: FloatDeserializer<'a, f32>) -> Self {
        Self::F32(value)
    }
}

impl<'a> From<FloatDeserializer<'a, f64>> for ArrayDeserializer<'a> {
    fn from(value: FloatDeserializer<'a, f64>) -> Self {
        Self::F64(value)
    }
}

impl<'a> From<DecimalDeserializer<'a>> for ArrayDeserializer<'a> {
    fn from(value: DecimalDeserializer<'a>) -> Self {
        Self::Decimal128(value)
    }
}

impl<'a> From<Date32Deserializer<'a>> for ArrayDeserializer<'a> {
    fn from(value: Date32Deserializer<'a>) -> Self {
        Self::Date32(value)
    }
}

impl<'a> From<Date64Deserializer<'a>> for ArrayDeserializer<'a> {
    fn from(value: Date64Deserializer<'a>) -> Self {
        Self::Date64(value)
    }
}

impl<'a> From<TimeDeserializer<'a, i32>> for ArrayDeserializer<'a> {
    fn from(value: TimeDeserializer<'a, i32>) -> Self {
        Self::Time32(value)
    }
}

impl<'a> From<TimeDeserializer<'a, i64>> for ArrayDeserializer<'a> {
    fn from(value: TimeDeserializer<'a, i64>) -> Self {
        Self::Time64(value)
    }
}

impl<'a> From<StructDeserializer<'a>> for ArrayDeserializer<'a> {
    fn from(value: StructDeserializer<'a>) -> Self {
        Self::Struct(value)
    }
}

impl<'a> From<ListDeserializer<'a, i32>> for ArrayDeserializer<'a> {
    fn from(value: ListDeserializer<'a, i32>) -> Self {
        Self::List(value)
    }
}

impl<'a> From<ListDeserializer<'a, i64>> for ArrayDeserializer<'a> {
    fn from(value: ListDeserializer<'a, i64>) -> Self {
        Self::LargeList(value)
    }
}

impl<'a> From<FixedSizeListDeserializer<'a>> for ArrayDeserializer<'a> {
    fn from(value: FixedSizeListDeserializer<'a>) -> Self {
        Self::FixedSizeList(value)
    }
}

impl<'a> From<BinaryDeserializer<'a, i32>> for ArrayDeserializer<'a> {
    fn from(value: BinaryDeserializer<'a, i32>) -> Self {
        Self::Binary(value)
    }
}

impl<'a> From<BinaryDeserializer<'a, i64>> for ArrayDeserializer<'a> {
    fn from(value: BinaryDeserializer<'a, i64>) -> Self {
        Self::LargeBinary(value)
    }
}

impl<'a> From<FixedSizeBinaryDeserializer<'a>> for ArrayDeserializer<'a> {
    fn from(value: FixedSizeBinaryDeserializer<'a>) -> Self {
        Self::FixedSizeBinary(value)
    }
}

impl<'a> From<StringDeserializer<'a, i32>> for ArrayDeserializer<'a> {
    fn from(value: StringDeserializer<'a, i32>) -> Self {
        Self::Utf8(value)
    }
}

impl<'a> From<StringDeserializer<'a, i64>> for ArrayDeserializer<'a> {
    fn from(value: StringDeserializer<'a, i64>) -> Self {
        Self::LargeUtf8(value)
    }
}

impl<'a> From<DictionaryDeserializer<'a, u8, i32>> for ArrayDeserializer<'a> {
    fn from(value: DictionaryDeserializer<'a, u8, i32>) -> Self {
        Self::DictionaryU8I32(value)
    }
}

impl<'a> From<DictionaryDeserializer<'a, u16, i32>> for ArrayDeserializer<'a> {
    fn from(value: DictionaryDeserializer<'a, u16, i32>) -> Self {
        Self::DictionaryU16I32(value)
    }
}

impl<'a> From<DictionaryDeserializer<'a, u32, i32>> for ArrayDeserializer<'a> {
    fn from(value: DictionaryDeserializer<'a, u32, i32>) -> Self {
        Self::DictionaryU32I32(value)
    }
}

impl<'a> From<DictionaryDeserializer<'a, u64, i32>> for ArrayDeserializer<'a> {
    fn from(value: DictionaryDeserializer<'a, u64, i32>) -> Self {
        Self::DictionaryU64I32(value)
    }
}

impl<'a> From<DictionaryDeserializer<'a, i8, i32>> for ArrayDeserializer<'a> {
    fn from(value: DictionaryDeserializer<'a, i8, i32>) -> Self {
        Self::DictionaryI8I32(value)
    }
}

impl<'a> From<DictionaryDeserializer<'a, i16, i32>> for ArrayDeserializer<'a> {
    fn from(value: DictionaryDeserializer<'a, i16, i32>) -> Self {
        Self::DictionaryI16I32(value)
    }
}

impl<'a> From<DictionaryDeserializer<'a, i32, i32>> for ArrayDeserializer<'a> {
    fn from(value: DictionaryDeserializer<'a, i32, i32>) -> Self {
        Self::DictionaryI32I32(value)
    }
}

impl<'a> From<DictionaryDeserializer<'a, i64, i32>> for ArrayDeserializer<'a> {
    fn from(value: DictionaryDeserializer<'a, i64, i32>) -> Self {
        Self::DictionaryI64I32(value)
    }
}

impl<'a> From<DictionaryDeserializer<'a, u8, i64>> for ArrayDeserializer<'a> {
    fn from(value: DictionaryDeserializer<'a, u8, i64>) -> Self {
        Self::DictionaryU8I64(value)
    }
}

impl<'a> From<DictionaryDeserializer<'a, u16, i64>> for ArrayDeserializer<'a> {
    fn from(value: DictionaryDeserializer<'a, u16, i64>) -> Self {
        Self::DictionaryU16I64(value)
    }
}

impl<'a> From<DictionaryDeserializer<'a, u32, i64>> for ArrayDeserializer<'a> {
    fn from(value: DictionaryDeserializer<'a, u32, i64>) -> Self {
        Self::DictionaryU32I64(value)
    }
}

impl<'a> From<DictionaryDeserializer<'a, u64, i64>> for ArrayDeserializer<'a> {
    fn from(value: DictionaryDeserializer<'a, u64, i64>) -> Self {
        Self::DictionaryU64I64(value)
    }
}

impl<'a> From<DictionaryDeserializer<'a, i8, i64>> for ArrayDeserializer<'a> {
    fn from(value: DictionaryDeserializer<'a, i8, i64>) -> Self {
        Self::DictionaryI8I64(value)
    }
}

impl<'a> From<DictionaryDeserializer<'a, i16, i64>> for ArrayDeserializer<'a> {
    fn from(value: DictionaryDeserializer<'a, i16, i64>) -> Self {
        Self::DictionaryI16I64(value)
    }
}

impl<'a> From<DictionaryDeserializer<'a, i32, i64>> for ArrayDeserializer<'a> {
    fn from(value: DictionaryDeserializer<'a, i32, i64>) -> Self {
        Self::DictionaryI32I64(value)
    }
}

impl<'a> From<DictionaryDeserializer<'a, i64, i64>> for ArrayDeserializer<'a> {
    fn from(value: DictionaryDeserializer<'a, i64, i64>) -> Self {
        Self::DictionaryI64I64(value)
    }
}

impl<'a> From<MapDeserializer<'a>> for ArrayDeserializer<'a> {
    fn from(value: MapDeserializer<'a>) -> Self {
        Self::Map(value)
    }
}

impl<'a> From<EnumDeserializer<'a>> for ArrayDeserializer<'a> {
    fn from(value: EnumDeserializer<'a>) -> Self {
        Self::Enum(value)
    }
}

macro_rules! dispatch {
    ($obj:expr, $wrapper:ident($name:ident) => $expr:expr) => {
        match $obj {
            $wrapper::Null($name) => $expr,
            $wrapper::Bool($name) => $expr,
            $wrapper::U8($name) => $expr,
            $wrapper::U16($name) => $expr,
            $wrapper::U32($name) => $expr,
            $wrapper::U64($name) => $expr,
            $wrapper::I8($name) => $expr,
            $wrapper::I16($name) => $expr,
            $wrapper::I32($name) => $expr,
            $wrapper::I64($name) => $expr,
            $wrapper::F16($name) => $expr,
            $wrapper::F32($name) => $expr,
            $wrapper::F64($name) => $expr,
            $wrapper::Decimal128($name) => $expr,
            $wrapper::Date32($name) => $expr,
            $wrapper::Date64($name) => $expr,
            $wrapper::Time32($name) => $expr,
            $wrapper::Time64($name) => $expr,
            $wrapper::Utf8($name) => $expr,
            $wrapper::LargeUtf8($name) => $expr,
            $wrapper::Struct($name) => $expr,
            $wrapper::List($name) => $expr,
            $wrapper::FixedSizeList($name) => $expr,
            $wrapper::LargeList($name) => $expr,
            $wrapper::Binary($name) => $expr,
            $wrapper::LargeBinary($name) => $expr,
            $wrapper::FixedSizeBinary($name) => $expr,
            $wrapper::Map($name) => $expr,
            $wrapper::Enum($name) => $expr,
            $wrapper::DictionaryU8I32($name) => $expr,
            $wrapper::DictionaryU16I32($name) => $expr,
            $wrapper::DictionaryU32I32($name) => $expr,
            $wrapper::DictionaryU64I32($name) => $expr,
            $wrapper::DictionaryI8I32($name) => $expr,
            $wrapper::DictionaryI16I32($name) => $expr,
            $wrapper::DictionaryI32I32($name) => $expr,
            $wrapper::DictionaryI64I32($name) => $expr,
            $wrapper::DictionaryU8I64($name) => $expr,
            $wrapper::DictionaryU16I64($name) => $expr,
            $wrapper::DictionaryU32I64($name) => $expr,
            $wrapper::DictionaryU64I64($name) => $expr,
            $wrapper::DictionaryI8I64($name) => $expr,
            $wrapper::DictionaryI16I64($name) => $expr,
            $wrapper::DictionaryI32I64($name) => $expr,
            $wrapper::DictionaryI64I64($name) => $expr,
        }
    };
}

impl<'de> SimpleDeserializer<'de> for ArrayDeserializer<'de> {
    fn name() -> &'static str {
        "ArrayDeserializer"
    }

    fn deserialize_any<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_any(visitor))
    }

    fn deserialize_ignored_any<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_ignored_any(visitor))
    }

    fn deserialize_option<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_option(visitor))
    }

    fn deserialize_unit<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_unit(visitor))
    }

    fn deserialize_unit_struct<V: Visitor<'de>>(
        &mut self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_unit_struct(name, visitor))
    }

    fn deserialize_bool<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_bool(visitor))
    }

    fn deserialize_char<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_char(visitor))
    }

    fn deserialize_u8<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_u8(visitor))
    }

    fn deserialize_u16<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_u16(visitor))
    }

    fn deserialize_u32<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_u32(visitor))
    }

    fn deserialize_u64<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_u64(visitor))
    }

    fn deserialize_i8<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_i8(visitor))
    }

    fn deserialize_i16<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_i16(visitor))
    }

    fn deserialize_i32<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_i32(visitor))
    }

    fn deserialize_i64<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_i64(visitor))
    }

    fn deserialize_f32<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_f32(visitor))
    }

    fn deserialize_f64<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_f64(visitor))
    }

    fn deserialize_str<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_str(visitor))
    }

    fn deserialize_string<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_string(visitor))
    }

    fn deserialize_struct<V: Visitor<'de>>(
        &mut self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_struct(name, fields, visitor))
    }

    fn deserialize_map<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_map(visitor))
    }

    fn deserialize_seq<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_seq(visitor))
    }

    fn deserialize_tuple<V: Visitor<'de>>(&mut self, len: usize, visitor: V) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_tuple(len, visitor))
    }

    fn deserialize_tuple_struct<V: Visitor<'de>>(
        &mut self,
        name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_tuple_struct(name, len, visitor))
    }

    fn deserialize_identifier<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_identifier(visitor))
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        &mut self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_newtype_struct(name, visitor))
    }

    fn deserialize_enum<V: Visitor<'de>>(
        &mut self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_enum(name, variants, visitor))
    }

    fn deserialize_bytes<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_bytes(visitor))
    }

    fn deserialize_byte_buf<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_byte_buf(visitor))
    }
}

impl<'a, 'de> VariantAccess<'de> for Mut<'a, ArrayDeserializer<'de>> {
    type Error = Error;

    fn newtype_variant_seed<T: DeserializeSeed<'de>>(self, seed: T) -> Result<T::Value> {
        seed.deserialize(self)
    }

    fn struct_variant<V: Visitor<'de>>(
        self,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        self.0
            .deserialize_struct("UNUSED_ENUM_STRUCT_NAME", fields, visitor)
    }

    fn tuple_variant<V: Visitor<'de>>(self, len: usize, visitor: V) -> Result<V::Value> {
        self.0.deserialize_tuple(len, visitor)
    }

    fn unit_variant(self) -> Result<()> {
        <()>::deserialize(self)
    }
}
