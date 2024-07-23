use crate::internal::{
    arrow::{
        ArrayView, BitsWithOffset, BooleanArrayView, DecimalArrayView, NullArrayView,
        PrimitiveArrayView, TimeArrayView, TimeUnit, TimestampArrayView,
    },
    deserialization::{
        array_deserializer::ArrayDeserializer,
        binary_deserializer::BinaryDeserializer,
        dictionary_deserializer::DictionaryDeserializer,
        enum_deserializer::EnumDeserializer,
        fixed_size_list_deserializer::FixedSizeListDeserializer,
        integer_deserializer::Integer,
        list_deserializer::ListDeserializer,
        map_deserializer::MapDeserializer,
        outer_sequence_deserializer::OuterSequenceDeserializer,
        string_deserializer::StringDeserializer,
        struct_deserializer::StructDeserializer,
        utils::{check_supported_list_layout, BitBuffer},
    },
    deserializer::Deserializer,
    error::{fail, Error, Result},
    schema::{GenericDataType, GenericField},
    utils::Offset,
};

use crate::_impl::arrow::{
    array::{
        Array, BooleanArray, DictionaryArray, FixedSizeListArray, GenericBinaryArray,
        GenericListArray, GenericStringArray, MapArray, NullArray, OffsetSizeTrait, PrimitiveArray,
        RecordBatch, StructArray, UnionArray,
    },
    datatypes::{
        ArrowDictionaryKeyType, DataType, Date32Type, Date64Type, Decimal128Type,
        DurationMicrosecondType, DurationMillisecondType, DurationNanosecondType,
        DurationSecondType, FieldRef, Float16Type, Float32Type, Float64Type, Int16Type, Int32Type,
        Int64Type, Int8Type, Time32MillisecondType, Time32SecondType, Time64MicrosecondType,
        Time64NanosecondType, TimestampMicrosecondType, TimestampMillisecondType,
        TimestampNanosecondType, TimestampSecondType, UInt16Type, UInt32Type, UInt64Type,
        UInt8Type, UnionMode,
    },
};

impl<'de> Deserializer<'de> {
    /// Construct a new deserializer from `arrow` arrays (*requires one of the
    /// `arrow-*` features*)
    ///
    /// Usage
    /// ```rust
    /// # fn main() -> serde_arrow::Result<()> {
    /// # let (_, arrays) = serde_arrow::_impl::docs::defs::example_arrow_arrays();
    /// # use serde_arrow::_impl::arrow;
    /// use arrow::datatypes::FieldRef;
    /// use serde::{Deserialize, Serialize};
    /// use serde_arrow::{Deserializer, schema::{SchemaLike, TracingOptions}};
    ///
    /// ##[derive(Deserialize, Serialize)]
    /// struct Record {
    ///     a: Option<f32>,
    ///     b: u64,
    /// }
    ///
    /// let fields = Vec::<FieldRef>::from_type::<Record>(TracingOptions::default())?;
    ///
    /// let deserializer = Deserializer::from_arrow(&fields, &arrays)?;
    /// let items = Vec::<Record>::deserialize(deserializer)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn from_arrow<A>(fields: &[FieldRef], arrays: &'de [A]) -> Result<Self>
    where
        A: AsRef<dyn Array>,
    {
        let fields = fields
            .iter()
            .map(|field| GenericField::try_from(field.as_ref()))
            .collect::<Result<Vec<_>>>()?;
        let arrays = arrays
            .iter()
            .map(|array| array.as_ref())
            .collect::<Vec<_>>();

        let (deserializers, len) = build_struct_fields(&fields, &arrays)?;

        let deserializer = OuterSequenceDeserializer::new(deserializers, len);
        let deserializer = Deserializer(deserializer);

        Ok(deserializer)
    }

    /// Construct a new deserializer from a record batch (*requires one of the
    /// `arrow-*` features*)
    ///
    /// Usage:
    ///
    /// ```rust
    /// # fn main() -> serde_arrow::Result<()> {
    /// # let record_batch = serde_arrow::_impl::docs::defs::example_record_batch();
    /// #
    /// use serde::Deserialize;
    /// use serde_arrow::Deserializer;
    ///
    /// ##[derive(Deserialize)]
    /// struct Record {
    ///     a: Option<f32>,
    ///     b: u64,
    /// }
    ///
    /// let deserializer = Deserializer::from_record_batch(&record_batch)?;
    /// let items = Vec::<Record>::deserialize(deserializer)?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    pub fn from_record_batch(record_batch: &'de RecordBatch) -> Result<Self> {
        let schema = record_batch.schema();
        Deserializer::from_arrow(schema.fields(), record_batch.columns())
    }
}

pub fn build_array_deserializer<'a>(
    field: &GenericField,
    array: &'a dyn Array,
) -> Result<ArrayDeserializer<'a>> {
    use GenericDataType as T;
    match &field.data_type {
        T::Utf8 => build_string_deserializer::<i32>(field, array),
        T::LargeUtf8 => build_string_deserializer::<i64>(field, array),
        T::Struct => build_struct_deserializer(field, array),
        T::List => build_list_deserializer::<i32>(field, array),
        T::LargeList => build_list_deserializer::<i64>(field, array),
        T::FixedSizeList(n) => build_fixed_size_list_deserializer(field, array, *n),
        T::Binary => build_binary_deserializer::<i32>(field, array),
        T::LargeBinary => build_binary_deserializer::<i64>(field, array),
        T::FixedSizeBinary(_) => build_fixed_size_binary_deserializer(field, array),
        T::Map => build_map_deserializer(field, array),
        T::Union => build_union_deserializer(field, array),
        T::Dictionary => build_dictionary_deserializer(field, array),
        _ => ArrayDeserializer::new(field, array.try_into()?),
    }
}

pub fn build_string_deserializer<'a, O>(
    _field: &GenericField,
    array: &'a dyn Array,
) -> Result<ArrayDeserializer<'a>>
where
    O: OffsetSizeTrait + Offset,
    ArrayDeserializer<'a>: From<StringDeserializer<'a, O>>,
{
    let Some(array) = array.as_any().downcast_ref::<GenericStringArray<O>>() else {
        fail!("cannot convert {} array into string", array.data_type());
    };

    let buffer = array.value_data();
    let offsets = array.value_offsets();
    let validity = get_validity(array);

    Ok(StringDeserializer::new(buffer, offsets, validity).into())
}

pub fn build_dictionary_deserializer<'a>(
    field: &GenericField,
    array: &'a dyn Array,
) -> Result<ArrayDeserializer<'a>> {
    use GenericDataType as T;

    let Some(key_field) = field.children.first() else {
        fail!("Missing key field");
    };
    let Some(value_field) = field.children.get(1) else {
        fail!("Missing key field");
    };

    return match (&key_field.data_type, &value_field.data_type) {
        (T::U8, T::Utf8) => typed::<UInt8Type, i32>(field, array),
        (T::U16, T::Utf8) => typed::<UInt16Type, i32>(field, array),
        (T::U32, T::Utf8) => typed::<UInt32Type, i32>(field, array),
        (T::U64, T::Utf8) => typed::<UInt64Type, i32>(field, array),
        (T::I8, T::Utf8) => typed::<Int8Type, i32>(field, array),
        (T::I16, T::Utf8) => typed::<Int16Type, i32>(field, array),
        (T::I32, T::Utf8) => typed::<Int32Type, i32>(field, array),
        (T::I64, T::Utf8) => typed::<Int64Type, i32>(field, array),
        (T::U8, T::LargeUtf8) => typed::<UInt8Type, i64>(field, array),
        (T::U16, T::LargeUtf8) => typed::<UInt16Type, i64>(field, array),
        (T::U32, T::LargeUtf8) => typed::<UInt32Type, i64>(field, array),
        (T::U64, T::LargeUtf8) => typed::<UInt64Type, i64>(field, array),
        (T::I8, T::LargeUtf8) => typed::<Int8Type, i64>(field, array),
        (T::I16, T::LargeUtf8) => typed::<Int16Type, i64>(field, array),
        (T::I32, T::LargeUtf8) => typed::<Int32Type, i64>(field, array),
        (T::I64, T::LargeUtf8) => typed::<Int64Type, i64>(field, array),
        _ => fail!("invalid dicitonary key / value data type"),
    };

    pub fn typed<'a, K, V>(
        _field: &GenericField,
        array: &'a dyn Array,
    ) -> Result<ArrayDeserializer<'a>>
    where
        K: ArrowDictionaryKeyType,
        K::Native: Integer,
        V: OffsetSizeTrait + Offset,
        DictionaryDeserializer<'a, K::Native, V>: Into<ArrayDeserializer<'a>>,
    {
        let Some(array) = array.as_any().downcast_ref::<DictionaryArray<K>>() else {
            fail!(
                "cannot convert {} array into dictionary array",
                array.data_type()
            );
        };
        let Some(values) = array
            .values()
            .as_any()
            .downcast_ref::<GenericStringArray<V>>()
        else {
            fail!("invalid values");
        };

        let keys_buffer = array.keys().values();
        let keys_validity = get_validity(array);

        let values_data = values.value_data();
        let values_offsets = values.value_offsets();

        Ok(
            DictionaryDeserializer::new(keys_buffer, keys_validity, values_data, values_offsets)
                .into(),
        )
    }
}

pub fn build_struct_deserializer<'a>(
    field: &GenericField,
    array: &'a dyn Array,
) -> Result<ArrayDeserializer<'a>> {
    let Some(array) = array.as_any().downcast_ref::<StructArray>() else {
        fail!("Cannot convert {} array into struct", array.data_type());
    };

    let fields = &field.children;
    let arrays = array
        .columns()
        .iter()
        .map(|array| array.as_ref())
        .collect::<Vec<_>>();
    let validity = get_validity(array);

    let (deserializers, len) = build_struct_fields(fields, &arrays)?;
    Ok(StructDeserializer::new(deserializers, validity, len).into())
}

pub fn build_struct_fields<'a>(
    fields: &[GenericField],
    arrays: &[&'a dyn Array],
) -> Result<(Vec<(String, ArrayDeserializer<'a>)>, usize)> {
    if fields.len() != arrays.len() {
        fail!(
            "different number of fields ({}) and arrays ({})",
            fields.len(),
            arrays.len()
        );
    }
    let len = arrays.first().map(|array| array.len()).unwrap_or_default();

    let mut deserializers = Vec::new();
    for (field, &array) in std::iter::zip(fields, arrays) {
        if array.len() != len {
            fail!("arrays of different lengths are not supported");
        }

        deserializers.push((field.name.clone(), build_array_deserializer(field, array)?));
    }

    Ok((deserializers, len))
}

pub fn build_list_deserializer<'a, O>(
    field: &GenericField,
    array: &'a dyn Array,
) -> Result<ArrayDeserializer<'a>>
where
    O: OffsetSizeTrait + Offset,
    ArrayDeserializer<'a>: From<ListDeserializer<'a, O>>,
{
    let Some(array) = array.as_any().downcast_ref::<GenericListArray<O>>() else {
        fail!(
            "Cannot interpret {} array as GenericListArray",
            array.data_type()
        );
    };

    let item = build_array_deserializer(&field.children[0], array.values())?;
    let offsets = array.value_offsets();
    let validity = get_validity(array);

    Ok(ListDeserializer::new(item, offsets, validity).into())
}

pub fn build_binary_deserializer<'a, O>(
    _field: &GenericField,
    array: &'a dyn Array,
) -> Result<ArrayDeserializer<'a>>
where
    O: OffsetSizeTrait + Offset,
    ArrayDeserializer<'a>: From<BinaryDeserializer<'a, O>>,
{
    let Some(array) = array.as_any().downcast_ref::<GenericBinaryArray<O>>() else {
        fail!("cannot convert {} array into string", array.data_type());
    };

    let buffer = array.value_data();
    let offsets = array.value_offsets();
    let validity = get_validity(array);

    Ok(BinaryDeserializer::new(buffer, offsets, validity).into())
}

#[cfg(has_arrow_fixed_binary_support)]
pub fn build_fixed_size_binary_deserializer<'a>(
    _field: &GenericField,
    array: &'a dyn Array,
) -> Result<ArrayDeserializer<'a>> {
    use crate::_impl::arrow::array::FixedSizeBinaryArray;
    use crate::internal::deserialization::fixed_size_binary_deserializer::FixedSizeBinaryDeserializer;

    let Some(array) = array.as_any().downcast_ref::<FixedSizeBinaryArray>() else {
        fail!("cannot convert {} array into string", array.data_type());
    };

    let shape = (array.len(), array.value_length().try_into()?);
    let buffer = array.value_data();
    let validity = get_validity(array);

    Ok(FixedSizeBinaryDeserializer::new(shape, buffer, validity).into())
}

#[cfg(not(has_arrow_fixed_binary_support))]
pub fn build_fixed_size_binary_deserializer<'a>(
    _field: &GenericField,
    _array: &'a dyn Array,
) -> Result<ArrayDeserializer<'a>> {
    fail!("FixedSizeBinary arrays are not supported for arrow<=46");
}

pub fn build_fixed_size_list_deserializer<'a>(
    field: &GenericField,
    array: &'a dyn Array,
    n: i32,
) -> Result<ArrayDeserializer<'a>> {
    let Some(array) = array.as_any().downcast_ref::<FixedSizeListArray>() else {
        fail!(
            "Cannot interpret {} array as GenericListArray",
            array.data_type()
        );
    };

    let n = n.try_into()?;
    let len = array.len();
    let item = build_array_deserializer(&field.children[0], array.values())?;
    let validity = get_validity(array);

    Ok(FixedSizeListDeserializer::new(item, validity, n, len).into())
}

pub fn build_map_deserializer<'a>(
    field: &GenericField,
    array: &'a dyn Array,
) -> Result<ArrayDeserializer<'a>> {
    let Some(entries_field) = field.children.first() else {
        fail!("cannot get children of map");
    };
    let Some(keys_field) = entries_field.children.first() else {
        fail!("cannot get keys field");
    };
    let Some(values_field) = entries_field.children.get(1) else {
        fail!("cannot get values field");
    };
    let Some(array) = array.as_any().downcast_ref::<MapArray>() else {
        fail!("cannot convert {} array into map array", array.data_type());
    };

    let offsets = array.value_offsets();
    let validity = get_validity(array);

    check_supported_list_layout(validity, offsets)?;

    let key = build_array_deserializer(keys_field, array.keys())?;
    let value = build_array_deserializer(values_field, array.values())?;

    Ok(MapDeserializer::new(key, value, offsets, validity).into())
}

pub fn build_union_deserializer<'a>(
    field: &GenericField,
    array: &'a dyn Array,
) -> Result<ArrayDeserializer<'a>> {
    let Some(array) = array.as_any().downcast_ref::<UnionArray>() else {
        fail!(
            "Cannot interpret {} array as a union array",
            array.data_type()
        );
    };

    if !matches!(array.data_type(), DataType::Union(_, UnionMode::Dense)) {
        fail!("Invalid data type: only dense unions are supported");
    }

    let type_ids = array.type_ids();

    let mut variants = Vec::new();
    for (type_id, field) in field.children.iter().enumerate() {
        // TODO: how to prevent a panic? + validate the order / type_ids
        let name = field.name.to_owned();
        let deser = build_array_deserializer(field, array.child(type_id.try_into()?).as_ref())?;

        variants.push((name, deser));
    }

    Ok(EnumDeserializer::new(type_ids, variants).into())
}

fn get_validity(arr: &dyn Array) -> Option<BitBuffer<'_>> {
    let validity = arr.nulls()?;
    let data = validity.validity();
    let offset = validity.offset();
    let number_of_bits = validity.len();
    Some(BitBuffer {
        data,
        offset,
        number_of_bits,
    })
}

impl<'a> TryFrom<&'a dyn Array> for ArrayView<'a> {
    type Error = Error;

    fn try_from(array: &'a dyn Array) -> Result<Self> {
        let any = array.as_any();
        if let Some(array) = any.downcast_ref::<NullArray>() {
            Ok(ArrayView::Null(NullArrayView { len: array.len() }))
        } else if let Some(array) = any.downcast_ref::<BooleanArray>() {
            Ok(ArrayView::Boolean(BooleanArrayView {
                len: array.len(),
                validity: get_bits_with_offset(array),
                values: BitsWithOffset {
                    offset: array.values().offset(),
                    data: array.values().values(),
                },
            }))
        } else if let Some(array) = any.downcast_ref::<PrimitiveArray<Int8Type>>() {
            Ok(ArrayView::Int8(PrimitiveArrayView {
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<PrimitiveArray<Int16Type>>() {
            Ok(ArrayView::Int16(PrimitiveArrayView {
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<PrimitiveArray<Int32Type>>() {
            Ok(ArrayView::Int32(PrimitiveArrayView {
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<PrimitiveArray<Int64Type>>() {
            Ok(ArrayView::Int64(PrimitiveArrayView {
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<PrimitiveArray<UInt8Type>>() {
            Ok(ArrayView::UInt8(PrimitiveArrayView {
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<PrimitiveArray<UInt16Type>>() {
            Ok(ArrayView::UInt16(PrimitiveArrayView {
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<PrimitiveArray<UInt32Type>>() {
            Ok(ArrayView::UInt32(PrimitiveArrayView {
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<PrimitiveArray<UInt64Type>>() {
            Ok(ArrayView::UInt64(PrimitiveArrayView {
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<PrimitiveArray<Float16Type>>() {
            Ok(ArrayView::Float16(PrimitiveArrayView {
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<PrimitiveArray<Float32Type>>() {
            Ok(ArrayView::Float32(PrimitiveArrayView {
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<PrimitiveArray<Float64Type>>() {
            Ok(ArrayView::Float64(PrimitiveArrayView {
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<PrimitiveArray<Decimal128Type>>() {
            let &DataType::Decimal128(precision, scale) = array.data_type() else {
                fail!(
                    "Invalid data type for Decimal128 array: {}",
                    array.data_type()
                );
            };
            Ok(ArrayView::Decimal128(DecimalArrayView {
                precision,
                scale,
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<PrimitiveArray<Date32Type>>() {
            Ok(ArrayView::Date32(PrimitiveArrayView {
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<PrimitiveArray<Date64Type>>() {
            Ok(ArrayView::Date64(PrimitiveArrayView {
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<PrimitiveArray<Time32MillisecondType>>() {
            Ok(ArrayView::Time32(TimeArrayView {
                unit: TimeUnit::Millisecond,
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<PrimitiveArray<Time32SecondType>>() {
            Ok(ArrayView::Time32(TimeArrayView {
                unit: TimeUnit::Second,
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<PrimitiveArray<Time64NanosecondType>>() {
            Ok(ArrayView::Time64(TimeArrayView {
                unit: TimeUnit::Nanosecond,
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<PrimitiveArray<Time64MicrosecondType>>() {
            Ok(ArrayView::Time64(TimeArrayView {
                unit: TimeUnit::Microsecond,
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<PrimitiveArray<TimestampNanosecondType>>() {
            Ok(ArrayView::Timestamp(TimestampArrayView {
                unit: TimeUnit::Nanosecond,
                timezone: array.timezone().map(str::to_owned),
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<PrimitiveArray<TimestampMicrosecondType>>() {
            Ok(ArrayView::Timestamp(TimestampArrayView {
                unit: TimeUnit::Microsecond,
                timezone: array.timezone().map(str::to_owned),
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<PrimitiveArray<TimestampMillisecondType>>() {
            Ok(ArrayView::Timestamp(TimestampArrayView {
                unit: TimeUnit::Millisecond,
                timezone: array.timezone().map(str::to_owned),
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<PrimitiveArray<TimestampSecondType>>() {
            Ok(ArrayView::Timestamp(TimestampArrayView {
                unit: TimeUnit::Second,
                timezone: array.timezone().map(str::to_owned),
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<PrimitiveArray<DurationNanosecondType>>() {
            Ok(ArrayView::Duration(TimeArrayView {
                unit: TimeUnit::Nanosecond,
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<PrimitiveArray<DurationMicrosecondType>>() {
            Ok(ArrayView::Duration(TimeArrayView {
                unit: TimeUnit::Microsecond,
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<PrimitiveArray<DurationMillisecondType>>() {
            Ok(ArrayView::Duration(TimeArrayView {
                unit: TimeUnit::Millisecond,
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<PrimitiveArray<DurationSecondType>>() {
            Ok(ArrayView::Duration(TimeArrayView {
                unit: TimeUnit::Second,
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else {
            fail!(
                "Cannot build an array view for {dt}",
                dt = array.data_type()
            );
        }
    }
}

fn get_bits_with_offset(array: &dyn Array) -> Option<BitsWithOffset<'_>> {
    let validity = array.nulls()?;
    Some(BitsWithOffset {
        offset: validity.offset(),
        data: validity.validity(),
    })
}
