use crate::internal::{
    deserialization::{
        array_deserializer::ArrayDeserializer,
        binary_deserializer::BinaryDeserializer,
        bool_deserializer::BoolDeserializer,
        construction,
        date32_deserializer::Date32Deserializer,
        date64_deserializer::Date64Deserializer,
        decimal_deserializer::DecimalDeserializer,
        dictionary_deserializer::DictionaryDeserializer,
        enum_deserializer::EnumDeserializer,
        fixed_size_list_deserializer::FixedSizeListDeserializer,
        float_deserializer::{Float, FloatDeserializer},
        integer_deserializer::{Integer, IntegerDeserializer},
        list_deserializer::ListDeserializer,
        map_deserializer::MapDeserializer,
        null_deserializer::NullDeserializer,
        outer_sequence_deserializer::OuterSequenceDeserializer,
        string_deserializer::StringDeserializer,
        struct_deserializer::StructDeserializer,
        utils::{check_supported_list_layout, BitBuffer},
    },
    deserializer::Deserializer,
    error::{fail, Result},
    schema::{GenericDataType, GenericField, GenericTimeUnit},
    utils::Offset,
};

use crate::_impl::arrow::{
    array::{
        Array, BooleanArray, DictionaryArray, FixedSizeListArray, GenericBinaryArray,
        GenericListArray, GenericStringArray, MapArray, OffsetSizeTrait, PrimitiveArray,
        RecordBatch, StructArray, UnionArray,
    },
    datatypes::{
        ArrowDictionaryKeyType, ArrowPrimitiveType, DataType, Date32Type, Date64Type,
        Decimal128Type, DurationMicrosecondType, DurationMillisecondType, DurationNanosecondType,
        DurationSecondType, Field, Float16Type, Float32Type, Float64Type, Int16Type, Int32Type,
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
    pub fn from_arrow<F, A>(fields: &[F], arrays: &'de [A]) -> Result<Self>
    where
        F: AsRef<Field>,
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
    use {GenericDataType as T, GenericTimeUnit as U};
    match &field.data_type {
        T::Null => Ok(NullDeserializer.into()),
        T::Bool => build_bool_deserializer(field, array),
        T::U8 => build_integer_deserializer::<UInt8Type>(field, array),
        T::U16 => build_integer_deserializer::<UInt16Type>(field, array),
        T::U32 => build_integer_deserializer::<UInt32Type>(field, array),
        T::U64 => build_integer_deserializer::<UInt64Type>(field, array),
        T::I8 => build_integer_deserializer::<Int8Type>(field, array),
        T::I16 => build_integer_deserializer::<Int16Type>(field, array),
        T::I32 => build_integer_deserializer::<Int32Type>(field, array),
        T::I64 => build_integer_deserializer::<Int64Type>(field, array),
        T::F16 => build_float16_deserializer(field, array),
        T::F32 => build_float_deserializer::<Float32Type>(field, array),
        T::F64 => build_float_deserializer::<Float64Type>(field, array),
        T::Decimal128(_, _) => build_decimal128_deserializer(field, array),
        T::Date32 => build_date32_deserializer(field, array),
        T::Date64 => build_date64_deserializer(field, array),
        T::Time32(unit) => construction::build_time32_deserializer(
            field,
            match unit {
                U::Second => as_primitive_values::<Time32SecondType>(array)?,
                U::Millisecond => as_primitive_values::<Time32MillisecondType>(array)?,
                // Not supported according to the arrow docs
                unit => fail!("cannot build deserializer for Time64({unit})"),
            },
            get_validity(array),
        ),
        T::Time64(unit) => construction::build_time64_deserializer(
            field,
            match unit {
                U::Microsecond => as_primitive_values::<Time64MicrosecondType>(array)?,
                U::Nanosecond => as_primitive_values::<Time64NanosecondType>(array)?,
                // Not supported according to the arrow docs
                unit => fail!("cannot build deserializer for Time64({unit})"),
            },
            get_validity(array),
        ),
        T::Timestamp(unit, _) => construction::build_timestamp_deserializer(
            field,
            match unit {
                U::Second => as_primitive_values::<TimestampSecondType>(array)?,
                U::Millisecond => as_primitive_values::<TimestampMillisecondType>(array)?,
                U::Microsecond => as_primitive_values::<TimestampMicrosecondType>(array)?,
                U::Nanosecond => as_primitive_values::<TimestampNanosecondType>(array)?,
            },
            get_validity(array),
        ),
        T::Duration(U::Second) => build_integer_deserializer::<DurationSecondType>(field, array),
        T::Duration(U::Millisecond) => {
            build_integer_deserializer::<DurationMillisecondType>(field, array)
        }
        T::Duration(U::Microsecond) => {
            build_integer_deserializer::<DurationMicrosecondType>(field, array)
        }
        T::Duration(U::Nanosecond) => {
            build_integer_deserializer::<DurationNanosecondType>(field, array)
        }
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
    }
}

pub fn build_bool_deserializer<'a>(
    _field: &GenericField,
    array: &'a dyn Array,
) -> Result<ArrayDeserializer<'a>> {
    let Some(array) = array.as_any().downcast_ref::<BooleanArray>() else {
        fail!("cannot convert {} array into bool", array.data_type());
    };

    let buffer = BitBuffer {
        data: array.values().values(),
        offset: array.values().offset(),
        number_of_bits: array.values().len(),
    };
    let validity = get_validity(array);

    Ok(BoolDeserializer::new(buffer, validity).into())
}

pub fn build_integer_deserializer<'a, T>(
    _field: &GenericField,
    array: &'a dyn Array,
) -> Result<ArrayDeserializer<'a>>
where
    T: ArrowPrimitiveType,
    T::Native: Integer,
    ArrayDeserializer<'a>: From<IntegerDeserializer<'a, T::Native>>,
{
    Ok(IntegerDeserializer::new(as_primitive_values::<T>(array)?, get_validity(array)).into())
}

pub fn build_float16_deserializer<'a>(
    _field: &GenericField,
    array: &'a dyn Array,
) -> Result<ArrayDeserializer<'a>> {
    Ok(FloatDeserializer::new(
        as_primitive_values::<Float16Type>(array)?,
        get_validity(array),
    )
    .into())
}

pub fn build_float_deserializer<'a, T>(
    _field: &GenericField,
    array: &'a dyn Array,
) -> Result<ArrayDeserializer<'a>>
where
    T: ArrowPrimitiveType,
    T::Native: Float,
    ArrayDeserializer<'a>: From<FloatDeserializer<'a, T::Native>>,
{
    Ok(FloatDeserializer::new(as_primitive_values::<T>(array)?, get_validity(array)).into())
}

pub fn build_decimal128_deserializer<'a>(
    field: &GenericField,
    array: &'a dyn Array,
) -> Result<ArrayDeserializer<'a>> {
    let GenericDataType::Decimal128(_, scale) = field.data_type else {
        fail!("Invalid data type for Decimal128Deserializer");
    };

    Ok(DecimalDeserializer::new(
        as_primitive_values::<Decimal128Type>(array)?,
        get_validity(array),
        scale,
    )
    .into())
}

pub fn build_date32_deserializer<'a>(
    _field: &GenericField,
    array: &'a dyn Array,
) -> Result<ArrayDeserializer<'a>> {
    Ok(Date32Deserializer::new(
        as_primitive_values::<Date32Type>(array)?,
        get_validity(array),
    )
    .into())
}

pub fn build_date64_deserializer<'a>(
    field: &GenericField,
    array: &'a dyn Array,
) -> Result<ArrayDeserializer<'a>> {
    Ok(Date64Deserializer::new(
        as_primitive_values::<Date64Type>(array)?,
        get_validity(array),
        GenericTimeUnit::Millisecond,
        field.is_utc()?,
    )
    .into())
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

fn as_primitive_values<T: ArrowPrimitiveType>(array: &dyn Array) -> Result<&[T::Native]> {
    let Some(array) = array.as_any().downcast_ref::<PrimitiveArray<T>>() else {
        fail!(
            "cannot convert {} array into {}",
            array.data_type(),
            T::DATA_TYPE,
        );
    };
    Ok(array.values())
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
