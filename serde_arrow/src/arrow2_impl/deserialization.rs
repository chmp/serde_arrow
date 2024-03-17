use crate::{
    _impl::arrow2::{
        array::{
            Array, BooleanArray, DictionaryArray, DictionaryKey, ListArray, MapArray,
            PrimitiveArray, StructArray, UnionArray, Utf8Array,
        },
        datatypes::{DataType, UnionMode},
        types::{f16, NativeType, Offset},
    },
    internal::deserialization_ng::{
        array_deserializer::ArrayDeserializer,
        bool_deserializer::BoolDeserializer,
        date64_deserializer::Date64Deserializer,
        decimal_deserializer::DecimalDeserializer,
        dictionary_deserializer::DictionaryDeserializer,
        enum_deserializer::EnumDeserializer,
        float_deserializer::{Float, FloatDeserializer},
        integer_deserializer::{Integer, IntegerDeserializer},
        list_deserializer::{IntoUsize, ListDeserializer},
        map_deserializer::MapDeserializer,
        null_deserializer::NullDeserializer,
        outer_sequence_deserializer::OuterSequenceDeserializer,
        string_deserializer::StringDeserializer,
        struct_deserializer::StructDeserializer,
    },
    schema::Strategy,
};
use crate::{
    internal::{
        common::BitBuffer,
        error::fail,
        schema::{GenericDataType, GenericField, GenericTimeUnit},
    },
    Result,
};

pub fn build_deserializer<'a>(
    fields: &[GenericField],
    arrays: &[&'a dyn Array],
) -> Result<OuterSequenceDeserializer<'a>> {
    let (deserializers, len) = build_struct_fields(fields, arrays)?;
    Ok(OuterSequenceDeserializer::new(deserializers, len))
}

pub fn build_array_deserializer<'a>(
    field: &GenericField,
    array: &'a dyn Array,
) -> Result<ArrayDeserializer<'a>> {
    use GenericDataType as T;
    match &field.data_type {
        T::Null => Ok(NullDeserializer.into()),
        T::Bool => build_bool_deserializer(field, array),
        T::U8 => build_integer_deserializer::<u8>(field, array),
        T::U16 => build_integer_deserializer::<u16>(field, array),
        T::U32 => build_integer_deserializer::<u32>(field, array),
        T::U64 => build_integer_deserializer::<u64>(field, array),
        T::I8 => build_integer_deserializer::<i8>(field, array),
        T::I16 => build_integer_deserializer::<i16>(field, array),
        T::I32 => build_integer_deserializer::<i32>(field, array),
        T::I64 => build_integer_deserializer::<i64>(field, array),
        T::F16 => build_float16_deserializer(field, array),
        T::F32 => build_float_deserializer::<f32>(field, array),
        T::F64 => build_float_deserializer::<f64>(field, array),
        T::Decimal128(_, _) => build_decimal128_deserializer(field, array),
        T::Date32 => build_date32_deserializer(field, array),
        T::Date64 => build_date64_deserializer(field, array),
        T::Time64(_) => build_time64_deserializer(field, array),
        T::Timestamp(_, _) => build_timestamp_deserializer(field, array),
        T::Utf8 => build_string_deserializer::<i32>(field, array),
        T::LargeUtf8 => build_string_deserializer::<i64>(field, array),
        T::Dictionary => build_dictionary_deserializer(field, array),
        T::Struct => build_struct_deserializer(field, array),
        T::List => build_list_deserializer::<i32>(field, array),
        T::LargeList => build_list_deserializer::<i64>(field, array),
        T::Map => build_map_deserializer(field, array),
        T::Union => build_union_deserializer(field, array),
    }
}

pub fn build_bool_deserializer<'a>(
    _field: &GenericField,
    array: &'a dyn Array,
) -> Result<ArrayDeserializer<'a>> {
    let Some(array) = array.as_any().downcast_ref::<BooleanArray>() else {
        fail!("cannot interpret array as Bool array");
    };

    let (data, offset, number_of_bits) = array.values().as_slice();
    let buffer = BitBuffer {
        data,
        offset,
        number_of_bits,
    };
    let validity = get_validity(array);

    Ok(BoolDeserializer::new(buffer, validity).into())
}

pub fn build_integer_deserializer<'a, T>(
    _field: &GenericField,
    array: &'a dyn Array,
) -> Result<ArrayDeserializer<'a>>
where
    T: Integer + NativeType + 'static,
    ArrayDeserializer<'a>: From<IntegerDeserializer<'a, T>>,
{
    let Some(array) = array.as_any().downcast_ref::<PrimitiveArray<T>>() else {
        fail!("cannot interpret array as integer array");
    };

    let buffer = array.values().as_slice();
    let validity = get_validity(array);

    Ok(IntegerDeserializer::new(buffer, validity).into())
}

pub fn build_float16_deserializer<'a>(
    _field: &GenericField,
    array: &'a dyn Array,
) -> Result<ArrayDeserializer<'a>> {
    let Some(array) = array.as_any().downcast_ref::<PrimitiveArray<f16>>() else {
        fail!("cannot interpret array as integer array");
    };

    let buffer = array.values().as_slice();
    let validity = get_validity(array);

    Ok(FloatDeserializer::new(bytemuck::cast_slice::<f16, half::f16>(buffer), validity).into())
}

pub fn build_float_deserializer<'a, T>(
    _field: &GenericField,
    array: &'a dyn Array,
) -> Result<ArrayDeserializer<'a>>
where
    T: Float + NativeType + 'static,
    ArrayDeserializer<'a>: From<FloatDeserializer<'a, T>>,
{
    let Some(array) = array.as_any().downcast_ref::<PrimitiveArray<T>>() else {
        fail!("cannot interpret array as integer array");
    };

    let buffer = array.values().as_slice();
    let validity = get_validity(array);

    Ok(FloatDeserializer::new(buffer, validity).into())
}

pub fn build_decimal128_deserializer<'a>(
    field: &GenericField,
    array: &'a dyn Array,
) -> Result<ArrayDeserializer<'a>> {
    use GenericDataType as T;

    let T::Decimal128(_, scale) = field.data_type else {
        fail!("Invalid data type for Decimal128Deserializer");
    };

    let Some(array) = array.as_any().downcast_ref::<PrimitiveArray<i128>>() else {
        fail!("Cannot convert array into Decimal128 array");
    };

    let buffer = array.values().as_slice();
    let validity = get_validity(array);

    Ok(DecimalDeserializer::new(buffer, validity, scale).into())
}

pub fn build_date32_deserializer<'a>(
    field: &GenericField,
    array: &'a dyn Array,
) -> Result<ArrayDeserializer<'a>> {
    todo!()
}

pub fn build_date64_deserializer<'a>(
    field: &GenericField,
    array: &'a dyn Array,
) -> Result<ArrayDeserializer<'a>> {
    let Some(array) = array.as_any().downcast_ref::<PrimitiveArray<i64>>() else {
        fail!("cannot interpret array as integer array");
    };

    let buffer = array.values().as_slice();
    let validity = get_validity(array);
    let is_utc = matches!(field.strategy, Some(Strategy::UtcStrAsDate64));

    Ok(Date64Deserializer::new(buffer, validity, is_utc).into())
}

pub fn build_time64_deserializer<'a>(
    field: &GenericField,
    array: &'a dyn Array,
) -> Result<ArrayDeserializer<'a>> {
    todo!()
}

pub fn build_timestamp_deserializer<'a>(
    field: &GenericField,
    array: &'a dyn Array,
) -> Result<ArrayDeserializer<'a>> {
    use {GenericDataType as T, GenericTimeUnit as U};

    let T::Timestamp(U::Millisecond, tz) = &field.data_type else {
        fail!("Invalid data type {} for timestamp array", field.data_type);
    };

    let Some(array) = array.as_any().downcast_ref::<PrimitiveArray<i64>>() else {
        fail!("cannot interpret array as integer array");
    };

    let buffer = array.values().as_slice();
    let validity = get_validity(array);

    let is_utc = match tz.as_deref() {
        Some(tz) if tz.to_lowercase() == "utc" => true,
        None => false,
        Some(tz) => fail!("Invalid timezone {tz}"),
    };

    Ok(Date64Deserializer::new(buffer, validity, is_utc).into())
}

pub fn build_string_deserializer<'a, O>(
    _field: &GenericField,
    array: &'a dyn Array,
) -> Result<ArrayDeserializer<'a>>
where
    O: IntoUsize + Offset,
    ArrayDeserializer<'a>: From<StringDeserializer<'a, O>>,
{
    let Some(array) = array.as_any().downcast_ref::<Utf8Array<O>>() else {
        fail!("cannot interpret array as Utf8 array");
    };

    let buffer = array.values().as_slice();
    let offsets = array.offsets().as_slice();
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
        (T::U8, T::Utf8) => typed::<u8, i32>(field, array),
        (T::U16, T::Utf8) => typed::<u16, i32>(field, array),
        (T::U32, T::Utf8) => typed::<u32, i32>(field, array),
        (T::U64, T::Utf8) => typed::<u64, i32>(field, array),
        (T::I8, T::Utf8) => typed::<i8, i32>(field, array),
        (T::I16, T::Utf8) => typed::<i16, i32>(field, array),
        (T::I32, T::Utf8) => typed::<i32, i32>(field, array),
        (T::I64, T::Utf8) => typed::<i64, i32>(field, array),
        (T::U8, T::LargeUtf8) => typed::<u8, i64>(field, array),
        (T::U16, T::LargeUtf8) => typed::<u16, i64>(field, array),
        (T::U32, T::LargeUtf8) => typed::<u32, i64>(field, array),
        (T::U64, T::LargeUtf8) => typed::<u64, i64>(field, array),
        (T::I8, T::LargeUtf8) => typed::<i8, i64>(field, array),
        (T::I16, T::LargeUtf8) => typed::<i16, i64>(field, array),
        (T::I32, T::LargeUtf8) => typed::<i32, i64>(field, array),
        (T::I64, T::LargeUtf8) => typed::<i64, i64>(field, array),
        _ => fail!("invalid dicitonary key / value data type"),
    };

    pub fn typed<'a, K, V>(
        _field: &GenericField,
        array: &'a dyn Array,
    ) -> Result<ArrayDeserializer<'a>>
    where
        K: DictionaryKey + Integer,
        V: Offset + IntoUsize,
        DictionaryDeserializer<'a, K, V>: Into<ArrayDeserializer<'a>>,
    {
        let Some(array) = array.as_any().downcast_ref::<DictionaryArray<K>>() else {
            fail!("cannot convert array into dictionary array");
        };
        let Some(values) = array.values().as_any().downcast_ref::<Utf8Array<V>>() else {
            fail!("invalid values");
        };

        let keys_buffer = array.keys().values();
        let keys_validity = get_validity(array);

        let values_data = values.values().as_slice();
        let values_offsets = values.offsets().as_slice();

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
        fail!("Cannot convert array into struct");
    };

    let fields = &field.children;
    let arrays = array
        .values()
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
    O: Offset + IntoUsize,
    ArrayDeserializer<'a>: From<ListDeserializer<'a, O>>,
{
    let Some(array) = array.as_any().downcast_ref::<ListArray<O>>() else {
        fail!("cannot interpret array as LargeList array");
    };

    let validity = get_validity(array);
    let offsets = array.offsets().as_slice();

    let Some(item_field) = field.children.first() else {
        fail!("cannot get first child of list array")
    };
    let item = build_array_deserializer(item_field, array.values().as_ref())?;

    Ok(ListDeserializer::new(item, offsets, validity).into())
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
        fail!("cannot convert array into map array");
    };
    let Some(entries) = array.field().as_any().downcast_ref::<StructArray>() else {
        fail!("cannot convert map field into struct array");
    };
    let Some(keys) = entries.values().first() else {
        fail!("cannot get keys array of map entries");
    };
    let Some(values) = entries.values().get(1) else {
        fail!("cannot get values array of map entries");
    };

    let offsets = array.offsets().as_slice();
    let validity = get_validity(array);

    let keys = build_array_deserializer(keys_field, keys.as_ref())?;
    let values = build_array_deserializer(values_field, values.as_ref())?;

    Ok(MapDeserializer::new(keys, values, offsets, validity).into())
}

pub fn build_union_deserializer<'a>(
    field: &GenericField,
    array: &'a dyn Array,
) -> Result<ArrayDeserializer<'a>> {
    let Some(array) = array.as_any().downcast_ref::<UnionArray>() else {
        fail!("Cannot interpret array as a union array");
    };

    if !matches!(array.data_type(), DataType::Union(_, _, UnionMode::Dense)) {
        fail!("Invalid data type: only dense unions are supported");
    }

    let type_ids = array.types().as_slice();

    let mut variants = Vec::new();
    for (type_id, field) in field.children.iter().enumerate() {
        let name = field.name.to_owned();
        let Some(child) = array.fields().get(type_id) else {
            fail!("Cannot get variant");
        };
        let deser = build_array_deserializer(field, child.as_ref())?;

        variants.push((name, deser));
    }

    Ok(EnumDeserializer::new(type_ids, variants).into())
}

fn get_validity(arr: &dyn Array) -> Option<BitBuffer<'_>> {
    let validity = arr.validity()?;
    let (data, offset, number_of_bits) = validity.as_slice();
    Some(BitBuffer {
        data,
        offset,
        number_of_bits,
    })
}
