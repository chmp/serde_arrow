use crate::{
    internal::{
        common::{check_supported_list_layout, BitBuffer},
        deserialization_ng::{
            array_deserializer::ArrayDeserializer,
            bool_deserializer::BoolDeserializer,
            date64_deserializer::Date64Deserializer,
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
        error::{error, fail, Result},
        schema::{GenericDataType, GenericField, GenericTimeUnit},
    },
    schema::Strategy,
};

use crate::_impl::arrow::{
    array::{
        Array, BooleanArray, DictionaryArray, GenericListArray, GenericStringArray,
        LargeStringArray, MapArray, OffsetSizeTrait, PrimitiveArray, StringArray, StructArray,
        UnionArray,
    },
    datatypes::{
        ArrowPrimitiveType, DataType, Date64Type, Decimal128Type, Float16Type, Float32Type,
        Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, TimestampMicrosecondType,
        TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt16Type,
        UInt32Type, UInt64Type, UInt8Type, UnionMode,
    },
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
        T::Bool => build_bool_deserializer(array),
        T::U8 => build_integer_deserializer::<UInt8Type>(field, array),
        T::U16 => build_integer_deserializer::<UInt16Type>(field, array),
        T::U32 => build_integer_deserializer::<UInt32Type>(field, array),
        T::U64 => build_integer_deserializer::<UInt64Type>(field, array),
        T::I8 => build_integer_deserializer::<Int8Type>(field, array),
        T::I16 => build_integer_deserializer::<Int16Type>(field, array),
        T::I32 => build_integer_deserializer::<Int32Type>(field, array),
        T::I64 => build_integer_deserializer::<Int64Type>(field, array),
        T::F32 => build_float_deserializer::<Float32Type>(field, array),
        T::F64 => build_float_deserializer::<Float64Type>(field, array),
        T::Date64 => build_date64_deserializer(field, array),
        T::Timestamp(_, _) => build_timestamp_deserializer(field, array),
        T::Utf8 => build_string_deserializer::<i32>(array),
        T::LargeUtf8 => build_string_deserializer::<i64>(array),
        T::Struct => build_struct_deserializer(field, array),
        T::List => build_list_deserializer::<i32>(field, array),
        T::LargeList => build_list_deserializer::<i64>(field, array),
        T::Map => build_map_deserializer(field, array),
        T::Union => build_union_deserializer(field, array),
        dt => fail!("Datatype {dt} is not supported for deserialization"),
    }
}

pub fn build_bool_deserializer<'a>(array: &'a dyn Array) -> Result<ArrayDeserializer<'a>> {
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
    field: &GenericField,
    array: &'a dyn Array,
) -> Result<ArrayDeserializer<'a>>
where
    T: ArrowPrimitiveType,
    T::Native: Integer,
    ArrayDeserializer<'a>: From<IntegerDeserializer<'a, T::Native>>,
{
    let Some(array) = array.as_any().downcast_ref::<PrimitiveArray<T>>() else {
        fail!(
            "cannot convert {} array into {}",
            array.data_type(),
            field.data_type,
        );
    };

    let validity = get_validity(array);
    Ok(IntegerDeserializer::new(array.values(), validity).into())
}

pub fn build_float_deserializer<'a, T>(
    field: &GenericField,
    array: &'a dyn Array,
) -> Result<ArrayDeserializer<'a>>
where
    T: ArrowPrimitiveType,
    T::Native: Float,
    ArrayDeserializer<'a>: From<FloatDeserializer<'a, T::Native>>,
{
    let Some(array) = array.as_any().downcast_ref::<PrimitiveArray<T>>() else {
        fail!(
            "cannot convert {} array into {}",
            array.data_type(),
            field.data_type,
        );
    };

    let validity = get_validity(array);
    Ok(FloatDeserializer::new(array.values(), validity).into())
}

pub fn build_date64_deserializer<'a>(
    field: &GenericField,
    array: &'a dyn Array,
) -> Result<ArrayDeserializer<'a>> {
    let Some(array) = array.as_any().downcast_ref::<PrimitiveArray<Date64Type>>() else {
        fail!(
            "canont convert {} array into Date64 array",
            array.data_type()
        );
    };

    let validity = get_validity(array);
    let is_utc = matches!(field.strategy, Some(Strategy::UtcStrAsDate64));

    Ok(Date64Deserializer::new(array.values(), validity, is_utc).into())
}

pub fn build_timestamp_deserializer<'a>(
    field: &GenericField,
    array: &'a dyn Array,
) -> Result<ArrayDeserializer<'a>> {
    use {GenericDataType as T, GenericTimeUnit as U};

    let T::Timestamp(U::Millisecond, tz) = &field.data_type else {
        fail!("Invalid data type {} for timestamp array", field.data_type);
    };

    let Some(array) = array
        .as_any()
        .downcast_ref::<PrimitiveArray<TimestampMillisecondType>>()
    else {
        fail!("Cannot cast {} array to timestamp", array.data_type());
    };

    let values = array.values();
    let validity = get_validity(array);
    let is_utc = match tz.as_deref() {
        Some(tz) if tz.to_lowercase() == "utc" => true,
        None => false,
        Some(tz) => fail!("Invalid timezone {tz}"),
    };

    Ok(Date64Deserializer::new(values, validity, is_utc).into())
}

pub fn build_string_deserializer<'a, O>(array: &'a dyn Array) -> Result<ArrayDeserializer<'a>>
where
    O: OffsetSizeTrait + IntoUsize,
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
    O: OffsetSizeTrait + IntoUsize,
    ArrayDeserializer<'a>: From<ListDeserializer<'a, O>>,
{
    let Some(array) = array.as_any().downcast_ref::<GenericListArray<O>>() else {
        fail!(
            "Cannot interpret {} array as GenericListArray",
            array.data_type()
        );
    };

    let item = build_array_deserializer(&field.children[0], array.values())?;
    let offsets = array.offsets();
    let validity = get_validity(array);

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

/*
impl BufferExtract for dyn Array {
    fn len(&self) -> usize {
        Array::len(self)
    }

    fn extract_buffers<'a>(
        &'a self,
        field: &GenericField,
        buffers: &mut Buffers<'a>,
    ) -> Result<ArrayMapping> {
        macro_rules! convert_primitive {
            ($arrow_type:ty, $variant:ident, $push_func:ident) => {{
                let typed = self
                    .as_any()
                    .downcast_ref::<PrimitiveArray<$arrow_type>>()
                    .ok_or_else(|| {
                        error!(
                            "cannot convert {} array into {}",
                            self.data_type(),
                            stringify!($arrow_type)
                        )
                    })?;

                let buffer = buffers.$push_func(typed.values())?;
                let validity = get_validity(typed).map(|v| buffers.push_u1(v));

                Ok(M::$variant {
                    field: field.clone(),
                    buffer,
                    validity,
                })
            }};
        }

        macro_rules! convert_list {
            ($offset_type:ty, $variant:ident, $push_func:ident) => {{
                let Some(typed) = self
                    .as_any()
                    .downcast_ref::<GenericListArray<$offset_type>>()
                else {
                    fail!("cannot convert array into GenericListArray<i64>");
                };

                let offsets = typed.value_offsets();
                let validity = get_validity(self);

                check_supported_list_layout(validity, offsets)?;

                let offsets = buffers.$push_func(offsets)?;
                let validity = validity.map(|v| buffers.push_u1(v));

                let Some(item_field) = field.children.first() else {
                    fail!("cannot get first child of list array");
                };
                let item = typed.values().extract_buffers(item_field, buffers)?;

                Ok(M::$variant {
                    field: field.clone(),
                    item: Box::new(item),
                    validity,
                    offsets,
                })
            }};
        }

        use {ArrayMapping as M, GenericDataType as T, GenericTimeUnit as U};

        match &field.data_type {
            T::Null => {
                if !matches!(self.data_type(), DataType::Null) {
                    fail!("non-null array with null field");
                }
                Ok(M::Null {
                    field: field.clone(),
                    validity: None,
                    buffer: usize::MAX,
                })
            }
            T::Bool => {
                let typed = self
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| error!("cannot convert {} array into bool", self.data_type()))?;
                let values = typed.values();

                let buffer = buffers.push_u1(BitBuffer {
                    data: values.values(),
                    offset: values.offset(),
                    number_of_bits: values.len(),
                });
                let validity = get_validity(self).map(|v| buffers.push_u1(v));

                Ok(M::Bool {
                    field: field.clone(),
                    validity,
                    buffer,
                })
            }
            T::U8 => convert_primitive!(UInt8Type, U8, push_u8_cast),
            T::U16 => convert_primitive!(UInt16Type, U16, push_u16_cast),
            T::U32 => convert_primitive!(UInt32Type, U32, push_u32_cast),
            T::U64 => convert_primitive!(UInt64Type, U64, push_u64_cast),
            T::I8 => convert_primitive!(Int8Type, I8, push_u8_cast),
            T::I16 => convert_primitive!(Int16Type, I16, push_u16_cast),
            T::I32 => convert_primitive!(Int32Type, I32, push_u32_cast),
            T::I64 => convert_primitive!(Int64Type, I64, push_u64_cast),
            T::F16 => convert_primitive!(Float16Type, F16, push_u16_cast),
            T::F32 => convert_primitive!(Float32Type, F32, push_u32_cast),
            T::F64 => convert_primitive!(Float64Type, F64, push_u64_cast),
            T::Date64 => convert_primitive!(Date64Type, Date64, push_u64_cast),
            T::Decimal128(_, _) => convert_primitive!(Decimal128Type, Decimal128, push_u128_cast),
            T::Timestamp(U::Second, _) => {
                convert_primitive!(TimestampSecondType, Date64, push_u64_cast)
            }
            T::Timestamp(U::Millisecond, _) => {
                convert_primitive!(TimestampMillisecondType, Date64, push_u64_cast)
            }
            T::Timestamp(U::Microsecond, _) => {
                convert_primitive!(TimestampMicrosecondType, Date64, push_u64_cast)
            }
            T::Timestamp(U::Nanosecond, _) => {
                convert_primitive!(TimestampNanosecondType, Date64, push_u64_cast)
            }
            T::Utf8 => convert_utf8!(StringArray, Utf8, push_u32_cast),
            T::LargeUtf8 => convert_utf8!(LargeStringArray, LargeUtf8, push_u64_cast),
            T::List => convert_list!(i32, List, push_u32_cast),
            T::LargeList => convert_list!(i64, LargeList, push_u64_cast),
            T::Struct => {
                let typed = self.as_any().downcast_ref::<StructArray>().ok_or_else(|| {
                    error!(
                        "cannot convert {} array into struct array",
                        self.data_type()
                    )
                })?;
                let validity = get_validity(self).map(|v| buffers.push_u1(v));
                let mut fields = Vec::new();

                for (field, col) in field.children.iter().zip(typed.columns()) {
                    fields.push(col.extract_buffers(field, buffers)?);
                }

                Ok(M::Struct {
                    field: field.clone(),
                    validity,
                    fields,
                })
            }
            T::Map => {
                let Some(entries_field) = field.children.first() else {
                    fail!("cannot get children of map");
                };
                let Some(keys_field) = entries_field.children.first() else {
                    fail!("cannot get keys field");
                };
                let Some(values_field) = entries_field.children.get(1) else {
                    fail!("cannot get values field");
                };
                let Some(typed) = self.as_any().downcast_ref::<MapArray>() else {
                    fail!("cannot convert array into map array");
                };

                let offsets = typed.value_offsets();
                let validity = get_validity(typed);

                check_supported_list_layout(validity, offsets)?;

                let offsets = buffers.push_u32_cast(offsets)?;
                let validity = validity.map(|b| buffers.push_u1(b));

                let keys = typed.keys().extract_buffers(keys_field, buffers)?;
                let values = typed.values().extract_buffers(values_field, buffers)?;

                let entries = Box::new(M::Struct {
                    field: entries_field.clone(),
                    validity: None,
                    fields: vec![keys, values],
                });

                Ok(M::Map {
                    field: field.clone(),
                    validity,
                    offsets,
                    entries,
                })
            }
            T::Dictionary => {
                let Some(keys_field) = field.children.first() else {
                    fail!("cannot get key field of dictionary");
                };
                let Some(values_field) = field.children.get(1) else {
                    fail!("cannot get values field");
                };

                macro_rules! convert_dictionary {
                    ($key_type:ty, $variant:ident) => {{
                        let typed = self
                            .as_any()
                            .downcast_ref::<DictionaryArray<$key_type>>()
                            .ok_or_else(|| error!("cannot convert {} array into u32 dictionary", self.data_type()))?;

                        // NOTE: the array is validity is given by the key validity
                        if typed.values().null_count() != 0 {
                            fail!("dictionaries with nullable values are not supported");
                        }

                        let validity = get_validity(typed).map(|b| buffers.push_u1(b));
                        let keys =
                            (typed.keys() as &dyn Array).extract_buffers(keys_field, buffers)?;

                        let M::$variant { buffer: index_buffer, .. } = keys else {
                            fail!("internal error unexpected array mapping for keys")
                        };

                        let values = typed.values().extract_buffers(values_field, buffers)?;

                        let dictionary = match values {
                            M::Utf8 { buffer, offsets, .. } => DictionaryValue::Utf8{ buffer, offsets },
                            M::LargeUtf8 { buffer, offsets, .. } => DictionaryValue::LargeUtf8{ buffer, offsets },
                            m => fail!("BufferExtract for dictionaries with values of type {m:?} is not implemented"),
                        };
                        Ok(M::Dictionary {
                            field: field.clone(),
                            validity,
                            dictionary,
                            indices: DictionaryIndex::$variant(index_buffer),
                        })

                    }};
                }

                match &keys_field.data_type {
                    T::U8 => convert_dictionary!(UInt8Type, U8),
                    T::U16 => convert_dictionary!(UInt16Type, U16),
                    T::U32 => convert_dictionary!(UInt32Type, U32),
                    T::U64 => convert_dictionary!(UInt64Type, U64),
                    T::I8 => convert_dictionary!(Int8Type, I8),
                    T::I16 => convert_dictionary!(Int16Type, I16),
                    T::I32 => convert_dictionary!(Int32Type, I32),
                    T::I64 => convert_dictionary!(Int64Type, I64),
                    dt => fail!("BufferExtract for dictionaries with key {dt} is not implemented"),
                }
            }
            T::Union => {
                use crate::_impl::arrow::array::UnionArray;

                // TODO: test assumptions
                let typed = self.as_any().downcast_ref::<UnionArray>().ok_or_else(|| {
                    error!("cannot convert {} array to union array", self.data_type())
                })?;

                let types = buffers.push_u8_cast(typed.type_ids())?;

                let mut fields = Vec::new();
                for (idx, field) in field.children.iter().enumerate() {
                    let array = typed.child(idx.try_into()?);
                    fields.push(array.extract_buffers(field, buffers)?);
                }

                Ok(M::Union {
                    field: field.clone(),
                    validity: None,
                    fields,
                    types,
                })
            }
        }
    }
}
 */
