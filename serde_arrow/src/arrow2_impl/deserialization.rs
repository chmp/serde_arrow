use crate::{
    _impl::arrow2::{
        array::{
            Array, BooleanArray, DictionaryArray, ListArray, MapArray, PrimitiveArray, StructArray,
            UnionArray, Utf8Array,
        },
        datatypes::DataType,
        types::{f16, NativeType, Offset},
    },
    internal::deserialization_ng::{
        array_deserializer::ArrayDeserializer,
        bool_deserializer::BoolDeserializer,
        float_deserializer::{Float, FloatDeserializer},
        integer_deserializer::{Integer, IntegerDeserializer},
        list_deserializer::{IntoUsize, ListDeserializer},
        null_deserializer::NullDeserializer,
        outer_sequence_deserializer::OuterSequenceDeserializer,
        string_deserializer::StringDeserializer,
    },
};
use crate::{
    internal::{
        common::{check_supported_list_layout, BitBuffer},
        error::{error, fail},
        schema::{GenericDataType, GenericField},
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
        T::Bool => build_bool_deserializer(array),
        T::U8 => build_integer_deserializer::<u8>(field, array),
        T::U16 => build_integer_deserializer::<u16>(field, array),
        T::U32 => build_integer_deserializer::<u32>(field, array),
        T::U64 => build_integer_deserializer::<u64>(field, array),
        T::I8 => build_integer_deserializer::<i8>(field, array),
        T::I16 => build_integer_deserializer::<i16>(field, array),
        T::I32 => build_integer_deserializer::<i32>(field, array),
        T::I64 => build_integer_deserializer::<i64>(field, array),
        T::F32 => build_float_deserializer::<f32>(field, array),
        T::F64 => build_float_deserializer::<f64>(field, array),
        T::Utf8 => build_string_deserializer::<i32>(array),
        T::LargeUtf8 => build_string_deserializer::<i64>(array),
        T::Struct => build_struct_deserializer(field, array),
        T::List => build_list_deserializer::<i32>(field, array),
        T::LargeList => build_list_deserializer::<i64>(field, array),
        T::Map => build_map_deserializer(field, array),
        dt => fail!("Datatype {dt} is not supported for deserialization"),
    }
}

pub fn build_bool_deserializer<'a>(array: &'a dyn Array) -> Result<ArrayDeserializer<'a>> {
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
    field: &GenericField,
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

pub fn build_float_deserializer<'a, T>(
    field: &GenericField,
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

pub fn build_string_deserializer<'a, O>(array: &'a dyn Array) -> Result<ArrayDeserializer<'a>>
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

pub fn build_struct_deserializer<'a>(
    field: &GenericField,
    array: &'a dyn Array,
) -> Result<ArrayDeserializer<'a>> {
    todo!()
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
    todo!()
}

pub fn build_map_deserializer<'a>(
    field: &GenericField,
    array: &'a dyn Array,
) -> Result<ArrayDeserializer<'a>> {
    todo!()
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

/*
impl BufferExtract for &dyn Array {
    fn len(&self) -> usize {
        (*self).len()
    }

    fn extract_buffers<'a>(
        &'a self,
        field: &GenericField,
        buffers: &mut Buffers<'a>,
    ) -> Result<ArrayMapping> {
        (*self).extract_buffers(field, buffers)
    }
}

impl BufferExtract for dyn Array {
    fn len(&self) -> usize {
        Array::len(self)
    }

    fn extract_buffers<'a>(
        &'a self,
        field: &GenericField,
        buffers: &mut Buffers<'a>,
    ) -> Result<ArrayMapping> {

        macro_rules! convert_list {
            ($offset_type:ty, $variant:ident, $push_func:ident) => {{
                let Some(typed) = self.as_any().downcast_ref::<ListArray<$offset_type>>() else {
                    fail!("cannot interpret array as LargeList array");
                };

                let validity = get_validity(typed);
                let offsets = typed.offsets();
                let offsets = offsets.as_slice();

                check_supported_list_layout(validity, offsets)?;

                let offsets = buffers.$push_func(offsets)?;
                let validity = validity.map(|v| buffers.push_u1(v));

                let Some(item_field) = field.children.first() else {
                    fail!("cannot get first child of list array")
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

        use {ArrayMapping as M, GenericDataType as T};

        match &field.data_type {
            T::Date64 => convert_primitive!(i64, Date64, push_u64_cast),
            T::Decimal128(_, _) => convert_primitive!(i128, Decimal128, push_u128_cast),
            T::Timestamp(_, _) => convert_primitive!(i64, Date64, push_u64_cast),
            T::List => convert_list!(i32, List, push_u32_cast),
            T::LargeList => convert_list!(i64, LargeList, push_u64_cast),
            T::Struct => {
                let typed = self
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(|| error!("cannot interpret array as Bool array"))?;

                let validity = get_validity(self).map(|v| buffers.push_u1(v));
                let mut fields = Vec::new();

                for (field, col) in field.children.iter().zip(typed.values()) {
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
                let Some(typed_entries) = typed.field().as_any().downcast_ref::<StructArray>()
                else {
                    fail!("cannot convert map field into struct array");
                };
                let Some(typed_keys) = typed_entries.values().first() else {
                    fail!("cannot get keys array of map entries");
                };
                let Some(typed_values) = typed_entries.values().get(1) else {
                    fail!("cannot get keys array of map entries");
                };

                let offsets = typed.offsets().as_slice();
                let validity = get_validity(typed);

                check_supported_list_layout(validity, offsets)?;
                let offsets = buffers.push_u32_cast(offsets)?;
                let validity = validity.map(|b| buffers.push_u1(b));

                let keys = typed_keys.extract_buffers(keys_field, buffers)?;
                let values = typed_values.extract_buffers(values_field, buffers)?;

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
                            .ok_or_else(|| error!("cannot convert array into u32 dictionary"))?;

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
                    T::U8 => convert_dictionary!(u8, U8),
                    T::U16 => convert_dictionary!(u16, U16),
                    T::U32 => convert_dictionary!(u32, U32),
                    T::U64 => convert_dictionary!(u64, U64),
                    T::I8 => convert_dictionary!(i8, I8),
                    T::I16 => convert_dictionary!(i16, I16),
                    T::I32 => convert_dictionary!(i32, I32),
                    T::I64 => convert_dictionary!(i64, I64),
                    dt => fail!("BufferExtract for dictionaries with key {dt} is not implemented"),
                }
            }
            T::Union => {
                // TODO: test assumptions
                let typed = self
                    .as_any()
                    .downcast_ref::<UnionArray>()
                    .ok_or_else(|| error!("cannot convert array to union array"))?;

                let types = buffers.push_u8_cast(typed.types().as_slice())?;
                let mut fields = Vec::new();
                for (field, array) in field.children.iter().zip(typed.fields()) {
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

fn get_validity(arr: &dyn Array) -> Option<BitBuffer<'_>> {
    let validity = arr.validity()?;
    let (data, offset, number_of_bits) = validity.as_slice();
    Some(BitBuffer {
        data,
        offset,
        number_of_bits,
    })
}
 */
