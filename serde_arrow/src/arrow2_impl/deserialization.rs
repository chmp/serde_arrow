use crate::{
    _impl::arrow2::{
        array::{
            Array, BooleanArray, DictionaryArray, ListArray, MapArray, PrimitiveArray, StructArray,
            UnionArray, Utf8Array,
        },
        datatypes::DataType,
        types::f16,
    },
    internal::common::{DictionaryIndex, DictionaryValue},
};
use crate::{
    internal::{
        common::{check_supported_list_layout, ArrayMapping, BitBuffer, BufferExtract, Buffers},
        error::{error, fail},
        schema::{GenericDataType, GenericField},
    },
    Result,
};

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
        macro_rules! convert_primitive {
            ($array_type:ty, $variant:ident, $push_func:ident) => {{
                let typed = self
                    .as_any()
                    .downcast_ref::<PrimitiveArray<$array_type>>()
                    .ok_or_else(|| error!("cannot interpret array as I32 array"))?;

                let buffer = buffers.$push_func(typed.values().as_slice())?;
                let validity = get_validity(typed).map(|v| buffers.push_u1(v));

                Ok(M::$variant {
                    field: field.clone(),
                    buffer,
                    validity,
                })
            }};
        }

        macro_rules! convert_utf8 {
            ($offset_type:ty, $variant:ident, $push_func:ident) => {{
                let typed = self
                    .as_any()
                    .downcast_ref::<Utf8Array<$offset_type>>()
                    .ok_or_else(|| error!("cannot interpret array as Utf8 array"))?;

                let buffer = buffers.push_u8(typed.values().as_slice());
                let offsets = buffers.$push_func(typed.offsets().as_slice())?;
                let validity = get_validity(typed).map(|v| buffers.push_u1(v));

                Ok(M::$variant {
                    field: field.clone(),
                    validity,
                    buffer,
                    offsets,
                })
            }};
        }

        macro_rules! convert_list {
            ($offset_type:ty, $variant:ident, $push_func:ident) => {{
                let typed = self
                    .as_any()
                    .downcast_ref::<ListArray<$offset_type>>()
                    .ok_or_else(|| error!("cannot interpret array as LargeList array"))?;

                let validity = get_validity(typed);
                let offsets = typed.offsets();
                let offsets = offsets.as_slice();

                check_supported_list_layout(validity, offsets)?;

                let offsets = buffers.$push_func(offsets)?;
                let validity = validity.map(|v| buffers.push_u1(v));

                let item_field = field
                    .children
                    .get(0)
                    .ok_or_else(|| error!("cannot get first child of list array"))?;
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
                    .ok_or_else(|| error!("cannot interpret array as Bool array"))?;

                let (data, offset, number_of_bits) = typed.values().as_slice();
                let buffer = buffers.push_u1(BitBuffer {
                    data,
                    offset,
                    number_of_bits,
                });
                let validity = get_validity(typed).map(|v| buffers.push_u1(v));

                Ok(M::Bool {
                    field: field.clone(),
                    validity,
                    buffer,
                })
            }
            T::U8 => convert_primitive!(u8, U8, push_u8_cast),
            T::U16 => convert_primitive!(u16, U16, push_u16_cast),
            T::U32 => convert_primitive!(u32, U32, push_u32_cast),
            T::U64 => convert_primitive!(u64, U64, push_u64_cast),
            T::I8 => convert_primitive!(i8, I8, push_u8_cast),
            T::I16 => convert_primitive!(i16, I16, push_u16_cast),
            T::I32 => convert_primitive!(i32, I32, push_u32_cast),
            T::I64 => convert_primitive!(i64, I64, push_u64_cast),
            T::F16 => convert_primitive!(f16, F16, push_u16_cast),
            T::F32 => convert_primitive!(f32, F32, push_u32_cast),
            T::F64 => convert_primitive!(f64, F64, push_u64_cast),
            T::Date64 => convert_primitive!(i64, Date64, push_u64_cast),
            T::Timestamp(_, _) => convert_primitive!(i64, Date64, push_u64_cast),
            T::Utf8 => convert_utf8!(i32, Utf8, push_u32_cast),
            T::LargeUtf8 => convert_utf8!(i64, LargeUtf8, push_u64_cast),
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
                let entries_field = field
                    .children
                    .get(0)
                    .ok_or_else(|| error!("cannot get children of map"))?;
                let keys_field = entries_field
                    .children
                    .get(0)
                    .ok_or_else(|| error!("cannot get keys field"))?;
                let values_field = entries_field
                    .children
                    .get(1)
                    .ok_or_else(|| error!("cannot get values field"))?;

                let typed = self
                    .as_any()
                    .downcast_ref::<MapArray>()
                    .ok_or_else(|| error!("cannot convert array into map array"))?;
                let typed_entries = typed
                    .field()
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(|| error!("cannot convert map field into struct array"))?;
                let typed_keys = typed_entries
                    .values()
                    .get(0)
                    .ok_or_else(|| error!("cannot get keys array of map entries"))?;
                let typed_values = typed_entries
                    .values()
                    .get(1)
                    .ok_or_else(|| error!("cannot get keys array of map entries"))?;

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
                let keys_field = field
                    .children
                    .get(0)
                    .ok_or_else(|| error!("cannot get key field of dictionary"))?;
                let values_field = field
                    .children
                    .get(1)
                    .ok_or_else(|| error!("cannot get values field"))?;

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
