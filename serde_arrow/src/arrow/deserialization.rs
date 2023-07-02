use crate::_impl::arrow::array::Array;
use crate::internal::common::{BitBuffer, DictionaryIndex, DictionaryValue};
use crate::internal::{
    common::{check_supported_list_layout, ArrayMapping, BufferExtract, Buffers},
    error::{error, fail, Result},
    schema::{GenericDataType, GenericField},
};

use crate::_impl::arrow::{
    array::{
        BooleanArray, DictionaryArray, GenericListArray, LargeStringArray, MapArray,
        PrimitiveArray, StringArray, StructArray,
    },
    datatypes::{
        DataType, Date64Type, Float16Type, Float32Type, Float64Type, Int16Type, Int32Type,
        Int64Type, Int8Type, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
    },
};

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
                    .ok_or_else(|| error!("Cannot interpret array as typed array"))?;

                let buffer = buffers.$push_func(typed.values())?;
                let validity = get_validity(typed).map(|v| buffers.push_u1(v));

                Ok(M::$variant {
                    field: field.clone(),
                    buffer,
                    validity,
                })
            }};
        }

        macro_rules! convert_utf8 {
            ($array_type:ty, $variant:ident, $push_func:ident) => {{
                let typed = self
                    .as_any()
                    .downcast_ref::<$array_type>()
                    .ok_or_else(|| error!("cannot convert array into string"))?;

                let buffer = buffers.push_u8(typed.value_data());
                let offsets = buffers.$push_func(typed.value_offsets())?;
                let validity = get_validity(self).map(|v| buffers.push_u1(v));

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
                    .downcast_ref::<GenericListArray<$offset_type>>()
                    .ok_or_else(|| error!("cannot convert array into GenericListArray<i64>"))?;

                let offsets = typed.value_offsets();
                let validity = get_validity(self);

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
                    .ok_or_else(|| error!("cannot convert array into bool"))?;
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
            T::Utf8 => convert_utf8!(StringArray, Utf8, push_u32_cast),
            T::LargeUtf8 => convert_utf8!(LargeStringArray, LargeUtf8, push_u64_cast),
            T::List => convert_list!(i32, List, push_u32_cast),
            T::LargeList => convert_list!(i64, LargeList, push_u64_cast),
            T::Struct => {
                let typed = self
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(|| error!("cannot convert array into struct array"))?;
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
            #[cfg(has_arrow_36)]
            T::Union => fail!("unions are not supported for arrow=36"),
            #[cfg(not(has_arrow_36))]
            T::Union => {
                use crate::_impl::arrow::array::UnionArray;

                // TODO: test assumptions
                let typed = self
                    .as_any()
                    .downcast_ref::<UnionArray>()
                    .ok_or_else(|| error!("cannot convert array to union array"))?;

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
