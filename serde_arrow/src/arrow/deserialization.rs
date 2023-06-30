use crate::_impl::arrow::array::Array;
use crate::internal::common::BitBuffer;
use crate::internal::{
    common::{ArrayMapping, BufferExtract, Buffers},
    error::{error, fail, Result},
    schema::{GenericDataType, GenericField},
};

use crate::_impl::arrow::{
    array::{
        ArrowPrimitiveType, BooleanArray, GenericListArray, LargeStringArray, PrimitiveArray,
        StringArray, StructArray,
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
            ($this:expr, $arrow_type:ty, $variant:ident, $push_func:ident) => {{
                let typed = downcast_primitive_array::<$arrow_type>($this)?;
                let buffer = buffers.$push_func(typed.values())?;
                let validity = get_validity($this).map(|v| buffers.push_u1(v));

                Ok(M::$variant {
                    field: field.clone(),
                    buffer,
                    validity,
                })
            }};
        }

        macro_rules! convert_utf8 {
            ($this:expr, $array_type:ty, $variant:ident, $push_func:ident) => {{
                let typed = $this
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
            ($this:expr, $offset_type:ty, $variant:ident, $push_func:ident) => {{
                let typed = $this
                    .as_any()
                    .downcast_ref::<GenericListArray<$offset_type>>()
                    .ok_or_else(|| error!("cannot convert array into GenericListArray<i64>"))?;

                let offsets = buffers.$push_func(typed.value_offsets())?;
                let validity = get_validity(self).map(|v| buffers.push_u1(v));

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
            T::U8 => convert_primitive!(self, UInt8Type, U8, push_u8_cast),
            T::U16 => convert_primitive!(self, UInt16Type, U16, push_u16_cast),
            T::U32 => convert_primitive!(self, UInt32Type, U32, push_u32_cast),
            T::U64 => convert_primitive!(self, UInt64Type, U64, push_u64_cast),
            T::I8 => convert_primitive!(self, Int8Type, I8, push_u8_cast),
            T::I16 => convert_primitive!(self, Int16Type, I16, push_u16_cast),
            T::I32 => convert_primitive!(self, Int32Type, I32, push_u32_cast),
            T::I64 => convert_primitive!(self, Int64Type, I64, push_u64_cast),
            T::F16 => convert_primitive!(self, Float16Type, F16, push_u16_cast),
            T::F32 => convert_primitive!(self, Float32Type, F32, push_u32_cast),
            T::F64 => convert_primitive!(self, Float64Type, F64, push_u64_cast),
            T::Date64 => convert_primitive!(self, Date64Type, Date64, push_u64_cast),
            T::Utf8 => convert_utf8!(self, StringArray, Utf8, push_u32_cast),
            T::LargeUtf8 => convert_utf8!(self, LargeStringArray, LargeUtf8, push_u64_cast),
            T::List => convert_list!(self, i32, List, push_u32_cast),
            T::LargeList => convert_list!(self, i64, LargeList, push_u64_cast),
            T::Struct => {
                let typed = self
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(|| error!("cannot convert array into string"))?;
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
            dt => fail!("BufferExtract for {dt} is not implemented"),
        }
    }
}

fn downcast_primitive_array<T: ArrowPrimitiveType>(arr: &dyn Array) -> Result<&PrimitiveArray<T>> {
    arr.as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .ok_or_else(|| error!("Cannot interpret array as typed array"))
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
