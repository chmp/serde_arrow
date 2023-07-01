use crate::_impl::arrow2::{
    array::{Array, BooleanArray, ListArray, PrimitiveArray, StructArray, Utf8Array},
    datatypes::DataType,
    types::f16,
};
use crate::{
    internal::{
        common::{check_supported_list_layout, ArrayMapping, BitBuffer, BufferExtract, Buffers},
        error::{error, fail},
        schema::{GenericDataType, GenericField},
    },
    Result,
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
            dt => fail!("BufferExtract for {dt} is not implemented"),
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
