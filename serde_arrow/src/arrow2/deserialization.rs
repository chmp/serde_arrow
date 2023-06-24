use crate::_impl::arrow2::{
    array::{Array, PrimitiveArray},
    types::f16,
};
use crate::{
    internal::{
        common::{ArrayMapping, BitBuffer, BufferExtract, Buffers},
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
            ($this:expr, $array_type:ty, $variant:ident, $push_func:ident) => {{
                let typed = $this
                    .as_any()
                    .downcast_ref::<PrimitiveArray<$array_type>>()
                    .ok_or_else(|| error!("Cannot interpret array as I32 array"))?;

                let buffer = buffers.$push_func(typed.values().as_slice())?;
                let validity = get_validity(typed).map(|v| buffers.push_u1(v));

                Ok(ArrayMapping::$variant {
                    field: field.clone(),
                    buffer,
                    validity,
                })
            }};
        }

        use GenericDataType as T;

        match &field.data_type {
            T::U8 => convert_primitive!(self, u8, U8, push_u8_cast),
            T::U16 => convert_primitive!(self, u16, U16, push_u16_cast),
            T::U32 => convert_primitive!(self, u32, U32, push_u32_cast),
            T::U64 => convert_primitive!(self, u64, U64, push_u64_cast),
            T::I8 => convert_primitive!(self, i8, I8, push_u8_cast),
            T::I16 => convert_primitive!(self, i16, I16, push_u16_cast),
            T::I32 => convert_primitive!(self, i32, I32, push_u32_cast),
            T::I64 => convert_primitive!(self, i64, I64, push_u64_cast),
            T::F16 => convert_primitive!(self, f16, F16, push_u16_cast),
            T::F32 => convert_primitive!(self, f32, F32, push_u32_cast),
            T::F64 => convert_primitive!(self, f64, F64, push_u64_cast),
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
