use crate::_impl::arrow::array::Array;
use crate::internal::{
    common::{ArrayMapping, BufferExtract, Buffers},
    error::{error, fail, Result},
    schema::{GenericDataType, GenericField},
};

use crate::_impl::arrow::{
    array::PrimitiveArray,
    datatypes::{Float16Type, Int32Type},
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
        match &field.data_type {
            GenericDataType::I32 => {
                if field.nullable {
                    fail!("nullable fields are not yet supported");
                }

                let data = self
                    .as_any()
                    .downcast_ref::<PrimitiveArray<Int32Type>>()
                    .ok_or_else(|| error!("Cannot interpret array as I32 array"))?
                    .values();
                let data: &[u32] = bytemuck::try_cast_slice(data).unwrap();

                let buffer = buffers.u32.len();
                buffers.u32.push(data);

                Ok(ArrayMapping::I32 {
                    field: field.clone(),
                    buffer,
                    validity: None,
                })
            }
            GenericDataType::F16 => {
                if field.nullable {
                    fail!("nullable fields are not yet supported");
                }

                let data = self
                    .as_any()
                    .downcast_ref::<PrimitiveArray<Float16Type>>()
                    .ok_or_else(|| error!("Cannot interpret array as F16 array"))?
                    .values();
                let data: &[u16] = bytemuck::try_cast_slice(data).unwrap();

                let buffer = buffers.u16.len();
                buffers.u16.push(data);

                Ok(ArrayMapping::F16 {
                    field: field.clone(),
                    buffer,
                    validity: None,
                })
            }
            dt => fail!("BufferExtract for {dt} is not implemented"),
        }
    }
}
