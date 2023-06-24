use crate::{
    internal::{
        common::{ArrayMapping, BufferExtract, Buffers},
        error::{error, fail},
        schema::{GenericDataType, GenericField},
    },
    Result,
};

impl<T> BufferExtract for T
where
    T: AsRef<dyn crate::_impl::arrow2::array::Array>,
{
    fn len(&self) -> usize {
        self.as_ref().len()
    }

    fn extract_buffers<'a>(
        &'a self,
        field: &GenericField,
        buffers: &mut Buffers<'a>,
    ) -> Result<ArrayMapping> {
        use crate::_impl::arrow2::{array::PrimitiveArray, types::f16};

        match &field.data_type {
            GenericDataType::I32 => {
                if field.nullable {
                    fail!("nullable fields are not yet supported");
                }

                let data = self
                    .as_ref()
                    .as_any()
                    .downcast_ref::<PrimitiveArray<i32>>()
                    .ok_or_else(|| error!("Cannot interpret array as I32 array"))?
                    .values()
                    .as_slice();
                let data: &[u32] = bytemuck::try_cast_slice(data)?;

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
                    .as_ref()
                    .as_any()
                    .downcast_ref::<PrimitiveArray<f16>>()
                    .ok_or_else(|| error!("Cannot interpret array as F16 array"))?
                    .values()
                    .as_slice();
                let data: &[u16] = bytemuck::try_cast_slice(data)?;

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
