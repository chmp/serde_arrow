use crate::{
    _impl::arrow2::{
        array::{Array as A2Array, NullArray as A2NullArray, PrimitiveArray as A2PrimitiveArray},
        bitmap::Bitmap,
        buffer::Buffer,
        datatypes::DataType,
        types::{f16, NativeType},
    },
    internal::{
        arrow::Array,
        error::{fail, Error, Result},
    },
};

impl TryFrom<Array> for Box<dyn A2Array> {
    type Error = Error;

    fn try_from(value: Array) -> Result<Self> {
        use {Array as A, DataType as T};
        match value {
            A::Null(arr) => Ok(Box::new(A2NullArray::new(T::Null, arr.len))),
            A::Int8(arr) => build_primitive_array(T::Int8, arr.values, arr.validity),
            A::Int16(arr) => build_primitive_array(T::Int16, arr.values, arr.validity),
            A::Int32(arr) => build_primitive_array(T::Int32, arr.values, arr.validity),
            A::Int64(arr) => build_primitive_array(T::Int64, arr.values, arr.validity),
            A::UInt8(arr) => build_primitive_array(T::UInt8, arr.values, arr.validity),
            A::UInt16(arr) => build_primitive_array(T::UInt16, arr.values, arr.validity),
            A::UInt32(arr) => build_primitive_array(T::UInt32, arr.values, arr.validity),
            A::UInt64(arr) => build_primitive_array(T::UInt64, arr.values, arr.validity),
            A::Float16(arr) => build_primitive_array(
                T::Float16,
                arr.values
                    .into_iter()
                    .map(|v| f16::from_bits(v.to_bits()))
                    .collect(),
                arr.validity,
            ),
            A::Float32(arr) => build_primitive_array(T::Float32, arr.values, arr.validity),
            A::Float64(arr) => build_primitive_array(T::Float64, arr.values, arr.validity),
            _ => fail!("cannot convert array to arrow2 array"),
        }
    }
}

fn build_primitive_array<T: NativeType>(
    data_type: DataType,
    buffer: Vec<T>,
    validity: Option<Vec<u8>>,
) -> Result<Box<dyn A2Array>> {
    let validity = validity.map(|v| Bitmap::from_u8_vec(v, buffer.len()));
    let buffer = Buffer::from(buffer);
    Ok(Box::new(A2PrimitiveArray::try_new(
        data_type, buffer, validity,
    )?))
}
