//! Build arrow2 arrays from individual buffers
//!

use crate::_impl::arrow2::{
    array::{Array, BooleanArray, PrimitiveArray},
    bitmap::Bitmap,
    buffer::Buffer,
    datatypes::DataType,
};

use crate::internal::{
    bytecode::{compiler::ArrayMapping, interpreter::Buffers, Interpreter},
    error::{fail, Result},
};

impl Interpreter {
    /// Build the arrow arrays
    pub fn build_arrow2_arrays(&mut self) -> Result<Vec<Box<dyn Array>>> {
        let mut res = Vec::new();
        for mapping in &self.structure.array_mapping {
            let array = build_array(&mut self.buffers, mapping)?;
            res.push(array);
        }
        Ok(res)
    }
}

macro_rules! build_array_primitive {
    ($buffers:expr, $array:ident, $variant:ident, $buffer:expr, $validity:expr) => {{
        let buffer = std::mem::take(&mut $buffers.$array[*$buffer]);
        let validity = $validity.map(|val| std::mem::take(&mut $buffers.validity[val]));
        let validity = validity.map(|val| Bitmap::from_u8_vec(val.buffer, val.len));
        let array =
            PrimitiveArray::try_new(DataType::$variant, Buffer::from(buffer.buffer), validity)?;
        Ok(Box::new(array))
    }};
}

fn build_array(buffers: &mut Buffers, mapping: &ArrayMapping) -> Result<Box<dyn Array>> {
    use ArrayMapping as M;
    match mapping {
        M::Bool {
            buffer, validity, ..
        } => {
            let buffer = std::mem::take(&mut buffers.bool[*buffer]);
            let buffer = Bitmap::from_u8_vec(buffer.buffer, buffer.len);
            let validity = validity.map(|val| std::mem::take(&mut buffers.validity[val]));
            let validity = validity.map(|val| Bitmap::from_u8_vec(val.buffer, val.len));
            let array = BooleanArray::try_new(DataType::Boolean, buffer, validity)?;
            Ok(Box::new(array))
        }
        M::U8 {
            buffer, validity, ..
        } => build_array_primitive!(buffers, u8, UInt8, buffer, validity),
        M::U16 {
            buffer, validity, ..
        } => build_array_primitive!(buffers, u16, UInt16, buffer, validity),
        M::U32 {
            buffer, validity, ..
        } => build_array_primitive!(buffers, u32, UInt32, buffer, validity),
        M::U64 {
            buffer, validity, ..
        } => build_array_primitive!(buffers, u64, UInt64, buffer, validity),
        M::I8 {
            buffer, validity, ..
        } => build_array_primitive!(buffers, i8, Int8, buffer, validity),
        M::I16 {
            buffer, validity, ..
        } => build_array_primitive!(buffers, i16, Int16, buffer, validity),
        M::I32 {
            buffer, validity, ..
        } => build_array_primitive!(buffers, i32, Int32, buffer, validity),
        M::I64 {
            buffer, validity, ..
        } => build_array_primitive!(buffers, i64, Int64, buffer, validity),
        M::F32 {
            buffer, validity, ..
        } => build_array_primitive!(buffers, f32, Float32, buffer, validity),
        M::F64 {
            buffer, validity, ..
        } => build_array_primitive!(buffers, f64, Float64, buffer, validity),
        _ => fail!("Not implemented"),
    }
}
