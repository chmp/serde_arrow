//! Helpers to convert between arrow and arrow2 arrays
//!
use crate::_impl::{
    arrow::{
        array::Array as ArrowArray, datatypes::Field as ArrowField,
        ffi::FFI_ArrowArray as ArrowFFI_ArrowArray,
    },
    arrow2::{
        array::Array as Arrow2Array,
        datatypes::Field as Arrow2Field,
        ffi::{self as arrow2_ffi, ArrowArray as Arrow2ArrowArray},
    },
};

use crate::internal::{error::Result, schema::GenericField};

pub fn deserialize_from_arrow_array<T, A>(field: &ArrowField, array: &A) -> Result<T>
where
    A: AsRef<dyn ArrowArray>,
    T: serde::de::DeserializeOwned,
{
    use crate::arrow2::deserialize_from_array;

    let (arrow2_field, arrow2_array) = arrow_to_arrow2(field, array.as_ref())?;
    let res = deserialize_from_array(&arrow2_field, &arrow2_array)?;
    Ok(res)
}

pub fn arrow_to_arrow2(
    field: &ArrowField,
    array: &dyn ArrowArray,
) -> Result<(Arrow2Field, Box<dyn Arrow2Array>)> {
    let data = array.to_data();
    let ffi_array = ArrowFFI_ArrowArray::new(&data);

    let ffi_array: Arrow2ArrowArray = unsafe { std::mem::transmute(ffi_array) };

    let generic_field: GenericField = field.try_into()?;
    let arrow2_field: Arrow2Field = (&generic_field).try_into()?;

    let arrow2_array =
        unsafe { arrow2_ffi::import_array_from_c(ffi_array, arrow2_field.data_type.clone()) }?;

    Ok((arrow2_field, arrow2_array))
}
