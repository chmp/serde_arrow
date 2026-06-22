// Implement the api starting from `arrow=47`
use crate::view::FixedSizeBinaryView;

#[inline]
fn convert_array_to_marrow(array: &dyn arrow_array::Array) -> Result<View<'_>> {
    let any = array.as_any();
    if let Some(array) = any.downcast_ref::<arrow_array::FixedSizeBinaryArray>() {
        Ok(View::FixedSizeBinary(FixedSizeBinaryView {
            n: array.value_length(),
            validity: get_bits_with_offset(array),
            data: array.value_data(),
        }))
    } else {
        fail!(
            ErrorKind::Unsupported,
            "Cannot build an array view for {dt}",
            dt = array.data_type()
        );
    }
}

#[inline]
fn build_array_data_from_marrow(array: Array) -> Result<arrow_data::ArrayData> {
    fail!(
        ErrorKind::Unsupported,
        "Cannot build an array for {dt:?}",
        dt = array.data_type()
    );
}

#[inline]
fn convert_data_type_to_marrow(
    data_type: &arrow_schema::DataType,
) -> Result<crate::datatypes::DataType> {
    fail!(
        ErrorKind::Unsupported,
        "Unsupported arrow data type {data_type}"
    );
}

#[inline]
fn convert_data_type_from_marrow(data_type: &DataType) -> Result<arrow_schema::DataType> {
    fail!(
        ErrorKind::Unsupported,
        "Unsupported data type {data_type:?}",
    )
}

include!("impl_api_base.rs");
