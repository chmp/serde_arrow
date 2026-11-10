// Implement the api starting from `arrow=47`
use crate::{
    array::BytesViewArray,
    view::{BytesViewView, FixedSizeBinaryView},
};

#[inline]
fn convert_array_to_marrow(array: &dyn arrow_array::Array) -> Result<View<'_>> {
    let any = array.as_any();
    if let Some(array) = any.downcast_ref::<arrow_array::FixedSizeBinaryArray>() {
        Ok(View::FixedSizeBinary(
            convert_fixed_size_binary_array_to_marrow(array),
        ))
    } else if let Some(array) = any.downcast_ref::<arrow_array::StringViewArray>() {
        Ok(View::Utf8View(convert_generic_bytes_view_array_to_marrow(
            array,
        )))
    } else if let Some(array) = any.downcast_ref::<arrow_array::BinaryViewArray>() {
        Ok(View::BinaryView(
            convert_generic_bytes_view_array_to_marrow(array),
        ))
    } else {
        fail!(
            ErrorKind::Unsupported,
            "Cannot build an array view for {dt}",
            dt = array.data_type()
        );
    }
}

fn convert_fixed_size_binary_array_to_marrow(
    array: &arrow_array::FixedSizeBinaryArray,
) -> FixedSizeBinaryView<'_> {
    FixedSizeBinaryView {
        n: array.value_length(),
        validity: get_bits_with_offset(array),
        data: array.value_data(),
    }
}

fn convert_generic_bytes_view_array_to_marrow<T: arrow_array::types::ByteViewType>(
    array: &arrow_array::GenericByteViewArray<T>,
) -> BytesViewView<'_> {
    let mut buffers = Vec::<&[u8]>::new();
    for buffer in array.data_buffers().iter() {
        buffers.push(buffer);
    }
    BytesViewView {
        validity: get_bits_with_offset(array),
        data: array.views(),
        buffers,
    }
}

#[inline]
fn build_array_data_from_marrow(
    array: Array,
    skip_validation: bool,
) -> Result<arrow_data::ArrayData> {
    match array {
        Array::Utf8View(array) => build_binary_view_array_data_from_marrow(
            arrow_schema::DataType::Utf8View,
            array,
            skip_validation,
        ),
        Array::BinaryView(array) => build_binary_view_array_data_from_marrow(
            arrow_schema::DataType::BinaryView,
            array,
            false,
        ),
        array => fail!(
            ErrorKind::Unsupported,
            "Cannot build an array for {dt:?}",
            dt = array.data_type()
        ),
    }
}

fn build_binary_view_array_data_from_marrow(
    data_type: arrow_schema::DataType,
    array: BytesViewArray,
    skip_validation: bool,
) -> Result<arrow_data::ArrayData> {
    let len = array.data.len();
    let mut buffers = vec![arrow_buffer::ScalarBuffer::from(array.data).into_inner()];
    for buffer in array.buffers {
        buffers.push(arrow_buffer::Buffer::from_vec(buffer));
    }

    let validity = array.validity.map(arrow_buffer::Buffer::from_vec);
    if skip_validation {
        // SAFETY: The caller guarantees valid Utf8View descriptors, referenced
        // UTF-8 data and sufficient validity bits.
        return Ok(unsafe {
            arrow_data::ArrayData::new_unchecked(data_type, len, None, validity, 0, buffers, vec![])
        });
    }
    Ok(arrow_data::ArrayData::try_new(
        data_type,
        len,
        validity,
        0,
        buffers,
        vec![],
    )?)
}

#[inline]
fn convert_data_type_to_marrow(
    data_type: &arrow_schema::DataType,
) -> Result<crate::datatypes::DataType> {
    match data_type {
        arrow_schema::DataType::Utf8View => Ok(crate::datatypes::DataType::Utf8View),
        arrow_schema::DataType::BinaryView => Ok(crate::datatypes::DataType::BinaryView),
        data_type => fail!(
            ErrorKind::Unsupported,
            "Unsupported arrow data type {data_type}"
        ),
    }
}

#[inline]
fn convert_data_type_from_marrow(data_type: &DataType) -> Result<arrow_schema::DataType> {
    use DataType as T;

    match data_type {
        T::Utf8View => Ok(arrow_schema::DataType::Utf8View),
        T::BinaryView => Ok(arrow_schema::DataType::BinaryView),
        data_type => fail!(
            ErrorKind::Unsupported,
            "Unsupported data type {data_type:?}",
        ),
    }
}

include!("impl_api_base.rs");
