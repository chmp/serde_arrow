use crate::{
    _impl::arrow2::{
        array::{
            Array as A2Array, BinaryArray, BooleanArray, DictionaryArray, DictionaryKey, ListArray,
            MapArray, NullArray, PrimitiveArray, StructArray, UnionArray, Utf8Array,
        },
        bitmap::Bitmap,
        buffer::Buffer,
        datatypes::{DataType, Field, IntegerType, UnionMode},
        types::{f16, NativeType, Offset},
    },
    internal::{
        arrow::{
            Array, ArrayView, BitsWithOffset, BooleanArrayView, BytesArrayView, DecimalArrayView,
            FieldMeta, NullArrayView, PrimitiveArray as InternalPrimitiveArray, PrimitiveArrayView,
            TimeArrayView, TimestampArrayView,
        },
        error::{fail, Error, Result},
    },
};

impl TryFrom<Array> for Box<dyn A2Array> {
    type Error = Error;

    fn try_from(value: Array) -> Result<Self> {
        use {Array as A, DataType as T, IntegerType as I};
        match value {
            A::Null(arr) => Ok(Box::new(NullArray::new(T::Null, arr.len))),
            A::Boolean(arr) => Ok(Box::new(BooleanArray::try_new(
                T::Boolean,
                Bitmap::from_u8_vec(arr.values, arr.len),
                arr.validity.map(|v| Bitmap::from_u8_vec(v, arr.len)),
            )?)),
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
            A::Date32(arr) => build_primitive_array(T::Date32, arr.values, arr.validity),
            A::Date64(arr) => build_primitive_array(T::Date64, arr.values, arr.validity),
            A::Duration(arr) => {
                build_primitive_array(T::Duration(arr.unit.into()), arr.values, arr.validity)
            }
            A::Time32(arr) => {
                build_primitive_array(T::Time32(arr.unit.into()), arr.values, arr.validity)
            }
            A::Time64(arr) => {
                build_primitive_array(T::Time64(arr.unit.into()), arr.values, arr.validity)
            }
            A::Timestamp(arr) => build_primitive_array(
                T::Timestamp(arr.unit.into(), arr.timezone),
                arr.values,
                arr.validity,
            ),
            A::Decimal128(arr) => build_primitive_array(
                T::Decimal(arr.precision as usize, usize::try_from(arr.scale)?),
                arr.values,
                arr.validity,
            ),
            A::Utf8(arr) => build_utf8_array(T::Utf8, arr.offsets, arr.data, arr.validity),
            A::LargeUtf8(arr) => {
                build_utf8_array(T::LargeUtf8, arr.offsets, arr.data, arr.validity)
            }
            A::Binary(arr) => build_binary_array(T::Binary, arr.offsets, arr.data, arr.validity),
            A::LargeBinary(arr) => {
                build_binary_array(T::LargeBinary, arr.offsets, arr.data, arr.validity)
            }
            A::Dictionary(arr) => match *arr.indices {
                A::Int8(indices) => build_dictionary_array(I::Int8, indices, *arr.values),
                A::Int16(indices) => build_dictionary_array(I::Int16, indices, *arr.values),
                A::Int32(indices) => build_dictionary_array(I::Int32, indices, *arr.values),
                A::Int64(indices) => build_dictionary_array(I::Int64, indices, *arr.values),
                A::UInt8(indices) => build_dictionary_array(I::UInt8, indices, *arr.values),
                A::UInt16(indices) => build_dictionary_array(I::UInt16, indices, *arr.values),
                A::UInt32(indices) => build_dictionary_array(I::UInt32, indices, *arr.values),
                A::UInt64(indices) => build_dictionary_array(I::UInt64, indices, *arr.values),
                // TODO: improve error message by including the data type
                _ => fail!("unsupported dictionary index array during arrow2 conversion"),
            },
            A::List(arr) => build_list_array(
                T::List,
                arr.offsets,
                arr.meta,
                (*arr.element).try_into()?,
                arr.validity,
            ),
            A::LargeList(arr) => build_list_array(
                T::LargeList,
                arr.offsets,
                arr.meta,
                (*arr.element).try_into()?,
                arr.validity,
            ),
            A::Struct(arr) => {
                let (values, fields) = array_with_meta_to_array_and_fields(arr.fields)?;
                Ok(Box::new(StructArray::new(
                    T::Struct(fields),
                    values,
                    arr.validity.map(|v| Bitmap::from_u8_vec(v, arr.len)),
                )))
            }
            A::Map(arr) => {
                let child: Box<dyn A2Array> = (*arr.element).try_into()?;
                let field = field_from_array_and_meta(child.as_ref(), arr.meta);
                let validity = arr
                    .validity
                    .map(|v| Bitmap::from_u8_vec(v, arr.offsets.len().saturating_sub(1)));
                Ok(Box::new(MapArray::new(
                    T::Map(Box::new(field), false),
                    arr.offsets.try_into()?,
                    child,
                    validity,
                )))
            }
            A::DenseUnion(arr) => {
                let (values, fields) = array_with_meta_to_array_and_fields(arr.fields)?;
                Ok(Box::new(UnionArray::try_new(
                    T::Union(fields, None, UnionMode::Dense),
                    arr.types.into(),
                    values,
                    Some(arr.offsets.into()),
                )?))
            }
            A::FixedSizeList(_) => fail!("FixedSizeList is not supported by arrow2"),
            A::FixedSizeBinary(_) => fail!("FixedSizeBinary is not supported by arrow2"),
        }
    }
}

impl<'a> TryFrom<&'a dyn A2Array> for ArrayView<'a> {
    type Error = Error;

    fn try_from(array: &'a dyn A2Array) -> Result<Self> {
        use {ArrayView as V, DataType as T};

        let any = array.as_any();
        if let Some(array) = any.downcast_ref::<NullArray>() {
            Ok(V::Null(NullArrayView { len: array.len() }))
        } else if let Some(array) = any.downcast_ref::<BooleanArray>() {
            let (values_data, values_offset, _) = array.values().as_slice();
            Ok(V::Boolean(BooleanArrayView {
                len: array.len(),
                validity: bits_with_offset_from_bitmap(array.validity()),
                values: BitsWithOffset {
                    offset: values_offset,
                    data: values_data,
                },
            }))
        } else if let Some(array) = any.downcast_ref::<PrimitiveArray<i8>>() {
            Ok(V::Int8(view_primitive_array(array)))
        } else if let Some(array) = any.downcast_ref::<PrimitiveArray<i16>>() {
            Ok(V::Int16(view_primitive_array(array)))
        } else if let Some(array) = any.downcast_ref::<PrimitiveArray<i32>>() {
            match array.data_type() {
                T::Int32 => Ok(V::Int32(view_primitive_array(array))),
                T::Date32 => Ok(V::Date32(view_primitive_array(array))),
                T::Time32(unit) => Ok(V::Time32(TimeArrayView {
                    unit: (*unit).into(),
                    validity: bits_with_offset_from_bitmap(array.validity()),
                    values: array.values().as_slice(),
                })),
                dt => fail!("unsupported data type {dt:?} for i32 arrow2 array"),
            }
        } else if let Some(array) = any.downcast_ref::<PrimitiveArray<i64>>() {
            match array.data_type() {
                T::Int64 => Ok(V::Int64(view_primitive_array(array))),
                T::Date64 => Ok(V::Date64(view_primitive_array(array))),
                T::Timestamp(unit, tz) => Ok(V::Timestamp(TimestampArrayView {
                    unit: (*unit).into(),
                    timezone: tz.to_owned(),
                    validity: bits_with_offset_from_bitmap(array.validity()),
                    values: array.values().as_slice(),
                })),
                T::Time64(unit) => Ok(V::Time64(TimeArrayView {
                    unit: (*unit).into(),
                    validity: bits_with_offset_from_bitmap(array.validity()),
                    values: array.values().as_slice(),
                })),
                T::Duration(unit) => Ok(V::Duration(TimeArrayView {
                    unit: (*unit).into(),
                    validity: bits_with_offset_from_bitmap(array.validity()),
                    values: array.values().as_slice(),
                })),
                dt => fail!("unsupported data type {dt:?} for i64 arrow2 array"),
            }
        } else if let Some(array) = any.downcast_ref::<PrimitiveArray<i128>>() {
            match array.data_type() {
                T::Decimal(precision, scale) => Ok(V::Decimal128(DecimalArrayView {
                    precision: (*precision).try_into()?,
                    scale: (*scale).try_into()?,
                    validity: bits_with_offset_from_bitmap(array.validity()),
                    values: array.values().as_slice(),
                })),
                dt => fail!("unsupported data type {dt:?} for i128 arrow2 array"),
            }
        } else if let Some(array) = any.downcast_ref::<PrimitiveArray<u8>>() {
            Ok(V::UInt8(view_primitive_array(array)))
        } else if let Some(array) = any.downcast_ref::<PrimitiveArray<u16>>() {
            Ok(V::UInt16(view_primitive_array(array)))
        } else if let Some(array) = any.downcast_ref::<PrimitiveArray<u32>>() {
            Ok(V::UInt32(view_primitive_array(array)))
        } else if let Some(array) = any.downcast_ref::<PrimitiveArray<u64>>() {
            Ok(V::UInt64(view_primitive_array(array)))
        } else if let Some(array) = any.downcast_ref::<PrimitiveArray<f16>>() {
            Ok(V::Float16(PrimitiveArrayView {
                values: bytemuck::cast_slice::<f16, half::f16>(array.values().as_slice()),
                validity: bits_with_offset_from_bitmap(array.validity()),
            }))
        } else if let Some(array) = any.downcast_ref::<PrimitiveArray<f32>>() {
            Ok(V::Float32(view_primitive_array(array)))
        } else if let Some(array) = any.downcast_ref::<PrimitiveArray<f64>>() {
            Ok(V::Float64(view_primitive_array(array)))
        } else if let Some(array) = any.downcast_ref::<Utf8Array<i32>>() {
            Ok(V::Utf8(BytesArrayView {
                validity: bits_with_offset_from_bitmap(array.validity()),
                offsets: array.offsets().as_slice(),
                data: array.values().as_slice(),
            }))
        } else if let Some(array) = any.downcast_ref::<Utf8Array<i64>>() {
            Ok(V::LargeUtf8(BytesArrayView {
                validity: bits_with_offset_from_bitmap(array.validity()),
                offsets: array.offsets().as_slice(),
                data: array.values().as_slice(),
            }))
        } else if let Some(array) = any.downcast_ref::<BinaryArray<i32>>() {
            Ok(V::Binary(BytesArrayView {
                validity: bits_with_offset_from_bitmap(array.validity()),
                offsets: array.offsets().as_slice(),
                data: array.values().as_slice(),
            }))
        } else if let Some(array) = any.downcast_ref::<BinaryArray<i64>>() {
            Ok(V::LargeBinary(BytesArrayView {
                validity: bits_with_offset_from_bitmap(array.validity()),
                offsets: array.offsets().as_slice(),
                data: array.values().as_slice(),
            }))
        } else {
            fail!(
                "Cannot convert array with data type {:?} into an array view",
                array.data_type()
            );
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
    Ok(Box::new(PrimitiveArray::try_new(
        data_type, buffer, validity,
    )?))
}

fn build_utf8_array<O: Offset>(
    data_type: DataType,
    offsets: Vec<O>,
    data: Vec<u8>,
    validity: Option<Vec<u8>>,
) -> Result<Box<dyn A2Array>> {
    let validity = validity.map(|v| Bitmap::from_u8_vec(v, offsets.len().saturating_sub(1)));
    Ok(Box::new(Utf8Array::new(
        data_type,
        offsets.try_into()?,
        Buffer::from(data),
        validity,
    )))
}

fn build_binary_array<O: Offset>(
    data_type: DataType,
    offsets: Vec<O>,
    data: Vec<u8>,
    validity: Option<Vec<u8>>,
) -> Result<Box<dyn A2Array>> {
    let validity = validity.map(|v| Bitmap::from_u8_vec(v, offsets.len().saturating_sub(1)));
    Ok(Box::new(BinaryArray::new(
        data_type,
        offsets.try_into()?,
        Buffer::from(data),
        validity,
    )))
}

fn build_list_array<F: FnOnce(Box<Field>) -> DataType, O: Offset>(
    data_type: F,
    offsets: Vec<O>,
    meta: FieldMeta,
    values: Box<dyn A2Array>,
    validity: Option<Vec<u8>>,
) -> Result<Box<dyn A2Array>> {
    let validity = validity.map(|v| Bitmap::from_u8_vec(v, offsets.len().saturating_sub(1)));
    Ok(Box::new(ListArray::new(
        data_type(Box::new(field_from_array_and_meta(values.as_ref(), meta))),
        offsets.try_into()?,
        values,
        validity,
    )))
}

fn field_from_array_and_meta(arr: &dyn A2Array, meta: FieldMeta) -> Field {
    Field::new(meta.name, arr.data_type().clone(), meta.nullable)
        .with_metadata(meta.metadata.into_iter().collect())
}

fn array_with_meta_to_array_and_fields(
    arrays: Vec<(Array, FieldMeta)>,
) -> Result<(Vec<Box<dyn A2Array>>, Vec<Field>)> {
    let mut res_fields = Vec::new();
    let mut res_arrays = Vec::new();

    for (child, meta) in arrays {
        let child: Box<dyn A2Array> = child.try_into()?;
        let field = field_from_array_and_meta(child.as_ref(), meta);

        res_arrays.push(child);
        res_fields.push(field);
    }

    Ok((res_arrays, res_fields))
}

fn build_dictionary_array<K: DictionaryKey>(
    indices_type: IntegerType,
    indices: InternalPrimitiveArray<K>,
    values: Array,
) -> Result<Box<dyn A2Array>> {
    let values: Box<dyn A2Array> = values.try_into()?;
    let validity = indices
        .validity
        .map(|v| Bitmap::from_u8_vec(v, indices.values.len()));
    let keys = PrimitiveArray::new(indices_type.into(), indices.values.into(), validity);

    Ok(Box::new(DictionaryArray::try_new(
        DataType::Dictionary(indices_type, Box::new(values.data_type().clone()), false),
        keys,
        values,
    )?))
}

fn view_primitive_array<T: NativeType>(array: &PrimitiveArray<T>) -> PrimitiveArrayView<'_, T> {
    PrimitiveArrayView {
        values: array.values().as_slice(),
        validity: bits_with_offset_from_bitmap(array.validity()),
    }
}

fn bits_with_offset_from_bitmap(bitmap: Option<&Bitmap>) -> Option<BitsWithOffset<'_>> {
    let (data, offset, _) = bitmap?.as_slice();
    Some(BitsWithOffset { data, offset })
}