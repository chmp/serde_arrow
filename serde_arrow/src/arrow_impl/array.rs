//! Convert between arrow arrays and the internal array representation
use std::sync::Arc;

use half::f16;

use crate::{
    _impl::arrow::{
        array::{
            make_array, Array, ArrayData, ArrayRef, BooleanArray, DictionaryArray,
            FixedSizeBinaryArray, FixedSizeListArray, GenericBinaryArray, GenericListArray,
            GenericStringArray, MapArray, NullArray, PrimitiveArray, StructArray, UnionArray,
        },
        buffer::{Buffer, ScalarBuffer},
        datatypes::{
            ArrowDictionaryKeyType, ArrowNativeType, ArrowPrimitiveType, DataType, Date32Type,
            Date64Type, Decimal128Type, DurationMicrosecondType, DurationMillisecondType,
            DurationNanosecondType, DurationSecondType, Field as ArrowField, Float16Type,
            Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
            Time32MillisecondType, Time32SecondType, Time64MicrosecondType, Time64NanosecondType,
            TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
            TimestampSecondType, UInt16Type, UInt32Type, UInt64Type, UInt8Type, UnionMode,
        },
    },
    internal::{
        arrow::{
            ArrayView, BitsWithOffset, BooleanArrayView, BytesArrayView, DecimalArrayView,
            DenseUnionArrayView, DictionaryArrayView, FixedSizeListArrayView, ListArrayView,
            NullArrayView, PrimitiveArrayView, StructArrayView, TimeArrayView, TimeUnit,
            TimestampArrayView,
        },
        arrow::{Field, FieldMeta},
        error::{fail, Error, Result},
        utils::meta_from_field,
    },
};

impl TryFrom<crate::internal::arrow::Array> for ArrayRef {
    type Error = Error;

    fn try_from(value: crate::internal::arrow::Array) -> Result<Self> {
        Ok(make_array(ArrayData::try_from(value)?))
    }
}

impl TryFrom<crate::internal::arrow::Array> for ArrayData {
    type Error = Error;

    fn try_from(value: crate::internal::arrow::Array) -> Result<ArrayData> {
        use {crate::internal::arrow::Array as A, DataType as T};
        type ArrowF16 = <Float16Type as ArrowPrimitiveType>::Native;

        fn f16_to_f16(v: f16) -> ArrowF16 {
            ArrowF16::from_bits(v.to_bits())
        }

        match value {
            A::Null(arr) => Ok(NullArray::new(arr.len).into_data()),
            A::Boolean(arr) => Ok(ArrayData::try_new(
                T::Boolean,
                // NOTE: use the explicit len
                arr.len,
                arr.validity.map(Buffer::from_vec),
                0,
                vec![ScalarBuffer::from(arr.values).into_inner()],
                vec![],
            )?),
            A::Int8(arr) => primitive_into_data(T::Int8, arr.validity, arr.values),
            A::Int16(arr) => primitive_into_data(T::Int16, arr.validity, arr.values),
            A::Int32(arr) => primitive_into_data(T::Int32, arr.validity, arr.values),
            A::Int64(arr) => primitive_into_data(T::Int64, arr.validity, arr.values),
            A::UInt8(arr) => primitive_into_data(T::UInt8, arr.validity, arr.values),
            A::UInt16(arr) => primitive_into_data(T::UInt16, arr.validity, arr.values),
            A::UInt32(arr) => primitive_into_data(T::UInt32, arr.validity, arr.values),
            A::UInt64(arr) => primitive_into_data(T::UInt64, arr.validity, arr.values),
            A::Float16(arr) => primitive_into_data(
                T::Float16,
                arr.validity,
                arr.values.into_iter().map(f16_to_f16).collect(),
            ),
            A::Float32(arr) => primitive_into_data(T::Float32, arr.validity, arr.values),
            A::Float64(arr) => primitive_into_data(T::Float64, arr.validity, arr.values),
            A::Date32(arr) => primitive_into_data(T::Date32, arr.validity, arr.values),
            A::Date64(arr) => primitive_into_data(T::Date64, arr.validity, arr.values),
            A::Timestamp(arr) => primitive_into_data(
                T::Timestamp(arr.unit.into(), arr.timezone.map(String::into)),
                arr.validity,
                arr.values,
            ),
            A::Time32(arr) => {
                primitive_into_data(T::Time32(arr.unit.into()), arr.validity, arr.values)
            }
            A::Time64(arr) => {
                primitive_into_data(T::Time64(arr.unit.into()), arr.validity, arr.values)
            }
            A::Duration(arr) => {
                primitive_into_data(T::Duration(arr.unit.into()), arr.validity, arr.values)
            }
            A::Decimal128(arr) => primitive_into_data(
                T::Decimal128(arr.precision, arr.scale),
                arr.validity,
                arr.values,
            ),
            A::Utf8(arr) => bytes_into_data(T::Utf8, arr.offsets, arr.data, arr.validity),
            A::LargeUtf8(arr) => bytes_into_data(T::LargeUtf8, arr.offsets, arr.data, arr.validity),
            A::Binary(arr) => bytes_into_data(T::Binary, arr.offsets, arr.data, arr.validity),
            A::LargeBinary(arr) => {
                bytes_into_data(T::LargeBinary, arr.offsets, arr.data, arr.validity)
            }
            A::Struct(arr) => {
                let mut fields = Vec::new();
                let mut data = Vec::new();

                for (field, meta) in arr.fields {
                    let child: ArrayData = field.try_into()?;
                    let field =
                        ArrowField::new(meta.name, child.data_type().clone(), meta.nullable)
                            .with_metadata(meta.metadata);
                    fields.push(Arc::new(field));
                    data.push(child);
                }
                let data_type = T::Struct(fields.into());

                Ok(ArrayData::builder(data_type)
                    .len(arr.len)
                    .null_bit_buffer(arr.validity.map(Buffer::from_vec))
                    .child_data(data)
                    .build()?)
            }
            A::List(arr) => {
                let child: ArrayData = (*arr.element).try_into()?;
                let field = field_from_data_and_meta(&child, arr.meta);
                list_into_data(
                    T::List(Arc::new(field)),
                    arr.offsets.len().saturating_sub(1),
                    arr.offsets,
                    child,
                    arr.validity,
                )
            }
            A::LargeList(arr) => {
                let child: ArrayData = (*arr.element).try_into()?;
                let field = field_from_data_and_meta(&child, arr.meta);
                list_into_data(
                    T::LargeList(Arc::new(field)),
                    arr.offsets.len().saturating_sub(1),
                    arr.offsets,
                    child,
                    arr.validity,
                )
            }
            A::FixedSizeList(arr) => {
                let child: ArrayData = (*arr.element).try_into()?;
                if (child.len() % usize::try_from(arr.n)?) != 0 {
                    fail!(
                        "Invalid FixedSizeList: number of child elements ({}) not divisible by n ({})",
                        child.len(),
                        arr.n,
                    );
                }
                let field = field_from_data_and_meta(&child, arr.meta);
                Ok(ArrayData::try_new(
                    T::FixedSizeList(Arc::new(field), arr.n),
                    child.len() / usize::try_from(arr.n)?,
                    arr.validity.map(Buffer::from_vec),
                    0,
                    vec![],
                    vec![child],
                )?)
            }
            A::FixedSizeBinary(arr) => {
                if (arr.data.len() % usize::try_from(arr.n)?) != 0 {
                    fail!(
                        "Invalid FixedSizeBinary: number of child elements ({}) not divisible by n ({})",
                        arr.data.len(),
                        arr.n,
                    );
                }
                Ok(ArrayData::try_new(
                    T::FixedSizeBinary(arr.n),
                    arr.data.len() / usize::try_from(arr.n)?,
                    arr.validity.map(Buffer::from_vec),
                    0,
                    vec![ScalarBuffer::from(arr.data).into_inner()],
                    vec![],
                )?)
            }
            A::Dictionary(arr) => {
                let indices: ArrayData = (*arr.indices).try_into()?;
                let values: ArrayData = (*arr.values).try_into()?;
                let data_type = T::Dictionary(
                    Box::new(indices.data_type().clone()),
                    Box::new(values.data_type().clone()),
                );

                Ok(indices
                    .into_builder()
                    .data_type(data_type)
                    .child_data(vec![values])
                    .build()?)
            }
            A::Map(arr) => {
                let child: ArrayData = (*arr.element).try_into()?;
                let field = field_from_data_and_meta(&child, arr.meta);
                Ok(ArrayData::try_new(
                    T::Map(Arc::new(field), false),
                    arr.offsets.len().saturating_sub(1),
                    arr.validity.map(Buffer::from_vec),
                    0,
                    vec![ScalarBuffer::from(arr.offsets).into_inner()],
                    vec![child],
                )?)
            }
            A::DenseUnion(arr) => {
                let mut fields = Vec::new();
                let mut child_data = Vec::new();

                for (type_id, array, meta) in arr.fields {
                    let child: ArrayData = array.try_into()?;
                    let field = field_from_data_and_meta(&child, meta);

                    fields.push((type_id, Arc::new(field)));
                    child_data.push(child);
                }

                Ok(ArrayData::try_new(
                    DataType::Union(fields.into_iter().collect(), UnionMode::Dense),
                    arr.types.len(),
                    None,
                    0,
                    vec![
                        ScalarBuffer::from(arr.types).into_inner(),
                        ScalarBuffer::from(arr.offsets).into_inner(),
                    ],
                    child_data,
                )?)
            }
        }
    }
}

impl<'a> TryFrom<&'a dyn Array> for ArrayView<'a> {
    type Error = Error;

    fn try_from(array: &'a dyn Array) -> Result<Self> {
        let any = array.as_any();
        if let Some(array) = any.downcast_ref::<NullArray>() {
            Ok(ArrayView::Null(NullArrayView { len: array.len() }))
        } else if let Some(array) = any.downcast_ref::<BooleanArray>() {
            Ok(ArrayView::Boolean(BooleanArrayView {
                len: array.len(),
                validity: get_bits_with_offset(array),
                values: BitsWithOffset {
                    offset: array.values().offset(),
                    data: array.values().values(),
                },
            }))
        } else if let Some(array) = any.downcast_ref::<PrimitiveArray<Int8Type>>() {
            Ok(ArrayView::Int8(PrimitiveArrayView {
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<PrimitiveArray<Int16Type>>() {
            Ok(ArrayView::Int16(PrimitiveArrayView {
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<PrimitiveArray<Int32Type>>() {
            Ok(ArrayView::Int32(PrimitiveArrayView {
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<PrimitiveArray<Int64Type>>() {
            Ok(ArrayView::Int64(PrimitiveArrayView {
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<PrimitiveArray<UInt8Type>>() {
            Ok(ArrayView::UInt8(PrimitiveArrayView {
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<PrimitiveArray<UInt16Type>>() {
            Ok(ArrayView::UInt16(PrimitiveArrayView {
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<PrimitiveArray<UInt32Type>>() {
            Ok(ArrayView::UInt32(PrimitiveArrayView {
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<PrimitiveArray<UInt64Type>>() {
            Ok(ArrayView::UInt64(PrimitiveArrayView {
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<PrimitiveArray<Float16Type>>() {
            Ok(ArrayView::Float16(PrimitiveArrayView {
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<PrimitiveArray<Float32Type>>() {
            Ok(ArrayView::Float32(PrimitiveArrayView {
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<PrimitiveArray<Float64Type>>() {
            Ok(ArrayView::Float64(PrimitiveArrayView {
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<PrimitiveArray<Decimal128Type>>() {
            let &DataType::Decimal128(precision, scale) = array.data_type() else {
                fail!(
                    "Invalid data type for Decimal128 array: {}",
                    array.data_type()
                );
            };
            Ok(ArrayView::Decimal128(DecimalArrayView {
                precision,
                scale,
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<PrimitiveArray<Date32Type>>() {
            Ok(ArrayView::Date32(PrimitiveArrayView {
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<PrimitiveArray<Date64Type>>() {
            Ok(ArrayView::Date64(PrimitiveArrayView {
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<PrimitiveArray<Time32MillisecondType>>() {
            Ok(ArrayView::Time32(TimeArrayView {
                unit: TimeUnit::Millisecond,
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<PrimitiveArray<Time32SecondType>>() {
            Ok(ArrayView::Time32(TimeArrayView {
                unit: TimeUnit::Second,
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<PrimitiveArray<Time64NanosecondType>>() {
            Ok(ArrayView::Time64(TimeArrayView {
                unit: TimeUnit::Nanosecond,
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<PrimitiveArray<Time64MicrosecondType>>() {
            Ok(ArrayView::Time64(TimeArrayView {
                unit: TimeUnit::Microsecond,
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<PrimitiveArray<TimestampNanosecondType>>() {
            Ok(ArrayView::Timestamp(TimestampArrayView {
                unit: TimeUnit::Nanosecond,
                timezone: array.timezone().map(str::to_owned),
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<PrimitiveArray<TimestampMicrosecondType>>() {
            Ok(ArrayView::Timestamp(TimestampArrayView {
                unit: TimeUnit::Microsecond,
                timezone: array.timezone().map(str::to_owned),
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<PrimitiveArray<TimestampMillisecondType>>() {
            Ok(ArrayView::Timestamp(TimestampArrayView {
                unit: TimeUnit::Millisecond,
                timezone: array.timezone().map(str::to_owned),
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<PrimitiveArray<TimestampSecondType>>() {
            Ok(ArrayView::Timestamp(TimestampArrayView {
                unit: TimeUnit::Second,
                timezone: array.timezone().map(str::to_owned),
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<PrimitiveArray<DurationNanosecondType>>() {
            Ok(ArrayView::Duration(TimeArrayView {
                unit: TimeUnit::Nanosecond,
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<PrimitiveArray<DurationMicrosecondType>>() {
            Ok(ArrayView::Duration(TimeArrayView {
                unit: TimeUnit::Microsecond,
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<PrimitiveArray<DurationMillisecondType>>() {
            Ok(ArrayView::Duration(TimeArrayView {
                unit: TimeUnit::Millisecond,
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<PrimitiveArray<DurationSecondType>>() {
            Ok(ArrayView::Duration(TimeArrayView {
                unit: TimeUnit::Second,
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<GenericStringArray<i32>>() {
            Ok(ArrayView::Utf8(BytesArrayView {
                validity: get_bits_with_offset(array),
                offsets: array.value_offsets(),
                data: array.value_data(),
            }))
        } else if let Some(array) = any.downcast_ref::<GenericStringArray<i64>>() {
            Ok(ArrayView::LargeUtf8(BytesArrayView {
                validity: get_bits_with_offset(array),
                offsets: array.value_offsets(),
                data: array.value_data(),
            }))
        } else if let Some(array) = any.downcast_ref::<GenericBinaryArray<i32>>() {
            Ok(ArrayView::Binary(BytesArrayView {
                validity: get_bits_with_offset(array),
                offsets: array.value_offsets(),
                data: array.value_data(),
            }))
        } else if let Some(array) = any.downcast_ref::<GenericBinaryArray<i64>>() {
            Ok(ArrayView::LargeBinary(BytesArrayView {
                validity: get_bits_with_offset(array),
                offsets: array.value_offsets(),
                data: array.value_data(),
            }))
        } else if let Some(array) = any.downcast_ref::<FixedSizeBinaryArray>() {
            wrap_fixed_size_binary_array(array)
        } else if let Some(array) = any.downcast_ref::<GenericListArray<i32>>() {
            let DataType::List(field) = array.data_type() else {
                fail!("invalid data type for list array: {}", array.data_type());
            };
            Ok(ArrayView::List(ListArrayView {
                validity: get_bits_with_offset(array),
                offsets: array.value_offsets(),
                meta: meta_from_field(field.as_ref().try_into()?),
                element: Box::new(array.values().as_ref().try_into()?),
            }))
        } else if let Some(array) = any.downcast_ref::<GenericListArray<i64>>() {
            let DataType::LargeList(field) = array.data_type() else {
                fail!("invalid data type for list array: {}", array.data_type());
            };
            Ok(ArrayView::LargeList(ListArrayView {
                validity: get_bits_with_offset(array),
                offsets: array.value_offsets(),
                meta: meta_from_field(field.as_ref().try_into()?),
                element: Box::new(array.values().as_ref().try_into()?),
            }))
        } else if let Some(array) = any.downcast_ref::<FixedSizeListArray>() {
            let DataType::FixedSizeList(field, n) = array.data_type() else {
                fail!("invalid data type for list array: {}", array.data_type());
            };
            Ok(ArrayView::FixedSizeList(FixedSizeListArrayView {
                len: array.len(),
                n: *n,
                validity: get_bits_with_offset(array),
                meta: meta_from_field(field.as_ref().try_into()?),
                element: Box::new(array.values().as_ref().try_into()?),
            }))
        } else if let Some(array) = any.downcast_ref::<StructArray>() {
            let DataType::Struct(column_fields) = array.data_type() else {
                fail!("invalid data type for struct array: {}", array.data_type());
            };

            let mut fields = Vec::new();
            for (field, array) in std::iter::zip(column_fields, array.columns()) {
                let view = ArrayView::try_from(array.as_ref())?;
                let meta = meta_from_field(Field::try_from(field.as_ref())?);
                fields.push((view, meta));
            }

            Ok(ArrayView::Struct(StructArrayView {
                len: array.len(),
                validity: get_bits_with_offset(array),
                fields,
            }))
        } else if let Some(array) = any.downcast_ref::<MapArray>() {
            let DataType::Map(entries_field, _) = array.data_type() else {
                fail!("invalid data type for map array: {}", array.data_type());
            };
            let entries_array: &dyn Array = array.entries();

            Ok(ArrayView::Map(ListArrayView {
                validity: get_bits_with_offset(array),
                offsets: array.value_offsets(),
                meta: meta_from_field(Field::try_from(entries_field.as_ref())?),
                element: Box::new(entries_array.try_into()?),
            }))
        } else if let Some(array) = any.downcast_ref::<DictionaryArray<UInt8Type>>() {
            wrap_dictionary_array::<UInt8Type>(array)
        } else if let Some(array) = any.downcast_ref::<DictionaryArray<UInt16Type>>() {
            wrap_dictionary_array::<UInt16Type>(array)
        } else if let Some(array) = any.downcast_ref::<DictionaryArray<UInt32Type>>() {
            wrap_dictionary_array::<UInt32Type>(array)
        } else if let Some(array) = any.downcast_ref::<DictionaryArray<UInt64Type>>() {
            wrap_dictionary_array::<UInt64Type>(array)
        } else if let Some(array) = any.downcast_ref::<DictionaryArray<Int8Type>>() {
            wrap_dictionary_array::<Int8Type>(array)
        } else if let Some(array) = any.downcast_ref::<DictionaryArray<Int16Type>>() {
            wrap_dictionary_array::<Int16Type>(array)
        } else if let Some(array) = any.downcast_ref::<DictionaryArray<Int32Type>>() {
            wrap_dictionary_array::<Int32Type>(array)
        } else if let Some(array) = any.downcast_ref::<DictionaryArray<Int64Type>>() {
            wrap_dictionary_array::<Int64Type>(array)
        } else if let Some(array) = any.downcast_ref::<UnionArray>() {
            let DataType::Union(union_fields, UnionMode::Dense) = array.data_type() else {
                fail!("Invalid data type: only dense unions are supported");
            };

            let mut fields = Vec::new();
            for (type_id, field) in union_fields.iter() {
                let meta = meta_from_field(Field::try_from(field.as_ref())?);
                let view: ArrayView = array.child(type_id).as_ref().try_into()?;
                fields.push((type_id, view, meta));
            }
            let Some(offsets) = array.offsets() else {
                fail!("Dense unions must have an offset array");
            };

            Ok(ArrayView::DenseUnion(DenseUnionArrayView {
                types: array.type_ids(),
                offsets,
                fields,
            }))
        } else {
            fail!(
                "Cannot build an array view for {dt}",
                dt = array.data_type()
            );
        }
    }
}

fn field_from_data_and_meta(data: &ArrayData, meta: FieldMeta) -> ArrowField {
    ArrowField::new(meta.name, data.data_type().clone(), meta.nullable).with_metadata(meta.metadata)
}

fn primitive_into_data<T: ArrowNativeType>(
    data_type: DataType,
    validity: Option<Vec<u8>>,
    values: Vec<T>,
) -> Result<ArrayData> {
    Ok(ArrayData::try_new(
        data_type,
        values.len(),
        validity.map(Buffer::from_vec),
        0,
        vec![ScalarBuffer::from(values).into_inner()],
        vec![],
    )?)
}

fn bytes_into_data<O: ArrowNativeType>(
    data_type: DataType,
    offsets: Vec<O>,
    data: Vec<u8>,
    validity: Option<Vec<u8>>,
) -> Result<ArrayData> {
    Ok(ArrayData::try_new(
        data_type,
        offsets.len().saturating_sub(1),
        validity.map(Buffer::from_vec),
        0,
        vec![
            ScalarBuffer::from(offsets).into_inner(),
            ScalarBuffer::from(data).into_inner(),
        ],
        vec![],
    )?)
}

fn list_into_data<O: ArrowNativeType>(
    data_type: DataType,
    len: usize,
    offsets: Vec<O>,
    child_data: ArrayData,
    validity: Option<Vec<u8>>,
) -> Result<ArrayData> {
    Ok(ArrayData::try_new(
        data_type,
        len,
        validity.map(Buffer::from_vec),
        0,
        vec![ScalarBuffer::from(offsets).into_inner()],
        vec![child_data],
    )?)
}

fn wrap_dictionary_array<K: ArrowDictionaryKeyType>(
    array: &DictionaryArray<K>,
) -> Result<ArrayView<'_>> {
    let keys: &dyn Array = array.keys();

    Ok(ArrayView::Dictionary(DictionaryArrayView {
        indices: Box::new(keys.try_into()?),
        values: Box::new(array.values().as_ref().try_into()?),
    }))
}

#[cfg(has_arrow_fixed_binary_support)]
pub fn wrap_fixed_size_binary_array(array: &FixedSizeBinaryArray) -> Result<ArrayView<'_>> {
    use crate::internal::arrow::FixedSizeBinaryArrayView;

    Ok(ArrayView::FixedSizeBinary(FixedSizeBinaryArrayView {
        n: array.value_length(),
        validity: get_bits_with_offset(array),
        data: array.value_data(),
    }))
}

#[cfg(not(has_arrow_fixed_binary_support))]
pub fn wrap_fixed_size_binary_array(_array: &FixedSizeBinaryArray) -> Result<ArrayView<'_>> {
    fail!("FixedSizeBinary arrays are not supported for arrow<=46");
}

fn get_bits_with_offset(array: &dyn Array) -> Option<BitsWithOffset<'_>> {
    let validity = array.nulls()?;
    Some(BitsWithOffset {
        offset: validity.offset(),
        data: validity.validity(),
    })
}
