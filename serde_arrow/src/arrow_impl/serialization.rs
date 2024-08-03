#![allow(missing_docs)]
use std::sync::Arc;

use half::f16;

use crate::{
    _impl::arrow::{
        array::{Array, ArrayData, NullArray},
        buffer::{Buffer, ScalarBuffer},
        datatypes::{ArrowNativeType, ArrowPrimitiveType, DataType, Field, Float16Type, UnionMode},
    },
    internal::{
        arrow::FieldMeta,
        error::{fail, Error, Result},
    },
};

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
                arr.validity.map(Buffer::from),
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
                    let field = Field::new(meta.name, child.data_type().clone(), meta.nullable)
                        .with_metadata(meta.metadata);
                    fields.push(Arc::new(field));
                    data.push(child);
                }
                let data_type = T::Struct(fields.into());

                Ok(ArrayData::builder(data_type)
                    .len(arr.len)
                    .null_bit_buffer(arr.validity.map(Buffer::from))
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
                    arr.validity.map(Buffer::from),
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
                    arr.validity.map(Buffer::from),
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
                    arr.validity.map(Buffer::from),
                    0,
                    vec![ScalarBuffer::from(arr.offsets).into_inner()],
                    vec![child],
                )?)
            }
            A::DenseUnion(arr) => {
                let mut fields = Vec::new();
                let mut child_data = Vec::new();

                for (idx, (array, meta)) in arr.fields.into_iter().enumerate() {
                    let child: ArrayData = array.try_into()?;
                    let field = field_from_data_and_meta(&child, meta);

                    fields.push((idx as i8, Arc::new(field)));
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

fn field_from_data_and_meta(data: &ArrayData, meta: FieldMeta) -> Field {
    Field::new(meta.name, data.data_type().clone(), meta.nullable).with_metadata(meta.metadata)
}

fn primitive_into_data<T: ArrowNativeType>(
    data_type: DataType,
    validity: Option<Vec<u8>>,
    values: Vec<T>,
) -> Result<ArrayData> {
    Ok(ArrayData::try_new(
        data_type,
        values.len(),
        validity.map(Buffer::from),
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
        validity.map(Buffer::from),
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
        validity.map(Buffer::from),
        0,
        vec![ScalarBuffer::from(offsets).into_inner()],
        vec![child_data],
    )?)
}
