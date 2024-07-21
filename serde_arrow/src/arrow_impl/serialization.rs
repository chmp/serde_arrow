#![allow(missing_docs)]
use std::sync::Arc;

use half::f16;

use crate::{
    _impl::arrow::{
        array::{make_array, Array, ArrayData, ArrayRef, NullArray, RecordBatch},
        buffer::{Buffer, ScalarBuffer},
        datatypes::{
            ArrowNativeType, ArrowPrimitiveType, DataType, Field, FieldRef, Float16Type, Schema,
        },
    },
    internal::{
        arrow::FieldMeta,
        error::{fail, Error, Result},
        schema::{GenericField, SerdeArrowSchema},
        serialization::{ArrayBuilder, OuterSequenceBuilder},
    },
};

/// Support `arrow` (*requires one of the `arrow-*` features*)
impl crate::internal::array_builder::ArrayBuilder {
    /// Build an ArrayBuilder from `arrow` fields (*requires one of the
    /// `arrow-*` features*)
    pub fn from_arrow(fields: &[FieldRef]) -> Result<Self> {
        let fields = fields
            .iter()
            .map(|f| GenericField::try_from(f.as_ref()))
            .collect::<Result<Vec<_>>>()?;
        Self::new(SerdeArrowSchema { fields })
    }

    /// Construct `arrow` arrays and reset the builder (*requires one of the
    /// `arrow-*` features*)
    pub fn to_arrow(&mut self) -> Result<Vec<ArrayRef>> {
        self.builder.build_arrow()
    }

    /// Construct a [`RecordBatch`] and reset the builder (*requires one of the
    /// `arrow-*` features*)
    pub fn to_record_batch(&mut self) -> Result<RecordBatch> {
        let arrays = self.builder.build_arrow()?;
        let fields = Vec::<FieldRef>::try_from(&self.schema)?;
        let schema = Schema::new(fields);
        Ok(RecordBatch::try_new(Arc::new(schema), arrays)?)
    }
}

impl OuterSequenceBuilder {
    pub fn build_arrow(&mut self) -> Result<Vec<ArrayRef>> {
        let fields = self.take_records()?;
        let arrays = fields
            .into_iter()
            .map(build_array)
            .collect::<Result<Vec<_>>>()?;
        Ok(arrays)
    }
}

fn build_array(builder: ArrayBuilder) -> Result<ArrayRef> {
    let data = build_array_data(builder)?;
    Ok(make_array(data))
}

fn build_array_data(builder: ArrayBuilder) -> Result<ArrayData> {
    use ArrayBuilder as A;

    match builder {
        builder @ (A::UnknownVariant(_)
        | A::Null(_)
        | A::Bool(_)
        | A::I8(_)
        | A::I16(_)
        | A::I32(_)
        | A::I64(_)
        | A::U8(_)
        | A::U16(_)
        | A::U32(_)
        | A::U64(_)
        | A::F16(_)
        | A::F32(_)
        | A::F64(_)
        | A::Date32(_)
        | A::Date64(_)
        | A::Time32(_)
        | A::Time64(_)
        | A::Duration(_)
        | A::Decimal128(_)
        | A::Utf8(_)
        | A::LargeUtf8(_)
        | A::Binary(_)
        | A::LargeBinary(_)
        | A::Struct(_)
        | A::LargeList(_)
        | A::List(_)
        | A::FixedSizedList(_)
        | A::FixedSizeBinary(_)
        | A::DictionaryUtf8(_)
        | A::Map(_)) => builder.into_array()?.try_into(),
        A::Union(builder) => {
            let data_type = Field::try_from(&builder.field)?.data_type().clone();
            let children = builder
                .fields
                .into_iter()
                .map(build_array_data)
                .collect::<Result<Vec<_>>>()?;
            let len = builder.types.len();

            Ok(ArrayData::builder(data_type)
                .len(len)
                .add_buffer(Buffer::from_vec(builder.types))
                .add_buffer(Buffer::from_vec(builder.offsets))
                .child_data(children)
                .build()?)
        }
    }
}

impl TryFrom<crate::internal::arrow::Array> for ArrayData {
    type Error = Error;

    fn try_from(value: crate::internal::arrow::Array) -> Result<ArrayData> {
        use {crate::internal::arrow::Array as A, DataType as ArrowT};
        type ArrowF16 = <Float16Type as ArrowPrimitiveType>::Native;

        fn f16_to_f16(v: f16) -> ArrowF16 {
            ArrowF16::from_bits(v.to_bits())
        }

        match value {
            A::Null(arr) => Ok(NullArray::new(arr.len).into_data()),
            A::Boolean(arr) => Ok(ArrayData::try_new(
                ArrowT::Boolean,
                // NOTE: use the explicit len
                arr.len,
                arr.validity.map(Buffer::from),
                0,
                vec![ScalarBuffer::from(arr.values).into_inner()],
                vec![],
            )?),
            A::Int8(arr) => primitive_into_data(ArrowT::Int8, arr.validity, arr.values),
            A::Int16(arr) => primitive_into_data(ArrowT::Int16, arr.validity, arr.values),
            A::Int32(arr) => primitive_into_data(ArrowT::Int32, arr.validity, arr.values),
            A::Int64(arr) => primitive_into_data(ArrowT::Int64, arr.validity, arr.values),
            A::UInt8(arr) => primitive_into_data(ArrowT::UInt8, arr.validity, arr.values),
            A::UInt16(arr) => primitive_into_data(ArrowT::UInt16, arr.validity, arr.values),
            A::UInt32(arr) => primitive_into_data(ArrowT::UInt32, arr.validity, arr.values),
            A::UInt64(arr) => primitive_into_data(ArrowT::UInt64, arr.validity, arr.values),
            A::Float16(arr) => primitive_into_data(
                ArrowT::Float16,
                arr.validity,
                arr.values.into_iter().map(f16_to_f16).collect(),
            ),
            A::Float32(arr) => primitive_into_data(ArrowT::Float32, arr.validity, arr.values),
            A::Float64(arr) => primitive_into_data(ArrowT::Float64, arr.validity, arr.values),
            A::Date32(arr) => primitive_into_data(ArrowT::Date32, arr.validity, arr.values),
            A::Date64(arr) => primitive_into_data(ArrowT::Date64, arr.validity, arr.values),
            A::Timestamp(arr) => primitive_into_data(
                ArrowT::Timestamp(arr.unit.into(), arr.timezone.map(String::into)),
                arr.validity,
                arr.values,
            ),
            A::Time32(arr) => {
                primitive_into_data(ArrowT::Time32(arr.unit.into()), arr.validity, arr.values)
            }
            A::Time64(arr) => {
                primitive_into_data(ArrowT::Time64(arr.unit.into()), arr.validity, arr.values)
            }
            A::Duration(arr) => {
                primitive_into_data(ArrowT::Duration(arr.unit.into()), arr.validity, arr.values)
            }
            A::Decimal128(arr) => primitive_into_data(
                ArrowT::Decimal128(arr.precision, arr.scale),
                arr.validity,
                arr.values,
            ),
            A::Utf8(arr) => bytes_into_data(ArrowT::Utf8, arr.offsets, arr.data, arr.validity),
            A::LargeUtf8(arr) => {
                bytes_into_data(ArrowT::LargeUtf8, arr.offsets, arr.data, arr.validity)
            }
            A::Binary(arr) => bytes_into_data(ArrowT::Binary, arr.offsets, arr.data, arr.validity),
            A::LargeBinary(arr) => {
                bytes_into_data(ArrowT::LargeBinary, arr.offsets, arr.data, arr.validity)
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
                let data_type = ArrowT::Struct(fields.into());

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
                    ArrowT::List(Arc::new(field)),
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
                    ArrowT::LargeList(Arc::new(field)),
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
                    ArrowT::FixedSizeList(Arc::new(field), arr.n),
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
                    ArrowT::FixedSizeBinary(arr.n),
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
                let data_type = ArrowT::Dictionary(
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
                    ArrowT::Map(Arc::new(field), false),
                    arr.offsets.len().saturating_sub(1),
                    arr.validity.map(Buffer::from),
                    0,
                    vec![ScalarBuffer::from(arr.offsets).into_inner()],
                    vec![child],
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
