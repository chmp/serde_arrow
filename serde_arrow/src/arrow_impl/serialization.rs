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
        error::{fail, Error, Result},
        schema::{GenericField, SerdeArrowSchema},
        serialization::{
            utils::{MutableBitBuffer, MutableOffsetBuffer},
            ArrayBuilder, OuterSequenceBuilder,
        },
        utils::Offset,
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
    use {ArrayBuilder as A, DataType as T};

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
        | A::Date64(_)) => builder.into_array().try_into(),
        A::Time32(builder) => build_array_data_primitive(
            Field::try_from(&builder.field)?.data_type().clone(),
            builder.buffer,
            builder.validity,
        ),
        A::Time64(builder) => build_array_data_primitive(
            Field::try_from(&builder.field)?.data_type().clone(),
            builder.buffer,
            builder.validity,
        ),
        A::Duration(builder) => build_array_data_primitive(
            T::Duration(builder.unit.into()),
            builder.buffer,
            builder.validity,
        ),
        A::Decimal128(builder) => build_array_data_primitive(
            T::Decimal128(builder.precision, builder.scale),
            builder.buffer,
            builder.validity,
        ),
        A::Utf8(builder) => build_array_data_utf8(
            T::Utf8,
            builder.offsets.offsets,
            builder.buffer,
            builder.validity,
        ),
        A::LargeUtf8(builder) => build_array_data_utf8(
            T::LargeUtf8,
            builder.offsets.offsets,
            builder.buffer,
            builder.validity,
        ),
        A::LargeList(builder) => build_array_data_list(
            T::LargeList(Arc::new(Field::try_from(&builder.field)?)),
            builder.offsets.offsets.len() - 1,
            builder.offsets.offsets,
            build_array_data(*builder.element)?,
            builder.validity,
        ),
        A::List(builder) => build_array_data_list(
            T::List(Arc::new(Field::try_from(&builder.field)?)),
            builder.offsets.offsets.len() - 1,
            builder.offsets.offsets,
            build_array_data(*builder.element)?,
            builder.validity,
        ),
        A::FixedSizedList(builder) => {
            let data_type = T::FixedSizeList(
                Arc::new(Field::try_from(&builder.field)?),
                builder.n.try_into()?,
            );
            let child_data = build_array_data(*builder.element)?;
            let validity = if let Some(validity) = builder.validity {
                Some(Buffer::from(validity.buffer))
            } else {
                None
            };

            Ok(ArrayData::builder(data_type)
                .len(builder.num_elements)
                .null_bit_buffer(validity)
                .add_child_data(child_data)
                .build()?)
        }
        A::Binary(builder) => {
            build_array_data_binary(T::Binary, builder.offsets, builder.buffer, builder.validity)
        }
        A::LargeBinary(builder) => build_array_data_binary(
            T::LargeBinary,
            builder.offsets,
            builder.buffer,
            builder.validity,
        ),
        A::FixedSizeBinary(builder) => {
            let data_buffer = ScalarBuffer::from(builder.buffer).into_inner();
            let validity = if let Some(validity) = builder.validity {
                Some(Buffer::from(validity.buffer))
            } else {
                None
            };

            Ok(
                ArrayData::builder(T::FixedSizeBinary(builder.n.try_into()?))
                    .len(builder.len)
                    .null_bit_buffer(validity)
                    .add_buffer(data_buffer)
                    .build()?,
            )
        }
        A::Struct(builder) => {
            let mut data = Vec::new();
            for (_, field) in builder.named_fields {
                data.push(build_array_data(field)?);
            }

            let (validity, len) = if let Some(validity) = builder.validity {
                (Some(Buffer::from(validity.buffer)), validity.len)
            } else {
                if data.is_empty() {
                    fail!("cannot built non-nullable structs without fields");
                }
                (None, data[0].len())
            };

            let fields = builder
                .fields
                .iter()
                .map(Field::try_from)
                .collect::<Result<Vec<_>>>()?;
            let data_type = T::Struct(fields.into());

            Ok(ArrayData::builder(data_type)
                .len(len)
                .null_bit_buffer(validity)
                .child_data(data)
                .build()?)
        }
        A::Map(builder) => Ok(ArrayData::builder(T::Map(
            Arc::new(Field::try_from(&builder.entry_field)?),
            false,
        ))
        .len(builder.offsets.offsets.len() - 1)
        .add_buffer(ScalarBuffer::from(builder.offsets.offsets).into_inner())
        .add_child_data(build_array_data(*builder.entry)?)
        .null_bit_buffer(builder.validity.map(|b| Buffer::from(b.buffer)))
        .build()?),
        A::DictionaryUtf8(builder) => {
            let indices = build_array_data(*builder.indices)?;
            let values = build_array_data(*builder.values)?;
            let data_type = Field::try_from(&builder.field)?.data_type().clone();

            Ok(indices
                .into_builder()
                .data_type(data_type)
                .child_data(vec![values])
                .build()?)
        }
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
                arr.len,
                arr.validity.map(Buffer::from),
                0,
                vec![ScalarBuffer::from(arr.values).into_inner()],
                vec![],
            )?),
            A::Int8(arr) => primitive_into_data(ArrowT::Int8, arr),
            A::Int16(arr) => primitive_into_data(ArrowT::Int16, arr),
            A::Int32(arr) => primitive_into_data(ArrowT::Int32, arr),
            A::Int64(arr) => primitive_into_data(ArrowT::Int64, arr),
            A::UInt8(arr) => primitive_into_data(ArrowT::UInt8, arr),
            A::UInt16(arr) => primitive_into_data(ArrowT::UInt16, arr),
            A::UInt32(arr) => primitive_into_data(ArrowT::UInt32, arr),
            A::UInt64(arr) => primitive_into_data(ArrowT::UInt64, arr),
            A::Float16(arr) => primitive_into_data(ArrowT::Float16, arr.map_values(f16_to_f16)),
            A::Float32(arr) => primitive_into_data(ArrowT::Float32, arr),
            A::Float64(arr) => primitive_into_data(ArrowT::Float64, arr),
            A::Date32(arr) => primitive_into_data(ArrowT::Date32, arr),
            A::Date64(arr) => primitive_into_data(ArrowT::Date64, arr),
            A::Timestamp(arr) => {
                let data_type = ArrowT::Timestamp(arr.unit.into(), arr.timezone.map(String::into));
                Ok(ArrayData::try_new(
                    data_type,
                    arr.values.len(),
                    arr.validity.map(Buffer::from),
                    0,
                    vec![ScalarBuffer::from(arr.values).into_inner()],
                    vec![],
                )?)
            }
            array => fail!("{:?} not implemented", array),
        }
    }
}

fn primitive_into_data<T: ArrowNativeType>(
    data_type: DataType,
    array: crate::internal::arrow::PrimitiveArray<T>,
) -> Result<ArrayData> {
    Ok(ArrayData::try_new(
        data_type,
        array.values.len(),
        array.validity.map(Buffer::from),
        0,
        vec![ScalarBuffer::from(array.values).into_inner()],
        vec![],
    )?)
}

fn build_array_data_primitive<T: ArrowNativeType>(
    data_type: DataType,
    data: Vec<T>,
    validity: Option<MutableBitBuffer>,
) -> Result<ArrayData> {
    let len = data.len();
    build_array_data_primitive_with_len(data_type, len, data, validity)
}

fn build_array_data_primitive_with_len<T: ArrowNativeType>(
    data_type: DataType,
    len: usize,
    data: Vec<T>,
    validity: Option<MutableBitBuffer>,
) -> Result<ArrayData> {
    Ok(ArrayData::try_new(
        data_type,
        len,
        validity.map(|b| Buffer::from(b.buffer)),
        0,
        vec![ScalarBuffer::from(data).into_inner()],
        vec![],
    )?)
}

fn build_array_data_utf8<O: ArrowNativeType>(
    data_type: DataType,
    offsets: Vec<O>,
    data: Vec<u8>,
    validity: Option<MutableBitBuffer>,
) -> Result<ArrayData> {
    let values_len = offsets.len() - 1;

    let offsets = ScalarBuffer::from(offsets).into_inner();
    let data = ScalarBuffer::from(data).into_inner();
    let validity = validity.map(|b| Buffer::from(b.buffer));

    Ok(ArrayData::try_new(
        data_type,
        values_len,
        validity,
        0,
        vec![offsets, data],
        vec![],
    )?)
}

fn build_array_data_binary<O: ArrowNativeType + Offset>(
    data_type: DataType,
    offsets: MutableOffsetBuffer<O>,
    data: Vec<u8>,
    validity: Option<MutableBitBuffer>,
) -> Result<ArrayData> {
    let len = offsets.len();
    let offset_buffer = ScalarBuffer::from(offsets.offsets).into_inner();
    let data_buffer = ScalarBuffer::from(data).into_inner();
    let validity = if let Some(validity) = validity {
        Some(Buffer::from(validity.buffer))
    } else {
        None
    };
    Ok(ArrayData::builder(data_type)
        .len(len)
        .null_bit_buffer(validity)
        .add_buffer(offset_buffer)
        .add_buffer(data_buffer)
        .build()?)
}

fn build_array_data_list<O: ArrowNativeType>(
    data_type: DataType,
    len: usize,
    offsets: Vec<O>,
    child_data: ArrayData,
    validity: Option<MutableBitBuffer>,
) -> Result<ArrayData> {
    let offset_buffer = ScalarBuffer::from(offsets).into_inner();
    let validity = validity.map(|b| Buffer::from(b.buffer));

    Ok(ArrayData::builder(data_type)
        .len(len)
        .add_buffer(offset_buffer)
        .add_child_data(child_data)
        .null_bit_buffer(validity)
        .build()?)
}
