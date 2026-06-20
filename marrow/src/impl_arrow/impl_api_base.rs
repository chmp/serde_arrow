// the implementation
use std::sync::Arc;

use half::f16;

use crate::{
    array::Array,
    datatypes::{
        meta_from_field, DataType, Field, FieldMeta, IntervalUnit, RunEndEncodedMeta, TimeUnit,
        UnionMode,
    },
    error::{fail, ErrorKind, MarrowError, Result},
    view::{
        BitsWithOffset, BooleanView, BytesView, DecimalView, DictionaryView, FixedSizeListView,
        ListView, MapView, NullView, PrimitiveView, RunEndEncodedView, StructView, TimeView,
        TimestampView, UnionView, View,
    },
};

impl From<arrow_schema::ArrowError> for MarrowError {
    fn from(err: arrow_schema::ArrowError) -> Self {
        MarrowError::with_cause(ErrorKind::ArrowError, err.to_string(), err)
    }
}

/// Converison from `arrow` data types (*requires one of the `arrow-{version}` features*)
// only some arrow version implement Copy for unit
#[allow(clippy::clone_on_copy)]
impl TryFrom<&arrow_schema::DataType> for DataType {
    type Error = MarrowError;

    fn try_from(value: &arrow_schema::DataType) -> Result<DataType> {
        use {arrow_schema::DataType as AT, DataType as T, Field as F};
        match value {
            AT::Boolean => Ok(T::Boolean),
            AT::Null => Ok(T::Null),
            AT::Int8 => Ok(T::Int8),
            AT::Int16 => Ok(T::Int16),
            AT::Int32 => Ok(T::Int32),
            AT::Int64 => Ok(T::Int64),
            AT::UInt8 => Ok(T::UInt8),
            AT::UInt16 => Ok(T::UInt16),
            AT::UInt32 => Ok(T::UInt32),
            AT::UInt64 => Ok(T::UInt64),
            AT::Float16 => Ok(T::Float16),
            AT::Float32 => Ok(T::Float32),
            AT::Float64 => Ok(T::Float64),
            AT::Utf8 => Ok(T::Utf8),
            AT::LargeUtf8 => Ok(T::LargeUtf8),
            AT::Date32 => Ok(T::Date32),
            AT::Date64 => Ok(T::Date64),
            AT::Decimal128(precision, scale) => Ok(T::Decimal128(*precision, *scale)),
            AT::Time32(unit) => Ok(T::Time32(unit.clone().try_into()?)),
            AT::Time64(unit) => Ok(T::Time64(unit.clone().try_into()?)),
            AT::Timestamp(unit, tz) => Ok(T::Timestamp(
                unit.clone().try_into()?,
                tz.as_ref().map(|s| s.to_string()),
            )),
            AT::Duration(unit) => Ok(T::Duration(unit.clone().try_into()?)),
            AT::Interval(unit) => Ok(T::Interval(unit.clone().try_into()?)),
            AT::Binary => Ok(T::Binary),
            AT::LargeBinary => Ok(T::LargeBinary),
            AT::FixedSizeBinary(n) => Ok(T::FixedSizeBinary(*n)),
            AT::List(field) => Ok(T::List(F::try_from(field.as_ref())?.into())),
            AT::LargeList(field) => Ok(T::LargeList(F::try_from(field.as_ref())?.into())),
            AT::FixedSizeList(field, n) => {
                Ok(T::FixedSizeList(F::try_from(field.as_ref())?.into(), *n))
            }
            AT::Map(field, sorted) => Ok(T::Map(F::try_from(field.as_ref())?.into(), *sorted)),
            AT::Struct(in_fields) => {
                let mut fields = Vec::new();
                for field in in_fields {
                    fields.push(field.as_ref().try_into()?);
                }
                Ok(T::Struct(fields))
            }
            AT::Dictionary(key, value) => Ok(T::Dictionary(
                T::try_from(key.as_ref())?.into(),
                T::try_from(value.as_ref())?.into(),
            )),
            AT::Union(in_fields, mode) => {
                let mut fields = Vec::new();
                for (type_id, field) in in_fields.iter() {
                    fields.push((type_id, F::try_from(field.as_ref())?));
                }
                Ok(T::Union(fields, (*mode).try_into()?))
            }
            AT::RunEndEncoded(keys, values) => Ok(T::RunEndEncoded(
                Box::new(keys.as_ref().try_into()?),
                Box::new(values.as_ref().try_into()?),
            )),
            data_type => convert_data_type_to_marrow(data_type),
        }
    }
}

/// Converison from `arrow` fields (*requires one of the `arrow-{version}` features*)
impl TryFrom<&arrow_schema::Field> for Field {
    type Error = MarrowError;

    fn try_from(field: &arrow_schema::Field) -> Result<Self> {
        Ok(Field {
            name: field.name().to_owned(),
            data_type: DataType::try_from(field.data_type())?,
            metadata: field.metadata().clone(),
            nullable: field.is_nullable(),
        })
    }
}

/// Converison to `arrow` data types (*requires one of the `arrow-{version}` features*)
impl TryFrom<&DataType> for arrow_schema::DataType {
    type Error = MarrowError;

    fn try_from(value: &DataType) -> std::result::Result<Self, Self::Error> {
        use {arrow_schema::DataType as AT, arrow_schema::Field as AF, DataType as T};
        match value {
            T::Boolean => Ok(AT::Boolean),
            T::Null => Ok(AT::Null),
            T::Int8 => Ok(AT::Int8),
            T::Int16 => Ok(AT::Int16),
            T::Int32 => Ok(AT::Int32),
            T::Int64 => Ok(AT::Int64),
            T::UInt8 => Ok(AT::UInt8),
            T::UInt16 => Ok(AT::UInt16),
            T::UInt32 => Ok(AT::UInt32),
            T::UInt64 => Ok(AT::UInt64),
            T::Float16 => Ok(AT::Float16),
            T::Float32 => Ok(AT::Float32),
            T::Float64 => Ok(AT::Float64),
            T::Utf8 => Ok(AT::Utf8),
            T::LargeUtf8 => Ok(AT::LargeUtf8),
            T::Date32 => Ok(AT::Date32),
            T::Date64 => Ok(AT::Date64),
            T::Decimal128(precision, scale) => Ok(AT::Decimal128(*precision, *scale)),
            T::Time32(unit) => Ok(AT::Time32((*unit).try_into()?)),
            T::Time64(unit) => Ok(AT::Time64((*unit).try_into()?)),
            T::Timestamp(unit, tz) => Ok(AT::Timestamp(
                (*unit).try_into()?,
                tz.as_ref().map(|s| s.to_string().into()),
            )),
            T::Duration(unit) => Ok(AT::Duration((*unit).try_into()?)),
            T::Interval(unit) => Ok(AT::Interval((*unit).try_into()?)),
            T::Binary => Ok(AT::Binary),
            T::LargeBinary => Ok(AT::LargeBinary),
            T::FixedSizeBinary(n) => Ok(AT::FixedSizeBinary(*n)),
            T::List(field) => Ok(AT::List(AF::try_from(field.as_ref())?.into())),
            T::LargeList(field) => Ok(AT::LargeList(AF::try_from(field.as_ref())?.into())),
            T::FixedSizeList(field, n) => {
                Ok(AT::FixedSizeList(AF::try_from(field.as_ref())?.into(), *n))
            }
            T::Map(field, sorted) => Ok(AT::Map(AF::try_from(field.as_ref())?.into(), *sorted)),
            T::Struct(in_fields) => {
                let mut fields: Vec<arrow_schema::FieldRef> = Vec::new();
                for field in in_fields {
                    fields.push(AF::try_from(field)?.into());
                }
                Ok(AT::Struct(fields.into()))
            }
            T::Dictionary(key, value) => Ok(AT::Dictionary(
                AT::try_from(key.as_ref())?.into(),
                AT::try_from(value.as_ref())?.into(),
            )),
            T::RunEndEncoded(indices, values) => Ok(AT::RunEndEncoded(
                AF::try_from(indices.as_ref())?.into(),
                AF::try_from(values.as_ref())?.into(),
            )),
            T::Union(in_fields, mode) => {
                let mut fields = Vec::new();
                for (type_id, field) in in_fields {
                    fields.push((*type_id, Arc::new(AF::try_from(field)?)));
                }
                Ok(AT::Union(fields.into_iter().collect(), (*mode).try_into()?))
            }
            data_type => convert_data_type_from_marrow(data_type),
        }
    }
}

/// Converison to `arrow` fields (*requires one of the `arrow-{version}` features*)
impl TryFrom<&Field> for arrow_schema::Field {
    type Error = MarrowError;

    fn try_from(value: &Field) -> Result<Self> {
        let mut field = arrow_schema::Field::new(
            &value.name,
            arrow_schema::DataType::try_from(&value.data_type)?,
            value.nullable,
        );
        field.set_metadata(value.metadata.clone());
        Ok(field)
    }
}

/// Conversion to `arrow` time units (*requires one of the `arrow-{version}` features*)
impl TryFrom<TimeUnit> for arrow_schema::TimeUnit {
    type Error = MarrowError;

    fn try_from(value: TimeUnit) -> Result<arrow_schema::TimeUnit> {
        match value {
            TimeUnit::Second => Ok(arrow_schema::TimeUnit::Second),
            TimeUnit::Millisecond => Ok(arrow_schema::TimeUnit::Millisecond),
            TimeUnit::Microsecond => Ok(arrow_schema::TimeUnit::Microsecond),
            TimeUnit::Nanosecond => Ok(arrow_schema::TimeUnit::Nanosecond),
        }
    }
}

/// Conversion from `arrow` time units (*requires one of the `arrow-{version}` features*)
impl TryFrom<arrow_schema::TimeUnit> for TimeUnit {
    type Error = MarrowError;

    fn try_from(value: arrow_schema::TimeUnit) -> Result<TimeUnit> {
        match value {
            arrow_schema::TimeUnit::Second => Ok(TimeUnit::Second),
            arrow_schema::TimeUnit::Millisecond => Ok(TimeUnit::Millisecond),
            arrow_schema::TimeUnit::Microsecond => Ok(TimeUnit::Microsecond),
            arrow_schema::TimeUnit::Nanosecond => Ok(TimeUnit::Nanosecond),
        }
    }
}

/// Conversion from `arrow` union modes (*requires one of the `arrow-{version}` features*)
impl TryFrom<arrow_schema::UnionMode> for UnionMode {
    type Error = MarrowError;

    fn try_from(value: arrow_schema::UnionMode) -> Result<Self> {
        match value {
            arrow_schema::UnionMode::Dense => Ok(UnionMode::Dense),
            arrow_schema::UnionMode::Sparse => Ok(UnionMode::Sparse),
        }
    }
}

/// Conversion to `arrow` union modes (*requires one of the `arrow-{version}` features*)
impl TryFrom<UnionMode> for arrow_schema::UnionMode {
    type Error = MarrowError;

    fn try_from(value: UnionMode) -> Result<Self> {
        match value {
            UnionMode::Dense => Ok(arrow_schema::UnionMode::Dense),
            UnionMode::Sparse => Ok(arrow_schema::UnionMode::Sparse),
        }
    }
}

/// Conversion to `arrow` arrays (*requires one of the `arrow-{version}` features*)
impl TryFrom<Array> for Arc<dyn arrow_array::Array> {
    type Error = MarrowError;

    fn try_from(value: Array) -> Result<Arc<dyn arrow_array::Array>> {
        Ok(arrow_array::make_array(build_array_data(value)?))
    }
}

/// Conversion from `arrow` interval units (*requires one of the `arrow2-{version}` features*)
impl TryFrom<arrow_schema::IntervalUnit> for IntervalUnit {
    type Error = MarrowError;

    fn try_from(value: arrow_schema::IntervalUnit) -> Result<Self> {
        match value {
            arrow_schema::IntervalUnit::YearMonth => Ok(IntervalUnit::YearMonth),
            arrow_schema::IntervalUnit::DayTime => Ok(IntervalUnit::DayTime),
            arrow_schema::IntervalUnit::MonthDayNano => Ok(IntervalUnit::MonthDayNano),
        }
    }
}

/// Conversion to `arrow` interval units (*requires one of the `arrow2-{version}` features*)
impl TryFrom<IntervalUnit> for arrow_schema::IntervalUnit {
    type Error = MarrowError;

    fn try_from(value: IntervalUnit) -> Result<Self> {
        match value {
            IntervalUnit::YearMonth => Ok(arrow_schema::IntervalUnit::YearMonth),
            IntervalUnit::DayTime => Ok(arrow_schema::IntervalUnit::DayTime),
            IntervalUnit::MonthDayNano => Ok(arrow_schema::IntervalUnit::MonthDayNano),
        }
    }
}

fn build_array_data(value: Array) -> Result<arrow_data::ArrayData> {
    use Array as A;
    type ArrowF16 =
        <arrow_array::types::Float16Type as arrow_array::types::ArrowPrimitiveType>::Native;

    fn f16_to_f16(v: f16) -> ArrowF16 {
        ArrowF16::from_bits(v.to_bits())
    }

    match value {
        A::Null(arr) => {
            use arrow_array::Array;
            Ok(arrow_array::NullArray::new(arr.len).into_data())
        }
        A::Boolean(arr) => Ok(arrow_data::ArrayData::try_new(
            arrow_schema::DataType::Boolean,
            // NOTE: use the explicit len
            arr.len,
            arr.validity.map(arrow_buffer::Buffer::from_vec),
            0,
            vec![arrow_buffer::ScalarBuffer::from(arr.values).into_inner()],
            vec![],
        )?),
        A::Int8(arr) => primitive_into_data(arrow_schema::DataType::Int8, arr.validity, arr.values),
        A::Int16(arr) => {
            primitive_into_data(arrow_schema::DataType::Int16, arr.validity, arr.values)
        }
        A::Int32(arr) => {
            primitive_into_data(arrow_schema::DataType::Int32, arr.validity, arr.values)
        }
        A::Int64(arr) => {
            primitive_into_data(arrow_schema::DataType::Int64, arr.validity, arr.values)
        }
        A::UInt8(arr) => {
            primitive_into_data(arrow_schema::DataType::UInt8, arr.validity, arr.values)
        }
        A::UInt16(arr) => {
            primitive_into_data(arrow_schema::DataType::UInt16, arr.validity, arr.values)
        }
        A::UInt32(arr) => {
            primitive_into_data(arrow_schema::DataType::UInt32, arr.validity, arr.values)
        }
        A::UInt64(arr) => {
            primitive_into_data(arrow_schema::DataType::UInt64, arr.validity, arr.values)
        }
        A::Float16(arr) => primitive_into_data(
            arrow_schema::DataType::Float16,
            arr.validity,
            arr.values.into_iter().map(f16_to_f16).collect(),
        ),
        A::Float32(arr) => {
            primitive_into_data(arrow_schema::DataType::Float32, arr.validity, arr.values)
        }
        A::Float64(arr) => {
            primitive_into_data(arrow_schema::DataType::Float64, arr.validity, arr.values)
        }
        A::Date32(arr) => {
            primitive_into_data(arrow_schema::DataType::Date32, arr.validity, arr.values)
        }
        A::Date64(arr) => {
            primitive_into_data(arrow_schema::DataType::Date64, arr.validity, arr.values)
        }
        A::Timestamp(arr) => primitive_into_data(
            arrow_schema::DataType::Timestamp(arr.unit.try_into()?, arr.timezone.map(String::into)),
            arr.validity,
            arr.values,
        ),
        A::Time32(arr) => primitive_into_data(
            arrow_schema::DataType::Time32(arr.unit.try_into()?),
            arr.validity,
            arr.values,
        ),
        A::Time64(arr) => primitive_into_data(
            arrow_schema::DataType::Time64(arr.unit.try_into()?),
            arr.validity,
            arr.values,
        ),
        A::Duration(arr) => primitive_into_data(
            arrow_schema::DataType::Duration(arr.unit.try_into()?),
            arr.validity,
            arr.values,
        ),
        A::YearMonthInterval(arr) => primitive_into_data(
            arrow_schema::DataType::Interval(arrow_schema::IntervalUnit::YearMonth),
            arr.validity,
            arr.values,
        ),
        A::DayTimeInterval(arr) => primitive_into_data(
            arrow_schema::DataType::Interval(arrow_schema::IntervalUnit::DayTime),
            arr.validity,
            // NOTE: bytemuck::allocation::try_cast_vec enforces exact alignment. This cannot be
            // guaranteed between different arrow version (arrow < 52 used i64, arrow >= 52 has its
            // own type with different alignment). Therefore covert the vector elementwise and
            // create a new vector.
            try_cast_vec::<_, i64>(arr.values)?,
        ),
        A::MonthDayNanoInterval(arr) => primitive_into_data(
            arrow_schema::DataType::Interval(arrow_schema::IntervalUnit::MonthDayNano),
            arr.validity,
            // See note for A::DayTimeInterval
            try_cast_vec::<_, i128>(arr.values)?,
        ),
        A::Decimal128(arr) => primitive_into_data(
            arrow_schema::DataType::Decimal128(arr.precision, arr.scale),
            arr.validity,
            arr.values,
        ),
        A::Utf8(arr) => bytes_into_data(
            arrow_schema::DataType::Utf8,
            arr.offsets,
            arr.data,
            arr.validity,
        ),
        A::LargeUtf8(arr) => bytes_into_data(
            arrow_schema::DataType::LargeUtf8,
            arr.offsets,
            arr.data,
            arr.validity,
        ),
        A::Binary(arr) => bytes_into_data(
            arrow_schema::DataType::Binary,
            arr.offsets,
            arr.data,
            arr.validity,
        ),
        A::LargeBinary(arr) => bytes_into_data(
            arrow_schema::DataType::LargeBinary,
            arr.offsets,
            arr.data,
            arr.validity,
        ),
        A::Struct(arr) => {
            let mut fields = Vec::new();
            let mut data = Vec::new();

            for (meta, field) in arr.fields {
                let child = build_array_data(field)?;
                fields.push(Arc::new(field_from_data_and_meta(&child, meta)));
                data.push(child);
            }
            let data_type = arrow_schema::DataType::Struct(fields.into());

            Ok(arrow_data::ArrayData::builder(data_type)
                .len(arr.len)
                .null_bit_buffer(arr.validity.map(arrow_buffer::Buffer::from_vec))
                .child_data(data)
                .build()?)
        }
        A::List(arr) => {
            let child = build_array_data(*arr.elements)?;
            let field = field_from_data_and_meta(&child, arr.meta);
            list_into_data(
                arrow_schema::DataType::List(Arc::new(field)),
                arr.offsets.len().saturating_sub(1),
                arr.offsets,
                child,
                arr.validity,
            )
        }
        A::LargeList(arr) => {
            let child = build_array_data(*arr.elements)?;
            let field = field_from_data_and_meta(&child, arr.meta);
            list_into_data(
                arrow_schema::DataType::LargeList(Arc::new(field)),
                arr.offsets.len().saturating_sub(1),
                arr.offsets,
                child,
                arr.validity,
            )
        }
        A::FixedSizeList(arr) => {
            let child = build_array_data(*arr.elements)?;
            if (child.len() % usize::try_from(arr.n)?) != 0 {
                fail!(
                    ErrorKind::Unsupported,
                    "Invalid FixedSizeList: number of child elements ({}) not divisible by n ({})",
                    child.len(),
                    arr.n,
                );
            }
            let field = field_from_data_and_meta(&child, arr.meta);
            Ok(arrow_data::ArrayData::try_new(
                arrow_schema::DataType::FixedSizeList(Arc::new(field), arr.n),
                child.len() / usize::try_from(arr.n)?,
                arr.validity.map(arrow_buffer::Buffer::from_vec),
                0,
                vec![],
                vec![child],
            )?)
        }
        A::FixedSizeBinary(arr) => {
            if (arr.data.len() % usize::try_from(arr.n)?) != 0 {
                fail!(
                    ErrorKind::Unsupported,
                    "Invalid FixedSizeBinary: number of child elements ({}) not divisible by n ({})",
                    arr.data.len(),
                    arr.n,
                );
            }
            Ok(arrow_data::ArrayData::try_new(
                arrow_schema::DataType::FixedSizeBinary(arr.n),
                arr.data.len() / usize::try_from(arr.n)?,
                arr.validity.map(arrow_buffer::Buffer::from_vec),
                0,
                vec![arrow_buffer::ScalarBuffer::from(arr.data).into_inner()],
                vec![],
            )?)
        }
        A::Dictionary(arr) => {
            let keys = build_array_data(*arr.keys)?;
            let values = build_array_data(*arr.values)?;
            let data_type = arrow_schema::DataType::Dictionary(
                Box::new(keys.data_type().clone()),
                Box::new(values.data_type().clone()),
            );

            Ok(keys
                .into_builder()
                .data_type(data_type)
                .child_data(vec![values])
                .build()?)
        }
        A::RunEndEncoded(arr) => {
            let len = get_ree_len_from_indices(&arr.run_ends)?;
            let run_ends = build_array_data(*arr.run_ends)?;
            let values = build_array_data(*arr.values)?;
            let data_type = arrow_schema::DataType::RunEndEncoded(
                field_from_data_and_meta(
                    &run_ends,
                    FieldMeta {
                        name: arr.meta.run_ends_name,
                        ..FieldMeta::default()
                    },
                )
                .into(),
                field_from_data_and_meta(&values, arr.meta.values).into(),
            );

            Ok(arrow_data::ArrayData::try_new(
                data_type,
                len,
                None,
                0,
                vec![],
                vec![run_ends, values],
            )?)
        }
        A::Map(arr) => {
            let (entries, entries_name, sorted, validity, offsets) = arr.into_logical_array()?;
            let entries = build_array_data(entries)?;
            let field = field_from_data_and_meta(
                &entries,
                FieldMeta {
                    name: entries_name,
                    ..FieldMeta::default()
                },
            );

            Ok(arrow_data::ArrayData::try_new(
                arrow_schema::DataType::Map(Arc::new(field), sorted),
                offsets.len().saturating_sub(1),
                validity.map(arrow_buffer::Buffer::from_vec),
                0,
                vec![arrow_buffer::ScalarBuffer::from(offsets).into_inner()],
                vec![entries],
            )?)
        }
        A::Union(arr) => {
            let (fields, child_data) = union_fields_into_fields_and_data(arr.fields)?;
            let len = arr.types.len();
            let mut buffers = vec![arrow_buffer::ScalarBuffer::from(arr.types).into_inner()];
            let mode;

            if let Some(offsets) = arr.offsets {
                buffers.push(arrow_buffer::ScalarBuffer::from(offsets).into_inner());
                mode = arrow_schema::UnionMode::Dense;
            } else {
                mode = arrow_schema::UnionMode::Sparse;
            }

            Ok(arrow_data::ArrayData::try_new(
                arrow_schema::DataType::Union(fields.into_iter().collect(), mode),
                len,
                None,
                0,
                buffers,
                child_data,
            )?)
        }
        array => build_array_data_from_marrow(array),
    }
}

fn get_ree_len_from_indices(indices: &Array) -> Result<usize> {
    let cand = match indices {
        Array::Int16(array) => array.values.last().copied().map(usize::try_from),
        Array::Int32(array) => array.values.last().copied().map(usize::try_from),
        Array::Int64(array) => array.values.last().copied().map(usize::try_from),
        // TODO: include data type
        _ => fail!(
            ErrorKind::Unsupported,
            "unsupported run ends in RunEndEncoded"
        ),
    };

    match cand {
        Some(Ok(len)) => Ok(len),
        Some(Err(err)) => Err(err.into()),
        None => Ok(0),
    }
}

#[allow(clippy::type_complexity)]
fn union_fields_into_fields_and_data(
    union_fields: Vec<(i8, FieldMeta, Array)>,
) -> Result<(
    Vec<(i8, arrow_schema::FieldRef)>,
    Vec<arrow_data::ArrayData>,
)> {
    let mut fields = Vec::new();
    let mut child_data = Vec::new();

    for (type_id, meta, array) in union_fields {
        let child = build_array_data(array)?;
        let field = field_from_data_and_meta(&child, meta);

        fields.push((type_id, Arc::new(field)));
        child_data.push(child);
    }

    Ok((fields, child_data))
}

/// Converison from `arrow` arrays (*requires one of the `arrow-{version}` features*)
impl<'a> TryFrom<&'a dyn arrow_array::Array> for View<'a> {
    type Error = MarrowError;

    fn try_from(array: &'a dyn arrow_array::Array) -> Result<Self> {
        use arrow_array::Array;

        let any = array.as_any();
        if let Some(array) = any.downcast_ref::<arrow_array::NullArray>() {
            use arrow_array::Array;

            Ok(View::Null(NullView { len: array.len() }))
        } else if let Some(array) = any.downcast_ref::<arrow_array::BooleanArray>() {
            Ok(View::Boolean(BooleanView {
                len: array.len(),
                validity: get_bits_with_offset(array),
                values: BitsWithOffset {
                    offset: array.values().offset(),
                    data: array.values().values(),
                },
            }))
        } else if let Some(array) = any.downcast_ref::<arrow_array::Int8Array>() {
            Ok(View::Int8(PrimitiveView {
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<arrow_array::Int16Array>() {
            Ok(View::Int16(PrimitiveView {
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<arrow_array::Int32Array>() {
            Ok(View::Int32(PrimitiveView {
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<arrow_array::Int64Array>() {
            Ok(View::Int64(PrimitiveView {
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<arrow_array::UInt8Array>() {
            Ok(View::UInt8(PrimitiveView {
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<arrow_array::UInt16Array>() {
            Ok(View::UInt16(PrimitiveView {
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<arrow_array::UInt32Array>() {
            Ok(View::UInt32(PrimitiveView {
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<arrow_array::UInt64Array>() {
            Ok(View::UInt64(PrimitiveView {
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<arrow_array::Float16Array>() {
            Ok(View::Float16(PrimitiveView {
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<arrow_array::Float32Array>() {
            Ok(View::Float32(PrimitiveView {
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<arrow_array::Float64Array>() {
            Ok(View::Float64(PrimitiveView {
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<arrow_array::Decimal128Array>() {
            use arrow_array::Array;

            let &arrow_schema::DataType::Decimal128(precision, scale) = array.data_type() else {
                fail!(
                    ErrorKind::Unsupported,
                    "Invalid data type for Decimal128 array: {}",
                    array.data_type()
                );
            };
            Ok(View::Decimal128(DecimalView {
                precision,
                scale,
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<arrow_array::Date32Array>() {
            Ok(View::Date32(PrimitiveView {
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<arrow_array::Date64Array>() {
            Ok(View::Date64(PrimitiveView {
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<arrow_array::Time32MillisecondArray>() {
            Ok(View::Time32(TimeView {
                unit: TimeUnit::Millisecond,
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<arrow_array::Time32SecondArray>() {
            Ok(View::Time32(TimeView {
                unit: TimeUnit::Second,
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<arrow_array::Time64NanosecondArray>() {
            Ok(View::Time64(TimeView {
                unit: TimeUnit::Nanosecond,
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<arrow_array::Time64MicrosecondArray>() {
            Ok(View::Time64(TimeView {
                unit: TimeUnit::Microsecond,
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<arrow_array::TimestampNanosecondArray>() {
            Ok(View::Timestamp(TimestampView {
                unit: TimeUnit::Nanosecond,
                timezone: array.timezone().map(str::to_owned),
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<arrow_array::TimestampMicrosecondArray>() {
            Ok(View::Timestamp(TimestampView {
                unit: TimeUnit::Microsecond,
                timezone: array.timezone().map(str::to_owned),
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<arrow_array::TimestampMillisecondArray>() {
            Ok(View::Timestamp(TimestampView {
                unit: TimeUnit::Millisecond,
                timezone: array.timezone().map(str::to_owned),
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<arrow_array::TimestampSecondArray>() {
            Ok(View::Timestamp(TimestampView {
                unit: TimeUnit::Second,
                timezone: array.timezone().map(str::to_owned),
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<arrow_array::DurationNanosecondArray>() {
            Ok(View::Duration(TimeView {
                unit: TimeUnit::Nanosecond,
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<arrow_array::DurationMicrosecondArray>() {
            Ok(View::Duration(TimeView {
                unit: TimeUnit::Microsecond,
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<arrow_array::DurationMillisecondArray>() {
            Ok(View::Duration(TimeView {
                unit: TimeUnit::Millisecond,
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<arrow_array::DurationSecondArray>() {
            Ok(View::Duration(TimeView {
                unit: TimeUnit::Second,
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<arrow_array::IntervalYearMonthArray>() {
            Ok(View::YearMonthInterval(PrimitiveView {
                validity: get_bits_with_offset(array),
                values: array.values(),
            }))
        } else if let Some(array) = any.downcast_ref::<arrow_array::IntervalDayTimeArray>() {
            Ok(View::DayTimeInterval(PrimitiveView {
                validity: get_bits_with_offset(array),
                // bytemuck checks the dynamically. This check always succeeds if the the target
                // alignment is smaller or equal to the source alignment. This is the case here, as
                // structs are aligned to their largest field (which is at most 64 bits) and arrow
                // aligns to 64 bits.
                values: bytemuck::try_cast_slice(array.values().inner().as_slice())?,
            }))
        } else if let Some(array) = any.downcast_ref::<arrow_array::IntervalMonthDayNanoArray>() {
            Ok(View::MonthDayNanoInterval(PrimitiveView {
                validity: get_bits_with_offset(array),
                // See note for DayTimeInterval
                values: bytemuck::try_cast_slice(array.values().inner().as_slice())?,
            }))
        } else if let Some(array) = any.downcast_ref::<arrow_array::StringArray>() {
            Ok(View::Utf8(BytesView {
                validity: get_bits_with_offset(array),
                offsets: array.value_offsets(),
                data: array.value_data(),
            }))
        } else if let Some(array) = any.downcast_ref::<arrow_array::LargeStringArray>() {
            Ok(View::LargeUtf8(BytesView {
                validity: get_bits_with_offset(array),
                offsets: array.value_offsets(),
                data: array.value_data(),
            }))
        } else if let Some(array) = any.downcast_ref::<arrow_array::BinaryArray>() {
            Ok(View::Binary(BytesView {
                validity: get_bits_with_offset(array),
                offsets: array.value_offsets(),
                data: array.value_data(),
            }))
        } else if let Some(array) = any.downcast_ref::<arrow_array::LargeBinaryArray>() {
            Ok(View::LargeBinary(BytesView {
                validity: get_bits_with_offset(array),
                offsets: array.value_offsets(),
                data: array.value_data(),
            }))
        } else if let Some(array) = any.downcast_ref::<arrow_array::ListArray>() {
            use arrow_array::Array;

            let arrow_schema::DataType::List(field) = array.data_type() else {
                fail!(
                    ErrorKind::Unsupported,
                    "invalid data type for list array: {}",
                    array.data_type()
                );
            };
            Ok(View::List(ListView {
                validity: get_bits_with_offset(array),
                offsets: array.value_offsets(),
                meta: meta_from_field(field.as_ref().try_into()?),
                elements: Box::new(array.values().as_ref().try_into()?),
            }))
        } else if let Some(array) = any.downcast_ref::<arrow_array::LargeListArray>() {
            let arrow_schema::DataType::LargeList(field) = array.data_type() else {
                fail!(
                    ErrorKind::Unsupported,
                    "invalid data type for list array: {}",
                    array.data_type()
                );
            };
            Ok(View::LargeList(ListView {
                validity: get_bits_with_offset(array),
                offsets: array.value_offsets(),
                meta: meta_from_field(field.as_ref().try_into()?),
                elements: Box::new(array.values().as_ref().try_into()?),
            }))
        } else if let Some(array) = any.downcast_ref::<arrow_array::FixedSizeListArray>() {
            let arrow_schema::DataType::FixedSizeList(field, n) = array.data_type() else {
                fail!(
                    ErrorKind::Unsupported,
                    "invalid data type for list array: {}",
                    array.data_type()
                );
            };
            Ok(View::FixedSizeList(FixedSizeListView {
                len: array.len(),
                n: *n,
                validity: get_bits_with_offset(array),
                meta: meta_from_field(field.as_ref().try_into()?),
                elements: Box::new(array.values().as_ref().try_into()?),
            }))
        } else if let Some(array) = any.downcast_ref::<arrow_array::StructArray>() {
            let arrow_schema::DataType::Struct(column_fields) = array.data_type() else {
                fail!(
                    ErrorKind::Unsupported,
                    "invalid data type for struct array: {}",
                    array.data_type()
                );
            };

            let mut fields = Vec::new();
            for (field, array) in std::iter::zip(column_fields, array.columns()) {
                let view = View::try_from(array.as_ref())?;
                let meta = meta_from_field(Field::try_from(field.as_ref())?);
                fields.push((meta, view));
            }

            Ok(View::Struct(StructView {
                len: array.len(),
                validity: get_bits_with_offset(array),
                fields,
            }))
        } else if let Some(array) = any.downcast_ref::<arrow_array::MapArray>() {
            let Some((entries_name, sorted)) = map_meta_from_data_type(array.data_type()) else {
                fail!(
                    ErrorKind::Unsupported,
                    "invalid data type for map array: {}",
                    array.data_type()
                );
            };
            let entries_view = View::try_from(array.entries() as &dyn arrow_array::Array)?;

            Ok(View::Map(MapView::from_logical_view(
                entries_view,
                entries_name,
                sorted,
                get_bits_with_offset(array),
                array.value_offsets(),
            )?))
        } else if let Some(array) = any.downcast_ref::<arrow_array::UInt8DictionaryArray>() {
            wrap_dictionary_array::<arrow_array::types::UInt8Type>(array)
        } else if let Some(array) = any.downcast_ref::<arrow_array::UInt16DictionaryArray>() {
            wrap_dictionary_array::<arrow_array::types::UInt16Type>(array)
        } else if let Some(array) = any.downcast_ref::<arrow_array::UInt32DictionaryArray>() {
            wrap_dictionary_array::<arrow_array::types::UInt32Type>(array)
        } else if let Some(array) = any.downcast_ref::<arrow_array::UInt64DictionaryArray>() {
            wrap_dictionary_array::<arrow_array::types::UInt64Type>(array)
        } else if let Some(array) = any.downcast_ref::<arrow_array::Int8DictionaryArray>() {
            wrap_dictionary_array::<arrow_array::types::Int8Type>(array)
        } else if let Some(array) = any.downcast_ref::<arrow_array::Int16DictionaryArray>() {
            wrap_dictionary_array::<arrow_array::types::Int16Type>(array)
        } else if let Some(array) = any.downcast_ref::<arrow_array::Int32DictionaryArray>() {
            wrap_dictionary_array::<arrow_array::types::Int32Type>(array)
        } else if let Some(array) = any.downcast_ref::<arrow_array::Int64DictionaryArray>() {
            wrap_dictionary_array::<arrow_array::types::Int64Type>(array)
        } else if let Some(run_array) =
            any.downcast_ref::<arrow_array::RunArray<arrow_array::types::Int16Type>>()
        {
            wrap_ree_array::<arrow_array::types::Int16Type, _>(
                View::Int16,
                array.data_type(),
                run_array,
            )
        } else if let Some(run_array) =
            any.downcast_ref::<arrow_array::RunArray<arrow_array::types::Int32Type>>()
        {
            wrap_ree_array::<arrow_array::types::Int32Type, _>(
                View::Int32,
                array.data_type(),
                run_array,
            )
        } else if let Some(run_array) =
            any.downcast_ref::<arrow_array::RunArray<arrow_array::types::Int64Type>>()
        {
            wrap_ree_array::<arrow_array::types::Int64Type, _>(
                View::Int64,
                array.data_type(),
                run_array,
            )
        } else if let Some(array) = any.downcast_ref::<arrow_array::UnionArray>() {
            use arrow_array::Array;

            let arrow_schema::DataType::Union(union_fields, mode) = array.data_type() else {
                fail!(ErrorKind::Unsupported, "Invalid data type for UnionArray");
            };

            let mut fields = Vec::new();
            for (type_id, field) in union_fields.iter() {
                let meta = meta_from_field(Field::try_from(field.as_ref())?);
                let view: View = array.child(type_id).as_ref().try_into()?;
                fields.push((type_id, meta, view));
            }

            let offsets = match mode {
                arrow_schema::UnionMode::Dense => {
                    let Some(offsets) = array.offsets() else {
                        fail!(
                            ErrorKind::Unsupported,
                            "Dense unions must have an offset array"
                        );
                    };
                    Some(offsets as &[i32])
                }
                arrow_schema::UnionMode::Sparse => {
                    if array.offsets().is_some() {
                        fail!(
                            ErrorKind::Unsupported,
                            "Sparse unions must not have an offset array"
                        );
                    };
                    None
                }
            };
            Ok(View::Union(UnionView {
                types: array.type_ids(),
                offsets,
                fields,
            }))
        } else {
            convert_array_to_marrow(array)
        }
    }
}

fn map_meta_from_data_type(data_type: &arrow_schema::DataType) -> Option<(String, bool)> {
    let arrow_schema::DataType::Map(entries_field, sorted) = data_type else {
        return None;
    };
    if entries_field.is_nullable() || !entries_field.metadata().is_empty() {
        return None;
    }
    Some((entries_field.name().clone(), *sorted))
}

fn field_from_data_and_meta(data: &arrow_data::ArrayData, meta: FieldMeta) -> arrow_schema::Field {
    arrow_schema::Field::new(meta.name, data.data_type().clone(), meta.nullable)
        .with_metadata(meta.metadata)
}

fn primitive_into_data<T: arrow_buffer::ArrowNativeType>(
    data_type: arrow_schema::DataType,
    validity: Option<Vec<u8>>,
    values: Vec<T>,
) -> Result<arrow_data::ArrayData> {
    Ok(arrow_data::ArrayData::try_new(
        data_type,
        values.len(),
        validity.map(arrow_buffer::Buffer::from_vec),
        0,
        vec![arrow_buffer::ScalarBuffer::from(values).into_inner()],
        vec![],
    )?)
}

fn bytes_into_data<O: arrow_buffer::ArrowNativeType>(
    data_type: arrow_schema::DataType,
    offsets: Vec<O>,
    data: Vec<u8>,
    validity: Option<Vec<u8>>,
) -> Result<arrow_data::ArrayData> {
    Ok(arrow_data::ArrayData::try_new(
        data_type,
        offsets.len().saturating_sub(1),
        validity.map(arrow_buffer::Buffer::from_vec),
        0,
        vec![
            arrow_buffer::ScalarBuffer::from(offsets).into_inner(),
            arrow_buffer::ScalarBuffer::from(data).into_inner(),
        ],
        vec![],
    )?)
}

fn list_into_data<O: arrow_buffer::ArrowNativeType>(
    data_type: arrow_schema::DataType,
    len: usize,
    offsets: Vec<O>,
    child_data: arrow_data::ArrayData,
    validity: Option<Vec<u8>>,
) -> Result<arrow_data::ArrayData> {
    Ok(arrow_data::ArrayData::try_new(
        data_type,
        len,
        validity.map(arrow_buffer::Buffer::from_vec),
        0,
        vec![arrow_buffer::ScalarBuffer::from(offsets).into_inner()],
        vec![child_data],
    )?)
}

fn wrap_dictionary_array<K: arrow_array::types::ArrowDictionaryKeyType>(
    array: &arrow_array::DictionaryArray<K>,
) -> Result<View<'_>> {
    let keys: &dyn arrow_array::Array = array.keys();

    Ok(View::Dictionary(DictionaryView {
        keys: Box::new(keys.try_into()?),
        values: Box::new(array.values().as_ref().try_into()?),
    }))
}

fn wrap_ree_array<'a, T, F>(
    wrap: F,
    dt: &arrow_schema::DataType,
    array: &'a arrow_array::RunArray<T>,
) -> Result<View<'a>>
where
    T: arrow_array::types::RunEndIndexType,
    F: FnOnce(PrimitiveView<'a, <T as arrow_array::ArrowPrimitiveType>::Native>) -> View<'a>,
{
    let arrow_schema::DataType::RunEndEncoded(run_ends_field, values_field) = dt else {
        fail!(
            ErrorKind::Unsupported,
            "Invalid data type for run end encoded array"
        );
    };

    if run_ends_field.is_nullable() {
        fail!(
            ErrorKind::Unsupported,
            "Nullable run ends are not supported"
        );
    }

    let run_ends = wrap(PrimitiveView {
        validity: None,
        values: array.run_ends().values(),
    });
    let values = View::try_from(array.values().as_ref())?;

    Ok(View::RunEndEncoded(RunEndEncodedView {
        meta: RunEndEncodedMeta {
            run_ends_name: run_ends_field.name().clone(),
            values: meta_from_field(values_field.as_ref().try_into()?),
        },
        run_ends: Box::new(run_ends),
        values: Box::new(values),
    }))
}

fn get_bits_with_offset(array: &dyn arrow_array::Array) -> Option<BitsWithOffset<'_>> {
    let validity = array.nulls()?;
    Some(BitsWithOffset {
        offset: validity.offset(),
        data: validity.validity(),
    })
}

fn try_cast_vec<A: bytemuck::NoUninit, B: bytemuck::AnyBitPattern>(a: Vec<A>) -> Result<Vec<B>> {
    let mut res = Vec::new();
    for item in a {
        res.push(bytemuck::try_cast(item)?);
    }
    Ok(res)
}
