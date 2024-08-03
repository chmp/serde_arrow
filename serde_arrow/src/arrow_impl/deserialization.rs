use crate::internal::{
    arrow::{
        ArrayView, BitsWithOffset, BooleanArrayView, BytesArrayView, DecimalArrayView,
        DenseUnionArrayView, DictionaryArrayView, FixedSizeListArrayView, ListArrayView,
        NullArrayView, PrimitiveArrayView, StructArrayView, TimeArrayView, TimeUnit,
        TimestampArrayView,
    },
    deserialization::{
        array_deserializer::ArrayDeserializer,
        outer_sequence_deserializer::OuterSequenceDeserializer,
    },
    deserializer::Deserializer,
    error::{fail, Error, Result},
    schema::GenericField,
    serialization::utils::meta_from_field,
};

use crate::_impl::arrow::{
    array::{
        Array, BooleanArray, DictionaryArray, FixedSizeBinaryArray, FixedSizeListArray,
        GenericBinaryArray, GenericListArray, GenericStringArray, MapArray, NullArray,
        PrimitiveArray, RecordBatch, StructArray, UnionArray,
    },
    datatypes::{
        ArrowDictionaryKeyType, DataType, Date32Type, Date64Type, Decimal128Type,
        DurationMicrosecondType, DurationMillisecondType, DurationNanosecondType,
        DurationSecondType, FieldRef, Float16Type, Float32Type, Float64Type, Int16Type, Int32Type,
        Int64Type, Int8Type, Time32MillisecondType, Time32SecondType, Time64MicrosecondType,
        Time64NanosecondType, TimestampMicrosecondType, TimestampMillisecondType,
        TimestampNanosecondType, TimestampSecondType, UInt16Type, UInt32Type, UInt64Type,
        UInt8Type, UnionMode,
    },
};

impl<'de> Deserializer<'de> {
    /// Construct a new deserializer from `arrow` arrays (*requires one of the
    /// `arrow-*` features*)
    ///
    /// Usage
    /// ```rust
    /// # fn main() -> serde_arrow::Result<()> {
    /// # let (_, arrays) = serde_arrow::_impl::docs::defs::example_arrow_arrays();
    /// # use serde_arrow::_impl::arrow;
    /// use arrow::datatypes::FieldRef;
    /// use serde::{Deserialize, Serialize};
    /// use serde_arrow::{Deserializer, schema::{SchemaLike, TracingOptions}};
    ///
    /// ##[derive(Deserialize, Serialize)]
    /// struct Record {
    ///     a: Option<f32>,
    ///     b: u64,
    /// }
    ///
    /// let fields = Vec::<FieldRef>::from_type::<Record>(TracingOptions::default())?;
    ///
    /// let deserializer = Deserializer::from_arrow(&fields, &arrays)?;
    /// let items = Vec::<Record>::deserialize(deserializer)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn from_arrow<A>(fields: &[FieldRef], arrays: &'de [A]) -> Result<Self>
    where
        A: AsRef<dyn Array>,
    {
        let fields = fields
            .iter()
            .map(|field| GenericField::try_from(field.as_ref()))
            .collect::<Result<Vec<_>>>()?;
        let arrays = arrays
            .iter()
            .map(|array| array.as_ref())
            .collect::<Vec<_>>();

        if fields.len() != arrays.len() {
            fail!(
                "different number of fields ({}) and arrays ({})",
                fields.len(),
                arrays.len()
            );
        }
        let len = arrays.first().map(|array| array.len()).unwrap_or_default();

        let mut deserializers = Vec::new();
        for (field, array) in std::iter::zip(&fields, arrays) {
            if array.len() != len {
                fail!("arrays of different lengths are not supported");
            }

            let deserializer = ArrayDeserializer::new(field.strategy.as_ref(), array.try_into()?)?;
            deserializers.push((field.name.clone(), deserializer));
        }

        let deserializer = OuterSequenceDeserializer::new(deserializers, len);
        let deserializer = Deserializer(deserializer);

        Ok(deserializer)
    }

    /// Construct a new deserializer from a record batch (*requires one of the
    /// `arrow-*` features*)
    ///
    /// Usage:
    ///
    /// ```rust
    /// # fn main() -> serde_arrow::Result<()> {
    /// # let record_batch = serde_arrow::_impl::docs::defs::example_record_batch();
    /// #
    /// use serde::Deserialize;
    /// use serde_arrow::Deserializer;
    ///
    /// ##[derive(Deserialize)]
    /// struct Record {
    ///     a: Option<f32>,
    ///     b: u64,
    /// }
    ///
    /// let deserializer = Deserializer::from_record_batch(&record_batch)?;
    /// let items = Vec::<Record>::deserialize(deserializer)?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    pub fn from_record_batch(record_batch: &'de RecordBatch) -> Result<Self> {
        let schema = record_batch.schema();
        Deserializer::from_arrow(schema.fields(), record_batch.columns())
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
                meta: meta_from_field(field.as_ref().try_into()?)?,
                element: Box::new(array.values().as_ref().try_into()?),
            }))
        } else if let Some(array) = any.downcast_ref::<GenericListArray<i64>>() {
            let DataType::LargeList(field) = array.data_type() else {
                fail!("invalid data type for list array: {}", array.data_type());
            };
            Ok(ArrayView::LargeList(ListArrayView {
                validity: get_bits_with_offset(array),
                offsets: array.value_offsets(),
                meta: meta_from_field(field.as_ref().try_into()?)?,
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
                meta: meta_from_field(field.as_ref().try_into()?)?,
                element: Box::new(array.values().as_ref().try_into()?),
            }))
        } else if let Some(array) = any.downcast_ref::<StructArray>() {
            let DataType::Struct(column_fields) = array.data_type() else {
                fail!("invalid data type for struct array: {}", array.data_type());
            };

            let mut fields = Vec::new();
            for (field, array) in std::iter::zip(column_fields, array.columns()) {
                let view = ArrayView::try_from(array.as_ref())?;
                let meta = meta_from_field(GenericField::try_from(field.as_ref())?)?;
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
                meta: meta_from_field(GenericField::try_from(entries_field.as_ref())?)?,
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
            for (type_idx, (type_id, field)) in union_fields.iter().enumerate() {
                if type_id < 0 || usize::try_from(type_id)? != type_idx {
                    fail!("invalid union, only unions with consecutive variants are supported");
                }

                let meta = meta_from_field(GenericField::try_from(field.as_ref())?)?;
                let view: ArrayView = array.child(type_id).as_ref().try_into()?;
                fields.push((view, meta));
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
pub fn wrap_fixed_size_binary_array<'a>(array: &'a FixedSizeBinaryArray) -> Result<ArrayView<'a>> {
    use crate::internal::arrow::FixedSizeBinaryArrayView;

    Ok(ArrayView::FixedSizeBinary(FixedSizeBinaryArrayView {
        n: array.value_length(),
        validity: get_bits_with_offset(array),
        data: array.value_data(),
    }))
}

#[cfg(not(has_arrow_fixed_binary_support))]
pub fn wrap_fixed_size_binary_array<'a>(_array: &'a FixedSizeBinaryArray) -> Result<ArrayView<'a>> {
    fail!("FixedSizeBinary arrays are not supported for arrow<=46");
}

fn get_bits_with_offset(array: &dyn Array) -> Option<BitsWithOffset<'_>> {
    let validity = array.nulls()?;
    Some(BitsWithOffset {
        offset: validity.offset(),
        data: validity.validity(),
    })
}
