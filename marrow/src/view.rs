//! Arrays with borrowed data
//!
//! [`View`] is the union of all views supported by `marrow`. Each view corresponds 1:1 to an array.
use half::f16;

use crate::{
    datatypes::{
        field_from_meta, DataType, Field, FieldMeta, IntervalUnit, MapMeta, RunEndEncodedMeta,
        TimeUnit, UnionMode,
    },
    error::{fail, ErrorKind, Result},
    types::{DayTimeInterval, MonthDayNanoInterval},
};

// assert that the `Array` implements the expected traits
#[allow(unused)]
const _: () = {
    trait AssertExpectedTraits: Clone + std::fmt::Debug + PartialEq + Send + Sync {}
    impl AssertExpectedTraits for View<'_> {}
};

/// An array with borrowed data
///
/// See [`Array`][crate::array::Array]
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum View<'a> {
    /// See [`Array::Null`][crate::array::Array::Null]
    Null(NullView),
    /// See [`Array::Boolean`][crate::array::Array::Boolean]
    Boolean(BooleanView<'a>),
    /// See [`Array::Int8`][crate::array::Array::Int8]
    Int8(PrimitiveView<'a, i8>),
    /// See [`Array::Int16`][crate::array::Array::Int16]
    Int16(PrimitiveView<'a, i16>),
    /// See [`Array::Int32`][crate::array::Array::Int32]
    Int32(PrimitiveView<'a, i32>),
    /// See [`Array::Int64`][crate::array::Array::Int64]
    Int64(PrimitiveView<'a, i64>),
    /// See [`Array::UInt8`][crate::array::Array::UInt8]
    UInt8(PrimitiveView<'a, u8>),
    /// See [`Array::UInt16`][crate::array::Array::UInt16]
    UInt16(PrimitiveView<'a, u16>),
    /// See [`Array::UInt32`][crate::array::Array::UInt32]
    UInt32(PrimitiveView<'a, u32>),
    /// See [`Array::UInt64`][crate::array::Array::UInt64]
    UInt64(PrimitiveView<'a, u64>),
    /// See [`Array::Float16`][crate::array::Array::Float16]
    Float16(PrimitiveView<'a, f16>),
    /// See [`Array::Float32`][crate::array::Array::Float32]
    Float32(PrimitiveView<'a, f32>),
    /// See [`Array::Float64`][crate::array::Array::Float64]
    Float64(PrimitiveView<'a, f64>),
    /// See [`Array::Date32`][crate::array::Array::Date32]
    Date32(PrimitiveView<'a, i32>),
    /// See [`Array::Date64`][crate::array::Array::Date64]
    Date64(PrimitiveView<'a, i64>),
    /// See [`Array::Time32`][crate::array::Array::Time32]
    Time32(TimeView<'a, i32>),
    /// See [`Array::Time64`][crate::array::Array::Time64]
    Time64(TimeView<'a, i64>),
    /// See [`Array::Timestamp`][crate::array::Array::Timestamp]
    Timestamp(TimestampView<'a>),
    /// See [`Array::Duration`][crate::array::Array::Duration]
    Duration(TimeView<'a, i64>),
    /// See [`Array::YearMonthInterval`][crate::array::Array::YearMonthInterval]
    YearMonthInterval(PrimitiveView<'a, i32>),
    /// See [`Array::DayTimeInterval`][crate::array::Array::DayTimeInterval]
    DayTimeInterval(PrimitiveView<'a, DayTimeInterval>),
    /// See [`Array::MonthDayNanoInterval`][crate::array::Array::MonthDayNanoInterval]
    MonthDayNanoInterval(PrimitiveView<'a, MonthDayNanoInterval>),
    /// See [`Array::Utf8`][crate::array::Array::Utf8]
    Utf8(BytesView<'a, i32>),
    /// See [`Array::LargeUtf8`][crate::array::Array::LargeUtf8]
    LargeUtf8(BytesView<'a, i64>),
    /// [`Array::Utf8View`][crate::array::Array::Utf8View]
    Utf8View(BytesViewView<'a>),
    /// See [`Array::Binary`][crate::array::Array::Binary]
    Binary(BytesView<'a, i32>),
    /// See [`Array::LargeBinary`][crate::array::Array::LargeBinary]
    LargeBinary(BytesView<'a, i64>),
    /// See [`Array::FixedSizeBinary`][crate::array::Array::FixedSizeBinary]
    FixedSizeBinary(FixedSizeBinaryView<'a>),
    /// [`Array::BinaryView`][crate::array::Array::BinaryView]
    BinaryView(BytesViewView<'a>),
    /// See [`Array::Decimal128`][crate::array::Array::Decimal128]
    Decimal128(DecimalView<'a, i128>),
    /// See [`Array::Struct`][crate::array::Array::Struct]
    Struct(StructView<'a>),
    /// See [`Array::List`][crate::array::Array::List]
    List(ListView<'a, i32>),
    /// See [`Array::LargeList`][crate::array::Array::LargeList]
    LargeList(ListView<'a, i64>),
    /// See [`Array::FixedSizeList`][crate::array::Array::FixedSizeList]
    FixedSizeList(FixedSizeListView<'a>),
    /// See [`Array::Dictionary`][crate::array::Array::Dictionary]
    Dictionary(DictionaryView<'a>),
    /// See [`Array::RunEndEncoded`][crate::array::Array::RunEndEncoded]
    RunEndEncoded(RunEndEncodedView<'a>),
    /// See [`Array::Map`][crate::array::Array::Map]
    Map(MapView<'a>),
    /// See [`Array::Union`][crate::array::Array::Union]
    Union(UnionView<'a>),
}

impl View<'_> {
    /// Get the data type of this array
    pub fn data_type(&self) -> DataType {
        use DataType as T;
        match self {
            Self::Null(_) => T::Null,
            Self::Boolean(_) => T::Boolean,
            Self::Int8(_) => T::Int8,
            Self::Int16(_) => T::Int16,
            Self::Int32(_) => T::Int32,
            Self::Int64(_) => T::Int64,
            Self::UInt8(_) => T::UInt8,
            Self::UInt16(_) => T::UInt16,
            Self::UInt32(_) => T::UInt32,
            Self::UInt64(_) => T::UInt64,
            Self::Float16(_) => T::Float16,
            Self::Float32(_) => T::Float32,
            Self::Float64(_) => T::Float64,
            Self::Decimal128(arr) => T::Decimal128(arr.precision, arr.scale),
            Self::Date32(_) => T::Date32,
            Self::Date64(_) => T::Date64,
            Self::Time32(arr) => T::Time32(arr.unit),
            Self::Time64(arr) => T::Time64(arr.unit),
            Self::Timestamp(arr) => T::Timestamp(arr.unit, arr.timezone.clone()),
            Self::Duration(arr) => T::Duration(arr.unit),
            Self::DayTimeInterval(_) => T::Interval(IntervalUnit::DayTime),
            Self::YearMonthInterval(_) => T::Interval(IntervalUnit::YearMonth),
            Self::MonthDayNanoInterval(_) => T::Interval(IntervalUnit::MonthDayNano),
            Self::Binary(_) => T::Binary,
            Self::LargeBinary(_) => T::LargeBinary,
            Self::FixedSizeBinary(arr) => T::FixedSizeBinary(arr.n),
            Self::BinaryView(_) => T::BinaryView,
            Self::Utf8(_) => T::Utf8,
            Self::LargeUtf8(_) => T::LargeUtf8,
            Self::Utf8View(_) => T::Utf8View,
            Self::Dictionary(arr) => T::Dictionary(
                Box::new(arr.keys.data_type()),
                Box::new(arr.values.data_type()),
            ),
            Self::List(arr) => T::List(Box::new(field_from_meta(
                arr.elements.data_type(),
                arr.meta.clone(),
            ))),
            Self::LargeList(arr) => T::LargeList(Box::new(field_from_meta(
                arr.elements.data_type(),
                arr.meta.clone(),
            ))),
            Self::FixedSizeList(arr) => T::FixedSizeList(
                Box::new(field_from_meta(arr.elements.data_type(), arr.meta.clone())),
                arr.n,
            ),
            Self::Struct(arr) => T::Struct(
                arr.fields
                    .iter()
                    .map(|(meta, field)| field_from_meta(field.data_type(), meta.clone()))
                    .collect(),
            ),
            Self::Union(arr) => T::Union(
                arr.fields
                    .iter()
                    .map(|(type_id, meta, field)| {
                        (*type_id, field_from_meta(field.data_type(), meta.clone()))
                    })
                    .collect(),
                match arr.offsets {
                    Some(_) => UnionMode::Dense,
                    None => UnionMode::Sparse,
                },
            ),
            Self::Map(arr) => T::Map(
                Box::new(Field {
                    name: arr.meta.entries_name.clone(),
                    data_type: DataType::Struct(vec![
                        field_from_meta(arr.keys.data_type(), arr.meta.keys.clone()),
                        field_from_meta(arr.values.data_type(), arr.meta.values.clone()),
                    ]),
                    ..Field::default()
                }),
                arr.meta.sorted,
            ),
            Self::RunEndEncoded(arr) => T::RunEndEncoded(
                Box::new(Field {
                    name: arr.meta.run_ends_name.clone(),
                    data_type: arr.run_ends.data_type(),
                    nullable: false,
                    metadata: Default::default(),
                }),
                Box::new(field_from_meta(
                    arr.values.data_type(),
                    arr.meta.values.clone(),
                )),
            ),
        }
    }
}

/// A bitmap with an optional offset
///
/// The `i`-th element is stored at bit `offset + i`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BitsWithOffset<'a> {
    /// The offset of the bits
    pub offset: usize,
    /// The data of the bitmap
    pub data: &'a [u8],
}

/// See [`NullArray`][crate::array::NullArray]
#[derive(Clone, Debug, PartialEq)]
pub struct NullView {
    /// See [`NullArray::len`][crate::array::NullArray::len]
    pub len: usize,
}

/// See [`BooleanArray`][crate::array::BooleanArray]
#[derive(Clone, Debug, PartialEq)]
pub struct BooleanView<'a> {
    /// See [`BooleanArray::len`][crate::array::BooleanArray::len]
    pub len: usize,
    /// See [`BooleanArray::validity`][crate::array::BooleanArray::validity]
    pub validity: Option<BitsWithOffset<'a>>,
    /// See [`BooleanArray::values`][crate::array::BooleanArray::values]
    pub values: BitsWithOffset<'a>,
}

/// See [`PrimitiveArray`][crate::array::PrimitiveArray]
#[derive(Clone, Debug, PartialEq)]
pub struct PrimitiveView<'a, T> {
    /// See [`PrimitiveArray::validity`][crate::array::PrimitiveArray::validity]
    pub validity: Option<BitsWithOffset<'a>>,
    /// See [`PrimitiveArray::values`][crate::array::PrimitiveArray::values]
    pub values: &'a [T],
}

/// See [`TimeArray`][crate::array::TimeArray]
#[derive(Debug, Clone, PartialEq)]
pub struct TimeView<'a, T> {
    /// See [`TimeArray::unit`][crate::array::TimeArray::unit]
    pub unit: TimeUnit,
    /// See [`TimeArray::validity`][crate::array::TimeArray::validity]
    pub validity: Option<BitsWithOffset<'a>>,
    /// See [`TimeArray::values`][crate::array::TimeArray::values]
    pub values: &'a [T],
}

/// See [`TimestampArray`][crate::array::TimestampArray]
#[derive(Debug, Clone, PartialEq)]
pub struct TimestampView<'a> {
    /// See [`TimestampArray::unit`][crate::array::TimestampArray::unit]
    pub unit: TimeUnit,
    /// See [`TimestampArray::timezone`][crate::array::TimestampArray::timezone]
    pub timezone: Option<String>,
    /// See [`TimestampArray::validity`][crate::array::TimestampArray::validity]
    pub validity: Option<BitsWithOffset<'a>>,
    /// See [`TimestampArray::values`][crate::array::TimestampArray::values]
    pub values: &'a [i64],
}

/// See [`StructArray`][crate::array::StructArray]
#[derive(Clone, Debug, PartialEq)]
pub struct StructView<'a> {
    /// See [`StructArray::len`][crate::array::StructArray::len]
    pub len: usize,
    /// See [`StructArray::validity`][crate::array::StructArray::validity]
    pub validity: Option<BitsWithOffset<'a>>,
    /// See [`StructArray::fields`][crate::array::StructArray::fields]
    pub fields: Vec<(FieldMeta, View<'a>)>,
}

/// See [`MapArray`][crate::array::MapArray]
#[derive(Clone, Debug, PartialEq)]
pub struct MapView<'a> {
    /// See [`MapArray::validity`][crate::array::MapArray::validity]
    pub validity: Option<BitsWithOffset<'a>>,
    /// See [`MapArray::offsets`][crate::array::MapArray::offsets]
    pub offsets: &'a [i32],
    /// See [`MapArray::meta`][crate::array::MapArray::meta]
    pub meta: MapMeta,
    /// See [`MapArray::keys`][crate::array::MapArray::keys]
    pub keys: Box<View<'a>>,
    /// See [`MapArray::values`][crate::array::MapArray::values]
    pub values: Box<View<'a>>,
}

impl<'a> MapView<'a> {
    #[allow(unused)]
    pub(crate) fn from_logical_view(
        entries: View<'a>,
        entries_name: String,
        sorted: bool,
        validity: Option<BitsWithOffset<'a>>,
        offsets: &'a [i32],
    ) -> Result<Self> {
        let View::Struct(entries) = entries else {
            fail!(ErrorKind::Unsupported, "Expected struct array");
        };
        let Ok(entries_fields) = <[(FieldMeta, View); 2]>::try_from(entries.fields) else {
            fail!(ErrorKind::Unsupported, "Expected two entries");
        };
        let [(keys_meta, keys_view), (values_meta, values_view)] = entries_fields;

        Ok(MapView {
            validity,
            offsets,
            meta: MapMeta {
                entries_name,
                sorted,
                keys: keys_meta,
                values: values_meta,
            },
            keys: Box::new(keys_view),
            values: Box::new(values_view),
        })
    }
}

/// See [`ListArray`][crate::array::ListArray]
#[derive(Clone, Debug, PartialEq)]
pub struct ListView<'a, O> {
    /// See [`ListArray::validity`][crate::array::ListArray::validity]
    pub validity: Option<BitsWithOffset<'a>>,
    /// See [`ListArray::offsets`][crate::array::ListArray::offsets]
    pub offsets: &'a [O],
    /// See [`ListArray::meta`][crate::array::ListArray::meta]
    pub meta: FieldMeta,
    /// See [`ListArray::elements`][crate::array::ListArray::elements]
    pub elements: Box<View<'a>>,
}

/// See [`FixedSizeListArray`][crate::array::FixedSizeListArray]
#[derive(Clone, Debug, PartialEq)]
pub struct FixedSizeListView<'a> {
    /// See [`FixedSizeListArray::len`][crate::array::FixedSizeListArray::len]
    pub len: usize,
    /// See [`FixedSizeListArray::n`][crate::array::FixedSizeListArray::n]
    pub n: i32,
    /// See [`FixedSizeListArray::validity`][crate::array::BytesArray::validity]
    pub validity: Option<BitsWithOffset<'a>>,
    /// See [`FixedSizeListArray::meta`][crate::array::FixedSizeListArray::meta]
    pub meta: FieldMeta,
    /// See [`FixedSizeListArray::elements`][crate::array::FixedSizeListArray::elements]
    pub elements: Box<View<'a>>,
}

/// See [`BytesArray`][crate::array::BytesArray]
#[derive(Clone, Debug, PartialEq)]
pub struct BytesView<'a, O> {
    /// See [`BytesArray::validity`][crate::array::BytesArray::validity]
    pub validity: Option<BitsWithOffset<'a>>,
    /// See [`BytesArray::offsets`][crate::array::BytesArray::offsets]
    pub offsets: &'a [O],
    /// See [`BytesArray::data`][crate::array::BytesArray::data]
    pub data: &'a [u8],
}

/// See [`BytesViewArray`][crate::array::BytesViewArray]
#[derive(Clone, Debug, PartialEq)]
pub struct BytesViewView<'a> {
    /// See [`BytesViewArray::validity`][crate::array::BytesViewArray::validity]
    pub validity: Option<BitsWithOffset<'a>>,
    /// See [`BytesViewArray::data`][crate::array::BytesViewArray::data]
    pub data: &'a [u128],
    /// See [`BytesViewArray::buffers`][crate::array::BytesViewArray::buffers]
    pub buffers: Vec<&'a [u8]>,
}

/// See [`FixedSizeBinaryArray`][crate::array::FixedSizeBinaryArray]
#[derive(Clone, Debug, PartialEq)]
pub struct FixedSizeBinaryView<'a> {
    /// See [`FixedSizeBinaryArray::n`][crate::array::FixedSizeBinaryArray::n]
    pub n: i32,
    /// See [`FixedSizeBinaryArray::validity`][crate::array::FixedSizeBinaryArray::validity]
    pub validity: Option<BitsWithOffset<'a>>,
    /// See [`FixedSizeBinaryArray::data`][crate::array::FixedSizeBinaryArray::data]
    pub data: &'a [u8],
}

/// See [`DecimalArray`][crate::array::DecimalArray]
#[derive(Clone, Debug, PartialEq)]
pub struct DecimalView<'a, T> {
    /// See [`DecimalArray::precision`][crate::array::DecimalArray::precision]
    pub precision: u8,
    /// See [`DecimalArray::scale`][crate::array::DecimalArray::scale]
    pub scale: i8,
    /// See [`DecimalArray::validity`][crate::array::DecimalArray::validity]
    pub validity: Option<BitsWithOffset<'a>>,
    /// See [`DecimalArray::values`][crate::array::DecimalArray::values]
    pub values: &'a [T],
}

/// See [`DictionaryArray`][crate::array::DictionaryArray]
#[derive(Clone, Debug, PartialEq)]
pub struct DictionaryView<'a> {
    /// See [`DictionaryArray::keys`][crate::array::DictionaryArray::keys]
    pub keys: Box<View<'a>>,
    /// See [`DictionaryArray::values`][crate::array::DictionaryArray::values]
    pub values: Box<View<'a>>,
}

/// See [`UnionArray`][crate::array::UnionArray]
#[derive(Clone, Debug, PartialEq)]
pub struct UnionView<'a> {
    /// See [`UnionArray::types`][crate::array::UnionArray::types]
    pub types: &'a [i8],
    /// See [`UnionArray::offsets`][crate::array::UnionArray::offsets]
    pub offsets: Option<&'a [i32]>,
    /// See [`UnionArray::fields`][crate::array::UnionArray::fields]
    pub fields: Vec<(i8, FieldMeta, View<'a>)>,
}

/// See [`RunEndEncodedArray`][crate::array::RunEndEncodedArray]
#[derive(Clone, Debug, PartialEq)]
pub struct RunEndEncodedView<'a> {
    /// See [`RunEndEncodedArray::meta`][crate::array::RunEndEncodedArray::meta]
    pub meta: RunEndEncodedMeta,
    /// See [`RunEndEncodedArray::run_ends`][crate::array::RunEndEncodedArray::run_ends]
    pub run_ends: Box<View<'a>>,
    /// See [`RunEndEncodedArray::values`][crate::array::RunEndEncodedArray::values]
    pub values: Box<View<'a>>,
}
