//! Arrays with owned data
//!
//! [`Array`] is a union of all array types supported by `marrow`.
use half::f16;

use crate::{
    datatypes::{
        field_from_meta, DataType, Field, FieldMeta, IntervalUnit, MapMeta, RunEndEncodedMeta,
        TimeUnit, UnionMode,
    },
    error::{fail, ErrorKind, Result},
    types::{DayTimeInterval, MonthDayNanoInterval},
    view::{
        BitsWithOffset, BooleanView, BytesView, BytesViewView, DecimalView, DictionaryView,
        FixedSizeBinaryView, FixedSizeListView, ListView, MapView, NullView, PrimitiveView,
        RunEndEncodedView, StructView, TimeView, TimestampView, UnionView, View,
    },
};

// assert that the `Array` implements the expected traits
#[allow(unused)]
const _: () = {
    trait AssertExpectedTraits: Clone + std::fmt::Debug + PartialEq + Send + Sync {}
    impl AssertExpectedTraits for Array {}
};

/// An array with owned data
///
/// The corresponding view is [`View`].
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum Array {
    /// An array without data
    Null(NullArray),
    /// `bool` array
    Boolean(BooleanArray),
    /// `i8` array
    Int8(PrimitiveArray<i8>),
    /// `i16` array
    Int16(PrimitiveArray<i16>),
    /// `i32` array
    Int32(PrimitiveArray<i32>),
    /// `i64` array
    Int64(PrimitiveArray<i64>),
    /// `u8` array
    UInt8(PrimitiveArray<u8>),
    /// `u16` array
    UInt16(PrimitiveArray<u16>),
    /// `u32` array
    UInt32(PrimitiveArray<u32>),
    /// `u64` array
    UInt64(PrimitiveArray<u64>),
    /// `f16` array
    Float16(PrimitiveArray<f16>),
    /// `f32` array
    Float32(PrimitiveArray<f32>),
    /// `f64` array
    Float64(PrimitiveArray<f64>),
    /// An `i32` array of dates
    Date32(PrimitiveArray<i32>),
    /// An `i64` array of dates
    Date64(PrimitiveArray<i64>),
    /// An `i32` array of times
    Time32(TimeArray<i32>),
    /// An `i64` array of times
    Time64(TimeArray<i64>),
    /// An `i64` array of timestamps
    Timestamp(TimestampArray),
    /// An `i64` array of durations
    Duration(TimeArray<i64>),
    /// Interval with `YearMonth` unit
    ///
    /// Interval arrays are not supported for `arrow2`.
    YearMonthInterval(PrimitiveArray<i32>),
    /// Interval with `DayTime` unit
    ///
    /// Interval arrays are not supported for `arrow2`.
    DayTimeInterval(PrimitiveArray<DayTimeInterval>),
    /// Interval with `MonthDayNano` unit
    ///
    /// Interval arrays are not supported for `arrow2`.
    MonthDayNanoInterval(PrimitiveArray<MonthDayNanoInterval>),
    /// A `[u8]` array with `i32` offsets interpreted as strings
    Utf8(BytesArray<i32>),
    /// A `[u8]` array with `i64` offsets interpreted as strings
    LargeUtf8(BytesArray<i64>),
    /// A `[u8]` array interpreted as strings with support for small inlined slices and references
    /// to external buffers
    Utf8View(BytesViewArray),
    /// A `[u8]` array with `i32` offsets
    Binary(BytesArray<i32>),
    /// A `[u8]` array with `i64` offsets
    LargeBinary(BytesArray<i64>),
    /// A `[u8; N]` array with `i32` offsets
    FixedSizeBinary(FixedSizeBinaryArray),
    /// A `[u8]` array with support for small inlined slices and references to external buffers
    BinaryView(BytesViewArray),
    /// An `i128` array of decimals
    Decimal128(DecimalArray<i128>),
    /// An array of structs
    Struct(StructArray),
    /// An array of lists with `i32` offsets
    List(ListArray<i32>),
    /// An array of lists with `i64` offsets
    LargeList(ListArray<i64>),
    /// An array of fixed sized list with `i32` offsets
    FixedSizeList(FixedSizeListArray),
    /// An array of dictionaries
    Dictionary(DictionaryArray),
    /// An array of run end encoded values
    RunEndEncoded(RunEndEncodedArray),
    /// An array of maps
    Map(MapArray),
    /// An array of unions
    Union(UnionArray),
}

impl Array {
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

    /// Get the view for this array
    ///
    /// While the data of the arrays is borrowed, any metadata is copied (e.g., field definitions
    /// for structs). The reason is that views may be constructed from foreign Arrow implementation
    /// with different ways to store metadata.
    pub fn as_view(&self) -> View<'_> {
        match self {
            Self::Null(array) => View::Null(array.as_view()),
            Self::Boolean(array) => View::Boolean(array.as_view()),
            Self::Int8(array) => View::Int8(array.as_view()),
            Self::Int16(array) => View::Int16(array.as_view()),
            Self::Int32(array) => View::Int32(array.as_view()),
            Self::Int64(array) => View::Int64(array.as_view()),
            Self::UInt8(array) => View::UInt8(array.as_view()),
            Self::UInt16(array) => View::UInt16(array.as_view()),
            Self::UInt32(array) => View::UInt32(array.as_view()),
            Self::UInt64(array) => View::UInt64(array.as_view()),
            Self::Float16(array) => View::Float16(array.as_view()),
            Self::Float32(array) => View::Float32(array.as_view()),
            Self::Float64(array) => View::Float64(array.as_view()),
            Self::Decimal128(array) => View::Decimal128(array.as_view()),
            Self::Date32(array) => View::Date32(array.as_view()),
            Self::Date64(array) => View::Date64(array.as_view()),
            Self::Time32(array) => View::Time32(array.as_view()),
            Self::Time64(array) => View::Time64(array.as_view()),
            Self::Timestamp(array) => View::Timestamp(array.as_view()),
            Self::Duration(array) => View::Duration(array.as_view()),
            Self::YearMonthInterval(array) => View::YearMonthInterval(array.as_view()),
            Self::DayTimeInterval(array) => View::DayTimeInterval(array.as_view()),
            Self::MonthDayNanoInterval(array) => View::MonthDayNanoInterval(array.as_view()),
            Self::Binary(array) => View::Binary(array.as_view()),
            Self::LargeBinary(array) => View::LargeBinary(array.as_view()),
            Self::FixedSizeBinary(array) => View::FixedSizeBinary(array.as_view()),
            Self::BinaryView(array) => View::BinaryView(array.as_view()),
            Self::Utf8(array) => View::Utf8(array.as_view()),
            Self::LargeUtf8(array) => View::LargeUtf8(array.as_view()),
            Self::Utf8View(array) => View::Utf8View(array.as_view()),
            Self::List(array) => View::List(array.as_view()),
            Self::LargeList(array) => View::LargeList(array.as_view()),
            Self::FixedSizeList(array) => View::FixedSizeList(array.as_view()),
            Self::Struct(array) => View::Struct(array.as_view()),
            Self::Map(array) => View::Map(array.as_view()),
            Self::Dictionary(array) => View::Dictionary(array.as_view()),
            Self::RunEndEncoded(array) => View::RunEndEncoded(array.as_view()),
            Self::Union(array) => View::Union(array.as_view()),
        }
    }
}

/// An array without data
///
/// The corresponding view is [`NullView`].
#[derive(Clone, Debug, PartialEq)]
pub struct NullArray {
    /// The len of the array
    pub len: usize,
}

impl NullArray {
    /// Get the view for this array
    pub fn as_view(&self) -> NullView {
        NullView { len: self.len }
    }
}

/// A `bool` array
///
/// The corresponding view is [`BooleanView`].
#[derive(Clone, Debug, PartialEq)]
pub struct BooleanArray {
    // Note: len is required to know how many bits of values are used
    /// The len of the array
    pub len: usize,
    /// The validity of the elements as a bitmap
    pub validity: Option<Vec<u8>>,
    /// The values as a bitmap
    pub values: Vec<u8>,
}

impl BooleanArray {
    /// Get the view for this array
    pub fn as_view(&self) -> BooleanView<'_> {
        BooleanView {
            len: self.len,
            validity: self
                .validity
                .as_ref()
                .map(|data| BitsWithOffset { offset: 0, data }),
            values: BitsWithOffset {
                offset: 0,
                data: &self.values,
            },
        }
    }
}

/// An array of primitive values
///
/// The corresponding view is [`PrimitiveView`].
#[derive(Clone, Debug, PartialEq)]
pub struct PrimitiveArray<T> {
    /// The validity of the elements as a bitmap
    pub validity: Option<Vec<u8>>,
    /// The values of the array
    pub values: Vec<T>,
}

impl<T> PrimitiveArray<T> {
    /// Get the view for this array
    pub fn as_view(&self) -> PrimitiveView<'_, T> {
        PrimitiveView {
            validity: self
                .validity
                .as_ref()
                .map(|data| BitsWithOffset { offset: 0, data }),
            values: &self.values,
        }
    }
}

/// An array time values (e.g., `"12:53"`)
///
/// The corresponding view is [`TimeView`].
#[derive(Debug, Clone, PartialEq)]
pub struct TimeArray<T> {
    /// The time unit of the values
    pub unit: TimeUnit,
    /// The validity of the elements as a bitmap
    pub validity: Option<Vec<u8>>,
    /// The values of the array stored as the offsets from midnight
    pub values: Vec<T>,
}

impl<T> TimeArray<T> {
    /// Get the view for this array
    pub fn as_view(&self) -> TimeView<'_, T> {
        TimeView {
            unit: self.unit,
            validity: self
                .validity
                .as_ref()
                .map(|data| BitsWithOffset { offset: 0, data }),
            values: &self.values,
        }
    }
}

/// An array of timestamps with an optional timezone
///
/// The corresponding view is [`TimestampView`].
#[derive(Debug, Clone, PartialEq)]

pub struct TimestampArray {
    /// The time unit of the values
    pub unit: TimeUnit,
    /// THe optional timezone
    pub timezone: Option<String>,
    /// The validity of the elements as a bitmap
    pub validity: Option<Vec<u8>>,
    /// The values as UTC timestamps
    pub values: Vec<i64>,
}

impl TimestampArray {
    /// Get the view for this array
    pub fn as_view(&self) -> TimestampView<'_> {
        TimestampView {
            unit: self.unit,
            timezone: self.timezone.clone(),
            validity: self
                .validity
                .as_ref()
                .map(|data| BitsWithOffset { offset: 0, data }),
            values: &self.values,
        }
    }
}

/// An array of structs
///
/// The corresponding view is [`StructView`].
#[derive(Clone, Debug, PartialEq)]
pub struct StructArray {
    /// The number of elements in the array
    pub len: usize,
    /// The validity of the elements as a bitmap
    pub validity: Option<Vec<u8>>,
    /// The fields with their metadata
    pub fields: Vec<(FieldMeta, Array)>,
}

impl StructArray {
    /// Get the view for this array
    pub fn as_view(&self) -> StructView<'_> {
        StructView {
            len: self.len,
            validity: self
                .validity
                .as_ref()
                .map(|data| BitsWithOffset { offset: 0, data }),
            fields: self
                .fields
                .iter()
                .map(|(meta, array)| (meta.clone(), array.as_view()))
                .collect(),
        }
    }
}

/// An array of maps
///
/// The corresponding view is [`MapView`].
#[derive(Clone, Debug, PartialEq)]
pub struct MapArray {
    /// The validity of the elements as a bitmap
    pub validity: Option<Vec<u8>>,
    /// The offsets of the elements
    pub offsets: Vec<i32>,
    /// The metadata of the map
    pub meta: MapMeta,
    /// The keys stored in the array
    pub keys: Box<Array>,
    /// The values stored in the array
    pub values: Box<Array>,
}

impl MapArray {
    /// Get the view for this array
    pub fn as_view(&self) -> MapView<'_> {
        MapView {
            validity: self
                .validity
                .as_ref()
                .map(|data| BitsWithOffset { offset: 0, data }),
            offsets: &self.offsets,
            meta: self.meta.clone(),
            keys: Box::new(self.keys.as_view()),
            values: Box::new(self.values.as_view()),
        }
    }
}

impl MapArray {
    #[allow(clippy::type_complexity, unused)]
    pub(crate) fn into_logical_array(
        self,
    ) -> Result<(Array, String, bool, Option<Vec<u8>>, Vec<i32>)> {
        let Some(&last_offset) = self.offsets.last() else {
            fail!(ErrorKind::Unsupported, "Invalid map array");
        };

        let entries = Array::Struct(StructArray {
            len: usize::try_from(last_offset)?,
            validity: None,
            fields: vec![
                (self.meta.keys, *self.keys),
                (self.meta.values, *self.values),
            ],
        });

        Ok((
            entries,
            self.meta.entries_name,
            self.meta.sorted,
            self.validity,
            self.offsets,
        ))
    }
}

/// An array of lists
///
/// The value element `i` is given by the pseudo code `elements[offsets[i]..[offsets[i+1]]`
///
/// The corresponding view is [`ListView`].
#[derive(Clone, Debug, PartialEq)]
pub struct ListArray<O> {
    /// The validity of the elements as a bitmap
    pub validity: Option<Vec<u8>>,
    /// The offsets of the elements
    pub offsets: Vec<O>,
    /// The metadata of the elements array
    pub meta: FieldMeta,
    /// The values stored in the array
    pub elements: Box<Array>,
}

impl<O> ListArray<O> {
    /// Get the view for this array
    pub fn as_view(&self) -> ListView<'_, O> {
        ListView {
            validity: self
                .validity
                .as_ref()
                .map(|data| BitsWithOffset { offset: 0, data }),
            offsets: &self.offsets,
            meta: self.meta.clone(),
            elements: Box::new(self.elements.as_view()),
        }
    }
}

/// An array of lists of fixed size
///
/// The value of element `i` is given by pseudo code `elements[(n * i)..(n * (i + 1))]`
///
/// The corresponding view is [`FixedSizeListView`].
#[derive(Clone, Debug, PartialEq)]
pub struct FixedSizeListArray {
    /// The number of elements in this array, each a list with `n` children
    pub len: usize,
    /// The number of children per element
    pub n: i32,
    /// The validity of the elements as a bitmap
    pub validity: Option<Vec<u8>>,
    /// The metadata of the elements array
    pub meta: FieldMeta,
    /// The values stored in the array
    pub elements: Box<Array>,
}

impl FixedSizeListArray {
    /// Get the view for this array
    pub fn as_view(&self) -> FixedSizeListView<'_> {
        FixedSizeListView {
            len: self.len,
            n: self.n,
            validity: self
                .validity
                .as_ref()
                .map(|data| BitsWithOffset { offset: 0, data }),
            meta: self.meta.clone(),
            elements: Box::new(self.elements.as_view()),
        }
    }
}

/// An array of bytes with varying sizes
///
/// The value of element `i` can be access by the pseudo code `data[offsets[i]..offsets[i + 1]]`
///
/// The corresponding view is [`BytesView`].
#[derive(Clone, Debug, PartialEq)]
pub struct BytesArray<O> {
    /// The validity of the elements as a bitmap
    pub validity: Option<Vec<u8>>,
    /// The offsets into the data array the first element is `0`
    pub offsets: Vec<O>,
    /// The underlying data with all elements concatenated
    pub data: Vec<u8>,
}

impl<O> BytesArray<O> {
    /// Get the view for this array
    pub fn as_view(&self) -> BytesView<'_, O> {
        BytesView {
            validity: self
                .validity
                .as_ref()
                .map(|data| BitsWithOffset { offset: 0, data }),
            offsets: &self.offsets,
            data: &self.data,
        }
    }
}

/// An array of (possibly inlined) byte strings
#[derive(Clone, Debug, PartialEq)]
pub struct BytesViewArray {
    /// The validity of the elements as a bitmap
    pub validity: Option<Vec<u8>>,
    /// The lengths and possible data
    pub data: Vec<u128>,
    /// The buffers containing the data for non-inlined byte strings
    pub buffers: Vec<Vec<u8>>,
}

impl BytesViewArray {
    /// Get the view for this array
    pub fn as_view(&self) -> BytesViewView<'_> {
        BytesViewView {
            validity: self
                .validity
                .as_ref()
                .map(|data| BitsWithOffset { offset: 0, data }),
            data: &self.data,
            buffers: self.buffers.iter().map(|b| b.as_slice()).collect(),
        }
    }
}

/// An array of byte vectors with fixed length
///
/// The corresponding view is [`FixedSizeBinaryView`].
#[derive(Clone, Debug, PartialEq)]
pub struct FixedSizeBinaryArray {
    /// The number of bytes per element
    pub n: i32,
    /// The validity of the elements as a bitmap
    pub validity: Option<Vec<u8>>,
    /// The data with each element concatenated
    pub data: Vec<u8>,
}

impl FixedSizeBinaryArray {
    /// Get the view for this array
    pub fn as_view(&self) -> FixedSizeBinaryView<'_> {
        FixedSizeBinaryView {
            n: self.n,
            validity: self
                .validity
                .as_ref()
                .map(|data| BitsWithOffset { offset: 0, data }),
            data: &self.data,
        }
    }
}

/// An array of fixed point values
///
/// The value of element `i` can be computed by the pseudo code: `values[i] * (10).pow(-scale)`
///
/// The corresponding view is [`DecimalView`].
#[derive(Clone, Debug, PartialEq)]
pub struct DecimalArray<T> {
    /// The precision, i.e., the number of digits
    pub precision: u8,
    /// The scale, i.e., the position of smallest value that can be represented
    pub scale: i8,
    /// The validity of the elements as a bitmap
    pub validity: Option<Vec<u8>>,
    /// The underlying values
    pub values: Vec<T>,
}

impl<T> DecimalArray<T> {
    /// Get the view for this array
    pub fn as_view(&self) -> DecimalView<'_, T> {
        DecimalView {
            precision: self.precision,
            scale: self.scale,
            validity: self
                .validity
                .as_ref()
                .map(|data| BitsWithOffset { offset: 0, data }),
            values: &self.values,
        }
    }
}

/// An array that deduplicates elements
///
/// For element `i`, the value can be looked up by the pseudo code `values[indices[i]]`
///
/// The corresponding view is [`DictionaryView`].
#[derive(Clone, Debug, PartialEq)]
pub struct DictionaryArray {
    /// The indices into the values array for each element
    pub keys: Box<Array>,
    /// The possible values of elements
    pub values: Box<Array>,
}

impl DictionaryArray {
    /// Get the view for this array
    pub fn as_view(&self) -> DictionaryView<'_> {
        DictionaryView {
            keys: Box::new(self.keys.as_view()),
            values: Box::new(self.values.as_view()),
        }
    }
}

/// A union of different data types
///
/// This corresponds roughly to Rust's enums. Each element has a type, which indicates the
/// underlying array to use.
///
/// The Arrow format supports two types of enums: sparse unions and dense unions. In dense unions
/// the lengths of all fields sums to the overall length, whereas in sparse unions each field has
/// the same length as the overall array.
///
/// The value of element `i` in a union can be looked up by the pseudo code
///
/// - `fields[types[i]].1[offsets[i]]` (for dense unions)
/// - `fields[types[i]].1[i]` (for sparse unions)
///
/// For sparse unions `offsets` must be `None`.
///
/// The corresponding view is [`UnionView`].
#[derive(Clone, Debug, PartialEq)]
pub struct UnionArray {
    /// The type of each element
    pub types: Vec<i8>,
    /// The offset into the underlying arrays
    pub offsets: Option<Vec<i32>>,
    /// The arrays with their metadata
    pub fields: Vec<(i8, FieldMeta, Array)>,
}

impl UnionArray {
    /// Get the view for this array
    pub fn as_view(&self) -> UnionView<'_> {
        UnionView {
            types: &self.types,
            offsets: self.offsets.as_deref(),
            fields: self
                .fields
                .iter()
                .map(|(type_id, meta, array)| (*type_id, meta.clone(), array.as_view()))
                .collect(),
        }
    }
}

/// An array with runs of deduplicated values
///
/// The value for element `i` can be looked up via `values[j]` for `j` such that `run_ends[j - 1] <=
/// i && i < run_ends[j]`.
///
/// The corresponding view is [`RunEndEncodedView`].
#[derive(Clone, Debug, PartialEq)]
pub struct RunEndEncodedArray {
    /// The metadata for the arrays
    pub meta: RunEndEncodedMeta,
    /// The run ends for each value
    pub run_ends: Box<Array>,
    /// The possible values of elements
    pub values: Box<Array>,
}

impl RunEndEncodedArray {
    /// Get the view for this array
    pub fn as_view(&self) -> RunEndEncodedView<'_> {
        RunEndEncodedView {
            meta: self.meta.clone(),
            run_ends: Box::new(self.run_ends.as_view()),
            values: Box::new(self.values.as_view()),
        }
    }
}
