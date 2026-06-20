//! Supported data types
use std::collections::HashMap;

use crate::error::{fail, ErrorKind, MarrowError, Result};

// assert that the `DataType` implements the expected traits
#[allow(unused)]
const _: () = {
    trait AssertExpectedTraits: Clone + std::fmt::Debug + PartialEq + Send + Sync {}
    impl AssertExpectedTraits for DataType {}
};

// assert that the `Field`, `FieldMeta`, etc. implement the expected traits
#[allow(unused)]
const _: () = {
    trait AssertExpectedTraits: Clone + std::fmt::Debug + Default + PartialEq + Send + Sync {}
    impl AssertExpectedTraits for Field {}
    impl AssertExpectedTraits for FieldMeta {}
    impl AssertExpectedTraits for MapMeta {}
    impl AssertExpectedTraits for RunEndEncodedMeta {}
};

/// The data type and metadata of a field
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Field {
    /// The name of the field
    pub name: String,
    /// The data type of the field
    pub data_type: DataType,
    /// Whether the field supports missing values
    pub nullable: bool,
    /// Additional metadata
    pub metadata: HashMap<String, String>,
}

impl std::default::Default for Field {
    fn default() -> Self {
        Self {
            data_type: DataType::Null,
            name: Default::default(),
            nullable: Default::default(),
            metadata: Default::default(),
        }
    }
}

/// Metadata for a field (everything but the data type)
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FieldMeta {
    /// The name of the field
    pub name: String,
    /// Nullability flag of the field
    pub nullable: bool,
    /// Additional metadata of the field
    pub metadata: HashMap<String, String>,
}

#[allow(unused)]
pub(crate) fn meta_from_field(field: Field) -> FieldMeta {
    FieldMeta {
        name: field.name,
        nullable: field.nullable,
        metadata: field.metadata,
    }
}

pub(crate) fn field_from_meta(data_type: DataType, meta: FieldMeta) -> Field {
    Field {
        data_type,
        name: meta.name,
        nullable: meta.nullable,
        metadata: meta.metadata,
    }
}

/// Metadata for map arrays
///
/// ```rust
/// # use marrow::datatypes::{FieldMeta, MapMeta};
/// assert_eq!(
///     MapMeta::default(),
///     MapMeta {
///         entries_name: String::from("entries"),
///         sorted: false,
///         keys: FieldMeta {
///             name: String::from("keys"),
///             ..FieldMeta::default()
///         },
///         values: FieldMeta {
///             name: String::from("values"),
///             nullable: true,
///             ..FieldMeta::default()
///         },
///     },
/// );
/// ```
///
/// Note: the defaults follow the defaults of `arrow`'s MapBuilder.
///
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MapMeta {
    /// The name of the entries field (defaults to `"entries"`)
    pub entries_name: String,
    /// Whether the maps are sorted (defaults to `false`)
    pub sorted: bool,
    /// The metadata of the keys array (defaults to a non-nullable field with name `"keys"`)
    pub keys: FieldMeta,
    /// The metadata of the values array (defaults to a nullable field with name `"values"`)
    pub values: FieldMeta,
}

impl std::default::Default for MapMeta {
    fn default() -> Self {
        MapMeta {
            entries_name: String::from("entries"),
            sorted: false,
            keys: FieldMeta {
                name: String::from("keys"),
                nullable: false,
                metadata: HashMap::new(),
            },
            values: FieldMeta {
                name: String::from("values"),
                nullable: true,
                metadata: HashMap::new(),
            },
        }
    }
}

/// Metadata for run end encoded arrays
///
/// ```rust
/// # use marrow::datatypes::{FieldMeta, RunEndEncodedMeta};
/// assert_eq!(
///     RunEndEncodedMeta::default(),
///     RunEndEncodedMeta {
///         run_ends_name: String::from("run_ends"),
///         values: FieldMeta {
///             name: String::from("values"),
///             nullable: true,
///             ..FieldMeta::default()
///         },
///     },
/// );
/// ```
///
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunEndEncodedMeta {
    /// The name for the run ends (defaults to `"run_ends"`)
    pub run_ends_name: String,
    /// The metadata for the values array (defaults to a nullable with with name `"values"`)
    pub values: FieldMeta,
}

impl std::default::Default for RunEndEncodedMeta {
    fn default() -> Self {
        RunEndEncodedMeta {
            run_ends_name: String::from("run_ends"),
            values: FieldMeta {
                name: String::from("values"),
                nullable: true,
                metadata: HashMap::new(),
            },
        }
    }
}

/// Data types of array
///
#[cfg_attr(
// arrow-version: replace:     feature = "arrow-{version}",
    feature = "arrow-53",
    doc = r#"
See also [`arrow::datatypes::DataType`][crate::impl::arrow::datatypes::DataType]
"#,
)]
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub enum DataType {
    /// Data type for fields without data
    Null,
    /// `bool` values stored as bitmaps
    Boolean,
    /// `i8` stored as a contiguous array
    Int8,
    /// `i16` stored as a contiguous array
    Int16,
    /// `i32` stored as a contiguous array
    Int32,
    /// `i64` stored as a contiguous array
    Int64,
    /// `u8` stored as a contiguous array
    UInt8,
    /// `u16` stored as a contiguous array
    UInt16,
    /// `u32` stored as a contiguous array
    UInt32,
    /// `u64` stored as a contiguous array
    UInt64,
    /// `f16` stored as a contiguous array
    Float16,
    /// `f32` stored as a contiguous array
    Float32,
    /// `f64` stored as a contiguous array
    Float64,
    /// Strings stored with `i32` offsets
    Utf8,
    /// Strings stored with `i64` offsets
    LargeUtf8,
    /// Strings stored with `i32` offsets or inline for small strings
    Utf8View,
    /// Byte arrays stored with `i32` offsets
    Binary,
    /// Byte arrays stored with `i64` offsets
    LargeBinary,
    /// Bytes stored with `u32` offsets or inline for small values
    BinaryView,
    /// Byte arrays with fixed length
    FixedSizeBinary(i32),
    /// Dates as the number of days since the epoch stored as `i32` (e.g., `"2022-10-11"`)
    Date32,
    /// Dates as the number of seconds since the epoch stored as `i64` (e.g., `"2022-10-11"`)
    Date64,
    /// A UTC timestamps stored as `i64` with the specified unit and an optional timezone
    Timestamp(TimeUnit, Option<String>),
    /// Times as an offset from midnight stored as `i32` with the given unit
    Time32(TimeUnit),
    /// Times as an offset from midnight stored as `i64` with the given unit
    Time64(TimeUnit),
    /// Durations stored as `i64` with the given unit
    Duration(TimeUnit),
    /// Calendar intervals with different layouts depending on the given unit
    Interval(IntervalUnit),
    /// Fixed point values stored with the given precision and scale
    Decimal128(u8, i8),
    /// Structs
    Struct(Vec<Field>),
    /// Lists with `i32` offsets
    List(Box<Field>),
    /// Lists with `i64` offsets
    LargeList(Box<Field>),
    /// Lists with a fixed number of element with `i32` offsets
    FixedSizeList(Box<Field>, i32),
    /// Maps
    ///
    /// The field should be a struct field with two children for the keys and values.
    Map(Box<Field>, bool),
    /// Deduplicated values
    ///
    /// The children are `Dictionary(indices, values)`
    Dictionary(Box<DataType>, Box<DataType>),
    /// Deduplicated values that continuously repeat
    ///
    /// The children are `RunEndEncoded(indices, values)`
    RunEndEncoded(Box<Field>, Box<Field>),
    /// Union o different types
    Union(Vec<(i8, Field)>, UnionMode),
}

/// The unit of temporal quantities
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Ord, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum TimeUnit {
    #[allow(missing_docs)]
    Second,
    #[allow(missing_docs)]
    Millisecond,
    #[allow(missing_docs)]
    Microsecond,
    #[allow(missing_docs)]
    Nanosecond,
}

impl std::fmt::Display for TimeUnit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TimeUnit::Second => write!(f, "Second"),
            TimeUnit::Millisecond => write!(f, "Millisecond"),
            TimeUnit::Microsecond => write!(f, "Microsecond"),
            TimeUnit::Nanosecond => write!(f, "Nanosecond"),
        }
    }
}

impl std::str::FromStr for TimeUnit {
    type Err = MarrowError;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "Second" => Ok(Self::Second),
            "Millisecond" => Ok(Self::Millisecond),
            "Microsecond" => Ok(Self::Microsecond),
            "Nanosecond" => Ok(Self::Nanosecond),
            s => fail!(ErrorKind::ParseError, "Invalid TimeUnit: {s}"),
        }
    }
}

#[test]
fn time_unit_as_str() {
    use std::str::FromStr;

    macro_rules! assert_variant {
        ($variant:ident) => {
            assert_eq!((TimeUnit::$variant).to_string(), stringify!($variant));
            assert_eq!(
                TimeUnit::from_str(stringify!($variant)).unwrap(),
                TimeUnit::$variant
            );
        };
    }

    assert_variant!(Second);
    assert_variant!(Millisecond);
    assert_variant!(Microsecond);
    assert_variant!(Nanosecond);
}

/// The storage mode of unions
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum UnionMode {
    /// The underlying arrays also store unused values
    ///
    /// Each underlying array has the same length as the union array.
    Sparse,
    /// The underlying arrays only store used values.
    ///
    /// The sum of all underlying array lengths is the same as the length of the union array.    
    Dense,
}

impl std::fmt::Display for UnionMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UnionMode::Sparse => write!(f, "Sparse"),
            UnionMode::Dense => write!(f, "Dense"),
        }
    }
}

impl std::str::FromStr for UnionMode {
    type Err = MarrowError;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "Sparse" => Ok(UnionMode::Sparse),
            "Dense" => Ok(UnionMode::Dense),
            s => fail!(ErrorKind::ParseError, "Invalid UnionMode: {s}"),
        }
    }
}

#[test]
fn union_mode_as_str() {
    use std::str::FromStr;

    macro_rules! assert_variant {
        ($variant:ident) => {
            assert_eq!((UnionMode::$variant).to_string(), stringify!($variant));
            assert_eq!(
                UnionMode::from_str(stringify!($variant)).unwrap(),
                UnionMode::$variant
            );
        };
    }

    assert_variant!(Dense);
    assert_variant!(Sparse);
}

/// The unit of calendar intervals
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum IntervalUnit {
    /// An interval as the number of months, stored as `i32`
    YearMonth,
    /// An interval as the number of days, stored as `i32`, and milliseconds, stored as `i32`
    DayTime,
    /// An interval as the number of months (stored as `i32`), days (stored as `i32`) and nanoseconds (stored as `i64`)
    MonthDayNano,
}

impl std::fmt::Display for IntervalUnit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::YearMonth => write!(f, "YearMonth"),
            Self::DayTime => write!(f, "DayTime"),
            Self::MonthDayNano => write!(f, "MonthDayNano"),
        }
    }
}

impl std::str::FromStr for IntervalUnit {
    type Err = MarrowError;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "YearMonth" => Ok(Self::YearMonth),
            "DayTime" => Ok(Self::DayTime),
            "MonthDayNano" => Ok(Self::MonthDayNano),
            s => fail!(ErrorKind::ParseError, "Invalid IntervalUnit: {s}"),
        }
    }
}

#[test]
fn interval_unit() {
    use std::str::FromStr;

    macro_rules! assert_variant {
        ($variant:ident) => {
            assert_eq!((IntervalUnit::$variant).to_string(), stringify!($variant));
            assert_eq!(
                IntervalUnit::from_str(stringify!($variant)).unwrap(),
                IntervalUnit::$variant
            );
        };
    }

    assert_variant!(YearMonth);
    assert_variant!(DayTime);
    assert_variant!(MonthDayNano);
}
