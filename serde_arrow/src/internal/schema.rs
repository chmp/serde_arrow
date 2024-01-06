use std::{
    collections::{BTreeMap, HashMap},
    str::FromStr,
};

use crate::internal::{
    error::{fail, Error, Result},
    tracing::{Tracer, TracingOptions},
};

use serde::{Deserialize, Serialize};

/// The metadata key under which to store the strategy
///
/// See the [module][crate::schema] for details.
///
pub const STRATEGY_KEY: &str = "SERDE_ARROW:strategy";

pub trait Sealed {}

/// A sealed trait to add support for constructing schema-like objects
///
/// There are three main ways to specify the schema:
///
/// 1. [`SchemaLike::from_value`]: specify the schema manually, e.g., as a JSON
///    value
/// 2. [`SchemaLike::from_type`]: determine the schema from the record type
/// 3. [`SchemaLike::from_samples`]: Determine the schema from samples of the
///    data
///
/// The following types implement [`SchemaLike`] and can be constructed in this
/// way:
///
/// - [`SerdeArrowSchema`]
#[cfg_attr(
    has_arrow,
    doc = "- `Vec<`[`arrow::datatypes::Field`][crate::_impl::arrow::datatypes::Field]`>`"
)]
#[cfg_attr(
    has_arrow2,
    doc = "- `Vec<`[`arrow2::datatypes::Field`][crate::_impl::arrow2::datatypes::Field]`>`"
)]
///
pub trait SchemaLike: Sized + Sealed {
    /// Build the schema from an object that implements serialize (e.g.,
    /// `serde_json::Value`)
    ///
    /// ```rust
    /// # #[cfg(feature = "has_arrow")]
    /// # fn main() -> serde_arrow::_impl::PanicOnError<()> {
    /// # use serde_arrow::_impl::arrow;
    /// use arrow::datatypes::Field;
    /// use serde_arrow::schema::SchemaLike;
    ///
    /// let schema = serde_json::json!([
    ///     {"name": "foo", "data_type": "U8"},
    ///     {"name": "bar", "data_type": "Utf8"},
    /// ]);
    ///
    /// let fields = Vec::<Field>::from_value(&schema)?;
    /// # Ok(())
    /// # }
    /// # #[cfg(not(feature = "has_arrow"))]
    /// # fn main() { }
    /// ```
    ///
    /// `SerdeArrowSchema` can also be directly serialized and deserialized.
    ///
    /// ```rust
    /// # fn main() -> serde_arrow::_impl::PanicOnError<()> {
    /// # let json_schema_str = "[]";
    /// #
    /// use serde_arrow::schema::SerdeArrowSchema;
    ///
    /// let schema: SerdeArrowSchema = serde_json::from_str(json_schema_str)?;
    /// serde_json::to_string(&schema)?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// The schema can be given in two ways:
    ///
    /// - an array of fields
    /// - or an object with a `"fields"` key that contains an array of fields
    ///
    /// Each field is an object with the following keys:
    ///
    /// - `"name"` (**required**): the name of the field
    /// - `"data_type"` (**required**): the data type of the field as a string
    /// - `"nullable"` (**optional**): if `true`, the field can contain null
    ///   values
    /// - `"strategy"` (**optional**): if given a string describing the strategy
    ///   to use (e.g., "NaiveStrAsDate64").
    /// - `"children"` (**optional**): a list of child fields, the semantics
    ///   depend on the data type
    ///
    /// The following data types are supported:
    ///
    /// - booleans: `"Bool"`
    /// - signed integers: `"I8"`, `"I16"`, `"I32"`, `"I64"`
    /// - unsigned integers: `"U8"`, `"U16"`, `"U32"`, `"U64"`
    /// - floats: `"F16"`, `"F32"`, `"F64"`
    /// - strings: `"Utf8"`, `"LargeUtf8"`
    /// - lists: `"List"`, `"LargeList"`. `"children"` must contain a single
    ///   field named `"element"` that describes the element types
    /// - structs: `"Struct"`. `"children"` must contain the child fields
    /// - maps: `"Map"`. `"children"` must contain two fields, named `"key"` and
    ///   `"value"` that encode the key and value types
    /// - unions: `"Union"`. `"children"` must contain the different variants
    /// - dictionaries: `"Dictionary"`. `"children"` must contain two different
    ///   fields, named `"key"` of integer type and named `"value"` of string
    ///   type
    ///
    fn from_value<T: Serialize>(value: &T) -> Result<Self>;

    /// Determine the schema from the given record type
    ///
    /// This approach requires the type `T` to implement
    /// [`Deserialize`][serde::Deserialize]. As only type information is used,
    /// it is not possible to detect data dependent properties. E.g., it is not
    /// possible to auto detect date time strings.
    ///
    /// Note, the type `T` must encode a single "row" in the resulting data
    /// frame. When encoding single arrays, use the [`Item`][crate::utils::Item]
    /// wrapper instead of [`Items`][crate::utils::Items].
    ///  
    /// See [`TracingOptions`] for customization options.
    ///
    /// ```rust
    /// # #[cfg(feature = "has_arrow")]
    /// # fn main() -> serde_arrow::_impl::PanicOnError<()> {
    /// # use serde_arrow::_impl::arrow;
    /// use arrow::datatypes::{DataType, Field};
    /// use serde::Deserialize;
    /// use serde_arrow::schema::{SchemaLike, TracingOptions};
    ///
    /// ##[derive(Deserialize)]
    /// struct Record {
    ///     int: i32,
    ///     float: f64,
    ///     string: String,
    /// }
    ///
    /// let fields = Vec::<Field>::from_type::<Record>(TracingOptions::default())?;
    ///
    /// assert_eq!(*fields[0].data_type(), DataType::Int32);
    /// assert_eq!(*fields[1].data_type(), DataType::Float64);
    /// assert_eq!(*fields[2].data_type(), DataType::LargeUtf8);
    /// # Ok(())
    /// # }
    /// # #[cfg(not(feature = "has_arrow"))]
    /// # fn main() { }
    /// ```
    ///
    fn from_type<'de, T: Deserialize<'de>>(options: TracingOptions) -> Result<Self>;

    /// Determine the schema from the given samples
    ///
    ///
    /// This approach requires the type `T` to implement
    /// [`Serialize`][serde::Serialize] and the samples to include all relevant
    /// values. It uses only the information encoded in the samples to generate
    /// the schema. Therefore, the following requirements must be met:
    ///
    /// - at least one `Some` value for `Option<..>` fields
    /// - all variants of enum fields
    /// - at least one element for sequence fields (e.g., `Vec<..>`)
    /// - at least one example for map types (e.g., `HashMap<.., ..>`). All
    ///   possible keys must be given, if [`options.map_as_struct ==
    ///   true`][TracingOptions::map_as_struct])
    ///
    /// See [`TracingOptions`] for customization options.
    ///
    /// ```rust
    /// # #[cfg(feature = "has_arrow")]
    /// # fn main() -> serde_arrow::_impl::PanicOnError<()> {
    /// # use serde_arrow::_impl::arrow;
    /// use arrow::datatypes::{DataType, Field};
    /// use serde::Serialize;
    /// use serde_arrow::schema::{SchemaLike, TracingOptions};
    ///
    /// ##[derive(Serialize)]
    /// struct Record {
    ///     int: i32,
    ///     float: f64,
    ///     string: String,
    /// }
    ///
    /// let samples = vec![
    ///     Record {
    ///         int: 1,
    ///         float: 2.0,
    ///         string: String::from("hello")
    ///     },
    ///     Record {
    ///         int: -1,
    ///         float: 32.0,
    ///         string: String::from("world")
    ///     },
    ///     // ...
    /// ];
    ///
    /// let fields = Vec::<Field>::from_samples(&samples, TracingOptions::default())?;
    ///
    /// assert_eq!(*fields[0].data_type(), DataType::Int32);
    /// assert_eq!(*fields[1].data_type(), DataType::Float64);
    /// assert_eq!(*fields[2].data_type(), DataType::LargeUtf8);
    /// # Ok(())
    /// # }
    /// # #[cfg(not(feature = "has_arrow"))]
    /// # fn main() { }
    /// ```
    ///
    fn from_samples<T: Serialize>(samples: &T, options: TracingOptions) -> Result<Self>;
}

/// A collection of fields as understood by `serde_arrow`
#[derive(Default, Debug, PartialEq, Clone, Serialize, Deserialize)]
#[serde(from = "SchemaSerializationOptions")]
pub struct SerdeArrowSchema {
    pub(crate) fields: Vec<GenericField>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum SchemaSerializationOptions {
    FieldsOnly(Vec<GenericField>),
    FullSchema { fields: Vec<GenericField> },
}

impl From<SchemaSerializationOptions> for SerdeArrowSchema {
    fn from(value: SchemaSerializationOptions) -> Self {
        use SchemaSerializationOptions::*;
        match value {
            FieldsOnly(fields) | FullSchema { fields } => Self { fields },
        }
    }
}

impl SerdeArrowSchema {
    /// Return a new schema without any fields
    pub fn new() -> Self {
        Self::default()
    }
}

impl Sealed for SerdeArrowSchema {}

impl SchemaLike for SerdeArrowSchema {
    fn from_value<T: Serialize>(value: &T) -> Result<Self> {
        // simple version of serde-transcode
        let mut events = Vec::<crate::internal::event::Event>::new();
        crate::internal::sink::serialize_into_sink(&mut events, value)?;
        let this: Self = crate::internal::source::deserialize_from_source(&events)?;
        Ok(this)
    }

    fn from_type<'de, T: Deserialize<'de>>(options: TracingOptions) -> Result<Self> {
        let mut tracer = Tracer::new(String::from("$"), options);
        tracer.trace_type::<T>()?;
        tracer.to_schema()
    }

    fn from_samples<T: Serialize>(samples: &T, options: TracingOptions) -> Result<Self> {
        let mut tracer = Tracer::new(String::from("$"), options);
        tracer.trace_samples(samples)?;
        tracer.to_schema()
    }
}

/// Strategies for handling types without direct match between arrow and serde
///
/// For the correct strategy both the field type and the field metadata must be
/// correctly configured. In particular, when determining the schema from the
/// Rust objects themselves, some field types are incorrectly recognized (e.g.,
/// datetimes).
///
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum Strategy {
    /// Marker that the type of the field could not be determined during tracing
    ///
    InconsistentTypes,
    /// Serialize Rust strings containing UTC datetimes with timezone as Arrows
    /// Date64
    ///
    /// This strategy makes sense for chrono's `DateTime<Utc>` types without
    /// additional configuration. As they are serialized as strings.
    UtcStrAsDate64,
    /// Serialize Rust strings containing datetimes without timezone as Arrow
    /// Date64
    ///
    /// This strategy makes sense for chrono's `NaiveDateTime` types without
    /// additional configuration. As they are serialized as strings.
    ///
    NaiveStrAsDate64,
    /// Serialize Rust tuples as Arrow structs with numeric field names starting
    /// at `"0"`
    ///
    /// This strategy is most-likely the most optimal one, as Rust tuples can
    /// contain different types, whereas Arrow sequences must be of uniform type
    ///
    TupleAsStruct,
    /// Serialize Rust maps as Arrow structs
    ///
    /// The field names are sorted by name to ensure unordered map (e.g.,
    /// HashMap) have a defined order.
    ///
    /// Fields that are not present in all instances of the map are marked as
    /// nullable in schema tracing. In serialization these fields are written as
    /// null value if not present.
    ///
    /// This strategy is most-likely the most optimal one:
    ///
    /// - using the `#[serde(flatten)]` attribute converts a struct into a map
    /// - the support for arrow maps in the data ecosystem is limited (e.g.,
    ///   polars does not support them)
    ///
    MapAsStruct,
    /// Mark a variant as unknown
    ///
    /// This strategy applies only to fields with DataType Null. If
    /// serialization or deserialization of such a field is attempted, it will
    /// result in an error.
    UnknownVariant,
}

impl std::fmt::Display for Strategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InconsistentTypes => write!(f, "InconsistentTypes"),
            Self::UtcStrAsDate64 => write!(f, "UtcStrAsDate64"),
            Self::NaiveStrAsDate64 => write!(f, "NaiveStrAsDate64"),
            Self::TupleAsStruct => write!(f, "TupleAsStruct"),
            Self::MapAsStruct => write!(f, "MapAsStruct"),
            Self::UnknownVariant => write!(f, "UnknownVariant"),
        }
    }
}

impl FromStr for Strategy {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "InconsistentTypes" => Ok(Self::InconsistentTypes),
            "UtcStrAsDate64" => Ok(Self::UtcStrAsDate64),
            "NaiveStrAsDate64" => Ok(Self::NaiveStrAsDate64),
            "TupleAsStruct" => Ok(Self::TupleAsStruct),
            "MapAsStruct" => Ok(Self::MapAsStruct),
            "UnknownVariant" => Ok(Self::UnknownVariant),
            _ => fail!("Unknown strategy {s}"),
        }
    }
}

impl From<Strategy> for BTreeMap<String, String> {
    fn from(value: Strategy) -> Self {
        let mut res = BTreeMap::new();
        res.insert(STRATEGY_KEY.to_string(), value.to_string());
        res
    }
}

impl From<Strategy> for HashMap<String, String> {
    fn from(value: Strategy) -> Self {
        let mut res = HashMap::new();
        res.insert(STRATEGY_KEY.to_string(), value.to_string());
        res
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Ord, Eq, Serialize, Deserialize)]
pub enum GenericTimeUnit {
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

impl std::fmt::Display for GenericTimeUnit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GenericTimeUnit::Second => write!(f, "Second"),
            GenericTimeUnit::Millisecond => write!(f, "Millisecond"),
            GenericTimeUnit::Microsecond => write!(f, "Microsecond"),
            GenericTimeUnit::Nanosecond => write!(f, "Nanosecond"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Ord, Eq, Serialize, Deserialize)]
#[serde(try_from = "GenericDataTypeString", into = "GenericDataTypeString")]
pub enum GenericDataType {
    Null,
    Bool,
    I8,
    I16,
    I32,
    I64,
    U8,
    U16,
    U32,
    U64,
    F16,
    F32,
    F64,
    Utf8,
    LargeUtf8,
    Date64,
    Struct,
    List,
    LargeList,
    Union,
    Map,
    Dictionary,
    Timestamp(GenericTimeUnit, Option<String>),
}

impl std::fmt::Display for GenericDataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use GenericDataType::*;
        match self {
            Null => write!(f, "Null"),
            Bool => write!(f, "Bool"),
            Utf8 => write!(f, "Utf8"),
            LargeUtf8 => write!(f, "LargeUtf8"),
            I8 => write!(f, "I8"),
            I16 => write!(f, "I16"),
            I32 => write!(f, "I32"),
            I64 => write!(f, "I64"),
            U8 => write!(f, "U8"),
            U16 => write!(f, "U16"),
            U32 => write!(f, "U32"),
            U64 => write!(f, "U64"),
            F16 => write!(f, "F16"),
            F32 => write!(f, "F32"),
            F64 => write!(f, "F64"),
            Date64 => write!(f, "Date64"),
            Struct => write!(f, "Struct"),
            List => write!(f, "List"),
            LargeList => write!(f, "LargeList"),
            Union => write!(f, "Union"),
            Map => write!(f, "Map"),
            Dictionary => write!(f, "Dictionary"),
            Timestamp(unit, timezone) => {
                if let Some(timezone) = timezone {
                    write!(f, "Timestamp({unit}, Some(\"{timezone}\"))")
                } else {
                    write!(f, "Timestamp({unit}, None)")
                }
            }
        }
    }
}

impl std::str::FromStr for GenericDataType {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let s = s.trim();

        if s == "Null" {
            Ok(GenericDataType::Null)
        } else if s == "Bool" || s == "Boolean" {
            Ok(GenericDataType::Bool)
        } else if s == "Utf8" {
            Ok(GenericDataType::Utf8)
        } else if s == "LargeUtf8" {
            Ok(GenericDataType::LargeUtf8)
        } else if s == "U8" || s == "UInt8" {
            Ok(GenericDataType::U8)
        } else if s == "U16" || s == "UInt16" {
            Ok(GenericDataType::U16)
        } else if s == "U32" || s == "UInt32" {
            Ok(GenericDataType::U32)
        } else if s == "U64" || s == "UInt64" {
            Ok(GenericDataType::U64)
        } else if s == "I8" || s == "Int8" {
            Ok(GenericDataType::I8)
        } else if s == "I16" || s == "Int16" {
            Ok(GenericDataType::I16)
        } else if s == "I32" || s == "Int32" {
            Ok(GenericDataType::I32)
        } else if s == "I64" || s == "Int64" {
            Ok(GenericDataType::I64)
        } else if s == "F16" || s == "Float16" {
            Ok(GenericDataType::F16)
        } else if s == "F32" || s == "Float32" {
            Ok(GenericDataType::F32)
        } else if s == "F64" || s == "Float64" {
            Ok(GenericDataType::F64)
        } else if s == "Date64" {
            Ok(GenericDataType::Date64)
        } else if s == "Struct" {
            Ok(GenericDataType::Struct)
        } else if s == "List" {
            Ok(GenericDataType::List)
        } else if s == "LargeList" {
            Ok(GenericDataType::LargeList)
        } else if s == "Union" {
            Ok(GenericDataType::Union)
        } else if s == "Map" {
            Ok(GenericDataType::Map)
        } else if s == "Dictionary" {
            Ok(GenericDataType::Dictionary)
        } else if let Some(s) = s.strip_prefix("Timestamp(") {
            let (s, unit) = if let Some(s) = s.strip_prefix("Second, ") {
                (s, GenericTimeUnit::Second)
            } else if let Some(s) = s.strip_prefix("Millisecond, ") {
                (s, GenericTimeUnit::Millisecond)
            } else if let Some(s) = s.strip_prefix("Microsecond, ") {
                (s, GenericTimeUnit::Microsecond)
            } else if let Some(s) = s.strip_prefix("Nanosecond, ") {
                (s, GenericTimeUnit::Nanosecond)
            } else {
                fail!("expected valid time unit");
            };

            if let Some(s) = s.strip_prefix("None)") {
                if !s.is_empty() {
                    fail!("unexpected trailing content");
                }
                return Ok(GenericDataType::Timestamp(unit, None));
            };

            let Some(s) = s.strip_prefix("Some(\"") else {
                fail!("expected either None or Some(..)), found: {s:?}");
            };
            let Some(s) = s.strip_suffix("\"))") else {
                fail!("expected either None or Some(..)), found: {s:?}");
            };

            Ok(GenericDataType::Timestamp(unit, Some(s.to_string())))
        } else {
            fail!("cannot parse data type")
        }
    }
}

#[derive(Serialize, Deserialize)]
struct GenericDataTypeString(String);

impl TryFrom<GenericDataTypeString> for GenericDataType {
    type Error = Error;

    fn try_from(value: GenericDataTypeString) -> std::result::Result<Self, Self::Error> {
        value.0.parse()
    }
}

impl From<GenericDataType> for GenericDataTypeString {
    fn from(value: GenericDataType) -> Self {
        Self(value.to_string())
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct GenericField {
    pub name: String,
    pub data_type: GenericDataType,

    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub strategy: Option<Strategy>,

    #[serde(default)]
    #[serde(skip_serializing_if = "is_false")]
    pub nullable: bool,

    #[serde(default)]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub children: Vec<GenericField>,
}

fn is_false(val: &bool) -> bool {
    !*val
}

impl GenericField {
    pub fn new(name: &str, data_type: GenericDataType, nullable: bool) -> Self {
        Self {
            name: name.to_string(),
            data_type,
            nullable,
            children: Vec::new(),
            strategy: None,
        }
    }

    pub fn is_valid(&self) -> bool {
        self.validate().is_ok()
    }

    pub fn validate(&self) -> Result<()> {
        match self.data_type {
            GenericDataType::Null => self.validate_null(),
            GenericDataType::Bool => self.validate_primitive(),
            GenericDataType::U8 => self.validate_primitive(),
            GenericDataType::U16 => self.validate_primitive(),
            GenericDataType::U32 => self.validate_primitive(),
            GenericDataType::U64 => self.validate_primitive(),
            GenericDataType::I8 => self.validate_primitive(),
            GenericDataType::I16 => self.validate_primitive(),
            GenericDataType::I32 => self.validate_primitive(),
            GenericDataType::I64 => self.validate_primitive(),
            GenericDataType::F16 => self.validate_primitive(),
            GenericDataType::F32 => self.validate_primitive(),
            GenericDataType::F64 => self.validate_primitive(),
            GenericDataType::Utf8 => self.validate_primitive(),
            GenericDataType::LargeUtf8 => self.validate_primitive(),
            GenericDataType::Date64 => self.validate_date64(),
            GenericDataType::Struct => self.validate_struct(),
            GenericDataType::Map => self.validate_map(),
            GenericDataType::List => self.validate_list(),
            GenericDataType::LargeList => self.validate_list(),
            GenericDataType::Union => self.validate_union(),
            GenericDataType::Dictionary => self.validate_dictionary(),
            GenericDataType::Timestamp(_, _) => self.validate_timestamp(),
        }
    }

    /// Test that the other field is compatible with the current one
    ///
    pub fn is_compatible(&self, other: &GenericField) -> bool {
        self.validate_compatibility(other).is_ok()
    }

    pub fn validate_compatibility(&self, other: &GenericField) -> Result<()> {
        self.validate()?;
        other
            .validate()
            .map_err(|err| Error::custom_from(format!("invalid other field: {err}"), err))?;

        if !field_is_compatible(self, other) {
            fail!("incompatible fields: {self:?}, {other:?}");
        }

        Ok(())
    }

    pub fn with_child(mut self, child: GenericField) -> Self {
        self.children.push(child);
        self
    }

    pub fn with_strategy(mut self, strategy: Strategy) -> Self {
        self.strategy = Some(strategy);
        self
    }

    pub fn with_optional_strategy(mut self, strategy: Option<Strategy>) -> Self {
        self.strategy = strategy;
        self
    }
}

impl GenericField {
    pub(crate) fn validate_null(&self) -> Result<()> {
        if !matches!(
            self.strategy,
            None | Some(Strategy::InconsistentTypes) | Some(Strategy::UnknownVariant)
        ) {
            fail!(
                "invalid strategy for Null field: {}",
                self.strategy.as_ref().unwrap()
            );
        }
        if !self.children.is_empty() {
            fail!("Null field must not have children");
        }
        Ok(())
    }

    pub(crate) fn validate_primitive(&self) -> Result<()> {
        if self.strategy.is_some() {
            fail!(
                "invalid strategy for {}: {}",
                self.data_type,
                self.strategy.as_ref().unwrap()
            );
        }
        if !self.children.is_empty() {
            fail!("{} field must not have children", self.data_type);
        }
        Ok(())
    }

    pub(crate) fn validate_date64(&self) -> Result<()> {
        if !matches!(
            self.strategy,
            None | Some(Strategy::UtcStrAsDate64) | Some(Strategy::NaiveStrAsDate64)
        ) {
            fail!(
                "invalid strategy for Date64 field: {}",
                self.strategy.as_ref().unwrap()
            );
        }
        Ok(())
    }

    pub(crate) fn validate_timestamp(&self) -> Result<()> {
        match &self.strategy {
            None => Ok(()),
            Some(strategy @ Strategy::UtcStrAsDate64) => {
                if !matches!(&self.data_type, GenericDataType::Timestamp(GenericTimeUnit::Millisecond, Some(tz)) if tz == "UTC")
                {
                    fail!(
                        "invalid strategy for timestamp field {}: {}",
                        self.data_type,
                        strategy,
                    );
                }
                Ok(())
            }
            Some(strategy @ Strategy::NaiveStrAsDate64) => {
                if !matches!(
                    &self.data_type,
                    GenericDataType::Timestamp(GenericTimeUnit::Millisecond, None)
                ) {
                    fail!(
                        "invalid strategy for timestamp field {}: {}",
                        self.data_type,
                        strategy,
                    );
                }
                Ok(())
            }
            Some(strategy) => fail!(
                "invalid strategy for timestamp field {}: {}",
                self.data_type,
                strategy
            ),
        }
    }

    pub(crate) fn validate_struct(&self) -> Result<()> {
        // NOTE: do not check number of children: arrow-rs can 0 children, arrow2 not
        if !matches!(
            self.strategy,
            None | Some(Strategy::MapAsStruct) | Some(Strategy::TupleAsStruct)
        ) {
            fail!(
                "invalid strategy for Struct field: {}",
                self.strategy.as_ref().unwrap()
            );
        }

        for child in &self.children {
            child.validate()?;
        }

        Ok(())
    }

    pub(crate) fn validate_map(&self) -> Result<()> {
        if self.strategy.is_some() {
            fail!(
                "invalid strategy for Map field: {}",
                self.strategy.as_ref().unwrap()
            );
        }
        if self.children.len() != 1 {
            fail!(
                "invalid number of children for Map field: {}",
                self.children.len()
            );
        }
        if self.children[0].data_type != GenericDataType::Struct {
            fail!(
                "invalid child for Map field, expected Struct, found: {}",
                self.children[0].data_type
            );
        }
        if self.children[0].children.len() != 2 {
            fail!("invalid child for Map field, expected Struct with two fields, found Struct wiht {} fields", self.children[0].children.len());
        }

        for child in &self.children {
            child.validate()?;
        }

        Ok(())
    }

    pub(crate) fn validate_list(&self) -> Result<()> {
        if self.strategy.is_some() {
            fail!(
                "invalid strategy for List field: {}",
                self.strategy.as_ref().unwrap()
            );
        }
        if self.children.len() != 1 {
            fail!(
                "invalid number of children for List field. Expected 1, found: {}",
                self.children.len()
            );
        }
        self.children[0].validate()?;

        Ok(())
    }

    pub(crate) fn validate_union(&self) -> Result<()> {
        if self.strategy.is_some() {
            fail!(
                "invalid strategy for Union field: {}",
                self.strategy.as_ref().unwrap()
            );
        }
        if self.children.is_empty() {
            fail!("Union field without children");
        }
        for child in &self.children {
            child.validate()?;
        }
        Ok(())
    }

    pub(crate) fn validate_dictionary(&self) -> Result<()> {
        if self.strategy.is_some() {
            fail!(
                "invalid strategy for Dictionary field: {}",
                self.strategy.as_ref().unwrap()
            );
        }
        if self.children.len() != 2 {
            fail!(
                "invalid number of children for Dictionary field. Expected 2, found: {}",
                self.children.len()
            );
        }
        if !matches!(
            self.children[0].data_type,
            GenericDataType::U8
                | GenericDataType::U16
                | GenericDataType::U32
                | GenericDataType::U64
                | GenericDataType::I8
                | GenericDataType::I16
                | GenericDataType::I32
                | GenericDataType::I64
        ) {
            fail!(
                "invalid child for Dictionary. Expected integer keys, found: {}",
                self.children[0].data_type
            );
        }
        if !matches!(
            self.children[1].data_type,
            GenericDataType::Utf8 | GenericDataType::LargeUtf8
        ) {
            fail!(
                "invalid child for Dictionary. Expected string values, found: {}",
                self.children[1].data_type
            );
        }
        for child in &self.children {
            child.validate()?;
        }
        Ok(())
    }
}

/// Test that two fields are compatible with each other
///
fn field_is_compatible(left: &GenericField, right: &GenericField) -> bool {
    if left == right {
        return true;
    }

    let (left, right) = if left.data_type > right.data_type {
        (right, left)
    } else {
        (left, right)
    };

    use GenericDataType as D;

    match &left.data_type {
        D::I8 => matches!(
            &right.data_type,
            D::I16 | D::I32 | D::I64 | D::U8 | D::U16 | D::U32 | D::U64
        ),
        D::I16 => matches!(
            &right.data_type,
            D::I32 | D::I64 | D::U8 | D::U16 | D::U32 | D::U64
        ),
        D::I32 => matches!(&right.data_type, D::I64 | D::U8 | D::U16 | D::U32 | D::U64),
        D::I64 => matches!(
            &right.data_type,
            D::U8 | D::U16 | D::U32 | D::U64 | D::Date64
        ),
        D::U8 => matches!(&right.data_type, D::U16 | D::U32 | D::U64),
        D::U16 => matches!(&right.data_type, D::U32 | D::U64),
        D::U32 => matches!(&right.data_type, D::U64),
        D::Utf8 => match &right.data_type {
            D::LargeUtf8 => true,
            D::Dictionary => true,
            D::Date64 => matches!(
                &right.strategy,
                Some(Strategy::NaiveStrAsDate64) | Some(Strategy::UtcStrAsDate64)
            ),
            _ => false,
        },
        D::LargeUtf8 => match &right.data_type {
            D::Dictionary => true,
            D::Date64 => matches!(
                &right.strategy,
                Some(Strategy::NaiveStrAsDate64) | Some(Strategy::UtcStrAsDate64)
            ),
            _ => false,
        },
        D::Dictionary => right.data_type == D::Dictionary,
        _ => false,
    }
}

#[cfg(test)]
mod test_schema_serialization {
    use crate::internal::schema::GenericDataType;

    use super::{GenericField, SerdeArrowSchema};

    impl SerdeArrowSchema {
        fn with_field(mut self, field: GenericField) -> Self {
            self.fields.push(field);
            self
        }
    }

    #[test]
    fn example() {
        let schema = SerdeArrowSchema::new()
            .with_field(GenericField::new("foo", GenericDataType::U8, false))
            .with_field(GenericField::new("bar", GenericDataType::Utf8, false));

        let actual = serde_json::to_string(&schema).unwrap();
        assert_eq!(
            actual,
            r#"{"fields":[{"name":"foo","data_type":"U8"},{"name":"bar","data_type":"Utf8"}]}"#
        );

        let round_tripped: SerdeArrowSchema = serde_json::from_str(&actual).unwrap();
        assert_eq!(round_tripped, schema);
    }

    #[test]
    fn example_without_wrapper() {
        let expected = SerdeArrowSchema::new()
            .with_field(GenericField::new("foo", GenericDataType::U8, false))
            .with_field(GenericField::new("bar", GenericDataType::Utf8, false));

        let input = r#"[{"name":"foo","data_type":"U8"},{"name":"bar","data_type":"Utf8"}]"#;
        let actual: SerdeArrowSchema = serde_json::from_str(&input).unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn list() {
        let schema =
            SerdeArrowSchema::new().with_field(
                GenericField::new("value", GenericDataType::List, false)
                    .with_child(GenericField::new("element", GenericDataType::I32, false)),
            );

        let actual = serde_json::to_string(&schema).unwrap();
        assert_eq!(
            actual,
            r#"{"fields":[{"name":"value","data_type":"List","children":[{"name":"element","data_type":"I32"}]}]}"#
        );

        let round_tripped: SerdeArrowSchema = serde_json::from_str(&actual).unwrap();
        assert_eq!(round_tripped, schema);
    }

    #[test]
    fn doc_schema() {
        let schema = r#"
            [
                {"name":"foo","data_type":"U8"},
                {"name":"bar","data_type":"Utf8"}
            ]
        "#;

        let actual: SerdeArrowSchema = serde_json::from_str(&schema).unwrap();
        let expected = SerdeArrowSchema::new()
            .with_field(GenericField::new("foo", GenericDataType::U8, false))
            .with_field(GenericField::new("bar", GenericDataType::Utf8, false));

        assert_eq!(actual, expected);
    }

    #[test]
    fn timestamp_second_serialization() {
        let dt = super::GenericDataType::Timestamp(super::GenericTimeUnit::Second, None);

        let s = serde_json::to_string(&dt).unwrap();
        assert_eq!(s, r#""Timestamp(Second, None)""#);

        let rt = serde_json::from_str(&s).unwrap();
        assert_eq!(dt, rt);
    }

    #[test]
    fn timestamp_second_utc_serialization() {
        let dt = super::GenericDataType::Timestamp(
            super::GenericTimeUnit::Second,
            Some(String::from("Utc")),
        );

        let s = serde_json::to_string(&dt).unwrap();
        assert_eq!(s, r#""Timestamp(Second, Some(\"Utc\"))""#);

        let rt = serde_json::from_str(&s).unwrap();
        assert_eq!(dt, rt);
    }

    #[test]
    fn test_long_form_types() {
        use super::GenericDataType as DT;
        use std::str::FromStr;

        assert_eq!(DT::from_str("Boolean").unwrap(), DT::Bool);
        assert_eq!(DT::from_str("Int8").unwrap(), DT::I8);
        assert_eq!(DT::from_str("Int16").unwrap(), DT::I16);
        assert_eq!(DT::from_str("Int32").unwrap(), DT::I32);
        assert_eq!(DT::from_str("Int64").unwrap(), DT::I64);
        assert_eq!(DT::from_str("UInt8").unwrap(), DT::U8);
        assert_eq!(DT::from_str("UInt16").unwrap(), DT::U16);
        assert_eq!(DT::from_str("UInt32").unwrap(), DT::U32);
        assert_eq!(DT::from_str("UInt64").unwrap(), DT::U64);
        assert_eq!(DT::from_str("Float16").unwrap(), DT::F16);
        assert_eq!(DT::from_str("Float32").unwrap(), DT::F32);
        assert_eq!(DT::from_str("Float64").unwrap(), DT::F64);
    }

    macro_rules! test_data_type {
        ($($variant:ident,)*) => {
            mod test_data_type {
                $(
                    #[allow(non_snake_case)]
                    #[test]
                    fn $variant() {
                        let ty = super::super::GenericDataType::$variant;

                        let s = serde_json::to_string(&ty).unwrap();
                        assert_eq!(s, concat!("\"", stringify!($variant), "\""));

                        let rt = serde_json::from_str(&s).unwrap();
                        assert_eq!(ty, rt);
                    }
                )*
            }
        };
    }

    test_data_type!(
        Null, Bool, I8, I16, I32, I64, U8, U16, U32, U64, F16, F32, F64, Utf8, LargeUtf8, List,
        LargeList, Struct, Dictionary, Union, Map, Date64,
    );
}
