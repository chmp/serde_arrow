pub mod extensions;
mod from_samples;
mod from_type;
mod serde;
mod strategy;
pub mod tracer;
mod tracing_options;

#[cfg(test)]
mod test;

use crate::internal::{
    error::{fail, Result},
    utils::value,
};

use ::serde::{Deserialize, Serialize};

pub use self::serde::serialize::PrettyField;
pub use strategy::{get_strategy_from_metadata, Strategy, STRATEGY_KEY};
use tracer::Tracer;
pub use tracing_options::{Overwrites, TracingMode, TracingOptions};

use marrow::datatypes::{DataType, Field, TimeUnit, UnionMode};

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
/// The following types implement [`SchemaLike`] and can be constructed with the
/// methods mentioned above:
///
/// - [`SerdeArrowSchema`]
#[cfg_attr(
    has_arrow,
    doc = "- `Vec<`[`arrow::datatypes::FieldRef`][crate::_impl::arrow::datatypes::FieldRef]`>`"
)]
#[cfg_attr(
    has_arrow,
    doc = "- `Vec<`[`arrow::datatypes::Field`][crate::_impl::arrow::datatypes::Field]`>`"
)]
#[cfg_attr(
    has_arrow2,
    doc = "- `Vec<`[`arrow2::datatypes::Field`][crate::_impl::arrow2::datatypes::Field]`>`"
)]
///
/// Instances of `SerdeArrowSchema` can be directly serialized and deserialized.
/// The format is that described in [`SchemaLike::from_value`].
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
pub trait SchemaLike: Sized + Sealed {
    /// Build the schema from an object that implements serialize (e.g., `serde_json::Value`)
    ///
    /// ```rust
    /// # #[cfg(has_arrow)]
    /// # fn main() -> serde_arrow::_impl::PanicOnError<()> {
    /// # use serde_arrow::_impl::arrow;
    /// use arrow::datatypes::FieldRef;
    /// use serde_arrow::schema::SchemaLike;
    ///
    /// let schema = serde_json::json!([
    ///     {"name": "foo", "data_type": "U8"},
    ///     {"name": "bar", "data_type": "Utf8"},
    /// ]);
    ///
    /// let fields = Vec::<FieldRef>::from_value(&schema)?;
    /// # Ok(())
    /// # }
    /// # #[cfg(not(has_arrow))]
    /// # fn main() { }
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
    /// - `"nullable"` (**optional**): if `true`, the field can contain null values
    /// - `"strategy"` (**optional**): if given a string describing the strategy to use
    /// - `"children"` (**optional**): a list of child fields, the semantics depend on the data type
    ///
    /// The following data types are supported:
    ///
    /// - booleans: `"Bool"`
    /// - signed integers: `"I8"`, `"I16"`, `"I32"`, `"I64"`
    /// - unsigned integers: `"U8"`, `"U16"`, `"U32"`, `"U64"`
    /// - floats: `"F16"`, `"F32"`, `"F64"`
    /// - strings: `"Utf8"`, `"LargeUtf8"`
    /// - decimals: `"Decimal128(precision, scale)"`, as in `"Decimal128(5, 2)"`
    /// - date objects: `"Date32"`
    /// - date time objects: , `"Date64"`, `"Timestamp(unit, timezone)"` with unit being one of
    ///   `Second`, `Millisecond`, `Microsecond`, `Nanosecond`.
    /// - time objects: `"Time32(unit)"`, `"Time64(unit)"` with unit being one of `Second`,
    ///   `Millisecond`, `Microsecond`, `Nanosecond`.
    /// - durations: `"Duration(unit)"` with unit being one of `Second`, `Millisecond`,
    ///   `Microsecond`, `Nanosecond`.
    /// - lists: `"List"`, `"LargeList"`. `"children"` must contain a single field named `"element"`
    ///   that describes the element type
    /// - structs: `"Struct"`. `"children"` must contain the child fields
    /// - maps: `"Map"`. `"children"` must contain two fields, named `"key"` and `"value"` that
    ///   encode the key and value types
    /// - unions: `"Union"`. `"children"` must contain the different variants
    /// - dictionaries: `"Dictionary"`. `"children"` must contain two different fields, named
    ///   `"key"` of integer type and named `"value"` of string type
    ///
    fn from_value<T: Serialize>(value: T) -> Result<Self>;

    /// Determine the schema from the given record type. See [`TracingOptions`] for customization
    /// options.
    ///
    /// This approach requires the type `T` to implement [`Deserialize`][::serde::Deserialize]. As
    /// only type information is used, it is not possible to detect data dependent properties.
    /// Examples of unsupported features:
    ///
    /// - auto detection of date time strings
    /// - non self-describing types such as `serde_json::Value`
    /// - flattened structure (`#[serde(flatten)]`)
    /// - types that require specific data to be deserialized, such as the `DateTime` type of
    ///   `chrono` or the `Uuid` type of the `uuid` package
    ///
    /// Consider using [`from_samples`][SchemaLike::from_samples] in these cases.
    ///
    /// ```rust
    /// # #[cfg(has_arrow)]
    /// # fn main() -> serde_arrow::_impl::PanicOnError<()> {
    /// # use serde_arrow::_impl::arrow;
    /// use arrow::datatypes::{DataType, FieldRef};
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
    /// let fields = Vec::<FieldRef>::from_type::<Record>(TracingOptions::default())?;
    ///
    /// assert_eq!(fields[0].data_type(), &DataType::Int32);
    /// assert_eq!(fields[1].data_type(), &DataType::Float64);
    /// assert_eq!(fields[2].data_type(), &DataType::LargeUtf8);
    /// # Ok(())
    /// # }
    /// # #[cfg(not(has_arrow))]
    /// # fn main() { }
    /// ```
    ///
    /// Note, the type `T` must encode a single "row" in the resulting data
    /// frame. When encoding single values, consider using the
    /// [`Item`][crate::utils::Item] wrapper.
    ///
    /// ```rust
    /// # #[cfg(has_arrow)]
    /// # fn main() -> serde_arrow::_impl::PanicOnError<()> {
    /// # use serde_arrow::_impl::arrow;
    /// use arrow::datatypes::{DataType, FieldRef};
    /// use serde_arrow::{schema::{SchemaLike, TracingOptions}, utils::Item};
    ///
    /// let fields = Vec::<FieldRef>::from_type::<Item<f32>>(TracingOptions::default())?;
    ///
    /// assert_eq!(fields[0].data_type(), &DataType::Float32);
    /// # Ok(())
    /// # }
    /// # #[cfg(not(has_arrow))]
    /// # fn main() { }
    /// ```
    fn from_type<'de, T: Deserialize<'de>>(options: TracingOptions) -> Result<Self>;

    /// Determine the schema from samples. See [`TracingOptions`] for customization options.
    ///
    /// This approach requires the type `T` to implement [`Serialize`][::serde::Serialize] and the
    /// samples to include all relevant values. It uses only the information encoded in the samples
    /// to generate the schema. Therefore, the following requirements must be met:
    ///
    /// - at least one `Some` value for `Option<..>` fields
    /// - all variants of enum fields
    /// - at least one element for sequence fields (e.g., `Vec<..>`)
    /// - at least one example for map types (e.g., `HashMap<.., ..>`). All possible keys must be
    ///   given, if [`options.map_as_struct == true`][TracingOptions::map_as_struct]).
    ///
    /// ```rust
    /// # #[cfg(has_arrow)]
    /// # fn main() -> serde_arrow::_impl::PanicOnError<()> {
    /// # use serde_arrow::_impl::arrow;
    /// use arrow::datatypes::{DataType, FieldRef};
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
    /// let fields = Vec::<FieldRef>::from_samples(&samples, TracingOptions::default())?;
    ///
    /// assert_eq!(fields[0].data_type(), &DataType::Int32);
    /// assert_eq!(fields[1].data_type(), &DataType::Float64);
    /// assert_eq!(fields[2].data_type(), &DataType::LargeUtf8);
    /// # Ok(())
    /// # }
    /// # #[cfg(not(has_arrow))]
    /// # fn main() { }
    /// ```
    ///
    /// Note, the samples must encode "rows" in the resulting data frame. When
    /// encoding single values, consider using the
    /// [`Items`][crate::utils::Items] wrapper.
    ///
    /// ```rust
    /// # #[cfg(has_arrow)]
    /// # fn main() -> serde_arrow::_impl::PanicOnError<()> {
    /// # use serde_arrow::_impl::arrow;
    /// use arrow::datatypes::{DataType, FieldRef};
    /// use serde_arrow::{schema::{SchemaLike, TracingOptions}, utils::Items};
    ///
    /// let fields = Vec::<FieldRef>::from_samples(
    ///     &Items(&[1.0_f32, 2.0_f32, 3.0_f32]),
    ///     TracingOptions::default(),
    /// )?;
    ///
    /// assert_eq!(fields[0].data_type(), &DataType::Float32);
    /// # Ok(())
    /// # }
    /// # #[cfg(not(has_arrow))]
    /// # fn main() { }
    /// ```
    fn from_samples<T: Serialize>(samples: T, options: TracingOptions) -> Result<Self>;
}

/// A collection of fields as understood by `serde_arrow`
///
/// It can be converted from / to arrow or arrow2 fields.
///
#[derive(Default, Debug, PartialEq, Clone)]
pub struct SerdeArrowSchema {
    pub(crate) fields: Vec<Field>,
}

impl Sealed for SerdeArrowSchema {}

impl SchemaLike for SerdeArrowSchema {
    fn from_value<T: Serialize>(value: T) -> Result<Self> {
        value::transmute(value)
    }

    fn from_type<'de, T: Deserialize<'de>>(options: TracingOptions) -> Result<Self> {
        Tracer::from_type::<T>(options)?.to_schema()
    }

    fn from_samples<T: Serialize>(samples: T, options: TracingOptions) -> Result<Self> {
        Tracer::from_samples(samples, options)?.to_schema()
    }
}

impl Sealed for Vec<Field> {}

impl SchemaLike for Vec<Field> {
    fn from_value<T: Serialize>(value: T) -> Result<Self> {
        Ok(SerdeArrowSchema::from_value(value)?.fields)
    }

    fn from_samples<T: Serialize>(samples: T, options: TracingOptions) -> Result<Self> {
        Ok(SerdeArrowSchema::from_samples(samples, options)?.fields)
    }

    fn from_type<'de, T: Deserialize<'de>>(options: TracingOptions) -> Result<Self> {
        Ok(SerdeArrowSchema::from_type::<T>(options)?.fields)
    }
}

/// Wrapper around `SerdeArrowSchema::from_value` to convert a single field
///
/// This function takes anything that serialized into a field and converts it into a field.
pub fn transmute_field(field: impl Serialize) -> Result<Field> {
    let expected = SerdeArrowSchema::from_value(&[field])?;
    let Some(field) = expected.fields.into_iter().next() else {
        fail!("unexpected error in transmute_field: no field found");
    };
    Ok(field)
}

pub fn validate_field(field: &Field) -> Result<()> {
    match &field.data_type {
        DataType::Null => validate_null_field(field),
        DataType::Boolean
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Decimal128(_, _)
        | DataType::Date32
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::Duration(_) => validate_primitive_field(field),
        DataType::FixedSizeBinary(n) => validate_fixed_size_binary_field(field, *n),
        DataType::Date64 => validate_date64_field(field),
        DataType::Timestamp(unit, tz) => validate_timestamp_field(field, *unit, tz.as_deref()),
        DataType::Time32(unit) => validate_time32_field(field, *unit),
        DataType::Time64(unit) => validate_time64_field(field, *unit),
        DataType::Struct(fields) => validate_struct_field(field, fields.as_slice()),
        DataType::Map(entry, _) => validate_map_field(field, entry.as_ref()),
        DataType::List(entry) => validate_list_field(field, entry.as_ref()),
        DataType::LargeList(entry) => validate_list_field(field, entry.as_ref()),
        DataType::FixedSizeList(entry, n) => {
            validate_fixed_size_list_field(field, entry.as_ref(), *n)
        }
        DataType::Union(fields, mode) => validate_union_field(field, fields.as_slice(), *mode),
        DataType::Dictionary(key, values) => {
            validate_dictionary_field(field, key.as_ref(), values.as_ref())
        }
        dt => fail!("Unsupported data type {dt:?}"),
    }
}

fn validate_null_field(field: &Field) -> Result<()> {
    match get_strategy_from_metadata(&field.metadata)? {
        None | Some(Strategy::InconsistentTypes) | Some(Strategy::UnknownVariant) => Ok(()),
        Some(strategy) => fail!("invalid strategy for Null field: {strategy}"),
    }
}

fn validate_primitive_field(field: &Field) -> Result<()> {
    if let Some(strategy) = get_strategy_from_metadata(&field.metadata)? {
        fail!(
            "invalid strategy for {data_type}: {strategy}",
            data_type = DataTypeDisplay(&field.data_type),
        );
    }
    Ok(())
}

fn validate_fixed_size_binary_field(field: &Field, n: i32) -> Result<()> {
    if n < 0 {
        fail!("Invalid FixedSizedBinary with negative number of elements");
    }
    validate_primitive_field(field)
}

fn validate_fixed_size_list_field(field: &Field, child: &Field, n: i32) -> Result<()> {
    if n < 0 {
        fail!("Invalid FixedSizeList with negative number of elements");
    }
    validate_list_field(field, child)
}

fn validate_list_field(field: &Field, child: &Field) -> Result<()> {
    if let Some(strategy) = get_strategy_from_metadata(&field.metadata)? {
        fail!("invalid strategy for List field: {strategy}");
    }
    validate_field(child)
}

fn validate_dictionary_field(field: &Field, key: &DataType, value: &DataType) -> Result<()> {
    if let Some(strategy) = get_strategy_from_metadata(&field.metadata)? {
        fail!("invalid strategy for Dictionary field: {strategy}");
    }
    if !matches!(
        key,
        DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
    ) {
        fail!(
            "invalid child for Dictionary. Expected integer keys, found: {key}",
            key = DataTypeDisplay(key),
        );
    }
    if !matches!(value, DataType::Utf8 | DataType::LargeUtf8) {
        fail!(
            "invalid child for Dictionary. Expected string values, found: {value}",
            value = DataTypeDisplay(value)
        );
    }
    Ok(())
}

fn validate_date64_field(field: &Field) -> Result<()> {
    if let Some(strategy) = get_strategy_from_metadata(&field.metadata)? {
        fail!("invalid strategy for Date64 field: {strategy}");
    }
    Ok(())
}

fn validate_timestamp_field(field: &Field, unit: TimeUnit, tz: Option<&str>) -> Result<()> {
    if let Some(strategy) = get_strategy_from_metadata(&field.metadata)? {
        fail!("invalid strategy for Timestamp({unit}, {tz:?}) field: {strategy}");
    }
    Ok(())
}

fn validate_time32_field(field: &Field, unit: TimeUnit) -> Result<()> {
    if let Some(strategy) = get_strategy_from_metadata(&field.metadata)? {
        fail!("invalid strategy for Time32({unit}) field: {strategy}");
    }
    if !matches!(unit, TimeUnit::Second | TimeUnit::Millisecond) {
        fail!("Time32 field must have Second or Millisecond unit");
    }
    Ok(())
}

fn validate_time64_field(field: &Field, unit: TimeUnit) -> Result<()> {
    if let Some(strategy) = get_strategy_from_metadata(&field.metadata)? {
        fail!("invalid strategy for Time64({unit}) field: {strategy}");
    }
    if !matches!(unit, TimeUnit::Microsecond | TimeUnit::Nanosecond) {
        fail!("Time64 field must have Microsecond or Nanosecond unit");
    }
    Ok(())
}

fn validate_struct_field(field: &Field, children: &[Field]) -> Result<()> {
    // NOTE: do not check number of children: arrow-rs can 0 children, arrow2 not
    match get_strategy_from_metadata(&field.metadata)? {
        None | Some(Strategy::MapAsStruct) | Some(Strategy::TupleAsStruct) => {}
        Some(strategy) => fail!("invalid strategy for Struct field: {strategy}"),
    }
    for child in children {
        validate_field(child)?;
    }
    Ok(())
}

fn validate_map_field(field: &Field, _entry: &Field) -> Result<()> {
    if let Some(strategy) = get_strategy_from_metadata(&field.metadata)? {
        fail!("invalid strategy for Map field: {strategy}");
    }
    let DataType::Map(entry, _) = &field.data_type else {
        fail!("Invalid data type for map child, expected a map");
    };
    let DataType::Struct(entry_fields) = &entry.data_type else {
        fail!("Invalid child data type for map, expected struct with 2 fields");
    };
    if entry_fields.len() != 2 {
        fail!("Invalid child data type for map, expected struct with 2 fields");
    }
    Ok(())
}

fn validate_union_field(field: &Field, children: &[(i8, Field)], _mode: UnionMode) -> Result<()> {
    if let Some(strategy) = get_strategy_from_metadata(&field.metadata)? {
        fail!("invalid strategy for Union field: {strategy}");
    }
    for (_, child) in children {
        validate_field(child)?;
    }
    Ok(())
}

pub struct DataTypeDisplay<'a>(pub &'a DataType);

impl std::fmt::Display for DataTypeDisplay<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            DataType::Null => write!(f, "Null"),
            DataType::Boolean => write!(f, "Boolean"),
            DataType::Int8 => write!(f, "Int8"),
            DataType::Int16 => write!(f, "Int16"),
            DataType::Int32 => write!(f, "Int32"),
            DataType::Int64 => write!(f, "Int64"),
            DataType::UInt8 => write!(f, "UInt8"),
            DataType::UInt16 => write!(f, "UInt16"),
            DataType::UInt32 => write!(f, "UInt32"),
            DataType::UInt64 => write!(f, "UInt64"),
            DataType::Float16 => write!(f, "Float16"),
            DataType::Float32 => write!(f, "Float32"),
            DataType::Float64 => write!(f, "Float64"),
            DataType::Utf8 => write!(f, "Utf8"),
            DataType::LargeUtf8 => write!(f, "LargeUtf8"),
            DataType::Binary => write!(f, "Binary"),
            DataType::LargeBinary => write!(f, "LargeBinary"),
            DataType::FixedSizeBinary(n) => write!(f, "FixedSizeBinary({n})"),
            DataType::Date32 => write!(f, "Date32"),
            DataType::Date64 => write!(f, "Date64"),
            DataType::Time32(unit) => write!(f, "Time32({unit})"),
            DataType::Time64(unit) => write!(f, "Time64({unit})"),
            DataType::Timestamp(unit, tz) => write!(f, "Timestamp({unit}, {tz:?})"),
            DataType::Duration(unit) => write!(f, "Duration({unit})"),
            DataType::List(_) => write!(f, "List"),
            DataType::LargeList(_) => write!(f, "LargeList"),
            DataType::FixedSizeList(_, n) => write!(f, "FixedSizeList({n})"),
            DataType::Decimal128(precision, scale) => write!(f, "Decimal128({precision}, {scale}"),
            DataType::Struct(_) => write!(f, "Struct"),
            DataType::Map(_, sorted) => write!(f, "Map({sorted})"),
            DataType::Dictionary(key, value) => write!(
                f,
                "Dictionary({key}, {value})",
                key = DataTypeDisplay(key),
                value = DataTypeDisplay(value),
            ),
            DataType::Union(_, mode) => write!(f, "Union({mode})"),
            _ => write!(f, "<unknown marrow data type>"),
        }
    }
}

const _: () = {
    trait AssertSendSync: Send + Sync {}
    impl AssertSendSync for SerdeArrowSchema {}
    impl AssertSendSync for TracingOptions {}
    impl AssertSendSync for Strategy {}
    impl AssertSendSync for Overwrites {}
};
