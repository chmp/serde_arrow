use std::collections::HashMap;

use serde::Serialize;

use crate::internal::arrow::DataType;
use crate::internal::{arrow::Field, error::Result, schema::transmute_field};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TracingMode {
    Unknown,
    FromType,
    FromSamples,
}

/// Configure schema tracing
///
/// Example:
///
/// ```rust
/// # use serde_arrow::schema::TracingOptions;
/// let tracing_options = TracingOptions::default()
///     .map_as_struct(true)
///     .string_dictionary_encoding(false);
/// ```
///
/// The defaults are:
///
/// ```rust
/// # use serde_arrow::schema::TracingOptions;
/// assert_eq!(
///     TracingOptions::default(),
///     TracingOptions::new()
///         .allow_null_fields(false)
///         .map_as_struct(true)
///         .sequence_as_large_list(true)
///         .strings_as_large_utf8(true)
///         .string_dictionary_encoding(false)
///         .coerce_numbers(false)
///         .guess_dates(false)
///         .from_type_budget(100),
/// );
/// ```
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct TracingOptions {
    /// If `true`, accept null-only fields (e.g., fields with type `()` or fields
    /// with only `None` entries). If `false` (the default), schema tracing will fail in this
    /// case.
    pub allow_null_fields: bool,

    /// If `true` trace maps as structs (the default). See
    /// [`Strategy::MapAsStruct`][crate::schema::Strategy::MapAsStruct] for details.
    pub map_as_struct: bool,

    /// If `true` trace lists as `LargeLists` (the default). Otherwise lists are traced `List`.
    pub sequence_as_large_list: bool,

    /// If `true` trace strings as `LargeUtf8` (the default). Otherwise strings are traced as `Utf8`.
    pub string_as_large_utf8: bool,

    /// If `true` trace strings with dictionary encoding. If `false` (the default), strings are
    /// traced as either `LargeUtf8` or `Utf8` according to
    /// [`string_as_large_utf8`][TracingOptions::string_as_large_utf8].
    ///
    /// With dictionary encoding, strings are stored as an array of unique string values and indices
    /// into this array. This encoding helps to reduce memory consumption with repeated values. This
    /// encoding corresponds to the categorical data types of Pandas or Polars.
    ///
    /// The dictionary data type is either `Dictionary(UInt32, LargeUtf8)`  or `Dictionary(UInt32,
    /// Utf8)` depending on [`string_as_large_utf8`][TracingOptions::string_as_large_utf8]. 32 bit
    ///indices are used, as they are the default index type in `polars`.
    pub string_dictionary_encoding: bool,

    /// If `true`, coerce different numeric types. The default is `false`.
    ///
    /// This option may be helpful when dealing with data formats with varying numeric types numeric
    /// type, e.g., JSON. The following rules are used:
    ///
    /// - unsigned + other unsigned -> u64
    /// - signed + other signed -> i64
    /// - float + other float -> f64
    /// - unsigned + signed -> i64
    /// - unsigned + float -> f64
    /// - signed  + float -> f64
    pub coerce_numbers: bool,

    /// If `true`, try to auto detect datetimes in string columns. The default is `false`.
    ///
    /// Currently the naive datetime (`YYYY-MM-DDThh:mm:ss`) and UTC datetimes
    /// (`YYYY-MM-DDThh:mm:ssZ`) are understood.
    ///
    /// For string fields where all values are either missing or conform to one
    /// of the format the data type is set as `Date64` with strategy
    /// [`NaiveStrAsDate64`][crate::schema::Strategy::NaiveStrAsDate64] or
    /// [`UtcStrAsDate64`][crate::schema::Strategy::UtcStrAsDate64].
    pub guess_dates: bool,

    /// How many tracing iterations to perform in `from_type`.
    ///
    /// The default value (`100`) may be too conservative for deeply nested types or enums with many
    /// variants.
    pub from_type_budget: usize,

    /// If `true`, encode enums without data as dictionary encoded strings. If `false` (the
    /// default), enums without data are encoded as Union arrays with `Null` fields.
    ///
    /// Example:
    ///
    /// ```rust
    /// # use serde::{Deserialize, Serialize};
    /// # #[cfg(has_arrow)]
    /// # fn main() -> serde_arrow::Result<()> {
    /// # use serde_arrow::_impl::arrow;
    /// # use arrow::datatypes::FieldRef;
    /// # use serde_arrow::{schema::{SchemaLike, TracingOptions}, utils::Item};
    /// #
    /// ##[derive(Serialize, Deserialize)]
    /// enum U {
    ///     A,
    ///     B,
    ///     C,
    /// }
    ///
    /// let items = [Item(U::A), Item(U::B), Item(U::C), Item(U::A)];
    ///
    /// let tracing_options = TracingOptions::default().enums_without_data_as_strings(true);
    /// let fields = Vec::<FieldRef>::from_type::<Item<U>>(tracing_options)?;
    /// let batch = serde_arrow::to_record_batch(&fields, &items)?;
    /// #
    /// # Ok(())
    /// # }
    /// # #[cfg(not(has_arrow))]
    /// # fn main() { }
    /// ```
    pub enums_without_data_as_strings: bool,

    /// A mapping of field paths to field definitions
    ///
    /// Overwrites can be added with `options.overwrite(path, field)`. The `field` parameter must
    /// serialize to a valid field. See [`from_value`][crate::schema::SchemaLike::from_value] for
    /// details. Examples are instances of `serde_json::Value` with the correct content or of
    /// `arrow::datatypes::Field`. Nested fields can be overwritten by using dotted paths, e.g.m
    /// `"foo.bar"`.
    ///
    /// Overwrites can be used to change the data type of field, e.g., to ensure a field is a
    /// `Timestamp`:
    ///
    /// ```rust
    /// # #[cfg(has_arrow)]
    /// # fn main() -> serde_arrow::Result<()> {
    /// # use serde_arrow::_impl::arrow;
    /// # use arrow::datatypes::FieldRef;
    /// # use serde_arrow::schema::{SchemaLike, TracingOptions};
    /// # use serde_json::json;
    /// # use serde::{Serialize, Deserialize};
    /// #
    /// use chrono::{DateTime, Utc};
    ///
    /// ##[derive(Debug, Serialize, Deserialize)]
    /// struct Example {
    ///     #[serde(with = "chrono::serde::ts_microseconds")]
    ///     pub expiry: DateTime<Utc>,
    /// }
    ///
    /// let options = TracingOptions::default().overwrite(
    ///     "expiry",
    ///     json!({"name": "expiry", "data_type": "Timestamp(Microsecond, None)"}),
    /// )?;
    /// let fields = Vec::<FieldRef>::from_type::<Example>(options)?;
    /// #
    /// # assert_eq!(fields, Vec::<FieldRef>::from_value(&json!([
    /// #     {"name": "expiry", "data_type": "Timestamp(Microsecond, None)"}
    /// # ]))?);
    /// # Ok(())
    /// # }
    /// # #[cfg(not(has_arrow))]
    /// # fn main() { }
    /// ```
    ///
    /// Using a field:
    ///
    /// ```rust
    /// # #[cfg(has_arrow)]
    /// # fn main() -> serde_arrow::Result<()> {
    /// # use serde_arrow::_impl::arrow;
    /// # use arrow::datatypes::{FieldRef, Field, DataType, TimeUnit};
    /// # use serde_arrow::schema::{SchemaLike, TracingOptions};
    /// # use serde_json::json;
    /// # use serde::{Serialize, Deserialize};
    /// #
    /// # use chrono::{DateTime, Utc};
    /// #
    /// # #[derive(Debug, Serialize, Deserialize)]
    /// # struct Example {
    /// #    #[serde(with = "chrono::serde::ts_microseconds")]
    /// #    pub expiry: DateTime<Utc>,
    /// # }
    /// #
    /// let options = TracingOptions::default().overwrite(
    ///     "expiry",
    ///     Field::new(
    ///         "expiry",
    ///         DataType::Timestamp(TimeUnit::Microsecond, None),
    ///         false,
    ///     ),
    /// )?;
    /// let fields = Vec::<FieldRef>::from_type::<Example>(options)?;
    /// #
    /// # assert_eq!(fields, Vec::<FieldRef>::from_value(&json!([
    /// #     {"name": "expiry", "data_type": "Timestamp(Microsecond, None)"}
    /// # ]))?);
    /// # Ok(())
    /// # }
    /// # #[cfg(not(has_arrow))]
    /// # fn main() { }
    /// ```
    ///
    pub overwrites: Overwrites,

    /// Internal field to improve error messages for the different tracing
    /// functions
    pub(crate) tracing_mode: TracingMode,

    /// Whether to encode enums with data as structs
    ///
    /// If `false` enums with data are encoded as Union arrays.
    /// If `true` enums with data are encoded as Structs.
    ///
    /// TODO: example
    /// ```
    pub enums_with_named_fields_as_structs: bool,
}

impl Default for TracingOptions {
    fn default() -> Self {
        Self {
            allow_null_fields: false,
            map_as_struct: true,
            string_dictionary_encoding: false,
            coerce_numbers: false,
            guess_dates: false,
            from_type_budget: 100,
            enums_without_data_as_strings: false,
            enums_with_named_fields_as_structs: false,
            overwrites: Overwrites::default(),
            sequence_as_large_list: true,
            string_as_large_utf8: true,
            tracing_mode: TracingMode::Unknown,
        }
    }
}

impl TracingOptions {
    pub fn new() -> Self {
        Default::default()
    }

    /// Set [`allow_null_fields`](#structfield.allow_null_fields)
    pub fn allow_null_fields(mut self, value: bool) -> Self {
        self.allow_null_fields = value;
        self
    }

    /// Set [`map_as_struct`](#structfield.map_as_struct)
    pub fn map_as_struct(mut self, value: bool) -> Self {
        self.map_as_struct = value;
        self
    }

    /// Set [`sequence_as_large_list`](#structfield.sequence_as_large_list)
    pub fn sequence_as_large_list(mut self, value: bool) -> Self {
        self.sequence_as_large_list = value;
        self
    }

    /// Set [`string_as_large_utf8`](#structfield.string_as_large_utf8)
    pub fn strings_as_large_utf8(mut self, value: bool) -> Self {
        self.string_as_large_utf8 = value;
        self
    }

    /// Set [`string_dictionary_encoding`](#structfield.string_dictionary_encoding)
    pub fn string_dictionary_encoding(mut self, value: bool) -> Self {
        self.string_dictionary_encoding = value;
        self
    }

    /// Set [`coerce_numbers`](#structfield.coerce_numbers)
    pub fn coerce_numbers(mut self, value: bool) -> Self {
        self.coerce_numbers = value;
        self
    }

    /// Set [`try_parse_dates`](#structfield.try_parse_dates)
    pub fn guess_dates(mut self, value: bool) -> Self {
        self.guess_dates = value;
        self
    }

    /// Set [`from_type_budget`](#structfield.from_type_budget)
    pub fn from_type_budget(mut self, value: usize) -> Self {
        self.from_type_budget = value;
        self
    }

    /// Set [`enums_without_data_as_strings`](#structfield.enums_without_data_as_strings)
    pub fn enums_without_data_as_strings(mut self, value: bool) -> Self {
        self.enums_without_data_as_strings = value;
        self
    }

    /// Set [`enums_with_named_fields_as_structs`](#structfield.enums_with_named_fields_as_structs)
    pub fn enums_with_named_fields_as_structs(mut self, value: bool) -> Self {
        self.enums_with_named_fields_as_structs = value;
        self
    }

    /// Add an overwrite to [`overwrites`](#structfield.overwrites)
    pub fn overwrite<P: Into<String>, F: Serialize>(mut self, path: P, field: F) -> Result<Self> {
        self.overwrites.0.insert(
            format!("$.{path}", path = path.into()),
            transmute_field(field)?,
        );
        Ok(self)
    }

    pub(crate) fn tracing_mode(mut self, value: TracingMode) -> Self {
        self.tracing_mode = value;
        self
    }

    pub(crate) fn get_overwrite(&self, path: &str) -> Option<&Field> {
        self.overwrites.0.get(path)
    }

    pub(crate) fn string_type(&self) -> DataType {
        if self.string_as_large_utf8 {
            DataType::LargeUtf8
        } else {
            DataType::Utf8
        }
    }
}

/// An opaque mapping of field paths to field definitions
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Overwrites(pub(crate) HashMap<String, Field>);
