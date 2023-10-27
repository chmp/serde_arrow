pub mod samples;
pub mod tracer;
pub mod types;

use serde::{Deserialize, Serialize};

use crate::internal::{
    schema::{GenericField, Schema},
    Result,
};

pub use samples::SamplesTracer;
pub use types::trace_type;

/// Configure how the schema is traced
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
/// # let defaults =
/// TracingOptions {
///     allow_null_fields: false,
///     map_as_struct: true,
///     string_dictionary_encoding: false,
///     coerce_numbers: false,
///     try_parse_dates: false,
/// }
/// # ;
/// # assert_eq!(defaults, TracingOptions::default());
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct TracingOptions {
    /// If `true`, accept null-only fields (e.g., fields with type `()` or fields
    /// with only `None` entries). If `false`, schema tracing will fail in this
    /// case.
    pub allow_null_fields: bool,

    /// If `true` serialize maps as structs (the default). See
    /// [`Strategy::MapAsStruct`] for details.
    pub map_as_struct: bool,

    /// If `true` serialize strings dictionary encoded. The default is `false`.
    ///
    /// If `true`, strings are traced as `Dictionary(UInt64, LargeUtf8)`. If
    /// `false`, strings are traced as `LargeUtf8`.
    pub string_dictionary_encoding: bool,

    /// If `true`, coerce different numeric types.
    ///
    /// This option may be helpful when dealing with data formats that do not
    /// encode the complete numeric type, e.g., JSON. The following rules are
    /// used:
    ///
    /// - unsigned + other unsigned -> u64
    /// - signed + other signed -> i64
    /// - float + other float -> f64
    /// - unsigned + signed -> i64
    /// - unsigned + float -> f64
    /// - signed  + float -> f64
    pub coerce_numbers: bool,

    /// If `true`, try to auto detect datetimes in string columns
    ///
    /// Currently the naive datetime (`YYYY-MM-DDThh:mm:ss`) and UTC datetimes
    /// (`YYYY-MM-DDThh:mm:ssZ`) are understood.
    ///
    /// For string fields where all values are either missing or conform to one
    /// of the format the data type is set as `Date64` with strategy
    /// [`NaiveStrAsDate64`][Strategy::NaiveStrAsDate64] or
    /// [`UtcStrAsDate64`][Strategy::UtcStrAsDate64].    
    pub try_parse_dates: bool,
}

impl Default for TracingOptions {
    fn default() -> Self {
        Self {
            allow_null_fields: false,
            map_as_struct: true,
            string_dictionary_encoding: false,
            coerce_numbers: false,
            try_parse_dates: false,
        }
    }
}

impl TracingOptions {
    pub fn new() -> Self {
        Default::default()
    }

    /// Configure `allow_null_fields`
    pub fn allow_null_fields(mut self, value: bool) -> Self {
        self.allow_null_fields = value;
        self
    }

    /// Configure `map_as_struct`
    pub fn map_as_struct(mut self, value: bool) -> Self {
        self.map_as_struct = value;
        self
    }

    /// Configure `string_dictionary_encoding`
    pub fn string_dictionary_encoding(mut self, value: bool) -> Self {
        self.string_dictionary_encoding = value;
        self
    }

    /// Configure `coerce_numbers`
    pub fn coerce_numbers(mut self, value: bool) -> Self {
        self.coerce_numbers = value;
        self
    }

    /// Configure `coerce_numbers`
    pub fn guess_dates(mut self, value: bool) -> Self {
        self.try_parse_dates = value;
        self
    }
}

pub struct TracedSchema {}

impl TracedSchema {
    pub fn new() -> Self {
        Self {}
    }

    pub fn get_schema(&self) -> Result<Schema> {
        todo!()
    }

    // TODO: add  get_arrow2_fields
    // TODO: add  get_arrow_fields
}

impl TracedSchema {
    pub fn trace_samples<T: Serialize + ?Sized>(&mut self, samples: &T) -> Result<()> {
        todo!()
    }

    pub fn trace_type<'de, T: Deserialize<'de>>(&mut self) -> Result<()> {
        todo!()
    }
}

#[test]
fn test_trace_type() {}
