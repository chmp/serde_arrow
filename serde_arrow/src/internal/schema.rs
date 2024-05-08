mod data_type;
mod deserialization;
mod strategy;

#[cfg(test)]
mod test;

use crate::internal::{
    error::{fail, Error, Result},
    tracing::{Tracer, TracingMode, TracingOptions},
};

use serde::{Deserialize, Serialize};

pub use data_type::{GenericDataType, GenericTimeUnit};
pub use strategy::{Strategy, STRATEGY_KEY};

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
    /// Build the schema from an object that implements serialize (e.g.,
    /// `serde_json::Value`)
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
    /// - decimals: `"Decimal128(precision, scale)"`, as in `"Decimal128(5, 2)"`
    /// - date objects: `"Date32"`
    /// - date time objects: , `"Date64"`, `"Timestamp(unit, timezone)"` with
    ///   unit being one of `Second`, `Millisecond`, `Microsecond`,
    ///   `Nanosecond`.
    /// - time objects: `"Time32(unit)"`, `"Time64(unit)"` with unit being one
    ///   of `Second`, `Millisecond`, `Microsecond`, `Nanosecond`.
    /// - durations: `"Duration(unit)"` with unit being one of `Second`,
    ///   `Millisecond`, `Microsecond`, `Nanosecond`.
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
    fn from_value<T: Serialize + ?Sized>(value: &T) -> Result<Self>;

    /// Determine the schema from the given record type. See [`TracingOptions`]
    /// for customization options.
    ///
    /// This approach requires the type `T` to implement
    /// [`Deserialize`][serde::Deserialize]. As only type information is used,
    /// it is not possible to detect data dependent properties. Examples of
    /// unsupported features:
    ///
    /// - auto detection of date time strings
    /// - non self-describing types such as `serde_json::Value`
    /// - flattened structure (`#[serde(flatten)]`)
    ///
    /// Consider using [`from_samples`][SchemaLike::from_samples] in these
    /// cases.
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
    /// frame. When encoding single arrays, consider using the
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
    fn from_type<'de, T: Deserialize<'de> + ?Sized>(options: TracingOptions) -> Result<Self>;

    /// Determine the schema from samples. See [`TracingOptions`] for
    /// customization options.
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
    ///   true`][TracingOptions::map_as_struct]).
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
    /// encoding single arrays, consider using the
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
    fn from_samples<T: Serialize + ?Sized>(samples: &T, options: TracingOptions) -> Result<Self>;
}

/// A collection of fields as understood by `serde_arrow`
#[derive(Default, Debug, PartialEq, Clone, Serialize)]
pub struct SerdeArrowSchema {
    pub(crate) fields: Vec<GenericField>,
}

impl SerdeArrowSchema {
    /// Return a new schema without any fields
    pub fn new() -> Self {
        Self::default()
    }
}

impl Sealed for SerdeArrowSchema {}

impl SchemaLike for SerdeArrowSchema {
    fn from_value<T: Serialize + ?Sized>(value: &T) -> Result<Self> {
        // simple version of serde-transcode
        let mut events = Vec::<crate::internal::event::Event>::new();
        crate::internal::sink::serialize_into_sink(&mut events, value)?;
        let this: Self = crate::internal::source::deserialize_from_source(&events)?;
        Ok(this)
    }

    fn from_type<'de, T: Deserialize<'de> + ?Sized>(options: TracingOptions) -> Result<Self> {
        let options = options.tracing_mode(TracingMode::FromType);

        let mut tracer = Tracer::new(String::from("$"), options);
        tracer.trace_type::<T>()?;
        tracer.to_schema()
    }

    fn from_samples<T: Serialize + ?Sized>(samples: &T, options: TracingOptions) -> Result<Self> {
        let options = options.tracing_mode(TracingMode::FromSamples);

        let mut tracer = Tracer::new(String::from("$"), options);
        tracer.trace_samples(samples)?;
        tracer.to_schema()
    }
}

#[derive(Serialize, Debug, Clone, PartialEq)]
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

impl<'de> Deserialize<'de> for GenericField {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::Error;

        #[derive(Deserialize)]
        struct Helper {
            pub name: String,
            pub data_type: GenericDataType,

            #[serde(default)]
            pub strategy: Option<Strategy>,

            #[serde(default)]
            pub nullable: bool,

            #[serde(default)]
            pub children: Vec<GenericField>,
        }

        let Helper {
            name,
            data_type,
            strategy,
            nullable,
            children,
        } = Helper::deserialize(deserializer)?;

        let result = GenericField {
            name,
            data_type,
            strategy,
            nullable,
            children,
        };
        result.validate().map_err(D::Error::custom)?;
        Ok(result)
    }
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
            GenericDataType::Date32 => self.validate_date32(),
            GenericDataType::Date64 => self.validate_date64(),
            GenericDataType::Struct => self.validate_struct(),
            GenericDataType::Map => self.validate_map(),
            GenericDataType::List => self.validate_list(),
            GenericDataType::LargeList => self.validate_list(),
            GenericDataType::Union => self.validate_union(),
            GenericDataType::Dictionary => self.validate_dictionary(),
            GenericDataType::Timestamp(_, _) => self.validate_timestamp(),
            GenericDataType::Time32(_) => self.validate_time32(),
            GenericDataType::Time64(_) => self.validate_time64(),
            GenericDataType::Duration(_) => self.validate_duration(),
            GenericDataType::Decimal128(_, _) => self.validate_primitive(),
        }
    }

    pub fn is_utc(&self) -> Result<bool> {
        match &self.data_type {
            GenericDataType::Date64 => match &self.strategy {
                None | Some(Strategy::UtcStrAsDate64) => Ok(true),
                Some(Strategy::NaiveStrAsDate64) => Ok(false),
                Some(strategy) => fail!("invalid strategy for date64 deserializer: {strategy}"),
            },
            GenericDataType::Timestamp(_, tz) => match tz {
                Some(tz) => Ok(tz.to_lowercase() == "utc"),
                None => Ok(false),
            },
            _ => fail!("non date time type {}", self.data_type),
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

    pub(crate) fn validate_date32(&self) -> Result<()> {
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
                if !matches!(&self.data_type, GenericDataType::Timestamp(_, Some(tz)) if tz.to_uppercase() == "UTC")
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
                if !matches!(&self.data_type, GenericDataType::Timestamp(_, None)) {
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

    pub(crate) fn validate_time32(&self) -> Result<()> {
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
        if !matches!(
            self.data_type,
            GenericDataType::Time32(GenericTimeUnit::Second | GenericTimeUnit::Millisecond)
        ) {
            fail!("Time32 field must have Second or Millisecond unit");
        }
        Ok(())
    }

    pub(crate) fn validate_time64(&self) -> Result<()> {
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
        if !matches!(
            self.data_type,
            GenericDataType::Time64(GenericTimeUnit::Microsecond | GenericTimeUnit::Nanosecond)
        ) {
            fail!("Time64 field must have Microsecond or Nanosecond unit");
        }
        Ok(())
    }

    pub(crate) fn validate_duration(&self) -> Result<()> {
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
