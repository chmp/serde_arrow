use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    iter,
    str::FromStr,
};

use crate::internal::{
    error::{fail, Error, Result},
    event::Event,
    sink::EventSink,
};

use super::sink::macros;

use serde::{Deserialize, Serialize};

/// The metadata key under which to store the strategy
///
/// See the [module][crate::schema] for details.
///
pub const STRATEGY_KEY: &str = "SERDE_ARROW:strategy";

/// A collection of fields that can be easily serialized and deserialized
#[derive(Default, Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct Schema {
    pub(crate) fields: Vec<GenericField>,
}

impl Schema {
    pub fn new() -> Self {
        Self::default()
    }

    #[allow(unused)]
    fn with_field(mut self, field: GenericField) -> Self {
        self.fields.push(field);
        self
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
    UtcStrAsDate64,
    /// Serialize Rust strings containing datetimes without timezone as Arrow
    /// Date64
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
                    write!(f, "Timestamp({unit}, Some({timezone:?}))")
                } else {
                    write!(f, "Timestamp({unit}, None)")
                }
            }
        }
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
                if !matches!(&self.data_type, GenericDataType::Timestamp(GenericTimeUnit::Second, Some(tz)) if tz == "UTC")
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
                    GenericDataType::Timestamp(GenericTimeUnit::Second, None)
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
#[derive(Debug, Clone)]
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
}

impl Default for TracingOptions {
    fn default() -> Self {
        Self {
            allow_null_fields: false,
            map_as_struct: true,
            string_dictionary_encoding: false,
            coerce_numbers: false,
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
}

pub enum Tracer {
    Unknown(UnknownTracer),
    Struct(StructTracer),
    List(ListTracer),
    Primitive(PrimitiveTracer),
    Tuple(TupleTracer),
    Union(UnionTracer),
    Map(MapTracer),
}

impl Tracer {
    pub fn new(path: String, options: TracingOptions) -> Self {
        Self::Unknown(UnknownTracer::new(path, options))
    }

    pub fn to_field(&self, name: &str) -> Result<GenericField> {
        use Tracer::*;
        match self {
            Unknown(t) => t.to_field(name),
            List(t) => t.to_field(name),
            Map(t) => t.to_field(name),
            Primitive(t) => t.to_field(name),
            Tuple(t) => t.to_field(name),
            Union(t) => t.to_field(name),
            Struct(t) => t.to_field(name),
        }
    }

    pub fn mark_nullable(&mut self) {
        use Tracer::*;
        match self {
            Unknown(_) => {}
            List(t) => {
                t.nullable = true;
            }
            Map(t) => {
                t.nullable = true;
            }
            Primitive(t) => {
                t.nullable = true;
            }
            Tuple(t) => {
                t.nullable = true;
            }
            Union(t) => {
                t.nullable = true;
            }
            Struct(t) => {
                t.nullable = true;
            }
        }
    }
}

impl EventSink for Tracer {
    macros::forward_specialized_to_generic!();

    fn accept(&mut self, event: Event<'_>) -> Result<()> {
        match self {
            // NOTE: unknown tracer is the only tracer that change the internal type
            Self::Unknown(tracer) => match event {
                Event::Some | Event::Null => tracer.nullable = true,
                Event::Bool(_)
                | Event::I8(_)
                | Event::I16(_)
                | Event::I32(_)
                | Event::I64(_)
                | Event::U8(_)
                | Event::U16(_)
                | Event::U32(_)
                | Event::U64(_)
                | Event::F32(_)
                | Event::F64(_)
                | Event::Str(_)
                | Event::OwnedStr(_) => {
                    let mut tracer = PrimitiveTracer::new(
                        tracer.nullable,
                        tracer.options.string_dictionary_encoding,
                        tracer.options.allow_null_fields,
                        tracer.options.coerce_numbers,
                    );
                    tracer.accept(event)?;
                    *self = Tracer::Primitive(tracer)
                }
                Event::StartSequence => {
                    let mut tracer = ListTracer::new(
                        tracer.path.clone(),
                        tracer.options.clone(),
                        tracer.nullable,
                    );
                    tracer.accept(event)?;
                    *self = Tracer::List(tracer);
                }
                Event::StartStruct => {
                    let mut tracer = StructTracer::new(
                        tracer.path.clone(),
                        tracer.options.clone(),
                        StructMode::Struct,
                        tracer.nullable,
                    );
                    tracer.accept(event)?;
                    *self = Tracer::Struct(tracer);
                }
                Event::StartTuple => {
                    let mut tracer = TupleTracer::new(
                        tracer.path.clone(),
                        tracer.options.clone(),
                        tracer.nullable,
                    );
                    tracer.accept(event)?;
                    *self = Tracer::Tuple(tracer);
                }
                Event::StartMap => {
                    if tracer.options.map_as_struct {
                        let mut tracer = StructTracer::new(
                            tracer.path.clone(),
                            tracer.options.clone(),
                            StructMode::Map,
                            tracer.nullable,
                        );
                        tracer.accept(event)?;
                        *self = Tracer::Struct(tracer);
                    } else {
                        let mut tracer = MapTracer::new(
                            tracer.path.clone(),
                            tracer.options.clone(),
                            tracer.nullable,
                        );
                        tracer.accept(event)?;
                        *self = Tracer::Map(tracer);
                    }
                }
                Event::Variant(_, _) => {
                    let mut tracer = UnionTracer::new(
                        tracer.path.clone(),
                        tracer.options.clone(),
                        tracer.nullable,
                    );
                    tracer.accept(event)?;
                    *self = Tracer::Union(tracer)
                }
                ev if ev.is_end() => fail!(
                    "Invalid end nesting events for unknown tracer ({path})",
                    path = tracer.path
                ),
                ev => fail!(
                    "Internal error unmatched event {ev} in Tracer ({path})",
                    path = tracer.path
                ),
            },
            Self::List(tracer) => tracer.accept(event)?,
            Self::Struct(tracer) => tracer.accept(event)?,
            Self::Primitive(tracer) => tracer.accept(event)?,
            Self::Tuple(tracer) => tracer.accept(event)?,
            Self::Union(tracer) => tracer.accept(event)?,
            Self::Map(tracer) => tracer.accept(event)?,
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        match self {
            Self::Unknown(tracer) => tracer.finish(),
            Self::List(tracer) => tracer.finish(),
            Self::Struct(tracer) => tracer.finish(),
            Self::Primitive(tracer) => tracer.finish(),
            Self::Tuple(tracer) => tracer.finish(),
            Self::Union(tracer) => tracer.finish(),
            Self::Map(tracer) => tracer.finish(),
        }
    }
}

pub struct UnknownTracer {
    pub nullable: bool,
    pub finished: bool,
    pub path: String,
    pub options: TracingOptions,
}

impl UnknownTracer {
    pub fn new(path: String, options: TracingOptions) -> Self {
        Self {
            nullable: false,
            finished: false,
            path,
            options,
        }
    }

    pub fn to_field(&self, name: &str) -> Result<GenericField> {
        if !self.finished {
            fail!("Cannot build field {name} from unfinished tracer");
        }
        if !self.options.allow_null_fields {
            fail!(concat!(
                "Encountered null only field. This error can be disabled by ",
                "setting `allow_null_fields` to `true` in `TracingOptions`",
            ));
        }

        Ok(GenericField::new(
            name,
            GenericDataType::Null,
            self.nullable,
        ))
    }

    pub fn finish(&mut self) -> Result<()> {
        self.finished = true;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum StructMode {
    Struct,
    Map,
}

pub struct StructTracer {
    pub mode: StructMode,
    pub field_tracers: Vec<Tracer>,
    pub nullable: bool,
    pub field_names: Vec<String>,
    pub index: HashMap<String, usize>,
    pub next: StructTracerState,
    pub item_index: usize,
    pub seen_this_item: BTreeSet<usize>,
    pub seen_previous_items: BTreeSet<usize>,
    pub finished: bool,
    pub path: String,
    pub options: TracingOptions,
}

#[derive(Debug, Clone, Copy)]
pub enum StructTracerState {
    Start,
    Key,
    Value(usize, usize),
}

impl StructTracer {
    pub fn new(path: String, options: TracingOptions, mode: StructMode, nullable: bool) -> Self {
        Self {
            path,
            options,
            mode,
            field_tracers: Vec::new(),
            field_names: Vec::new(),
            index: HashMap::new(),
            nullable,
            next: StructTracerState::Start,
            item_index: 0,
            seen_this_item: BTreeSet::new(),
            seen_previous_items: BTreeSet::new(),
            finished: false,
        }
    }

    pub fn to_field(&self, name: &str) -> Result<GenericField> {
        if !self.finished {
            fail!("Cannot build field {name} from unfinished tracer");
        }
        let mut field = GenericField::new(name, GenericDataType::Struct, self.nullable);
        for (tracer, name) in iter::zip(&self.field_tracers, &self.field_names) {
            field.children.push(tracer.to_field(name)?);
        }

        if let StructMode::Map = self.mode {
            field.children.sort_by(|a, b| a.name.cmp(&b.name));
            field.strategy = Some(Strategy::MapAsStruct);
        }
        Ok(field)
    }

    pub fn mark_seen(&mut self, field: usize) {
        self.seen_this_item.insert(field);
    }
}

impl EventSink for StructTracer {
    macros::forward_specialized_to_generic!();

    fn accept(&mut self, event: Event<'_>) -> Result<()> {
        use StructTracerState::*;
        type E<'a> = Event<'a>;

        self.next = match (self.next, event) {
            (Start, E::StartStruct | E::StartMap) => Key,
            (Start, E::Null | E::Some) => {
                self.nullable = true;
                Start
            }
            (Start, ev) => fail!("Invalid event {ev} for struct tracer in state Start"),
            (Key, E::Item) => Key,
            (Key, E::Str(key)) => {
                if let Some(&field) = self.index.get(key) {
                    self.mark_seen(field);
                    Value(field, 0)
                } else {
                    let field = self.field_tracers.len();
                    self.field_tracers.push(Tracer::new(
                        format!("{path}.{key}", path = self.path),
                        self.options.clone(),
                    ));
                    self.field_names.push(key.to_owned());
                    self.index.insert(key.to_owned(), field);
                    self.mark_seen(field);
                    Value(field, 0)
                }
            }
            (Key, E::EndStruct | E::EndMap) => {
                if self.item_index == 0 {
                    self.seen_previous_items = self.seen_this_item.clone();
                }

                for (field, tracer) in self.field_tracers.iter_mut().enumerate() {
                    if !self.seen_this_item.contains(&field)
                        || !self.seen_previous_items.contains(&field)
                    {
                        tracer.mark_nullable();
                    }
                }
                for seen in &self.seen_this_item {
                    self.seen_previous_items.insert(*seen);
                }
                self.seen_this_item.clear();
                self.item_index += 1;

                Start
            }
            (Key, ev) => fail!("Invalid event {ev} for struct tracer in state Key"),
            (Value(field, depth), ev) if ev.is_start() => {
                self.field_tracers[field].accept(ev)?;
                Value(field, depth + 1)
            }
            (Value(field, depth), ev) if ev.is_end() => {
                self.field_tracers[field].accept(ev)?;
                match depth {
                    0 => fail!("Invalid closing event in struct tracer in state Value"),
                    1 => Key,
                    depth => Value(field, depth - 1),
                }
            }
            (Value(field, depth), ev) if ev.is_marker() => {
                self.field_tracers[field].accept(ev)?;
                // markers are always followed by the actual  value
                Value(field, depth)
            }
            (Value(field, depth), ev) => {
                self.field_tracers[field].accept(ev)?;
                match depth {
                    // Any event at depth == 0 that does not start a structure (is a complete value)
                    0 => Key,
                    _ => Value(field, depth),
                }
            }
        };
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        if !matches!(self.next, StructTracerState::Start) {
            fail!("Incomplete struct in schema tracing");
        }

        for tracer in &mut self.field_tracers {
            tracer.finish()?;
        }

        self.finished = true;

        Ok(())
    }
}

pub struct TupleTracer {
    pub field_tracers: Vec<Tracer>,
    pub nullable: bool,
    pub next: TupleTracerState,
    pub finished: bool,
    pub path: String,
    pub options: TracingOptions,
}

impl TupleTracer {
    pub fn new(path: String, options: TracingOptions, nullable: bool) -> Self {
        Self {
            path,
            options,
            field_tracers: Vec::new(),
            nullable,
            next: TupleTracerState::WaitForStart,
            finished: false,
        }
    }

    pub fn to_field(&self, name: &str) -> Result<GenericField> {
        if !self.finished {
            fail!("Cannot build field {name} from unfinished tracer");
        }

        let mut field = GenericField::new(name, GenericDataType::Struct, self.nullable);
        for (idx, tracer) in self.field_tracers.iter().enumerate() {
            field.children.push(tracer.to_field(&idx.to_string())?);
        }
        field.strategy = Some(Strategy::TupleAsStruct);

        Ok(field)
    }

    fn field_tracer(&mut self, idx: usize) -> &mut Tracer {
        while self.field_tracers.len() <= idx {
            self.field_tracers.push(Tracer::new(
                format!("{path}.{idx}", path = self.path),
                self.options.clone(),
            ));
        }
        &mut self.field_tracers[idx]
    }
}

#[derive(Debug, Clone, Copy)]
pub enum TupleTracerState {
    WaitForStart,
    WaitForItem(usize),
    Item(usize, usize),
}

impl EventSink for TupleTracer {
    macros::forward_specialized_to_generic!();

    fn accept(&mut self, event: Event<'_>) -> Result<()> {
        use TupleTracerState::*;
        type E<'a> = Event<'a>;

        self.next = match (self.next, event) {
            (WaitForStart, Event::StartTuple) => WaitForItem(0),
            (WaitForStart, E::Null | E::Some) => {
                self.nullable = true;
                WaitForStart
            }
            (WaitForStart, ev) => fail!(
                "Invalid event {ev} for TupleTracer in state Start [{path}]",
                path = self.path
            ),
            (WaitForItem(field), Event::Item) => Item(field, 0),
            (WaitForItem(_), E::EndTuple) => WaitForStart,
            (WaitForItem(field), ev) => fail!(
                "Invalid event {ev} for TupleTracer in state WaitForItem({field}) [{path}]",
                path = self.path
            ),
            (Item(field, depth), ev) if ev.is_start() => {
                self.field_tracer(field).accept(ev)?;
                Item(field, depth + 1)
            }
            (Item(field, depth), ev) if ev.is_end() => {
                self.field_tracer(field).accept(ev)?;
                match depth {
                    0 => fail!(
                        "Invalid closing event in TupleTracer in state Value [{path}]",
                        path = self.path
                    ),
                    1 => WaitForItem(field + 1),
                    depth => Item(field, depth - 1),
                }
            }
            (Item(field, depth), ev) if ev.is_marker() => {
                self.field_tracer(field).accept(ev)?;
                // markers are always followed by the actual  value
                Item(field, depth)
            }
            (Item(field, depth), ev) => {
                self.field_tracer(field).accept(ev)?;
                match depth {
                    // Any event at depth == 0 that does not start a structure (is a complete value)
                    0 => WaitForItem(field + 1),
                    _ => Item(field, depth),
                }
            }
        };
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        if !matches!(self.next, TupleTracerState::WaitForStart) {
            fail!("Incomplete tuple in schema tracing");
        }
        for tracer in &mut self.field_tracers {
            tracer.finish()?;
        }
        self.finished = true;
        Ok(())
    }
}

pub struct ListTracer {
    pub item_tracer: Box<Tracer>,
    pub nullable: bool,
    pub next: ListTracerState,
    pub finished: bool,
    pub path: String,
}

#[derive(Debug, Clone, Copy)]
pub enum ListTracerState {
    WaitForStart,
    WaitForItem,
    Item(usize),
}

impl ListTracer {
    pub fn new(path: String, options: TracingOptions, nullable: bool) -> Self {
        Self {
            path: path.clone(),
            item_tracer: Box::new(Tracer::new(path, options)),
            nullable,
            next: ListTracerState::WaitForStart,
            finished: false,
        }
    }

    fn to_field(&self, name: &str) -> Result<GenericField> {
        if !self.finished {
            fail!("Cannot build field {name} from unfinished tracer");
        }

        let mut field = GenericField::new(name, GenericDataType::LargeList, self.nullable);
        field.children.push(self.item_tracer.to_field("element")?);

        Ok(field)
    }
}

impl EventSink for ListTracer {
    macros::forward_specialized_to_generic!();

    fn accept(&mut self, event: Event<'_>) -> Result<()> {
        use {Event as E, ListTracerState as S};

        self.next = match (self.next, event) {
            (S::WaitForStart, E::Null | E::Some) => {
                self.nullable = true;
                S::WaitForStart
            }
            (S::WaitForStart, E::StartSequence) => S::WaitForItem,
            (S::WaitForItem, E::EndSequence) => S::WaitForStart,
            (S::WaitForItem, E::Item) => S::Item(0),
            (S::Item(depth), ev) if ev.is_start() => {
                self.item_tracer.accept(ev)?;
                S::Item(depth + 1)
            }
            (S::Item(depth), ev) if ev.is_end() => match depth {
                0 => fail!(
                    "Invalid event {ev} for list tracer ({path}) in state Item(0)",
                    path = self.path
                ),
                1 => {
                    self.item_tracer.accept(ev)?;
                    S::WaitForItem
                }
                depth => {
                    self.item_tracer.accept(ev)?;
                    S::Item(depth - 1)
                }
            },
            (S::Item(0), ev) if ev.is_value() => {
                self.item_tracer.accept(ev)?;
                S::WaitForItem
            }
            (S::Item(depth), ev) => {
                self.item_tracer.accept(ev)?;
                S::Item(depth)
            }
            (state, ev) => fail!(
                "Invalid event {ev} for list tracer ({path}) in state {state:?}",
                path = self.path
            ),
        };
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        if !matches!(self.next, ListTracerState::WaitForStart) {
            fail!("Incomplete list in schema tracing");
        }
        self.item_tracer.finish()?;
        self.finished = true;
        Ok(())
    }
}

pub struct UnionTracer {
    pub variants: Vec<Option<String>>,
    pub tracers: BTreeMap<usize, Tracer>,
    pub nullable: bool,
    pub next: UnionTracerState,
    pub finished: bool,
    pub path: String,
    pub options: TracingOptions,
}

impl UnionTracer {
    pub fn new(path: String, options: TracingOptions, nullable: bool) -> Self {
        Self {
            path,
            options,
            variants: Vec::new(),
            tracers: BTreeMap::new(),
            nullable,
            next: UnionTracerState::Inactive,
            finished: false,
        }
    }

    pub fn to_field(&self, name: &str) -> Result<GenericField> {
        if !self.finished {
            fail!("Cannot build field {name} from unfinished tracer");
        }

        let mut field = GenericField::new(name, GenericDataType::Union, self.nullable);
        for (idx, variant_name) in self.variants.iter().enumerate() {
            if let Some(variant_name) = variant_name {
                let Some(tracer) = self.tracers.get(&idx) else {
                    panic!(concat!(
                        "invalid state: tracer for variant {idx} with name {variant_name:?} not initialized. ",
                        "This should not happen, please open an issue at https://github.com/chmp/serde_arrow",
                    ), idx=idx, variant_name=variant_name);
                };

                field.children.push(tracer.to_field(variant_name)?);
            } else {
                field.children.push(
                    GenericField::new("", GenericDataType::Null, true)
                        .with_strategy(Strategy::UnknownVariant),
                );
            }
        }

        Ok(field)
    }

    fn ensure_variant<S: Into<String> + AsRef<str>>(
        &mut self,
        variant: S,
        idx: usize,
    ) -> Result<()> {
        while self.variants.len() <= idx {
            self.variants.push(None);
        }

        self.tracers.entry(idx).or_insert_with(|| {
            Tracer::new(
                format!("{path}.{key}", path = self.path, key = variant.as_ref()),
                self.options.clone(),
            )
        });

        if let Some(prev) = self.variants[idx].as_ref() {
            let variant = variant.as_ref();
            if prev != variant {
                fail!("Incompatible names for variant {idx}: {prev}, {variant}");
            }
        } else {
            self.variants[idx] = Some(variant.into());
        }
        Ok(())
    }
}

impl EventSink for UnionTracer {
    macros::forward_specialized_to_generic!();

    fn accept(&mut self, event: Event<'_>) -> Result<()> {
        type S = UnionTracerState;
        type E<'a> = Event<'a>;

        self.next = match self.next {
            S::Inactive => match event {
                E::Variant(variant, idx) => {
                    self.ensure_variant(variant, idx)?;
                    S::Active(idx, 0)
                }
                E::Some => fail!("Nullable unions are not supported"),
                E::OwnedVariant(variant, idx) => {
                    self.ensure_variant(variant, idx)?;
                    S::Active(idx, 0)
                }
                ev => fail!("Invalid event {ev} for UnionTracer in State Inactive"),
            },
            S::Active(idx, depth) => match event {
                ev if ev.is_start() => {
                    self.tracers.get_mut(&idx).unwrap().accept(ev)?;
                    S::Active(idx, depth + 1)
                }
                ev if ev.is_end() => match depth {
                    0 => fail!("Invalid end event {ev} at depth 0 in UnionTracer"),
                    1 => {
                        self.tracers.get_mut(&idx).unwrap().accept(ev)?;
                        S::Inactive
                    }
                    _ => {
                        self.tracers.get_mut(&idx).unwrap().accept(ev)?;
                        S::Active(idx, depth - 1)
                    }
                },
                ev if ev.is_marker() => {
                    self.tracers.get_mut(&idx).unwrap().accept(ev)?;
                    S::Active(idx, depth)
                }
                ev if ev.is_value() => {
                    self.tracers.get_mut(&idx).unwrap().accept(ev)?;
                    match depth {
                        0 => S::Inactive,
                        _ => S::Active(idx, depth),
                    }
                }
                _ => unreachable!(),
            },
        };
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        for tracer in self.tracers.values_mut() {
            tracer.finish()?;
        }
        self.finished = true;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub enum UnionTracerState {
    Inactive,
    Active(usize, usize),
}

pub struct MapTracer {
    pub path: String,
    pub key: Box<Tracer>,
    pub value: Box<Tracer>,
    pub nullable: bool,
    pub finished: bool,
    next: MapTracerState,
}

impl MapTracer {
    pub fn new(path: String, options: TracingOptions, nullable: bool) -> Self {
        Self {
            nullable,
            key: Box::new(Tracer::new(format!("{path}.$key"), options.clone())),
            value: Box::new(Tracer::new(format!("{path}.$value"), options)),
            next: MapTracerState::Start,
            path,
            finished: true,
        }
    }

    pub fn to_field(&self, name: &str) -> Result<GenericField> {
        if !self.finished {
            fail!("Cannot build field {name} from unfinished tracer");
        }

        let mut entries = GenericField::new("entries", GenericDataType::Struct, false);
        entries.children.push(self.key.to_field("key")?);
        entries.children.push(self.value.to_field("value")?);

        let mut field = GenericField::new(name, GenericDataType::Map, self.nullable);
        field.children.push(entries);

        Ok(field)
    }
}

impl EventSink for MapTracer {
    macros::forward_specialized_to_generic!();

    fn accept(&mut self, event: Event<'_>) -> Result<()> {
        type S = MapTracerState;
        type E<'a> = Event<'a>;

        self.next = match self.next {
            S::Start => match event {
                Event::StartMap => S::Key(0),
                Event::Null | Event::Some => {
                    self.nullable = true;
                    S::Start
                }
                ev => fail!("Unexpected event {ev} in state Start of MapTracer"),
            },
            S::Key(depth) => match event {
                Event::Item if depth == 0 => S::Key(depth),
                ev if ev.is_end() => match depth {
                    0 => {
                        if !matches!(ev, E::EndMap) {
                            fail!("Unexpected event {ev} in State Key at depth 0 in MapTracer")
                        }
                        S::Start
                    }
                    1 => {
                        self.key.accept(ev)?;
                        S::Value(0)
                    }
                    _ => {
                        self.key.accept(ev)?;
                        S::Key(depth - 1)
                    }
                },
                ev if ev.is_start() => {
                    self.key.accept(ev)?;
                    S::Key(depth + 1)
                }
                ev if ev.is_marker() => {
                    self.key.accept(ev)?;
                    S::Key(depth)
                }
                ev if ev.is_value() => {
                    self.key.accept(ev)?;
                    if depth == 0 {
                        S::Value(0)
                    } else {
                        S::Key(depth)
                    }
                }
                _ => unreachable!(),
            },
            S::Value(depth) => match event {
                ev if ev.is_end() => match depth {
                    0 => fail!("Unexpected event {ev} in State Value at depth 0 in MapTracer"),
                    1 => {
                        self.value.accept(ev)?;
                        S::Key(0)
                    }
                    _ => {
                        self.value.accept(ev)?;
                        S::Value(depth - 1)
                    }
                },
                ev if ev.is_start() => {
                    self.value.accept(ev)?;
                    S::Value(depth + 1)
                }
                ev if ev.is_marker() => {
                    self.value.accept(ev)?;
                    S::Value(depth)
                }
                ev if ev.is_value() => {
                    self.value.accept(ev)?;
                    if depth == 0 {
                        S::Key(0)
                    } else {
                        S::Value(depth)
                    }
                }
                _ => unreachable!(),
            },
        };
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        self.key.finish()?;
        self.value.finish()?;
        self.finished = true;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub enum MapTracerState {
    Start,
    Key(usize),
    Value(usize),
}

pub struct PrimitiveTracer {
    pub string_dictionary_encoding: bool,
    pub allow_null_fields: bool,
    pub coerce_numbers: bool,
    pub item_type: GenericDataType,
    pub nullable: bool,
    pub finished: bool,
}

impl PrimitiveTracer {
    pub fn new(
        nullable: bool,
        string_dictionary_encoding: bool,
        allow_null_fields: bool,
        coerce_numbers: bool,
    ) -> Self {
        Self {
            item_type: GenericDataType::Null,
            allow_null_fields,
            coerce_numbers,
            nullable,
            string_dictionary_encoding,
            finished: false,
        }
    }

    pub fn to_field(&self, name: &str) -> Result<GenericField> {
        type D = GenericDataType;

        if !self.finished {
            fail!("Cannot build field {name} from unfinished tracer");
        }

        if !self.allow_null_fields && matches!(self.item_type, D::Null) {
            fail!(concat!(
                "Encountered null only field. This error can be disabled by ",
                "setting `allow_null_fields` to `true` in `TracingOptions`",
            ));
        }

        match &self.item_type {
            dt @ (D::LargeUtf8 | D::Utf8) => {
                if !self.string_dictionary_encoding {
                    Ok(GenericField::new(name, dt.clone(), self.nullable))
                } else {
                    let field = GenericField::new(name, D::Dictionary, self.nullable)
                        .with_child(GenericField::new("key", D::U32, false))
                        .with_child(GenericField::new("value", dt.clone(), false));
                    Ok(field)
                }
            }
            dt => Ok(GenericField::new(name, dt.clone(), self.nullable)),
        }
    }
}

impl EventSink for PrimitiveTracer {
    macros::forward_specialized_to_generic!();

    fn accept(&mut self, event: Event<'_>) -> Result<()> {
        use GenericDataType::*;

        let ev_type = match event {
            Event::Some | Event::Null => Null,
            Event::Bool(_) => Bool,
            Event::Str(_) | Event::OwnedStr(_) => LargeUtf8,
            Event::U8(_) => U8,
            Event::U16(_) => U16,
            Event::U32(_) => U32,
            Event::U64(_) => U64,
            Event::I8(_) => I8,
            Event::I16(_) => I16,
            Event::I32(_) => I32,
            Event::I64(_) => I64,
            Event::F32(_) => F32,
            Event::F64(_) => F64,
            ev => fail!("Cannot handle event {ev} in primitive tracer"),
        };

        self.item_type = match (&self.item_type, ev_type) {
            (ty, Null) => {
                self.nullable = true;
                ty.clone()
            }
            (Bool | Null, Bool) => Bool,
            (I8 | Null, I8) => I8,
            (I16 | Null, I16) => I16,
            (I32 | Null, I32) => I32,
            (I64 | Null, I64) => I64,
            (U8 | Null, U8) => U8,
            (U16 | Null, U16) => U16,
            (U32 | Null, U32) => U32,
            (U64 | Null, U64) => U64,
            (F32 | Null, F32) => F32,
            (F64 | Null, F64) => F64,
            (LargeUtf8 | Null, LargeUtf8) => LargeUtf8,
            (ty, ev) if self.coerce_numbers => match (ty, ev) {
                // unsigned x unsigned -> u64
                (U8 | U16 | U32 | U64, U8 | U16 | U32 | U64) => U64,
                // signed x signed -> i64
                (I8 | I16 | I32 | I64, I8 | I16 | I32 | I64) => I64,
                // signed x unsigned -> i64
                (I8 | I16 | I32 | I64, U8 | U16 | U32 | U64) => I64,
                // unsigned x signed -> i64
                (U8 | U16 | U32 | U64, I8 | I16 | I32 | I64) => I64,
                // float x float -> f64
                (F32 | F64, F32 | F64) => F64,
                // int x float -> f64
                (I8 | I16 | I32 | I64 | U8 | U16 | U32 | U64, F32 | F64) => F64,
                // float x int -> f64
                (F32 | F64, I8 | I16 | I32 | I64 | U8 | U16 | U32 | U64) => F64,
                (ty, ev) => fail!("Cannot accept event {ev} for tracer of primitive type {ty}"),
            },
            (ty, ev) => fail!("Cannot accept event {ev} for tracer of primitive type {ty}"),
        };
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        self.finished = true;
        Ok(())
    }
}

#[cfg(test)]
mod test_schema_serialization {
    use crate::internal::schema::GenericDataType;

    use super::{GenericField, Schema};

    #[test]
    fn example() {
        let schema = Schema::new()
            .with_field(GenericField::new("foo", GenericDataType::U8, false))
            .with_field(GenericField::new("bar", GenericDataType::Utf8, false));

        let actual = serde_json::to_string(&schema).unwrap();
        assert_eq!(
            actual,
            r#"{"fields":[{"name":"foo","data_type":"U8"},{"name":"bar","data_type":"Utf8"}]}"#
        );

        let round_tripped: Schema = serde_json::from_str(&actual).unwrap();
        assert_eq!(round_tripped, schema);
    }

    #[test]
    fn list() {
        let schema =
            Schema::new().with_field(
                GenericField::new("value", GenericDataType::List, false)
                    .with_child(GenericField::new("element", GenericDataType::I32, false)),
            );

        let actual = serde_json::to_string(&schema).unwrap();
        assert_eq!(
            actual,
            r#"{"fields":[{"name":"value","data_type":"List","children":[{"name":"element","data_type":"I32"}]}]}"#
        );

        let round_tripped: Schema = serde_json::from_str(&actual).unwrap();
        assert_eq!(round_tripped, schema);
    }
}
