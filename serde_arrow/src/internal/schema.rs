use std::{
    collections::{BTreeMap, HashMap},
    iter,
    str::FromStr,
};

use crate::{
    internal::{error::fail, event::Event, sink::EventSink},
    Error, Result,
};

use super::sink::macros;

/// The metadata key under which to store the strategy
///
/// See the [module][crate::schema] for details.
///
pub const STRATEGY_KEY: &str = "SERDE_ARROW:strategy";

/// Strategies for handling types without direct match between arrow and serde
///
/// For the correct strategy both the field type and the field metadata must be
/// correctly configured. In particular, when determining the schema from the
/// Rust objects themselves, some field types are incorrectly recognized (e.g.,
/// datetimes).
///
#[derive(Debug, Clone, PartialEq)]
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
}

impl std::fmt::Display for Strategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InconsistentTypes => write!(f, "InconsistentTypes"),
            Self::UtcStrAsDate64 => write!(f, "UtcStrAsDate64"),
            Self::NaiveStrAsDate64 => write!(f, "NaiveStrAsDate64"),
            Self::TupleAsStruct => write!(f, "TupleAsStruct"),
            Self::MapAsStruct => write!(f, "MapAsStruct"),
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

#[derive(Debug, Clone, Copy, PartialEq)]
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
            Date64 => write!(f, "F64"),
            Struct => write!(f, "Struct"),
            List => write!(f, "List"),
            LargeList => write!(f, "LargeList"),
            Union => write!(f, "Union"),
            Map => write!(f, "Map"),
            Dictionary => write!(f, "Dictionary"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct GenericField {
    pub data_type: GenericDataType,
    pub name: String,
    pub strategy: Option<Strategy>,
    pub nullable: bool,
    pub children: Vec<GenericField>,
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

    pub fn with_child(mut self, child: GenericField) -> Self {
        self.children.push(child);
        self
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
    /// If `true` serialize maps as structs (the default). See
    /// [Strategy::MapAsStruct] for details.
    pub map_as_struct: bool,

    /// If `true` serialize strings dictionary encoded. The default is `false`.
    pub string_dictionary_encoding: bool,
}

impl TracingOptions {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn map_as_struct(mut self, value: bool) -> Self {
        self.map_as_struct = value;
        self
    }

    pub fn string_dictionary_encoding(mut self, value: bool) -> Self {
        self.string_dictionary_encoding = value;
        self
    }
}

impl Default for TracingOptions {
    fn default() -> Self {
        Self {
            map_as_struct: true,
            string_dictionary_encoding: false,
        }
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
    pub fn new(options: TracingOptions) -> Self {
        Self::Unknown(UnknownTracer::new(options))
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
                    );
                    tracer.accept(event)?;
                    *self = Tracer::Primitive(tracer)
                }
                Event::StartSequence => {
                    let mut tracer = ListTracer::new(tracer.options.clone(), tracer.nullable);
                    tracer.accept(event)?;
                    *self = Tracer::List(tracer);
                }
                Event::StartStruct => {
                    let mut tracer = StructTracer::new(
                        tracer.options.clone(),
                        StructMode::Struct,
                        tracer.nullable,
                    );
                    tracer.accept(event)?;
                    *self = Tracer::Struct(tracer);
                }
                Event::StartTuple => {
                    let mut tracer = TupleTracer::new(tracer.options.clone(), tracer.nullable);
                    tracer.accept(event)?;
                    *self = Tracer::Tuple(tracer);
                }
                Event::StartMap => {
                    if tracer.options.map_as_struct {
                        let mut tracer = StructTracer::new(
                            tracer.options.clone(),
                            StructMode::Map,
                            tracer.nullable,
                        );
                        tracer.accept(event)?;
                        *self = Tracer::Struct(tracer);
                    } else {
                        let mut tracer = MapTracer::new(tracer.options.clone(), tracer.nullable);
                        tracer.accept(event)?;
                        *self = Tracer::Map(tracer);
                    }
                }
                Event::Variant(_, _) => {
                    let mut tracer = UnionTracer::new(tracer.options.clone(), tracer.nullable);
                    tracer.accept(event)?;
                    *self = Tracer::Union(tracer)
                }
                ev if ev.is_end() => fail!("Invalid end nesting events for unknown tracer"),
                ev => fail!("Internal error unmatched event {ev} in Tracer"),
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
    pub options: TracingOptions,
}

impl UnknownTracer {
    pub fn new(options: TracingOptions) -> Self {
        Self {
            nullable: false,
            finished: false,
            options,
        }
    }

    pub fn to_field(&self, name: &str) -> Result<GenericField> {
        if !self.finished {
            fail!("Cannot build field {name} from unfinished tracer");
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
    pub counts: BTreeMap<usize, usize>,
    pub finished: bool,
    pub options: TracingOptions,
}

#[derive(Debug, Clone, Copy)]
pub enum StructTracerState {
    Start,
    Key,
    Value(usize, usize),
}

impl StructTracer {
    pub fn new(options: TracingOptions, mode: StructMode, nullable: bool) -> Self {
        Self {
            options,
            mode,
            field_tracers: Vec::new(),
            field_names: Vec::new(),
            index: HashMap::new(),
            nullable,
            next: StructTracerState::Start,
            counts: BTreeMap::new(),
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
            field.strategy = Some(Strategy::MapAsStruct);
        }
        Ok(field)
    }

    pub fn mark_seen(&mut self, field: usize) {
        self.counts.insert(
            field,
            self.counts.get(&field).copied().unwrap_or_default() + 1,
        );
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
            (Key, E::Str(key)) => {
                if let Some(&field) = self.index.get(key) {
                    self.mark_seen(field);
                    Value(field, 0)
                } else {
                    let field = self.field_tracers.len();
                    self.field_tracers.push(Tracer::new(self.options.clone()));
                    self.field_names.push(key.to_owned());
                    self.index.insert(key.to_owned(), field);
                    self.mark_seen(field);
                    Value(field, 0)
                }
            }
            (Key, E::EndStruct | E::EndMap) => Start,
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

        let max_count = self.counts.values().copied().max().unwrap_or_default();
        for (&field, &count) in self.counts.iter() {
            if count != max_count {
                self.field_tracers[field].mark_nullable();
            }
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
    pub options: TracingOptions,
}

impl TupleTracer {
    pub fn new(options: TracingOptions, nullable: bool) -> Self {
        Self {
            options,
            field_tracers: Vec::new(),
            nullable,
            next: TupleTracerState::Start,
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
            self.field_tracers.push(Tracer::new(self.options.clone()));
        }
        &mut self.field_tracers[idx]
    }
}

impl EventSink for TupleTracer {
    macros::forward_specialized_to_generic!();

    fn accept(&mut self, event: Event<'_>) -> Result<()> {
        use TupleTracerState::*;
        type E<'a> = Event<'a>;

        self.next = match (self.next, event) {
            (Start, Event::StartTuple) => Item(0, 0),
            (Start, E::Null | E::Some) => {
                self.nullable = true;
                Start
            }
            (Start, ev) => fail!("Invalid event {ev} for tuple tracer in state Start"),
            (Item(_, 0), E::EndTuple) => Start,
            (Item(field, depth), ev) if ev.is_start() => {
                self.field_tracer(field).accept(ev)?;
                Item(field, depth + 1)
            }
            (Item(field, depth), ev) if ev.is_end() => {
                self.field_tracer(field).accept(ev)?;
                match depth {
                    0 => fail!("Invalid closing event in struct tracer in state Value"),
                    1 => Item(field + 1, 0),
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
                    0 => Item(field + 1, 0),
                    _ => Item(field, depth),
                }
            }
        };
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        if !matches!(self.next, TupleTracerState::Start) {
            fail!("Incomplete tuple in schema tracing");
        }
        for tracer in &mut self.field_tracers {
            tracer.finish()?;
        }
        self.finished = true;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub enum TupleTracerState {
    Start,
    Item(usize, usize),
}

pub struct ListTracer {
    pub item_tracer: Box<Tracer>,
    pub nullable: bool,
    pub next: ListTracerState,
    pub finished: bool,
}

#[derive(Debug, Clone, Copy)]
pub enum ListTracerState {
    Start,
    Item(usize),
}

impl ListTracer {
    pub fn new(options: TracingOptions, nullable: bool) -> Self {
        Self {
            item_tracer: Box::new(Tracer::new(options)),
            nullable,
            next: ListTracerState::Start,
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
        use ListTracerState::*;
        type E<'a> = Event<'a>;

        self.next = match (self.next, event) {
            (Start, E::StartSequence) => Item(0),
            (Start, E::Null | E::Some) => {
                self.nullable = true;
                Start
            }
            (Start, ev) => fail!("Invalid event {ev} for list tracer in state Start"),
            (Item(0), ev) if ev.is_end() => {
                if matches!(ev, E::EndSequence) {
                    Start
                } else {
                    fail!("Invalid event {ev} for list tracer in state Item(0)")
                }
            }
            (Item(depth), ev) if ev.is_start() => {
                self.item_tracer.accept(ev)?;
                Item(depth + 1)
            }
            (Item(depth), ev) if ev.is_end() => {
                self.item_tracer.accept(ev)?;
                Item(depth - 1)
            }
            (Item(depth), ev) => {
                self.item_tracer.accept(ev)?;
                Item(depth)
            }
        };
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        if !matches!(self.next, ListTracerState::Start) {
            fail!("Incomplete list in schema tracing");
        }
        self.item_tracer.finish()?;
        self.finished = true;
        Ok(())
    }
}

pub struct UnionTracer {
    pub variants: Vec<Option<String>>,
    pub tracers: Vec<Tracer>,
    pub nullable: bool,
    pub next: UnionTracerState,
    pub finished: bool,
    pub options: TracingOptions,
}

impl UnionTracer {
    pub fn new(options: TracingOptions, nullable: bool) -> Self {
        Self {
            options,
            variants: Vec::new(),
            tracers: Vec::new(),
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
        for (idx, (name, tracer)) in std::iter::zip(&self.variants, &self.tracers).enumerate() {
            field.children.push(if let Some(name) = name {
                tracer.to_field(name)?
            } else {
                tracer.to_field(&format!("unknown_variant_{idx}"))?
            });
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
            self.tracers.push(Tracer::new(self.options.clone()));
        }

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
                    self.tracers[idx].accept(ev)?;
                    S::Active(idx, depth + 1)
                }
                ev if ev.is_end() => match depth {
                    0 => fail!("Invalid end event {ev} at depth 0 in UnionTracer"),
                    1 => {
                        self.tracers[idx].accept(ev)?;
                        S::Inactive
                    }
                    _ => {
                        self.tracers[idx].accept(ev)?;
                        S::Active(idx, depth - 1)
                    }
                },
                ev if ev.is_marker() => {
                    self.tracers[idx].accept(ev)?;
                    S::Active(idx, depth)
                }
                ev if ev.is_value() => {
                    self.tracers[idx].accept(ev)?;
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
        // TODO: add checks here?
        for tracer in &mut self.tracers {
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
    pub key: Box<Tracer>,
    pub value: Box<Tracer>,
    pub nullable: bool,
    pub finished: bool,
    next: MapTracerState,
}

impl MapTracer {
    pub fn new(options: TracingOptions, nullable: bool) -> Self {
        Self {
            nullable,
            key: Box::new(Tracer::new(options.clone())),
            value: Box::new(Tracer::new(options)),
            next: MapTracerState::Start,
            finished: true,
        }
    }

    pub fn to_field(&self, name: &str) -> Result<GenericField> {
        if !self.finished {
            fail!("Cannot build field {name} from unfinished tracer");
        }

        let mut field = GenericField::new(name, GenericDataType::Map, self.nullable);
        field.children.push(self.key.to_field("key")?);
        field.children.push(self.value.to_field("value")?);

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
        // TODO: add checks
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
    pub item_type: GenericDataType,
    pub nullable: bool,
    pub finished: bool,
}

impl PrimitiveTracer {
    pub fn new(nullable: bool, string_dictionary_encoding: bool) -> Self {
        Self {
            item_type: GenericDataType::Null,
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

        match self.item_type {
            dt @ (D::LargeUtf8 | D::Utf8) => {
                if !self.string_dictionary_encoding {
                    Ok(GenericField::new(name, dt, self.nullable))
                } else {
                    let field = GenericField::new(name, D::Dictionary, self.nullable)
                        .with_child(GenericField::new("key", D::U32, false))
                        .with_child(GenericField::new("value", dt, false));
                    Ok(field)
                }
            }
            dt => Ok(GenericField::new(name, dt, self.nullable)),
        }
    }
}

impl EventSink for PrimitiveTracer {
    macros::forward_specialized_to_generic!();

    fn accept(&mut self, event: Event<'_>) -> Result<()> {
        type D = GenericDataType;
        type E<'a> = Event<'a>;

        match (event, self.item_type) {
            (E::Some | Event::Null, _) => {
                self.nullable = true;
            }
            (E::Bool(_), D::Bool | D::Null) => {
                self.item_type = D::Bool;
            }
            (E::I8(_), D::I8 | D::Null) => {
                self.item_type = D::I8;
            }
            (E::I16(_), D::I16 | D::Null) => {
                self.item_type = D::I16;
            }
            (E::I32(_), D::I32 | D::Null) => {
                self.item_type = D::I32;
            }
            (E::I64(_), D::I64 | D::Null) => {
                self.item_type = D::I64;
            }
            (E::U8(_), D::U8 | D::Null) => {
                self.item_type = D::U8;
            }
            (E::U16(_), D::U16 | D::Null) => {
                self.item_type = D::U16;
            }
            (E::U32(_), D::U32 | D::Null) => {
                self.item_type = D::U32;
            }
            (E::U64(_), D::U64 | D::Null) => {
                self.item_type = D::U64;
            }
            (E::F32(_), D::F32 | D::Null) => {
                self.item_type = D::F32;
            }
            (E::F64(_), D::F64 | D::Null) => {
                self.item_type = D::F64;
            }
            (E::Str(_) | E::OwnedStr(_), D::LargeUtf8 | D::Null) => {
                self.item_type = D::LargeUtf8;
            }
            (ev, ty) => fail!("Cannot accept event {ev} for primitive type {ty}"),
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        self.finished = true;
        Ok(())
    }
}
