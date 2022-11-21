use std::{collections::HashMap, str::FromStr};

use crate::{
    base::{error::fail, Event, EventSink},
    Error, Result,
};

/// The metadata key under which to store the strategy (see [Strategy] for details)
///
pub const STRATEGY_KEY: &str = "SERDE_ARROW:strategy";

/// Strategies for handling types without direct match between arrow and serde
///
/// For the correct strategy both the field type and the field metadata must be
/// correctly configured. In particular, when determining the schema from the
/// Rust objects themselves, some field types are incorrectly recognized (e.g.,
/// datetimes).
///
/// For example, to let `serde_arrow` handle date time objects that are
/// serialized to strings (chrono's default), use
///
/// ```rust
/// # #[cfg(feature="arrow2")]
/// # fn main() {
/// # use arrow2::datatypes::{DataType, Field};
/// # use serde_arrow::{STRATEGY_KEY, Strategy};
/// # let mut field = Field::new("my_field", DataType::Null, false);
/// field.data_type = DataType::Date64;
/// field.metadata.insert(
///     STRATEGY_KEY.to_string(),
///     Strategy::UtcDateTimeStr.to_string(),
/// );
/// # }
/// # #[cfg(not(feature="arrow2"))]
/// # fn main() {}
/// ```
///
#[non_exhaustive]
pub enum Strategy {
    /// Arrow: Date64, serde: strings with UTC timezone
    UtcDateTimeStr,
    /// Arrow: Date64, serde: strings without a timezone
    NaiveDateTimeStr,
    /// Serialize Rust tuples as Arrow structs
    Tuple,
}

impl std::fmt::Display for Strategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UtcDateTimeStr => write!(f, "UtcDateTimeStr"),
            Self::NaiveDateTimeStr => write!(f, "NaiveDateTimeStr"),
            Self::Tuple => write!(f, "Tuple"),
        }
    }
}

impl FromStr for Strategy {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "UtcDateTimeStr" => Ok(Self::UtcDateTimeStr),
            "NaiveDateTimeStr" => Ok(Self::NaiveDateTimeStr),
            "Tuple" => Ok(Self::Tuple),
            _ => fail!("Unknown strategy {s}"),
        }
    }
}

pub struct SchemaTracer {
    pub tracer: Tracer,
    pub next: SchemaTracerState,
}

impl SchemaTracer {
    pub fn new() -> Self {
        Self {
            tracer: Tracer::new(),
            next: SchemaTracerState::StartSequence,
        }
    }
}

impl EventSink for SchemaTracer {
    fn accept(&mut self, event: Event<'_>) -> Result<()> {
        type S = SchemaTracerState;
        type E<'a> = Event<'a>;

        self.next = match self.next {
            S::StartSequence => match event {
                E::StartSequence | E::StartTuple => S::StartRecord,
                ev => fail!("Invalid event {ev} in SchemaTracer, expected start of outer sequence"),
            },
            S::StartRecord => match event {
                E::EndSequence | E::EndTuple => S::Done,
                ev @ E::StartStruct => {
                    self.tracer.accept(ev)?;
                    S::Record(0)
                }
                ev => fail!("Invalid event {ev} in SchemaTracer, expected start of record"),
            },
            S::Record(depth) => match event {
                ev @ E::EndStruct if depth == 0 => {
                    self.tracer.accept(ev)?;
                    S::StartRecord
                }
                ev @ (E::EndMap | E::EndSequence | E::EndTuple) if depth == 0 => {
                    fail!("Invalid event {ev} in SchemaTracer, expected non-closing tag at depth 0")
                }
                ev @ (E::StartMap | E::StartSequence | E::StartTuple | E::StartStruct) => {
                    self.tracer.accept(ev)?;
                    S::Record(depth + 1)
                }
                ev @ (E::EndMap | E::EndSequence | E::EndTuple | E::EndStruct) => {
                    self.tracer.accept(ev)?;
                    S::Record(depth - 1)
                }
                ev => {
                    self.tracer.accept(ev)?;
                    S::Record(depth)
                }
            },
            S::Done => fail!("Invalid event {event} in SchemaTracer, expected no more events"),
        };
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        if !matches!(self.next, SchemaTracerState::Done) {
            fail!("Incomplete structure in SchemaTracer");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub enum SchemaTracerState {
    StartSequence,
    StartRecord,
    Record(usize),
    Done,
}

pub enum Tracer {
    Unknown(UnknownTracer),
    Struct(StructTracer),
    List(ListTracer),
    Primitive(PrimitiveTracer),
    Tuple(TupleTracer),
}

impl Tracer {
    pub fn new() -> Self {
        Self::Unknown(UnknownTracer::new())
    }
}

pub trait FieldBuilder<F> {
    fn to_field(&self, name: &str) -> Result<F>;
}

impl EventSink for Tracer {
    fn accept(&mut self, event: Event<'_>) -> Result<()> {
        //
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
                    let mut tracer = PrimitiveTracer::new(tracer.nullable);
                    tracer.accept(event)?;
                    *self = Tracer::Primitive(tracer)
                }
                Event::StartSequence => {
                    let mut tracer = ListTracer::new(tracer.nullable);
                    tracer.accept(event)?;
                    *self = Tracer::List(tracer);
                }
                Event::StartStruct => {
                    let mut tracer = StructTracer::new(tracer.nullable);
                    tracer.accept(event)?;
                    *self = Tracer::Struct(tracer);
                }
                Event::StartTuple => {
                    let mut tracer = TupleTracer::new(tracer.nullable);
                    tracer.accept(event)?;
                    *self = Tracer::Tuple(tracer);
                }
                Event::EndSequence | Event::EndStruct | Event::EndTuple => {
                    fail!("Invalid end nesting events for unknown tracer")
                }
                Event::StartMap | Event::EndMap => {
                    fail!("Maps are not yet supported")
                }
            },
            Self::List(tracer) => tracer.accept(event)?,
            Self::Struct(tracer) => tracer.accept(event)?,
            Self::Primitive(tracer) => tracer.accept(event)?,
            Self::Tuple(tracer) => tracer.accept(event)?,
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
        }
    }
}

pub struct UnknownTracer {
    pub nullable: bool,
}

impl UnknownTracer {
    pub fn new() -> Self {
        Self { nullable: false }
    }

    pub fn finish(&mut self) -> Result<()> {
        Ok(())
    }
}

pub struct StructTracer {
    pub field_tracers: Vec<Tracer>,
    pub nullable: bool,
    pub field_names: Vec<String>,
    pub index: HashMap<String, usize>,
    pub next: StructTracerState,
}

#[derive(Debug, Clone, Copy)]
pub enum StructTracerState {
    Start,
    Key,
    Value(usize, usize),
}

impl StructTracer {
    pub fn new(nullable: bool) -> Self {
        Self {
            field_tracers: Vec::new(),
            field_names: Vec::new(),
            index: HashMap::new(),
            nullable,
            next: StructTracerState::Start,
        }
    }

    pub fn accept(&mut self, event: Event<'_>) -> Result<()> {
        use StructTracerState::*;
        type E<'a> = Event<'a>;

        self.next = match (self.next, event) {
            (Start, E::StartStruct) => Key,
            (Start, E::Null | E::Some) => {
                self.nullable = true;
                Start
            }
            (Start, ev) => fail!("Invalid event {ev} for struct tracer in state Start"),
            (Key, E::Str(key)) => {
                if let Some(&field) = self.index.get(key) {
                    Value(field, 0)
                } else {
                    let field = self.field_tracers.len();
                    self.field_tracers.push(Tracer::new());
                    self.field_names.push(key.to_owned());
                    self.index.insert(key.to_owned(), field);
                    Value(field, 0)
                }
            }
            (Key, E::EndStruct) => Start,
            (Key, ev) => fail!("Invalid event {ev} for struct tracer in state Key"),
            (
                Value(field, depth),
                ev @ (E::StartSequence | E::StartStruct | E::StartMap | E::StartTuple),
            ) => {
                self.field_tracers[field].accept(ev)?;
                Value(field, depth + 1)
            }
            (
                Value(field, depth),
                ev @ (E::EndSequence | E::EndStruct | E::EndTuple | E::EndMap),
            ) => {
                self.field_tracers[field].accept(ev)?;
                match depth {
                    0 => fail!("Invalid closing event in struct tracer in state Value"),
                    1 => Key,
                    depth => Value(field, depth - 1),
                }
            }
            // Some is always followed by the actual  value
            (Value(field, 0), E::Some) => {
                self.field_tracers[field].accept(E::Some)?;
                Value(field, 0)
            }
            // Any event at depth == 0 that does not start a structure (is a complete value)
            (Value(field, 0), ev) => {
                self.field_tracers[field].accept(ev)?;
                Key
            }
            (Value(field, depth), ev) => {
                self.field_tracers[field].accept(ev)?;
                Value(field, depth)
            }
        };
        Ok(())
    }

    pub fn finish(&mut self) -> Result<()> {
        if !matches!(self.next, StructTracerState::Start) {
            fail!("Incomplete struct in schema tracing");
        }
        Ok(())
    }
}

pub struct TupleTracer {
    pub field_tracers: Vec<Tracer>,
    pub nullable: bool,
    pub next: TupleTracerState,
}

impl TupleTracer {
    pub fn new(nullable: bool) -> Self {
        Self {
            field_tracers: Vec::new(),
            nullable,
            next: TupleTracerState::Start,
        }
    }

    pub fn accept(&mut self, event: Event<'_>) -> Result<()> {
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
            (
                Item(field, depth),
                ev @ (E::StartSequence | E::StartStruct | E::StartTuple | E::StartMap),
            ) => {
                self.field_tracer(field).accept(ev)?;
                Item(field, depth + 1)
            }
            (
                Item(field, depth),
                ev @ (E::EndSequence | E::EndStruct | E::EndTuple | E::EndMap),
            ) => {
                self.field_tracer(field).accept(ev)?;
                match depth {
                    0 => fail!("Invalid closing event in struct tracer in state Value"),
                    1 => Item(field + 1, 0),
                    depth => Item(field, depth - 1),
                }
            }
            // Some is always followed by the actual  value
            (Item(field, 0), E::Some) => {
                self.field_tracer(field).accept(E::Some)?;
                Item(field, 0)
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

    pub fn finish(&mut self) -> Result<()> {
        if !matches!(self.next, TupleTracerState::Start) {
            fail!("Incomplete tuple in schema tracing");
        }
        Ok(())
    }

    fn field_tracer(&mut self, idx: usize) -> &mut Tracer {
        while self.field_tracers.len() <= idx {
            self.field_tracers.push(Tracer::new());
        }
        &mut self.field_tracers[idx]
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
}

#[derive(Debug, Clone, Copy)]
pub enum ListTracerState {
    Start,
    Item(usize),
}

impl ListTracer {
    pub fn new(nullable: bool) -> Self {
        Self {
            item_tracer: Box::new(Tracer::new()),
            nullable,
            next: ListTracerState::Start,
        }
    }

    pub fn accept(&mut self, event: Event<'_>) -> Result<()> {
        use ListTracerState::*;
        type E<'a> = Event<'a>;

        self.next = match (self.next, event) {
            (Start, E::StartSequence) => Item(0),
            (Start, E::Null | E::Some) => {
                self.nullable = true;
                Start
            }
            (Start, ev) => fail!("Invalid event {ev} for list tracer in state Start"),
            (Item(0), E::EndSequence) => Start,
            (Item(0), ev @ (E::EndStruct | E::EndMap | E::EndTuple)) => {
                fail!("Invalid event {ev} for list tracer in state Item(0)")
            }
            (Item(depth), ev @ (E::StartSequence | E::StartStruct)) => {
                self.item_tracer.accept(ev)?;
                Item(depth + 1)
            }
            (Item(depth), ev @ (E::EndSequence | E::EndStruct)) => {
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

    pub fn finish(&mut self) -> Result<()> {
        if !matches!(self.next, ListTracerState::Start) {
            fail!("Incomplete list in schema tracing");
        }
        Ok(())
    }
}

pub struct PrimitiveTracer {
    pub item_type: PrimitiveType,
    pub nullable: bool,
}

impl PrimitiveTracer {
    pub fn new(nullable: bool) -> Self {
        Self {
            item_type: PrimitiveType::Unknown,
            nullable,
        }
    }

    pub fn accept(&mut self, event: Event<'_>) -> Result<()> {
        type T = PrimitiveType;
        type E<'a> = Event<'a>;

        match (event, self.item_type) {
            (E::Some | Event::Null, _) => {
                self.nullable = true;
            }
            (E::Bool(_), T::Bool | T::Unknown) => {
                self.item_type = T::Bool;
            }
            (E::I8(_), T::I8 | T::Unknown) => {
                self.item_type = T::I8;
            }
            (E::I16(_), T::I16 | T::Unknown) => {
                self.item_type = T::I16;
            }
            (E::I32(_), T::I32 | T::Unknown) => {
                self.item_type = T::I32;
            }
            (E::I64(_), T::I64 | T::Unknown) => {
                self.item_type = T::I64;
            }
            (E::U8(_), T::U8 | T::Unknown) => {
                self.item_type = T::U8;
            }
            (E::U16(_), T::U16 | T::Unknown) => {
                self.item_type = T::U16;
            }
            (E::U32(_), T::U32 | T::Unknown) => {
                self.item_type = T::U32;
            }
            (E::U64(_), T::U64 | T::Unknown) => {
                self.item_type = T::U64;
            }
            (E::F32(_), T::F32 | T::Unknown) => {
                self.item_type = T::F32;
            }
            (E::F64(_), T::F64 | T::Unknown) => {
                self.item_type = T::F64;
            }
            (E::Str(_) | E::OwnedStr(_), T::Str | T::Unknown) => {
                self.item_type = T::Str;
            }
            (ev, ty) => fail!("Cannot accept event {ev} for primitive type {ty:?}"),
        }
        Ok(())
    }

    pub fn finish(&mut self) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub enum PrimitiveType {
    Unknown,
    Bool,
    Str,
    I8,
    I16,
    I32,
    I64,
    U8,
    U16,
    U32,
    U64,
    F32,
    F64,
}
