use std::collections::{HashMap, HashSet};

use crate::{
    base::{
        error::{error, fail},
        Event, EventSink,
    },
    Result,
};

pub trait ArrayBuilder<A>: EventSink {
    fn box_into_array(self: Box<Self>) -> Result<A>;
    fn into_array(self) -> Result<A>
    where
        Self: Sized;
}

pub struct DynamicArrayBuilder<A> {
    builder: Box<dyn ArrayBuilder<A>>,
}

impl<A> DynamicArrayBuilder<A> {
    pub fn new<B: ArrayBuilder<A> + 'static>(builder: B) -> Self {
        Self {
            builder: Box::new(builder),
        }
    }
}

impl<A> EventSink for DynamicArrayBuilder<A> {
    fn accept(&mut self, event: Event<'_>) -> Result<()> {
        self.builder.accept(event)
    }
}

impl<A> ArrayBuilder<A> for DynamicArrayBuilder<A> {
    fn box_into_array(self: Box<Self>) -> Result<A> {
        self.builder.box_into_array()
    }

    fn into_array(self) -> Result<A> {
        self.builder.box_into_array()
    }
}

impl<A> From<Box<dyn ArrayBuilder<A>>> for DynamicArrayBuilder<A> {
    fn from(builder: Box<dyn ArrayBuilder<A>>) -> Self {
        Self { builder }
    }
}

pub struct RecordsBuilder<A> {
    builders: Vec<DynamicArrayBuilder<A>>,
    field_index: HashMap<String, usize>,
    next: State,
    seen: HashSet<usize>,
}

impl<A> RecordsBuilder<A> {
    pub fn new(columns: Vec<String>, builders: Vec<DynamicArrayBuilder<A>>) -> Result<Self> {
        if columns.len() != builders.len() {
            fail!("Number of columns must be equal to the number of builders");
        }

        let mut field_index = HashMap::new();
        for (i, col) in columns.iter().enumerate() {
            if field_index.contains_key(col) {
                fail!("Duplicate field {}", col);
            }
            field_index.insert(col.to_owned(), i);
        }

        Ok(Self {
            builders,
            field_index,
            next: State::StartSequence,
            seen: HashSet::new(),
        })
    }
}

impl<A> RecordsBuilder<A> {
    pub fn into_records(self) -> Result<Vec<A>> {
        if !matches!(self.next, State::Done) {
            fail!("Invalid state");
        }
        let arrays: Result<Vec<A>> = self
            .builders
            .into_iter()
            .map(|builder| builder.into_array())
            .collect();
        let arrays = arrays?;
        Ok(arrays)
    }
}

#[derive(Debug, Clone, Copy)]
enum State {
    StartSequence,
    StartMap,
    Key,
    Value(usize, usize),
    Done,
}

impl<A> EventSink for RecordsBuilder<A> {
    fn accept(&mut self, event: Event<'_>) -> Result<()> {
        use State::*;
        type E<'a> = Event<'a>;

        self.next = match (self.next, event.to_self()) {
            (StartSequence, E::StartSequence | E::StartTuple) => StartMap,
            (StartMap, E::EndSequence | E::EndTuple) => Done,
            (StartMap, E::StartStruct) => {
                self.seen.clear();
                Key
            }
            (Key, E::Str(k)) => {
                let &idx = self
                    .field_index
                    .get(k)
                    .ok_or_else(|| error!("Unknown field {k}"))?;
                if self.seen.contains(&idx) {
                    fail!("Duplicate field {k}");
                }
                self.seen.insert(idx);

                Value(idx, 0)
            }
            (Key, E::EndStruct) => StartMap,
            // Ignore some events
            (Value(idx, depth), Event::Some) => Value(idx, depth),
            (Value(idx, depth), ev) => {
                let next = match ev {
                    E::StartSequence | E::StartStruct | E::StartMap | E::StartTuple => {
                        Value(idx, depth + 1)
                    }
                    E::EndSequence | E::EndStruct | E::EndMap | E::EndTuple if depth > 1 => {
                        Value(idx, depth - 1)
                    }
                    E::EndSequence | E::EndStruct | E::EndTuple | E::EndMap if depth == 0 => {
                        fail!("Invalid state")
                    }
                    // the closing event for the nested type
                    E::EndSequence | E::EndStruct | E::EndTuple | E::EndMap => Key,
                    _ if depth == 0 => Key,
                    _ => Value(idx, depth),
                };

                self.builders[idx].accept(ev)?;
                next
            }
            (state, ev) => fail!("Invalid event {ev} in state {state:?}"),
        };
        Ok(())
    }
}

pub struct StructArrayBuilder<B> {
    pub(crate) columns: Vec<String>,
    pub(crate) nullable: Vec<bool>,
    pub(crate) builders: Vec<B>,
    pub(crate) state: StructArrayBuilderState,
    pub(crate) seen: Vec<bool>,
}

impl<B> StructArrayBuilder<B> {
    pub fn new(columns: Vec<String>, nullable: Vec<bool>, builders: Vec<B>) -> Self {
        let num_columns = columns.len();
        Self {
            columns,
            builders,
            nullable,
            state: StructArrayBuilderState::Start,
            seen: vec![false; num_columns],
        }
    }
}

impl<B: EventSink> EventSink for StructArrayBuilder<B> {
    fn accept(&mut self, event: Event<'_>) -> Result<()> {
        use StructArrayBuilderState::*;

        match self.state {
            Start => match event {
                Event::StartStruct => {
                    self.state = Field;
                    self.seen = vec![false; self.columns.len()];
                }
                _ => fail!("Expected start map"),
            },
            Field => {
                let key = match event {
                    Event::Str(key) => key,
                    Event::OwnedStr(ref key) => key,
                    Event::EndStruct => {
                        if !self.seen.iter().all(|&seen| seen) {
                            // TODO: improve error message
                            fail!("Missing fields");
                        }
                        self.state = Start;
                        return Ok(());
                    }
                    event => fail!("Unexpected event while waiting for field: {event}"),
                };
                let idx = self
                    .columns
                    .iter()
                    .position(|col| col == key)
                    .ok_or_else(|| error!("unknown field {key}"))?;
                if self.seen[idx] {
                    fail!("Duplicate field {}", self.columns[idx]);
                }
                self.seen[idx] = true;
                self.state = Value(idx, 0);
            }
            Value(active, depth) => {
                self.state = match &event {
                    Event::StartStruct
                    | Event::StartSequence
                    | Event::StartTuple
                    | Event::StartMap => Value(active, depth + 1),
                    Event::EndStruct | Event::EndSequence | Event::EndTuple | Event::EndMap => {
                        match depth {
                            // the last closing event for the current value
                            1 => Field,
                            // TODO: check is this event possible?
                            0 => fail!("Unbalanced opening / close events in StructArrayBuilder"),
                            _ => Value(active, depth - 1),
                        }
                    }
                    _ if depth == 0 => Field,
                    _ => self.state,
                };
                self.builders[active].accept(event)?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub enum StructArrayBuilderState {
    Start,
    Field,
    Value(usize, usize),
}

pub struct TupleStructBuilder<B> {
    pub(crate) nullable: Vec<bool>,
    pub(crate) builders: Vec<B>,
    pub(crate) state: TupleArrayBuilderState,
}

impl<B> TupleStructBuilder<B> {
    pub fn new(nullable: Vec<bool>, builders: Vec<B>) -> Self {
        Self {
            builders,
            nullable,
            state: TupleArrayBuilderState::Start,
        }
    }
}

impl<B: EventSink> EventSink for TupleStructBuilder<B> {
    fn accept(&mut self, event: Event<'_>) -> Result<()> {
        use TupleArrayBuilderState::*;

        self.state = match (self.state, event) {
            (Start, Event::StartTuple) => Value(0, 0),
            (
                Value(active, depth),
                ev @ (Event::StartStruct
                | Event::StartSequence
                | Event::StartMap
                | Event::StartTuple),
            ) => {
                self.builders[active].accept(ev)?;
                Value(active, depth + 1)
            }
            (Value(_, 0), Event::EndTuple) => Start,
            (Value(_, 0), Event::EndStruct | Event::EndSequence | Event::EndMap) => {
                fail!("Unbalanced opening / close events in TupleStructBuilder")
            }
            (
                Value(active, 1),
                ev @ (Event::EndStruct | Event::EndSequence | Event::EndMap | Event::EndTuple),
            ) => {
                self.builders[active].accept(ev)?;
                Value(active + 1, 0)
            }
            (Value(active, 0), ev) => {
                self.builders[active].accept(ev)?;
                Value(active + 1, 0)
            }
            (Value(active, depth), ev) => {
                self.builders[active].accept(ev)?;
                Value(active, depth)
            }
            (state, ev) => fail!("Invalid event {ev} in state {state:?}"),
        };
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub enum TupleArrayBuilderState {
    Start,
    Value(usize, usize),
}

// TODO: move the generic parts into generic module
pub struct ListArrayBuilder<B> {
    pub builder: B,
    next: ListBuilderState,
    pub offsets: Vec<i64>,
    pub validity: Vec<bool>,
    pub item_name: String,
    pub nullable: bool,
}

impl<B> ListArrayBuilder<B> {
    pub fn new(builder: B, item_name: String, nullable: bool) -> Self {
        Self {
            builder,
            next: ListBuilderState::Start { offset: 0 },
            offsets: vec![0],
            validity: Vec::new(),
            item_name,
            nullable,
        }
    }

    fn finalize_item(&mut self, end_offset: i64, nullable: bool) {
        self.offsets.push(end_offset);
        self.validity.push(!nullable);
    }
}

impl<B: EventSink> EventSink for ListArrayBuilder<B> {
    fn accept(&mut self, event: Event<'_>) -> Result<()> {
        use ListBuilderState::*;
        self.next = match self.next {
            Start { offset } => match &event {
                Event::StartSequence => Value { offset, depth: 0 },
                Event::Null => {
                    self.finalize_item(offset, true);
                    Start { offset }
                }
                ev => fail!("Invalid event {ev} in state Start"),
            },
            Value { offset, depth } => match &event {
                Event::EndSequence => {
                    if depth != 0 {
                        self.builder.accept(event)?;
                        Value {
                            offset: if depth == 1 { offset + 1 } else { offset },
                            depth: depth - 1,
                        }
                    } else {
                        self.finalize_item(offset, false);
                        Start { offset }
                    }
                }
                Event::EndStruct => {
                    if depth != 0 {
                        self.builder.accept(event)?;
                        Value {
                            offset: if depth == 1 { offset + 1 } else { offset },
                            depth: depth - 1,
                        }
                    } else {
                        fail!("Invalid EndMap in list array")
                    }
                }
                Event::StartSequence | Event::StartStruct => {
                    self.builder.accept(event)?;
                    Value {
                        offset,
                        depth: depth + 1,
                    }
                }
                _ => {
                    self.builder.accept(event)?;
                    Value {
                        offset: if depth == 0 { offset + 1 } else { offset },
                        depth,
                    }
                }
            },
        };
        Ok(())
    }
}

/// The state of the list builder
///
/// Fields:
///
/// - `offset`: the next offset of a value
///
#[derive(Debug, Clone, Copy)]
enum ListBuilderState {
    Start { offset: i64 },
    Value { offset: i64, depth: usize },
}
