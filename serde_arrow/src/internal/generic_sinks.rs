use crate::{
    internal::{
        error::{error, fail},
        event::Event,
        sink::EventSink,
    },
    Error, Result,
};

pub trait ArrayBuilder<A>: EventSink {
    fn box_into_array(self: Box<Self>) -> Result<A>;
    fn into_array(self) -> Result<A>
    where
        Self: Sized;
}

impl<A, T: ArrayBuilder<A>> ArrayBuilder<A> for Box<T> {
    fn box_into_array(self: Box<Self>) -> Result<A> {
        (*self).into_array()
    }

    fn into_array(self) -> Result<A>
    where
        Self: Sized,
    {
        self.box_into_array()
    }
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
    // TODO: forward the the other methods?

    fn accept(&mut self, event: Event<'_>) -> Result<()> {
        self.builder.accept(event)
    }

    fn finish(&mut self) -> Result<()> {
        self.builder.finish()
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

pub struct StructArrayBuilder<B> {
    /// the names of the fields
    pub(crate) columns: Vec<String>,
    /// the nullability of the fields
    pub(crate) nullable: Vec<bool>,
    /// the builders of the sub arrays
    pub(crate) builders: Vec<B>,
    /// the validity of the items
    pub(crate) validity: Vec<bool>,
    pub(crate) state: StructArrayBuilderState,
    pub(crate) seen: Vec<bool>,
    pub(crate) finished: bool,
}

impl<B> StructArrayBuilder<B> {
    pub fn new(columns: Vec<String>, nullable: Vec<bool>, builders: Vec<B>) -> Self {
        let num_columns = columns.len();
        Self {
            columns,
            builders,
            validity: Vec::new(),
            nullable,
            state: StructArrayBuilderState::Start,
            seen: vec![false; num_columns],
            finished: false,
        }
    }
}

impl<B: EventSink> EventSink for StructArrayBuilder<B> {
    fn accept(&mut self, event: Event<'_>) -> Result<()> {
        use StructArrayBuilderState::*;

        match self.state {
            Start => match event {
                Event::StartStruct | Event::StartMap => {
                    self.seen = vec![false; self.columns.len()];
                    self.state = Field;
                }
                // ignore marker events
                ev if ev.is_marker() => {
                    self.state = Start;
                }
                Event::Null => {
                    for b in &mut self.builders {
                        b.accept(Event::Default)?;
                    }
                    self.validity.push(false);
                }
                Event::Default => {
                    for b in &mut self.builders {
                        b.accept(Event::Default)?;
                    }
                    self.validity.push(true);
                }
                ev => fail!(
                    "Expected StartStruct, StartMap or marker in StructArrayBuilder, not {ev}"
                ),
            },
            Field => {
                let key = match event {
                    Event::Str(key) => key,
                    Event::OwnedStr(ref key) => key,
                    Event::EndStruct | Event::EndMap => {
                        for (idx, seen) in self.seen.iter().enumerate() {
                            if !seen {
                                if !self.nullable[idx] {
                                    fail!("Missing field {} is not nullable", self.columns[idx]);
                                }
                                self.builders[idx].accept_null()?;
                            }
                        }
                        self.validity.push(true);
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
                    ev if ev.is_start() => Value(active, depth + 1),
                    ev if ev.is_end() => {
                        match depth {
                            // the last closing event for the current value
                            1 => Field,
                            // TODO: check is this event possible?
                            0 => fail!("Unbalanced opening / close events in StructArrayBuilder"),
                            _ => Value(active, depth - 1),
                        }
                    }
                    ev if ev.is_marker() => self.state,
                    _ if depth == 0 => Field,
                    _ => self.state,
                };
                self.builders[active].accept(event)?;
            }
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        for builder in &mut self.builders {
            builder.finish()?;
        }
        self.finished = true;
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
    pub(crate) validity: Vec<bool>,
    pub(crate) state: TupleArrayBuilderState,
    pub(crate) finished: bool,
}

impl<B> TupleStructBuilder<B> {
    pub fn new(nullable: Vec<bool>, builders: Vec<B>) -> Self {
        Self {
            builders,
            nullable,
            validity: Vec::new(),
            state: TupleArrayBuilderState::Start,
            finished: false,
        }
    }
}

impl<B: EventSink> EventSink for TupleStructBuilder<B> {
    fn accept(&mut self, event: Event<'_>) -> Result<()> {
        use TupleArrayBuilderState::*;

        self.state = match (self.state, event) {
            (Start, Event::StartTuple) => Value(0, 0),
            (Start, Event::Null) => {
                for builder in &mut self.builders {
                    builder.accept(Event::Default)?;
                }
                self.validity.push(false);
                Start
            }
            (Start, Event::Default) => {
                for builder in &mut self.builders {
                    builder.accept(Event::Default)?;
                }
                self.validity.push(true);
                Start
            }
            (Start, ev) if ev.is_marker() => Start,
            (Value(active, depth), ev) if ev.is_start() => {
                self.builders[active].accept(ev)?;
                Value(active, depth + 1)
            }
            (Value(_, 0), Event::EndTuple) => {
                self.validity.push(true);
                Start
            }
            (Value(_, 0), ev) if ev.is_end() => {
                fail!("Unbalanced opening / close events in TupleStructBuilder")
            }
            (Value(active, 1), ev) if ev.is_end() => {
                self.builders[active].accept(ev)?;
                Value(active + 1, 0)
            }
            (Value(active, 0), ev) if !ev.is_marker() => {
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

    fn finish(&mut self) -> Result<()> {
        for builder in &mut self.builders {
            builder.finish()?;
        }
        self.finished = true;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub enum TupleArrayBuilderState {
    Start,
    Value(usize, usize),
}

pub struct ListArrayBuilder<B, O> {
    pub builder: B,
    next: ListBuilderState,
    pub offsets: Vec<O>,
    pub validity: Vec<bool>,
    pub item_name: String,
    pub nullable: bool,
    pub finished: bool,
}

impl<B, O: Default> ListArrayBuilder<B, O> {
    pub fn new(builder: B, item_name: String, nullable: bool) -> Self {
        Self {
            builder,
            next: ListBuilderState::Start { offset: 0 },
            offsets: vec![Default::default()],
            validity: Vec::new(),
            item_name,
            nullable,
            finished: false,
        }
    }
}

impl<B, O> ListArrayBuilder<B, O> {
    fn finalize_item(&mut self, end_offset: O, nullable: bool) {
        self.offsets.push(end_offset);
        self.validity.push(!nullable);
    }
}

impl<B: EventSink, O: TryFrom<i64>> EventSink for ListArrayBuilder<B, O>
where
    Error: From<O::Error>,
{
    fn accept(&mut self, event: Event<'_>) -> Result<()> {
        use ListBuilderState::*;
        self.next = match self.next {
            Start { offset } => match &event {
                Event::StartSequence => Value { offset, depth: 0 },
                // ignore marker events
                ev if ev.is_marker() => Start { offset },
                Event::Null => {
                    self.finalize_item(offset.try_into()?, true);
                    Start { offset }
                }
                ev => fail!("Invalid event {ev} in state Start"),
            },
            Value { offset, depth } => match &event {
                ev if ev.is_start() => {
                    self.builder.accept(event)?;
                    Value {
                        offset,
                        depth: depth + 1,
                    }
                }
                ev if ev.is_end() => {
                    if depth != 0 {
                        self.builder.accept(event)?;
                        Value {
                            offset: if depth == 1 { offset + 1 } else { offset },
                            depth: depth - 1,
                        }
                    } else if matches!(ev, Event::EndSequence) {
                        self.finalize_item(offset.try_into()?, false);
                        Start { offset }
                    } else {
                        fail!("Invalid {ev} in list array")
                    }
                }
                ev if ev.is_marker() => {
                    self.builder.accept(event)?;
                    Value { offset, depth }
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

    fn finish(&mut self) -> Result<()> {
        self.builder.finish()?;
        self.finished = true;
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

pub struct UnionArrayBuilder<B> {
    next: UnionBuilderState,
    pub current_field_offsets: Vec<i32>,
    pub field_builders: Vec<B>,
    pub field_nullable: Vec<bool>,
    pub field_offsets: Vec<i32>,
    pub field_types: Vec<i8>,
    pub nullable: bool,
    pub finished: bool,
}

impl<B> UnionArrayBuilder<B> {
    pub fn new(field_builders: Vec<B>, field_nullable: Vec<bool>, nullable: bool) -> Self {
        let current_field_offsets = vec![0; field_builders.len()];
        Self {
            next: UnionBuilderState::Inactive,
            current_field_offsets,
            field_builders,
            field_nullable,
            field_offsets: Vec::new(),
            field_types: Vec::new(),
            nullable,
            finished: false,
        }
    }
}

impl<B: EventSink> EventSink for UnionArrayBuilder<B> {
    fn accept(&mut self, event: Event<'_>) -> Result<()> {
        type S = UnionBuilderState;
        type E<'a> = Event<'a>;

        self.next = match self.next {
            S::Inactive => match event {
                E::Variant(_, idx) | E::OwnedVariant(_, idx) => {
                    self.field_offsets.push(self.current_field_offsets[idx]);
                    self.current_field_offsets[idx] += 1;
                    self.field_types.push(i8::try_from(idx)?);

                    S::Active(idx, 0)
                }
                E::Null | E::Some => fail!("Nullable Union arrays are not supported"), 
                ev => fail!("Unexpected event {ev} in state Inactive of UnionArrayBuilder"),
            },
            S::Active(field, depth) => match event {
                ev if ev.is_start() => {
                    self.field_builders[field].accept(ev)?;
                    S::Active(field, depth + 1)
                }
                ev if ev.is_end() => {
                    match depth {
                        0 => fail!("Invalid end event {ev} in state Active({field}, {depth}) in UnionArrayBuilder"),
                        1 => {
                            self.field_builders[field].accept(ev)?;
                            S::Inactive
                        }
                        _ => {
                            self.field_builders[field].accept(ev)?;
                            S::Active(field, depth - 1)
                        }
                    }
                }
                ev if ev.is_marker() => {
                    self.field_builders[field].accept(ev)?;
                    S::Active(field, depth)
                }
                ev if ev.is_value() => {
                    self.field_builders[field].accept(ev)?;
                    match depth {
                        0 => S::Inactive,
                        _ => S::Active(field, depth),
                    }
                }
                _ => unreachable!(),
            }
        };
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        for builder in &mut self.field_builders {
            builder.finish()?;
        }
        self.finished = true;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub enum UnionBuilderState {
    Inactive,
    Active(usize, usize),
}

pub struct MapArrayBuilder<B> {
    next: MapBuilderState,
    pub key_builder: B,
    pub val_builder: B,
    pub offsets: Vec<i32>,
    pub offset: i32,
    pub validity: Vec<bool>,
    pub nullable: bool,
    pub finished: bool,
}

#[derive(Debug, Clone, Copy)]
enum MapBuilderState {
    Start,
    Key(usize),
    Value(usize),
}

impl<B> MapArrayBuilder<B> {
    pub fn new(key_builder: B, val_builder: B, nullable: bool) -> Self {
        Self {
            next: MapBuilderState::Start,
            key_builder,
            val_builder,
            offsets: vec![0],
            offset: 0,
            validity: Vec::new(),
            nullable,
            finished: false,
        }
    }
}

impl<B: EventSink> EventSink for MapArrayBuilder<B> {
    fn accept(&mut self, event: Event<'_>) -> Result<()> {
        type S = MapBuilderState;
        type E<'a> = Event<'a>;

        self.next = match self.next {
            S::Start => match event {
                E::StartMap => S::Key(0),
                E::Null => {
                    self.offsets.push(self.offset);
                    self.validity.push(false);
                    S::Start
                }
                E::Some => S::Start,
                ev => fail!("Unexpected event {ev} in state Start of MapArrayBuilder"),
            },
            S::Key(depth) => match event {
                E::EndMap if depth == 0 => {
                    self.offsets.push(self.offset);
                    self.validity.push(true);
                    S::Start
                }
                ev if ev.is_start() => {
                    self.key_builder.accept(ev)?;
                    S::Key(depth + 1)
                }
                ev if ev.is_end() => match depth {
                    0 => fail!("Unexpected event {ev} in state Key(0) in MapArrayBuilder"),
                    1 => {
                        self.key_builder.accept(ev)?;
                        S::Value(0)
                    }
                    _ => {
                        self.key_builder.accept(ev)?;
                        S::Key(depth - 1)
                    }
                },
                ev if ev.is_marker() => {
                    self.key_builder.accept(ev)?;
                    S::Key(depth)
                }
                ev if ev.is_value() => {
                    self.key_builder.accept(ev)?;
                    if depth == 0 {
                        S::Value(0)
                    } else {
                        S::Key(depth)
                    }
                }
                _ => unreachable!(),
            },
            S::Value(depth) => match event {
                ev if ev.is_start() => {
                    self.val_builder.accept(ev)?;
                    S::Value(depth + 1)
                }
                ev if ev.is_end() => match depth {
                    0 => fail!("Unexpected event {ev} in state Value(0) of MapArrayBuilder"),
                    1 => {
                        self.val_builder.accept(ev)?;
                        self.offset += 1;
                        S::Key(0)
                    }
                    _ => {
                        self.val_builder.accept(ev)?;
                        S::Value(depth - 1)
                    }
                },
                ev if ev.is_marker() => {
                    self.val_builder.accept(ev)?;
                    S::Value(depth)
                }
                ev if ev.is_value() => {
                    self.val_builder.accept(ev)?;
                    if depth == 0 {
                        self.offset += 1;
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
        self.key_builder.finish()?;
        self.val_builder.finish()?;
        self.finished = true;
        Ok(())
    }
}