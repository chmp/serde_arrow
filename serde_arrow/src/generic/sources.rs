use crate::{
    base::{
        error::{error, fail},
        source::{DynamicSource, PeekableEventSource},
        Event, EventSource,
    },
    Result,
};

pub struct RecordSource<'a, S: EventSource<'a> + 'a> {
    columns: Vec<&'a str>,
    values: Vec<PeekableEventSource<'a, S>>,
    next: RecordSourceState,
}

impl<'a, S: EventSource<'a> + 'a> RecordSource<'a, S> {
    pub fn new(columns: Vec<&'a str>, values: Vec<S>) -> Self {
        let values = values.into_iter().map(PeekableEventSource::new).collect();

        RecordSource {
            columns,
            values,
            next: RecordSourceState::StartSequence,
        }
    }
}

impl<'a, S: EventSource<'a> + 'a> EventSource<'a> for RecordSource<'a, S> {
    fn next(&mut self) -> Result<Option<Event<'a>>> {
        use RecordSourceState::*;
        type E<'a> = Event<'a>;

        match self.next {
            StartSequence => {
                self.next = StartMapOrEnd;
                Ok(Some(E::StartSequence))
            }
            StartMapOrEnd => {
                if self.columns.is_empty() || self.values[0].peek()?.is_none() {
                    self.next = Done;
                    Ok(Some(E::EndSequence))
                } else {
                    self.next = Key(0);
                    Ok(Some(E::StartStruct))
                }
            }
            Key(i) if i >= self.columns.len() => {
                self.next = StartMapOrEnd;
                Ok(Some(E::EndStruct))
            }
            Key(i) => {
                self.next = Value(i, 0);
                Ok(Some(E::Str(self.columns[i])))
            }
            Value(i, depth) => {
                let ev = self.values[i]
                    .next()?
                    .ok_or_else(|| error!("Unbalanced values"))?;

                self.next = match (&ev, depth) {
                    (E::StartStruct | E::StartSequence | E::StartTuple | E::StartMap, _) => {
                        Value(i, depth + 1)
                    }
                    (E::EndStruct | E::EndSequence | E::EndTuple | E::EndMap, 0) => {
                        fail!("Invalid nested value")
                    }
                    (E::EndStruct | E::EndSequence | E::EndTuple | E::EndMap, 1) => Key(i + 1),
                    (E::EndStruct | E::EndSequence | E::EndTuple | E::EndMap, _) => {
                        Value(i, depth - 1)
                    }
                    (_, 0) => Key(i + 1),
                    _ => Value(i, depth),
                };

                Ok(Some(ev))
            }
            Done => Ok(None),
        }
    }
}

#[derive(Clone, Copy)]
enum RecordSourceState {
    StartSequence,
    StartMapOrEnd,
    Key(usize),
    Value(usize, usize),
    Done,
}

pub struct StructSource<'a> {
    fields: Vec<&'a str>,
    values: Vec<PeekableEventSource<'a, DynamicSource<'a>>>,
    validity: Vec<bool>,
    next: StructSourceState,
    offset: usize,
}

impl<'a> StructSource<'a> {
    pub fn new(fields: Vec<&'a str>, validity: Vec<bool>, values: Vec<DynamicSource<'a>>) -> Self {
        let values = values.into_iter().map(PeekableEventSource::new).collect();
        Self {
            fields,
            values,
            validity,
            next: StructSourceState::Start,
            offset: 0,
        }
    }
}

impl<'a> EventSource<'a> for StructSource<'a> {
    fn next(&mut self) -> Result<Option<Event<'a>>>
    where
        Self: 'a,
    {
        use StructSourceState::*;

        match self.next {
            Start => {
                if self.fields.is_empty() || self.values[0].peek()?.is_none() {
                    Ok(None)
                } else if !self.validity[self.offset] {
                    for val in &mut self.values {
                        val.next()?
                            .ok_or_else(|| error!("Null structs must still contain values"))?;
                    }
                    Ok(Some(Event::Null))
                } else {
                    self.next = Key(0);
                    self.offset += 1;
                    Ok(Some(Event::StartStruct))
                }
            }
            Key(i) if i >= self.fields.len() => {
                self.next = Start;
                Ok(Some(Event::EndStruct))
            }
            Key(i) => {
                self.next = Value(i, 0);
                Ok(Some(Event::Str(self.fields[i])))
            }
            Value(i, depth) => {
                let ev = self.values[i]
                    .next()?
                    .ok_or_else(|| error!("unbalanced array"))?;
                self.next = match (&ev, depth) {
                    (
                        Event::StartStruct
                        | Event::StartSequence
                        | Event::StartMap
                        | Event::StartTuple,
                        _,
                    ) => Value(i, depth + 1),
                    (
                        Event::EndStruct | Event::EndSequence | Event::EndTuple | Event::EndMap,
                        0,
                    ) => fail!("Invalid nested value"),
                    (
                        Event::EndStruct | Event::EndSequence | Event::EndTuple | Event::EndMap,
                        1,
                    ) => Key(i + 1),
                    (
                        Event::EndStruct | Event::EndSequence | Event::EndTuple | Event::EndMap,
                        _,
                    ) => Value(i, depth - 1),
                    (_, 0) => Key(i + 1),
                    _ => Value(i, depth),
                };
                Ok(Some(ev))
            }
        }
    }
}

enum StructSourceState {
    Start,
    Key(usize),
    Value(usize, usize),
}

pub struct TupleSource<'a> {
    values: Vec<PeekableEventSource<'a, DynamicSource<'a>>>,
    next: TupleSourceState,
}

impl<'a> TupleSource<'a> {
    pub fn new(values: Vec<DynamicSource<'a>>) -> Self {
        let values = values.into_iter().map(PeekableEventSource::new).collect();
        Self {
            values,
            next: TupleSourceState::Start,
        }
    }
}

impl<'a> EventSource<'a> for TupleSource<'a> {
    fn next(&mut self) -> Result<Option<Event<'a>>>
    where
        Self: 'a,
    {
        use TupleSourceState::*;

        match self.next {
            Start => {
                if self.values.is_empty() || self.values[0].peek()?.is_none() {
                    Ok(None)
                } else {
                    self.next = Value(0, 0);
                    Ok(Some(Event::StartTuple))
                }
            }
            Value(i, _) if i >= self.values.len() => {
                self.next = Start;
                Ok(Some(Event::EndTuple))
            }
            Value(i, depth) => {
                let ev = self.values[i]
                    .next()?
                    .ok_or_else(|| error!("unbalanced array"))?;
                self.next = match (&ev, depth) {
                    (
                        Event::StartStruct
                        | Event::StartSequence
                        | Event::StartMap
                        | Event::StartTuple,
                        _,
                    ) => Value(i, depth + 1),
                    (
                        Event::EndStruct | Event::EndSequence | Event::EndTuple | Event::EndMap,
                        0,
                    ) => fail!("Invalid nested value"),
                    (
                        Event::EndStruct | Event::EndSequence | Event::EndTuple | Event::EndMap,
                        1,
                    ) => Value(i + 1, 0),
                    (
                        Event::EndStruct | Event::EndSequence | Event::EndTuple | Event::EndMap,
                        _,
                    ) => Value(i, depth - 1),
                    (_, 0) => Value(i + 1, 0),
                    _ => Value(i, depth),
                };
                Ok(Some(ev))
            }
        }
    }
}

enum TupleSourceState {
    Start,
    Value(usize, usize),
}

pub struct ListSource<'a> {
    values: DynamicSource<'a>,
    offsets: Vec<usize>,
    validity: Vec<bool>,
    state: ListSourceState,
}

impl<'a> ListSource<'a> {
    pub fn new(values: DynamicSource<'a>, offsets: Vec<usize>, validity: Vec<bool>) -> Self {
        Self {
            values,
            offsets,
            validity,
            state: ListSourceState::Start {
                outer: 0,
                offset: 0,
            },
        }
    }
}

impl<'a> EventSource<'a> for ListSource<'a> {
    fn next(&mut self) -> Result<Option<Event<'a>>> {
        use ListSourceState::*;
        let res;

        (self.state, res) = match self.state {
            Start { outer, offset } => {
                if outer >= self.validity.len() {
                    return Ok(None);
                }

                if !self.validity[outer] {
                    (
                        Start {
                            outer: outer + 1,
                            offset,
                        },
                        Some(Event::Null),
                    )
                } else {
                    (
                        Value {
                            outer,
                            offset,
                            depth: 0,
                        },
                        Some(Event::StartSequence),
                    )
                }
            }
            Value {
                outer,
                offset,
                depth,
            } => {
                if offset >= self.offsets[outer + 1] {
                    if depth != 0 {
                        fail!("Internal error: ended sequence at non-zero depth");
                    }
                    (
                        Start {
                            outer: outer + 1,
                            offset,
                        },
                        Some(Event::EndSequence),
                    )
                } else {
                    let ev = self.values.next()?;

                    match &ev {
                        Some(Event::StartSequence | Event::StartStruct) => (
                            Value {
                                outer,
                                offset,
                                depth: depth + 1,
                            },
                            ev,
                        ),
                        Some(Event::EndSequence | Event::EndStruct) => {
                            let offset = match depth {
                                0 => fail!("Internal error: ended sequence at zero depth"),
                                1 => offset + 1,
                                _ => offset,
                            };
                            (
                                Value {
                                    outer,
                                    offset,
                                    depth: depth - 1,
                                },
                                ev,
                            )
                        }
                        Some(_) => {
                            let offset = if depth == 0 { offset + 1 } else { offset };
                            (
                                Value {
                                    outer,
                                    offset,
                                    depth,
                                },
                                ev,
                            )
                        }
                        None => fail!("Unexpected end of value source"),
                    }
                }
            }
        };
        Ok(res)
    }
}

#[derive(Debug, Clone, Copy)]
enum ListSourceState {
    Start {
        outer: usize,
        offset: usize,
    },
    Value {
        outer: usize,
        offset: usize,
        depth: usize,
    },
}
