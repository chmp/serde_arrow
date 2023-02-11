use crate::{
    base::{
        error::{error, fail},
        source::{DynamicSource, PeekableEventSource},
        Event, EventSource,
    },
    Result,
};

pub struct StructSource<'a> {
    as_map: bool,
    fields: Vec<&'a str>,
    values: Vec<PeekableEventSource<'a, DynamicSource<'a>>>,
    validity: Vec<bool>,
    next: StructSourceState,
    offset: usize,
}

impl<'a> StructSource<'a> {
    pub fn new(
        fields: Vec<&'a str>,
        validity: Vec<bool>,
        values: Vec<DynamicSource<'a>>,
        as_map: bool,
    ) -> Self {
        let values = values.into_iter().map(PeekableEventSource::new).collect();
        Self {
            as_map,
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
                        consume_value(val)?;
                    }
                    self.offset += 1;
                    Ok(Some(Event::Null))
                } else {
                    self.next = Key(0);
                    self.offset += 1;
                    if !self.as_map {
                        Ok(Some(Event::StartStruct))
                    } else {
                        Ok(Some(Event::StartMap))
                    }
                }
            }
            Key(i) if i >= self.fields.len() => {
                self.next = Start;
                if !self.as_map {
                    Ok(Some(Event::EndStruct))
                } else {
                    Ok(Some(Event::EndMap))
                }
            }
            Key(i) => {
                self.next = Value(i, 0);
                Ok(Some(Event::Str(self.fields[i])))
            }
            Value(i, depth) => {
                let ev = self.values[i]
                    .next()?
                    .ok_or_else(|| error!("unbalanced array"))?;

                self.next = match &ev {
                    ev if ev.is_start() => Value(i, depth + 1),
                    ev if ev.is_end() => match depth {
                        0 => fail!("Invalid nested value"),
                        1 => Key(i + 1),
                        _ => Value(i, depth - 1),
                    },
                    ev if ev.is_marker() => Value(i, depth),
                    ev if ev.is_value() => match depth {
                        0 => Key(i + 1),
                        _ => Value(i, depth),
                    },
                    _ => unreachable!(),
                };

                Ok(Some(ev))
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum StructSourceState {
    Start,
    Key(usize),
    Value(usize, usize),
}

pub struct TupleSource<'a> {
    values: Vec<PeekableEventSource<'a, DynamicSource<'a>>>,
    next: TupleSourceState,
    validity: Vec<bool>,
    offset: usize,
}

impl<'a> TupleSource<'a> {
    pub fn new(validity: Vec<bool>, values: Vec<DynamicSource<'a>>) -> Self {
        let values = values.into_iter().map(PeekableEventSource::new).collect();
        Self {
            values,
            validity,
            offset: 0,
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
                } else if !self.validity[self.offset] {
                    self.offset += 1;
                    for val in &mut self.values {
                        consume_value(val)?;
                    }
                    Ok(Some(Event::Null))
                } else {
                    self.offset += 1;
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

        self.state = match self.state {
            Start { outer, offset } => {
                if outer >= self.validity.len() {
                    return Ok(None);
                }

                if !self.validity[outer] {
                    res = Event::Null;
                    Start {
                        outer: outer + 1,
                        offset,
                    }
                } else {
                    res = Event::StartSequence;
                    Value {
                        outer,
                        offset,
                        depth: 0,
                    }
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
                    res = Event::EndSequence;
                    Start {
                        outer: outer + 1,
                        offset,
                    }
                } else {
                    res = self
                        .values
                        .next()?
                        .ok_or_else(|| error!("Unexpected end of value source"))?;

                    match &res {
                        ev if ev.is_start() => Value {
                            outer,
                            offset,
                            depth: depth + 1,
                        },
                        ev if ev.is_end() => match depth {
                            0 => fail!("Internal error: ended sequence at zero depth"),
                            1 => Value {
                                outer,
                                offset: offset + 1,
                                depth: depth - 1,
                            },
                            _ => Value {
                                outer,
                                offset,
                                depth: depth - 1,
                            },
                        },
                        ev if ev.is_marker() => Value {
                            outer,
                            offset,
                            depth,
                        },
                        ev if ev.is_value() => match depth {
                            0 => Value {
                                outer,
                                offset: offset + 1,
                                depth,
                            },
                            _ => Value {
                                outer,
                                offset,
                                depth,
                            },
                        },
                        _ => unreachable!(),
                    }
                }
            }
        };
        Ok(Some(res))
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

pub struct UnionSource<'a> {
    next: UnionSourceState,
    sources: Vec<DynamicSource<'a>>,
    names: Vec<&'a str>,
    types: Vec<u8>,
}

impl<'a> UnionSource<'a> {
    pub fn new(names: Vec<&'a str>, sources: Vec<DynamicSource<'a>>, types: Vec<u8>) -> Self {
        Self {
            next: UnionSourceState::Start { offset: 0 },
            sources,
            names,
            types,
        }
    }
}

impl<'a> EventSource<'a> for UnionSource<'a> {
    fn next(&mut self) -> Result<Option<Event<'a>>> {
        type S = UnionSourceState;
        type E<'a> = Event<'a>;

        let res;
        self.next = match self.next {
            S::Start { offset } => {
                if offset >= self.types.len() {
                    return Ok(None);
                }

                let variant = self.types[offset] as usize;
                res = E::Variant(self.names[variant], variant);
                S::Value {
                    offset,
                    variant,
                    depth: 0,
                }
            }
            S::Value {
                offset,
                variant,
                depth,
            } => {
                res = self.sources[variant].next()?.ok_or_else(|| {
                    error!("Unexpected end of child array {variant} in UnionSource")
                })?;

                match &res {
                    ev if ev.is_start() => S::Value {
                        offset,
                        variant,
                        depth: depth + 1,
                    },
                    ev if ev.is_end() => match depth {
                        0 => fail!("Unexpected end event in UnionSource"),
                        1 => S::Start { offset: offset + 1 },
                        _ => S::Value {
                            offset,
                            variant,
                            depth: depth - 1,
                        },
                    },
                    ev if ev.is_marker() => S::Value {
                        offset,
                        variant,
                        depth,
                    },
                    ev if ev.is_value() => match depth {
                        0 => S::Start { offset: offset + 1 },
                        _ => S::Value {
                            offset,
                            variant,
                            depth,
                        },
                    },
                    _ => unreachable!(),
                }
            }
        };
        Ok(Some(res))
    }
}

#[derive(Debug, Clone, Copy)]
enum UnionSourceState {
    Start {
        offset: usize,
    },
    Value {
        offset: usize,
        variant: usize,
        depth: usize,
    },
}

pub struct MapSource<'a> {
    key_source: DynamicSource<'a>,
    val_source: DynamicSource<'a>,
    offsets: Vec<usize>,
    validity: Vec<bool>,
    next: MapSourceState,
}

#[derive(Debug, Clone, Copy)]
enum MapSourceState {
    Start {
        outer: usize,
    },
    Key {
        outer: usize,
        inner: usize,
        depth: usize,
    },
    Value {
        outer: usize,
        inner: usize,
        depth: usize,
    },
}

impl<'a> MapSource<'a> {
    pub fn new(
        key_source: DynamicSource<'a>,
        val_source: DynamicSource<'a>,
        offsets: Vec<usize>,
        validity: Vec<bool>,
    ) -> Self {
        Self {
            next: MapSourceState::Start { outer: 0 },
            key_source,
            val_source,
            validity,
            offsets,
        }
    }
}

impl<'a> EventSource<'a> for MapSource<'a> {
    fn next(&mut self) -> Result<Option<Event<'a>>> {
        type S = MapSourceState;
        type E<'a> = Event<'a>;

        let old_state = self.next;

        let res: Event;
        self.next =
            match self.next {
                S::Start { outer } if outer >= self.validity.len() => return Ok(None),
                S::Start { outer } if !self.validity[outer] => {
                    res = E::Null;
                    S::Start { outer: outer + 1 }
                }
                S::Start { outer } => {
                    res = E::StartMap;
                    S::Key {
                        outer,
                        inner: self.offsets[outer],
                        depth: 0,
                    }
                }
                S::Key { outer, inner, .. } if inner >= self.offsets[outer + 1] => {
                    res = E::EndMap;
                    S::Start { outer: outer + 1 }
                }
                S::Key {
                    outer,
                    inner,
                    depth,
                } => {
                    res = self.key_source.next()?.ok_or_else(|| {
                        error!("Unexpected early stop of key_source in MapSource")
                    })?;
                    match &res {
                        ev if ev.is_start() => S::Key {
                            outer,
                            inner,
                            depth: depth + 1,
                        },
                        ev if ev.is_end() && depth == 0 => {
                            fail!("Invalid close event {ev} at depth 0 in MapSource")
                        }
                        ev if ev.is_end() && depth == 1 => S::Value {
                            outer,
                            inner,
                            depth: 0,
                        },
                        ev if ev.is_end() => S::Key {
                            outer,
                            inner,
                            depth: depth - 1,
                        },
                        ev if ev.is_marker() => S::Key {
                            outer,
                            inner,
                            depth,
                        },
                        ev if ev.is_value() && depth == 0 => S::Value {
                            outer,
                            inner,
                            depth: 0,
                        },
                        ev if ev.is_value() => S::Key {
                            outer,
                            inner,
                            depth,
                        },
                        _ => unreachable!(),
                    }
                }
                S::Value {
                    outer,
                    inner,
                    depth,
                } => {
                    res = self.val_source.next()?.ok_or_else(|| {
                        error!("Unexpected early stop of val_source in MapSource")
                    })?;
                    match &res {
                        ev if ev.is_start() => S::Value {
                            outer,
                            inner,
                            depth: depth + 1,
                        },
                        ev if ev.is_end() && depth == 0 => {
                            fail!("Invalid close event {ev} at depth 0 in MapSource")
                        }
                        ev if ev.is_end() && depth == 1 => S::Key {
                            outer,
                            inner: inner + 1,
                            depth: 0,
                        },
                        ev if ev.is_end() => S::Value {
                            outer,
                            inner,
                            depth: depth - 1,
                        },
                        ev if ev.is_marker() => S::Value {
                            outer,
                            inner,
                            depth,
                        },
                        ev if ev.is_value() && depth == 0 => S::Key {
                            outer,
                            inner: inner + 1,
                            depth: 0,
                        },
                        ev if ev.is_value() => S::Value {
                            outer,
                            inner,
                            depth,
                        },
                        _ => unreachable!(),
                    }
                }
            };
        println!("{old_state:?} {res}");

        Ok(Some(res))
    }
}

// consume a complete value
fn consume_value<'a, S: EventSource<'a>>(source: &mut S) -> Result<()> {
    let mut depth = 0;

    while let Some(ev) = source.next()? {
        if ev.is_start() {
            depth += 1;
        } else if ev.is_end() && depth > 1 {
            depth -= 1;
        } else if (ev.is_value() && depth == 0) || (ev.is_end() && depth == 1) {
            return Ok(());
        }
    }
    fail!("Could not consume value");
}
