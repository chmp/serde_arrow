use crate::{
    base::{Event, EventSink},
    internal::{
        error::{error, fail},
        schema::FieldMeta,
        sink::{macros, ArrayBuilder},
    },
    Result,
};

pub struct StructArrayBuilder<B> {
    pub(crate) field_meta: Vec<FieldMeta>,
    /// the builders of the sub arrays
    pub(crate) builders: Vec<B>,
    /// the validity of the items
    pub(crate) validity: Vec<bool>,
    pub(crate) state: StructArrayBuilderState,
    pub(crate) seen: Vec<bool>,
    pub(crate) finished: bool,
}

impl<B> StructArrayBuilder<B> {
    pub fn new(field_meta: Vec<FieldMeta>, builders: Vec<B>) -> Self {
        let num_columns = field_meta.len();
        Self {
            field_meta,
            builders,
            validity: Vec::new(),
            state: StructArrayBuilderState::Start,
            seen: vec![false; num_columns],
            finished: false,
        }
    }

    pub fn build_arrays<A>(&mut self) -> Result<Vec<A>>
    where
        B: ArrayBuilder<A>,
    {
        if !self.finished {
            fail!("Cannot build array from unfinished StructArrayBuilder");
        }

        let values: Result<Vec<A>> = self.builders.iter_mut().map(|b| b.build_array()).collect();
        let values = values?;
        Ok(values)
    }
}

#[derive(Debug, Clone, Copy)]
pub enum StructArrayBuilderState {
    Start,
    Field,
    Value(usize, usize),
}

impl<B: EventSink> EventSink for StructArrayBuilder<B> {
    macros::forward_generic_to_specialized!();
    macros::accept_start!((this, ev, val, next) {
        use StructArrayBuilderState::*;
        this.state = match this.state {
            Start => {
                if matches!(ev, Event::StartStruct | Event::StartMap) {
                    this.seen = vec![false; this.field_meta.len()];
                    Field
                } else {
                    fail!(
                            "Expected StartStruct, StartMap or marker in StructArrayBuilder, not {ev}"
                        )
                }
            }
            Field => fail!("Unexpected event while waiting for field: {ev}"),
            Value(active, depth) => {
                next(&mut this.builders[active], val)?;
                Value(active, depth + 1)
            }
        };
        Ok(())
    });
    macros::accept_end!((this, ev, val, next) {
        use StructArrayBuilderState::*;

        this.state = match this.state {
            Start => fail!(
                "Expected StartStruct, StartMap or marker in StructArrayBuilder, not {ev}"
            ),
            Field => if matches!(ev, Event::EndStruct | Event::EndMap) {
                for (idx, seen) in this.seen.iter().enumerate() {
                    if !seen {
                        if !this.field_meta[idx].nullable {
                            fail!("Missing field {} is not nullable", this.field_meta[idx].name);
                        }
                        this.builders[idx].accept_null()?;
                    }
                }
                this.validity.push(true);
                Start
            } else {
                fail!("Unexpected event while waiting for field: {ev}")
            }
            Value(active, depth) => {
                next(&mut this.builders[active], val)?;
                match depth {
                    // the last closing event for the current value
                    1 => Field,
                    // TODO: check is this event possible?
                    0 => fail!("Unbalanced opening / close events in StructArrayBuilder"),
                    _ => Value(active, depth - 1),
                }
            }
        };

        Ok(())
    });
    macros::accept_marker!((this, ev, val, next) {
        use StructArrayBuilderState::*;

        this.state = match this.state {
            Start => Start,
            Field => fail!("Unexpected event while waiting for field: {ev}"),
            Value(active, depth) => {
                next(&mut this.builders[active], val)?;
                Value(active, depth)
            }
        };

        Ok(())
    });
    macros::accept_value!((this, ev, val, next) {
        use StructArrayBuilderState::*;

        this.state = match this.state {
            Start => {
                if matches!(ev, Event::Null) {
                    for b in &mut this.builders {
                        // NOTE: Don't use null so the underlying arrays don't
                        // have to be nullable
                        b.accept_default()?;
                    }
                    this.validity.push(false);
                } else if matches!(ev, Event::Default) {
                    for b in &mut this.builders {
                        b.accept_default()?;
                    }
                    this.validity.push(true);
                } else {
                    fail!(
                    "Expected StartStruct, StartMap or marker in StructArrayBuilder, not {ev}"
                    )
                }
                Start
            }
            Field => {
                let key = match ev {
                    Event::Str(key) => key,
                    Event::OwnedStr(ref key) => key,
                    ev => fail!("Unexpected event while waiting for field: {ev}"),
                };

                let idx = this
                    .field_meta
                    .iter()
                    .position(|m| m.name == key)
                    .ok_or_else(|| error!("unknown field {key}"))?;
                if this.seen[idx] {
                    fail!("Duplicate field {}", this.field_meta[idx].name);
                }
                this.seen[idx] = true;
                Value(idx, 0)
            }
            Value(active, depth) => {
                next(&mut this.builders[active], val)?;
                if depth == 0 {
                    Field
                } else {
                    Value(active, depth)
                }
            }
        };

        Ok(())
    });

    fn finish(&mut self) -> Result<()> {
        if !matches!(self.state, StructArrayBuilderState::Start) {
            fail!("Invalid state at array construction");
        }
        for builder in &mut self.builders {
            builder.finish()?;
        }
        self.finished = true;
        Ok(())
    }
}
