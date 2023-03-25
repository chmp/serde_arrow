use crate::{
    base::{Event, EventSink},
    internal::{
        error::{error, fail},
        sink::macros,
    },
    Result,
};

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
                    this.seen = vec![false; this.columns.len()];
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
                        if !this.nullable[idx] {
                            fail!("Missing field {} is not nullable", this.columns[idx]);
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
                    .columns
                    .iter()
                    .position(|col| col == key)
                    .ok_or_else(|| error!("unknown field {key}"))?;
                if this.seen[idx] {
                    fail!("Duplicate field {}", this.columns[idx]);
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
