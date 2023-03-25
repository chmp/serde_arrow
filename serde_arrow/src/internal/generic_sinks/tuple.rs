use crate::{
    base::{Event, EventSink},
    internal::{
        error::{fail, Result},
        sink::macros,
    },
};

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

#[derive(Debug, Clone, Copy)]
pub enum TupleArrayBuilderState {
    Start,
    Value(usize, usize),
}

impl<B: EventSink> EventSink for TupleStructBuilder<B> {
    macros::forward_generic_to_specialized!();
    macros::accept_start!((this, ev, val, next) {
        use TupleArrayBuilderState::*;

        this.state = match this.state {
            Start => {
                if matches!(ev, Event::StartTuple) {
                    Value(0, 0)
                } else {
                    fail!("Invalid event {ev} in state {:?}", this.state)
                }
            }
            Value(active, depth) => {
                next(&mut this.builders[active], val)?;
                Value(active, depth + 1)
            }
        };
        Ok(())
    });
    macros::accept_end!((this, ev, val, next) {
        use TupleArrayBuilderState::*;

        this.state = match this.state {
            Start => fail!("Invalid event {ev} in state {:?}", this.state),
            Value(_, 0) => {
                if matches!(ev, Event::EndTuple) {
                    this.validity.push(true);
                    Start
                } else {
                    fail!("Unbalanced opening / close events in TupleStructBuilder")
                }
            }
            Value(active, depth) => {
                next(&mut this.builders[active], val)?;
                Value(active + 1, depth - 1)
            }
        };
        Ok(())
    });
    macros::accept_marker!((this, ev, val, next) {
        use TupleArrayBuilderState::*;

        this.state = match this.state {
            Start => Start,
            Value(_, 0) => {
                fail!("Invalid event {ev} in state {:?}", this.state)
            }
            Value(active, depth) => {
                next(&mut this.builders[active], val)?;
                Value(active, depth)
            }
        };
        Ok(())
    });
    macros::accept_value!((this, ev, val, next) {
        use TupleArrayBuilderState::*;

        this.state = match this.state {
            Start => {
                if matches!(ev, Event::Null) {
                    for builder in &mut this.builders {
                        builder.accept_default()?;
                    }
                    this.validity.push(false);
                    Start
                } else if matches!(ev, Event::Default) {
                    for builder in &mut this.builders {
                        builder.accept_default()?;
                    }
                    this.validity.push(true);
                    Start
                } else {
                    fail!("Invalid event {ev} in state {:?}", this.state)
                }
            }
            Value(active, 0) => {
                next(&mut this.builders[active], val)?;
                Value(active + 1, 0)
            }
            Value(active, depth) => {
                next(&mut this.builders[active], val)?;
                Value(active, depth)
            }
        };
        Ok(())
    });

    fn finish(&mut self) -> Result<()> {
        if !matches!(self.state, TupleArrayBuilderState::Start) {
            fail!("Invalid state at array construction");
        }
        for builder in &mut self.builders {
            builder.finish()?;
        }
        self.finished = true;
        Ok(())
    }
}
