use crate::internal::{
    error::{fail, Result},
    event::Event,
    schema::GenericField,
    sink::{macros, EventSink},
};

pub struct TupleStructBuilder<B> {
    pub(crate) path: String,
    pub(crate) field: GenericField,
    pub(crate) builders: Vec<B>,
    pub(crate) validity: Vec<bool>,
    pub(crate) state: TupleArrayBuilderState,
    pub(crate) finished: bool,
}

impl<B> TupleStructBuilder<B> {
    pub fn new(path: String, field: GenericField, builders: Vec<B>) -> Self {
        Self {
            path,
            field,
            builders,
            validity: Vec::new(),
            state: TupleArrayBuilderState::WaitForStart,
            finished: false,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum TupleArrayBuilderState {
    WaitForStart,
    WaitForItem(usize),
    Item(usize, usize),
}

impl<B: EventSink> EventSink for TupleStructBuilder<B> {
    macros::forward_generic_to_specialized!();
    macros::accept_start!((this, ev, val, next) {
        use {TupleArrayBuilderState as S, Event as E};
        this.state = match (this.state, ev) {
            (S::WaitForStart, E::StartTuple) => S::WaitForItem(0),
            (S::Item(active, depth), _) => {
                next(&mut this.builders[active], val)?;
                S::Item(active, depth + 1)
            }
            (state, ev) => fail!("TupleStructBuilder: Invalid event {ev} in state {state:?} [{path}]", path=this.path),
        };
        Ok(())
    });
    macros::accept_end!((this, ev, val, next) {
        use {TupleArrayBuilderState as S, Event as E};
        this.state = match (this.state, ev) {
            (S::Item(active, 1), _) => {
                next(&mut this.builders[active], val)?;
                S::WaitForItem(active + 1)
            }
            (S::Item(active, depth), _)  if depth > 1 => {
                next(&mut this.builders[active], val)?;
                S::Item(active, depth - 1)
            }
            (S::WaitForItem(_), E::EndTuple) => {
                this.validity.push(true);
                S::WaitForStart
            },
            (state, ev) => fail!("TupleStructBuilder: Invalid event {ev} in state {state:?} [{path}]", path=this.path),
        };
        Ok(())
    });
    macros::accept_marker!((this, ev, val, next) {
        use {TupleArrayBuilderState as S, Event as E};
        this.state = match (this.state, ev) {
            (S::WaitForStart, E::Some) => S::WaitForStart,
            (S::WaitForItem(field), E::Some) => S::WaitForItem(field),
            (S::WaitForItem(field), E::Item) => S::Item(field, 0),
            (S::Item(active, depth), _) => {
                next(&mut this.builders[active], val)?;
                S::Item(active, depth)
            }
            (state, ev) => fail!("TupleStructBuilder: Invalid event {ev} in state {state:?} [{path}]", path=this.path),
        };
        Ok(())
    });
    macros::accept_value!((this, ev, val, next) {
        use {TupleArrayBuilderState as S, Event as E};
        this.state = match (this.state, ev) {
            (S::WaitForStart, E::Null) => {
                for builder in &mut this.builders {
                    builder.accept_default()?;
                }
                this.validity.push(false);
                S::WaitForStart
            }
            (S::WaitForStart, Event::Default) => {
                for builder in &mut this.builders {
                    builder.accept_default()?;
                }
                this.validity.push(true);
                S::WaitForStart
            } 
            (S::Item(active, 0), _) => {
                next(&mut this.builders[active], val)?;
                S::WaitForItem(active + 1)
            }
            (S::Item(active, depth), _) => {
                next(&mut this.builders[active], val)?;
                S::Item(active, depth)
            }
            (state, ev) => fail!("TupleStructBuilder: Invalid event {ev} in state {state:?} [{path}]", path=this.path),
        };
        Ok(())
    });

    fn finish(&mut self) -> Result<()> {
        use TupleArrayBuilderState as S;
        if !matches!(self.state, S::WaitForStart) {
            fail!(
                "Invalid state {:?} in finish [TupleStructBuilder]",
                self.state
            );
        }
        for builder in &mut self.builders {
            builder.finish()?;
        }
        self.finished = true;
        Ok(())
    }
}
