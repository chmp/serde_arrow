use crate::internal::{
    error::{fail, Error, Result},
    event::Event,
    schema::GenericField,
    sink::{macros, EventSink},
};

pub struct ListArrayBuilder<B, O> {
    pub field: GenericField,
    pub builder: B,
    next: ListBuilderState,
    pub offsets: Vec<O>,
    pub validity: Vec<bool>,
    pub finished: bool,
}

impl<B, O: Default> ListArrayBuilder<B, O> {
    pub fn new(field: GenericField, builder: B) -> Self {
        Self {
            field,
            builder,
            next: ListBuilderState::Start { offset: 0 },
            offsets: vec![Default::default()],
            validity: Vec::new(),
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

impl<B: EventSink, O: TryFrom<i64>> EventSink for ListArrayBuilder<B, O>
where
    Error: From<O::Error>,
{
    macros::forward_generic_to_specialized!();
    macros::accept_start!((this, ev, val, next) {
        use ListBuilderState::*;
        this.next = match this.next {
            Start { offset } => {
                if matches!(ev, Event::StartSequence) {
                    Value { offset, depth: 0 }
                } else {
                    fail!("Invalid event {ev} in state Start")
                }
            }
            Value { offset, depth } => {
                next(&mut this.builder, val)?;
                Value {
                    offset,
                    depth: depth + 1,
                }
            }
        };

        Ok(())
    });
    macros::accept_end!((this, ev, val, next) {
        use ListBuilderState::*;
        this.next = match this.next {
            Start { .. } => fail!("Invalid event {ev} in state Start"),
            Value { offset, depth } => {
                if depth != 0 {
                    next(&mut this.builder, val)?;
                    Value {
                        offset: if depth == 1 { offset + 1 } else { offset },
                        depth: depth - 1,
                    }
                } else if matches!(ev, Event::EndSequence) {
                    this.finalize_item(offset.try_into()?, false);
                    Start { offset }
                } else {
                    fail!("Invalid {ev} in list array")
                }
            }
        };

        Ok(())
    });
    macros::accept_marker!((this, _ev, val, next) {
        use ListBuilderState::*;
        this.next = match this.next {
            Start { offset } => Start { offset },
            Value { offset, depth } => {
                next(&mut this.builder, val)?;
                Value { offset, depth }
            }
        };

        Ok(())
    });
    macros::accept_value!((this, ev, val, next) {
        use ListBuilderState::*;
        this.next = match this.next {
            Start { offset } => {
                if matches!(ev, Event::Null) {
                    this.finalize_item(offset.try_into()?, true);
                    Start { offset }
                } else {
                    fail!("Invalid event {ev} in state Start")
                }
            }
            Value { offset, depth } => {
                next(&mut this.builder, val)?;
                Value {
                    offset: if depth == 0 { offset + 1 } else { offset },
                    depth,
                }
            }
        };
        Ok(())
    });

    fn finish(&mut self) -> Result<()> {
        self.builder.finish()?;
        self.finished = true;
        Ok(())
    }
}
