use crate::internal::{
    error::{fail, Error, Result},
    event::Event,
    schema::GenericField,
    sink::{macros, EventSink},
};

pub struct ListArrayBuilder<B, O> {
    pub path: String,
    pub field: GenericField,
    pub builder: B,
    next: ListBuilderState,
    pub offsets: Vec<O>,
    pub validity: Vec<bool>,
    pub finished: bool,
}

impl<B, O: Default> ListArrayBuilder<B, O> {
    pub fn new(path: String, field: GenericField, builder: B) -> Self {
        Self {
            path,
            field,
            builder,
            next: ListBuilderState::WaitForStart { offset: 0 },
            offsets: vec![Default::default()],
            validity: Vec::new(),
            finished: false,
        }
    }
}

impl<B, O> ListArrayBuilder<B, O> {
    fn finalize_item(&mut self, end_offset: O, null: bool) {
        self.offsets.push(end_offset);
        self.validity.push(!null);
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
    WaitForStart { offset: i64 },
    WaitForItem { offset: i64 },
    Value { offset: i64, depth: usize },
}

impl<B: EventSink, O: TryFrom<i64>> EventSink for ListArrayBuilder<B, O>
where
    Error: From<O::Error>,
{
    macros::forward_generic_to_specialized!();
    macros::accept_start!((this, ev, val, next) {
        use {ListBuilderState as S, Event as E};
        this.next = match (this.next, ev) {
            (S::WaitForStart { offset }, E::StartSequence)  => {
                S::WaitForItem { offset }
            }
            (S::Value { offset, depth }, _) => {
                next(&mut this.builder, val)?;
                S::Value {
                    offset,
                    depth: depth + 1,
                }
            }
            (state, ev) => fail!("ListBuilder: Cannot handle event {ev} in state {state:?} [{path}]", path=this.path),
        };

        Ok(())
    });
    macros::accept_end!((this, ev, val, next) {
        use {ListBuilderState as S, Event as E};
        this.next = match (this.next, ev) {
            (S::WaitForItem { offset }, E::EndSequence) => {
                this.finalize_item(offset.try_into()?, false);
                S::WaitForStart { offset }
            }
            (S::Value { offset, depth: 1 }, _) => {
                next(&mut this.builder, val)?;
                S::WaitForItem { offset: offset + 1 }
            }
            (S::Value { offset, depth }, _) => {
                next(&mut this.builder, val)?;
                S::Value { offset, depth: depth - 1 }
            }
            (state, ev) => fail!("ListBuilder: Cannot handle event {ev} in state {state:?} [{path}]", path=this.path),
        };

        Ok(())
    });
    macros::accept_marker!((this, ev, val, next) {
        use {ListBuilderState as S, Event as E};
        this.next = match (this.next, ev) {
            (S::WaitForStart { offset }, E::Some) => S::WaitForStart { offset },
            (S::WaitForItem { offset }, E::Item) => S::Value { offset, depth: 0 },
            (S::Value { offset, depth }, _) => {
                next(&mut this.builder, val)?;
                S::Value { offset, depth }
            }
            (state, ev) => fail!("ListBuilder: Cannot handle event {ev} in state {state:?} [{path}]", path=this.path),
        };

        Ok(())
    });
    macros::accept_value!((this, ev, val, next) {
        use {ListBuilderState as S, Event as E};
        this.next = match (this.next, ev) {
            (S::WaitForStart { offset }, E::Null) => {
                this.finalize_item(offset.try_into()?, true);
                S::WaitForStart { offset }
            }
            (S::Value { offset, depth: 0}, _) => {
                next(&mut this.builder, val)?;
                S::WaitForItem { offset: offset + 1 }
            }
            (S::Value { offset, depth }, _) => {
                next(&mut this.builder, val)?;
                S::Value { offset, depth }
            }
            (state, ev) => fail!("ListBuilder: Cannot handle event {ev} in state {state:?} [{path}]", path=this.path),
        };
        Ok(())
    });

    fn finish(&mut self) -> Result<()> {
        self.builder.finish()?;
        self.finished = true;
        Ok(())
    }
}
