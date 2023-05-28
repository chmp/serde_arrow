use crate::internal::{
    error::{fail, Result},
    event::Event,
    schema::GenericField,
    sink::{macros, EventSink},
};

pub struct UnionArrayBuilder<B> {
    next: UnionBuilderState,
    pub field: GenericField,

    pub current_field_offsets: Vec<i32>,
    pub field_builders: Vec<B>,
    pub field_offsets: Vec<i32>,
    pub field_types: Vec<i8>,
    pub finished: bool,
}

impl<B> UnionArrayBuilder<B> {
    pub fn new(field: GenericField, field_builders: Vec<B>) -> Self {
        let current_field_offsets = vec![0; field_builders.len()];
        Self {
            field,

            next: UnionBuilderState::Inactive,
            current_field_offsets,
            field_builders,
            field_offsets: Vec::new(),
            field_types: Vec::new(),
            finished: false,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum UnionBuilderState {
    Inactive,
    Active(usize, usize),
}

impl<B: EventSink> EventSink for UnionArrayBuilder<B> {
    macros::forward_generic_to_specialized!();
    macros::accept_start!((this, ev, val, next) {
        type S = UnionBuilderState;

        this.next = match this.next {
            S::Inactive => {
                fail!("Unexpected event {ev} in state Inactive of UnionArrayBuilder")
            }
            S::Active(field, depth) => {
                next(&mut this.field_builders[field], val)?;
                S::Active(field, depth + 1)
            }
        };
        Ok(())
    });
    macros::accept_end!((this, ev, val, next) {
        type S = UnionBuilderState;

        this.next = match this.next {
            S::Inactive => fail!("Unexpected event {ev} in state Inactive of UnionArrayBuilder"),
            S::Active(field, depth) => {
                match depth {
                    0 => fail!("Invalid end event {ev} in state Active({field}, {depth}) in UnionArrayBuilder"),
                    1 => {
                        next(&mut this.field_builders[field], val)?;
                        S::Inactive
                    }
                    _ => {
                        next(&mut this.field_builders[field], val)?;
                        S::Active(field, depth - 1)
                    }
                }
            }
        };
        Ok(())
    });
    macros::accept_marker!((this, ev, val, next) {
        type S = UnionBuilderState;
        type E<'a> = Event<'a>;

        this.next = match this.next {
            S::Inactive => match ev {
                E::Variant(_, idx) | E::OwnedVariant(_, idx) => {
                    if idx >= this.current_field_offsets.len() {
                        fail!(
                            "encountered unknown variant with index {idx} in serialization with {len} known variants",
                            len=this.current_field_offsets.len(),
                        );
                    }

                    this.field_offsets.push(this.current_field_offsets[idx]);
                    this.current_field_offsets[idx] += 1;
                    this.field_types.push(i8::try_from(idx)?);

                    S::Active(idx, 0)
                }
                E::Some => fail!("Nullable Union arrays are not supported"),
                _ => {
                    fail!("Unexpected event {ev} in state Inactive of UnionArrayBuilder")
                }
            },
            S::Active(field, depth) => {
                next(&mut this.field_builders[field], val)?;
                S::Active(field, depth)
            }
        };
        Ok(())
    });
    macros::accept_value!((this, ev, val, next) {
        type S = UnionBuilderState;
        type E<'a> = Event<'a>;

        this.next = match this.next {
            S::Inactive => match ev {
                E::Null => fail!("Nullable Union arrays are not supported"),
                ev => fail!("Unexpected event {ev} in state Inactive of UnionArrayBuilder"),
            },
            S::Active(field, depth) => {
                next(&mut this.field_builders[field], val)?;
                match depth {
                    0 => S::Inactive,
                    _ => S::Active(field, depth),
                }
            }
        };
        Ok(())
    });

    fn finish(&mut self) -> Result<()> {
        for builder in &mut self.field_builders {
            builder.finish()?;
        }
        self.finished = true;
        Ok(())
    }
}
