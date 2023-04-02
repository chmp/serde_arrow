use crate::{
    base::{Event, EventSink},
    internal::{
        error::{fail, Result},
        schema::FieldMetadata,
        sink::macros,
    },
};

pub struct MapArrayBuilder<B> {
    next: MapBuilderState,
    pub field_meta: FieldMetadata,
    pub key_meta: FieldMetadata,
    pub key_builder: B,
    pub val_meta: FieldMetadata,
    pub val_builder: B,
    pub offsets: Vec<i32>,
    pub offset: i32,
    pub validity: Vec<bool>,
    pub finished: bool,
}

#[derive(Debug, Clone, Copy)]
enum MapBuilderState {
    Start,
    Key(usize),
    Value(usize),
}

impl<B> MapArrayBuilder<B> {
    pub fn new(
        field_meta: FieldMetadata,
        key_meta: FieldMetadata,
        key_builder: B,
        val_meta: FieldMetadata,
        val_builder: B,
    ) -> Self {
        Self {
            field_meta,
            key_meta,
            key_builder,
            val_meta,
            val_builder,
            next: MapBuilderState::Start,
            offsets: vec![0],
            offset: 0,
            validity: Vec::new(),
            finished: false,
        }
    }
}

impl<B: EventSink> EventSink for MapArrayBuilder<B> {
    macros::forward_generic_to_specialized!();
    macros::accept_start!((this, ev, val, next) {
        type S = MapBuilderState;
        type E<'a> = Event<'a>;

        this.next = match this.next {
            S::Start => {
                if matches!(ev, E::StartMap) {
                    S::Key(0)
                } else {
                    fail!("Unexpected event {ev} in state Start of MapArrayBuilder")
                }
            }
            S::Key(depth) => {
                next(&mut this.key_builder, val)?;
                S::Key(depth + 1)
            }
            S::Value(depth) => {
                next(&mut this.val_builder, val)?;
                S::Value(depth + 1)
            }
        };
        Ok(())
    });
    macros::accept_end!((this, ev, val, next) {
        type S = MapBuilderState;
        type E<'a> = Event<'a>;

        this.next = match this.next {
            S::Start => fail!("Unexpected event {ev} in state Start of MapArrayBuilder"),
            S::Key(depth) => match depth {
                0 => {
                    if matches!(ev, E::EndMap) {
                        this.offsets.push(this.offset);
                        this.validity.push(true);
                        S::Start
                    } else {
                        fail!("Unexpected event {ev} in state Key(0) in MapArrayBuilder")
                    }
                }
                1 => {
                    next(&mut this.key_builder, val)?;
                    S::Value(0)
                }
                _ => {
                    next(&mut this.key_builder, val)?;
                    S::Key(depth - 1)
                }
            },
            S::Value(depth) => match depth {
                0 => fail!("Unexpected event {ev} in state Value(0) of MapArrayBuilder"),
                1 => {
                    next(&mut this.val_builder, val)?;
                    this.offset += 1;
                    S::Key(0)
                }
                _ => {
                    next(&mut this.val_builder, val)?;
                    S::Value(depth - 1)
                }
            },
        };
        Ok(())
    });
    macros::accept_marker!((this, ev, val, next) {
        type S = MapBuilderState;
        type E<'a> = Event<'a>;

        this.next = match this.next {
            S::Start => {
                if matches!(ev, E::Some) {
                    S::Start
                } else {
                    fail!("Unexpected event {ev} in state Start of MapArrayBuilder")
                }
            }
            S::Key(depth) => {
                next(&mut this.key_builder, val)?;
                S::Key(depth)
            }
            S::Value(depth) => {
                next(&mut this.val_builder, val)?;
                S::Value(depth)
            }
        };
        Ok(())
    });
    macros::accept_value!((this, ev, val, next) {
        type S = MapBuilderState;
        type E<'a> = Event<'a>;

        this.next = match this.next {
            S::Start => {
                if matches!(ev, E::Null) {
                    this.offsets.push(this.offset);
                    this.validity.push(false);
                    S::Start
                } else {
                    fail!("Unexpected event {ev} in state Start of MapArrayBuilder")
                }
            }
            S::Key(depth) => {
                next(&mut this.key_builder, val)?;
                if depth == 0 {
                    S::Value(0)
                } else {
                    S::Key(depth)
                }
            }
            S::Value(depth) => {
                next(&mut this.val_builder, val)?;
                if depth == 0 {
                    this.offset += 1;
                    S::Key(0)
                } else {
                    S::Value(depth)
                }
            }
        };
        Ok(())
    });

    fn finish(&mut self) -> Result<()> {
        self.key_builder.finish()?;
        self.val_builder.finish()?;
        self.finished = true;
        Ok(())
    }
}
