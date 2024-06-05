//! Support for `from_samples`
mod alt;

#[cfg(test)]
mod test_error_messages;

use std::collections::HashMap;

use serde::Serialize;

use crate::internal::{
    error::{fail, Result},
    event::Event,
    schema::{GenericDataType, Strategy},
    sink::macros,
    sink::{serialize_into_sink, EventSink},
};

use super::tracing_options::TracingOptions;
use super::{
    tracer::{
        ListTracer, ListTracerState, MapTracer, MapTracerState, PrimitiveTracer, StructField,
        StructMode, StructTracer, StructTracerState, Tracer, TupleTracer, TupleTracerState,
        UnionTracer, UnionTracerState,
    },
    SerdeArrowSchema, TracingMode,
};

pub fn schema_from_samples<T: Serialize + ?Sized>(
    samples: &T,
    options: TracingOptions,
) -> Result<SerdeArrowSchema> {
    let options = options.tracing_mode(TracingMode::FromSamples);

    let mut tracer = Tracer::new(String::from("$"), options);
    tracer.trace_samples(samples)?;
    tracer.to_schema()
}

impl Tracer {
    pub fn trace_samples<T: Serialize + ?Sized>(&mut self, samples: &T) -> Result<()> {
        self.reset()?;
        let mut tracer = StripOuterSequenceSink::new(&mut *self);
        serialize_into_sink(&mut tracer, samples)
    }
}

pub(crate) struct StripOuterSequenceSink<E> {
    wrapped: E,
    state: StripOuterSequenceState,
}

#[derive(Debug, Clone, Copy)]
enum StripOuterSequenceState {
    WaitForStart,
    WaitForItem,
    Item(usize),
}

impl<E> StripOuterSequenceSink<E> {
    pub fn new(wrapped: E) -> Self {
        Self {
            wrapped,
            state: StripOuterSequenceState::WaitForStart,
        }
    }
}

impl<E: EventSink> EventSink for StripOuterSequenceSink<E> {
    fn accept(&mut self, event: Event<'_>) -> Result<()> {
        use Event::*;
        match event {
            StartSequence => self.accept_start_sequence(),
            StartTuple => self.accept_start_tuple(),
            StartMap => self.accept_start_map(),
            StartStruct => self.accept_start_struct(),
            EndSequence => self.accept_end_sequence(),
            EndTuple => self.accept_end_tuple(),
            EndMap => self.accept_end_map(),
            EndStruct => self.accept_end_struct(),
            Item => self.accept_item(),
            Null => self.accept_null(),
            Some => self.accept_some(),
            Default => self.accept_default(),
            Bool(val) => self.accept_bool(val),
            I8(val) => self.accept_i8(val),
            I16(val) => self.accept_i16(val),
            I32(val) => self.accept_i32(val),
            I64(val) => self.accept_i64(val),
            U8(val) => self.accept_u8(val),
            U16(val) => self.accept_u16(val),
            U32(val) => self.accept_u32(val),
            U64(val) => self.accept_u64(val),
            F32(val) => self.accept_f32(val),
            F64(val) => self.accept_f64(val),
            Str(val) => self.accept_str(val),
            OwnedStr(val) => self.accept_str(&val),
            Variant(name, idx) => self.accept_variant(name, idx),
            OwnedVariant(name, idx) => self.accept_variant(&name, idx),
        }
    }

    macros::accept_start!((this, ev, val, next) {
        use StripOuterSequenceState::*;
        this.state = match this.state {
            WaitForStart => {
                if !matches!(ev, Event::StartSequence | Event::StartTuple) {
                    fail!(concat!(
                        "Cannot trace non-sequences with `from_samples`. ",
                        "Samples must be given as a sequence. ",
                        "Consider wrapping the argument in an array. ",
                        "E.g., `from_samples(&[arg], options)`.",
                    ));
                }
                WaitForItem
            },
            Item(depth) => {
                next(&mut this.wrapped, val)?;
                Item(depth + 1)
            }
            state => fail!("Invalid event {ev} in state {state:?} for StripOuterSequence"),
        };
        Ok(())
    });
    macros::accept_end!((this, ev, val, next) {
        use StripOuterSequenceState::*;
        this.state = match this.state {
            Item(1) => {
                next(&mut this.wrapped, val)?;
                WaitForItem
            }
            Item(depth) if depth > 1 => {
                next(&mut this.wrapped, val)?;
                Item(depth - 1)
            }
            WaitForItem => WaitForStart,
            state => fail!("Invalid event {ev} in state {state:?} for StripOuterSequence"),
        };
        Ok(())
    });
    macros::accept_value!((this, ev, val, next) {
        use StripOuterSequenceState::*;
        this.state = match this.state {
            Item(0) => {
                next(&mut this.wrapped, val)?;
                WaitForItem
            }
            Item(depth) => {
                next(&mut this.wrapped, val)?;
                Item(depth)
            }
            state => fail!("Invalid event {ev} in state {state:?} for StripOuterSequence"),
        };
        Ok(())
    });
    macros::accept_marker!((this, ev, val, next) {
        use StripOuterSequenceState::*;
        this.state = match this.state {
            WaitForItem if matches!(ev, Event::Item) => Item(0),
            Item(depth) => {
                next(&mut this.wrapped, val)?;
                Item(depth)
            }
            state => fail!("Invalid event {ev} in state {state:?} for StripOuterSequence"),
        };
        Ok(())
    });

    fn finish(&mut self) -> Result<()> {
        self.wrapped.finish()
    }
}

impl<'a> EventSink for &'a mut Tracer {
    macros::forward_specialized_to_generic!();

    fn accept(&mut self, event: Event<'_>) -> Result<()> {
        (*self).accept(event)
    }

    fn finish(&mut self) -> Result<()> {
        (*self).finish()
    }
}

impl EventSink for Tracer {
    macros::forward_specialized_to_generic!();

    fn accept(&mut self, event: Event<'_>) -> Result<()> {
        match self {
            // NOTE: unknown tracer is the only tracer that change the internal type
            Self::Unknown(tracer) => match event {
                Event::Some | Event::Null => tracer.nullable = true,
                Event::Bool(_)
                | Event::I8(_)
                | Event::I16(_)
                | Event::I32(_)
                | Event::I64(_)
                | Event::U8(_)
                | Event::U16(_)
                | Event::U32(_)
                | Event::U64(_)
                | Event::F32(_)
                | Event::F64(_)
                | Event::Str(_)
                | Event::OwnedStr(_) => {
                    let mut tracer = PrimitiveTracer::new(
                        tracer.path.clone(),
                        tracer.options.clone(),
                        GenericDataType::Null,
                        tracer.nullable,
                    );
                    tracer.accept(event)?;
                    *self = Tracer::Primitive(tracer)
                }
                Event::StartSequence => {
                    let mut tracer = ListTracer::new(
                        tracer.path.clone(),
                        tracer.options.clone(),
                        tracer.nullable,
                    );
                    tracer.accept(event)?;
                    *self = Tracer::List(tracer);
                }
                Event::StartStruct => {
                    let mut tracer = StructTracer::new(
                        tracer.path.clone(),
                        tracer.options.clone(),
                        StructMode::Struct,
                        tracer.nullable,
                    );
                    tracer.accept(event)?;
                    *self = Tracer::Struct(tracer);
                }
                Event::StartTuple => {
                    let mut tracer = TupleTracer::new(
                        tracer.path.clone(),
                        tracer.options.clone(),
                        tracer.nullable,
                    );
                    tracer.accept(event)?;
                    *self = Tracer::Tuple(tracer);
                }
                Event::StartMap => {
                    if tracer.options.map_as_struct {
                        let mut tracer = StructTracer::new(
                            tracer.path.clone(),
                            tracer.options.clone(),
                            StructMode::Map,
                            tracer.nullable,
                        );
                        tracer.accept(event)?;
                        *self = Tracer::Struct(tracer);
                    } else {
                        let mut tracer = MapTracer::new(
                            tracer.path.clone(),
                            tracer.options.clone(),
                            tracer.nullable,
                        );
                        tracer.accept(event)?;
                        *self = Tracer::Map(tracer);
                    }
                }
                Event::Variant(_, _) => {
                    let mut tracer = UnionTracer::new(
                        tracer.path.clone(),
                        tracer.options.clone(),
                        tracer.nullable,
                    );
                    tracer.accept(event)?;
                    *self = Tracer::Union(tracer)
                }
                ev if ev.is_end() => fail!(
                    "Invalid end nesting events for unknown tracer ({path})",
                    path = tracer.path
                ),
                ev => fail!(
                    "Internal error unmatched event {ev} in Tracer ({path})",
                    path = tracer.path
                ),
            },
            Self::List(tracer) => tracer.accept(event)?,
            Self::Struct(tracer) => tracer.accept(event)?,
            Self::Primitive(tracer) => tracer.accept(event)?,
            Self::Tuple(tracer) => tracer.accept(event)?,
            Self::Union(tracer) => tracer.accept(event)?,
            Self::Map(tracer) => tracer.accept(event)?,
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        Tracer::finish(self)
    }
}

impl StructTracer {
    pub fn new(path: String, options: TracingOptions, mode: StructMode, nullable: bool) -> Self {
        Self {
            path,
            options,
            mode,
            fields: Vec::new(),
            index: HashMap::new(),
            nullable,
            state: StructTracerState::WaitForKey,
            seen_samples: 0,
        }
    }
}

impl StructTracer {
    pub fn get_field_tracer_mut(&mut self, idx: usize) -> Option<&mut Tracer> {
        Some(&mut self.fields.get_mut(idx)?.tracer)
    }

    pub fn ensure_field(&mut self, key: &str) -> Result<usize> {
        if let Some(&field_idx) = self.index.get(key) {
            let Some(field) = self.fields.get_mut(field_idx) else {
                fail!("invalid state");
            };
            field.last_seen_in_sample = self.seen_samples;

            Ok(field_idx)
        } else {
            let mut field = StructField {
                tracer: Tracer::new(
                    format!("{path}.{key}", path = self.path),
                    self.options.clone(),
                ),
                name: key.to_owned(),
                last_seen_in_sample: self.seen_samples,
            };

            // field was missing in previous samples
            if self.seen_samples != 0 {
                field.tracer.mark_nullable();
            }

            let field_idx = self.fields.len();
            self.fields.push(field);
            self.index.insert(key.to_owned(), field_idx);
            Ok(field_idx)
        }
    }

    pub fn end(&mut self) -> Result<()> {
        for field in &mut self.fields {
            // field. was not seen in this sample
            if field.last_seen_in_sample != self.seen_samples {
                field.tracer.mark_nullable();
            }
        }
        self.seen_samples += 1;
        Ok(())
    }
}

impl EventSink for StructTracer {
    macros::forward_specialized_to_generic!();

    fn accept(&mut self, event: Event<'_>) -> Result<()> {
        use StructTracerState::*;
        type E<'a> = Event<'a>;

        self.state = match (self.state, event) {
            (WaitForKey, E::StartStruct | E::StartMap) => InKey,
            (WaitForKey, E::Null | E::Some) => {
                self.nullable = true;
                WaitForKey
            }
            (WaitForKey, ev) => fail!("Invalid event {ev} for struct tracer in state Start"),
            (InKey, E::Item) => InKey,
            (InKey, E::Str(key)) => {
                let field_idx = self.ensure_field(key)?;
                InValue(field_idx, 0)
            }
            (InKey, E::EndStruct | E::EndMap) => {
                self.end()?;

                WaitForKey
            }
            (InKey, ev) => fail!("Invalid event {ev} for struct tracer in state Key"),
            (InValue(field, depth), ev) if ev.is_start() => {
                self.fields[field].tracer.accept(ev)?;
                InValue(field, depth + 1)
            }
            (InValue(field, depth), ev) if ev.is_end() => {
                self.fields[field].tracer.accept(ev)?;
                match depth {
                    0 => fail!("Invalid closing event in struct tracer in state Value"),
                    1 => InKey,
                    depth => InValue(field, depth - 1),
                }
            }
            (InValue(field, depth), ev) if ev.is_marker() => {
                self.fields[field].tracer.accept(ev)?;
                // markers are always followed by the actual  value
                InValue(field, depth)
            }
            (InValue(field, depth), ev) => {
                self.fields[field].tracer.accept(ev)?;
                match depth {
                    // Any event at depth == 0 that does not start a structure (is a complete value)
                    0 => InKey,
                    _ => InValue(field, depth),
                }
            }
            (Finished, _) => fail!("finished StructTracer cannot handle events"),
        };
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        StructTracer::finish(self)
    }
}

impl EventSink for TupleTracer {
    macros::forward_specialized_to_generic!();

    fn accept(&mut self, event: Event<'_>) -> Result<()> {
        use TupleTracerState::*;
        type E<'a> = Event<'a>;

        self.state = match (self.state, event) {
            (WaitForStart, Event::StartTuple) => WaitForItem(0),
            (WaitForStart, E::Null | E::Some) => {
                self.nullable = true;
                WaitForStart
            }
            (WaitForStart, ev) => fail!(
                "Invalid event {ev} for TupleTracer in state Start [{path}]",
                path = self.path
            ),
            (WaitForItem(field), Event::Item) => InItem(field, 0),
            (WaitForItem(_), E::EndTuple) => WaitForStart,
            (WaitForItem(field), ev) => fail!(
                "Invalid event {ev} for TupleTracer in state WaitForItem({field}) [{path}]",
                path = self.path
            ),
            (InItem(field, depth), ev) if ev.is_start() => {
                self.field_tracer(field).accept(ev)?;
                InItem(field, depth + 1)
            }
            (InItem(field, depth), ev) if ev.is_end() => {
                self.field_tracer(field).accept(ev)?;
                match depth {
                    0 => fail!(
                        "Invalid closing event in TupleTracer in state Value [{path}]",
                        path = self.path
                    ),
                    1 => WaitForItem(field + 1),
                    depth => InItem(field, depth - 1),
                }
            }
            (InItem(field, depth), ev) if ev.is_marker() => {
                self.field_tracer(field).accept(ev)?;
                // markers are always followed by the actual  value
                InItem(field, depth)
            }
            (InItem(field, depth), ev) => {
                self.field_tracer(field).accept(ev)?;
                match depth {
                    // Any event at depth == 0 that does not start a structure (is a complete value)
                    0 => WaitForItem(field + 1),
                    _ => InItem(field, depth),
                }
            }
            (Finished, ev) => fail!("finished tuple tracer cannot handle event {ev}"),
        };
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        TupleTracer::finish(self)
    }
}

impl EventSink for ListTracer {
    macros::forward_specialized_to_generic!();

    fn accept(&mut self, event: Event<'_>) -> Result<()> {
        use {Event as E, ListTracerState as S};

        self.state = match (self.state, event) {
            (S::WaitForStart, E::Null | E::Some) => {
                self.nullable = true;
                S::WaitForStart
            }
            (S::WaitForStart, E::StartSequence) => S::WaitForItem,
            (S::WaitForItem, E::EndSequence) => S::WaitForStart,
            (S::WaitForItem, E::Item) => S::InItem(0),
            (S::InItem(depth), ev) if ev.is_start() => {
                self.item_tracer.accept(ev)?;
                S::InItem(depth + 1)
            }
            (S::InItem(depth), ev) if ev.is_end() => match depth {
                0 => fail!(
                    "Invalid event {ev} for list tracer ({path}) in state Item(0)",
                    path = self.path
                ),
                1 => {
                    self.item_tracer.accept(ev)?;
                    S::WaitForItem
                }
                depth => {
                    self.item_tracer.accept(ev)?;
                    S::InItem(depth - 1)
                }
            },
            (S::InItem(0), ev) if ev.is_value() => {
                self.item_tracer.accept(ev)?;
                S::WaitForItem
            }
            (S::InItem(depth), ev) => {
                self.item_tracer.accept(ev)?;
                S::InItem(depth)
            }
            (state, ev) => fail!(
                "Invalid event {ev} for list tracer ({path}) in state {state:?}",
                path = self.path
            ),
        };
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        ListTracer::finish(self)
    }
}

impl EventSink for UnionTracer {
    macros::forward_specialized_to_generic!();

    fn accept(&mut self, event: Event<'_>) -> Result<()> {
        type S = UnionTracerState;
        type E<'a> = Event<'a>;

        self.state = match self.state {
            S::WaitForVariant => match event {
                E::Variant(variant, idx) => {
                    self.ensure_variant(variant, idx)?;
                    S::InVariant(idx, 0)
                }
                E::Some => fail!("Nullable unions are not supported"),
                E::OwnedVariant(variant, idx) => {
                    self.ensure_variant(variant, idx)?;
                    S::InVariant(idx, 0)
                }
                ev => fail!("Invalid event {ev} for UnionTracer in State Inactive"),
            },
            S::InVariant(idx, depth) => match event {
                ev if ev.is_start() => {
                    self.variants[idx].as_mut().unwrap().tracer.accept(ev)?;
                    S::InVariant(idx, depth + 1)
                }
                ev if ev.is_end() => match depth {
                    0 => fail!("Invalid end event {ev} at depth 0 in UnionTracer"),
                    1 => {
                        self.variants[idx].as_mut().unwrap().tracer.accept(ev)?;
                        S::WaitForVariant
                    }
                    _ => {
                        self.variants[idx].as_mut().unwrap().tracer.accept(ev)?;
                        S::InVariant(idx, depth - 1)
                    }
                },
                ev if ev.is_marker() => {
                    self.variants[idx].as_mut().unwrap().tracer.accept(ev)?;
                    S::InVariant(idx, depth)
                }
                ev if ev.is_value() => {
                    self.variants[idx].as_mut().unwrap().tracer.accept(ev)?;
                    match depth {
                        0 => S::WaitForVariant,
                        _ => S::InVariant(idx, depth),
                    }
                }
                _ => unreachable!(),
            },
            S::Finished => fail!("finished union tracer cannot handle event"),
        };
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        UnionTracer::finish(self)
    }
}

impl EventSink for MapTracer {
    macros::forward_specialized_to_generic!();

    fn accept(&mut self, event: Event<'_>) -> Result<()> {
        type S = MapTracerState;
        type E<'a> = Event<'a>;

        self.state = match self.state {
            S::WaitForKey => match event {
                Event::StartMap => S::InKey(0),
                Event::Null | Event::Some => {
                    self.nullable = true;
                    S::WaitForKey
                }
                ev => fail!("Unexpected event {ev} in state Start of MapTracer"),
            },
            S::InKey(depth) => match event {
                Event::Item if depth == 0 => S::InKey(depth),
                ev if ev.is_end() => match depth {
                    0 => {
                        if !matches!(ev, E::EndMap) {
                            fail!("Unexpected event {ev} in State Key at depth 0 in MapTracer")
                        }
                        S::WaitForKey
                    }
                    1 => {
                        self.key_tracer.accept(ev)?;
                        S::InValue(0)
                    }
                    _ => {
                        self.key_tracer.accept(ev)?;
                        S::InKey(depth - 1)
                    }
                },
                ev if ev.is_start() => {
                    self.key_tracer.accept(ev)?;
                    S::InKey(depth + 1)
                }
                ev if ev.is_marker() => {
                    self.key_tracer.accept(ev)?;
                    S::InKey(depth)
                }
                ev if ev.is_value() => {
                    self.key_tracer.accept(ev)?;
                    if depth == 0 {
                        S::InValue(0)
                    } else {
                        S::InKey(depth)
                    }
                }
                _ => unreachable!(),
            },
            S::InValue(depth) => match event {
                ev if ev.is_end() => match depth {
                    0 => fail!("Unexpected event {ev} in State Value at depth 0 in MapTracer"),
                    1 => {
                        self.value_tracer.accept(ev)?;
                        S::InKey(0)
                    }
                    _ => {
                        self.value_tracer.accept(ev)?;
                        S::InValue(depth - 1)
                    }
                },
                ev if ev.is_start() => {
                    self.value_tracer.accept(ev)?;
                    S::InValue(depth + 1)
                }
                ev if ev.is_marker() => {
                    self.value_tracer.accept(ev)?;
                    S::InValue(depth)
                }
                ev if ev.is_value() => {
                    self.value_tracer.accept(ev)?;
                    if depth == 0 {
                        S::InKey(0)
                    } else {
                        S::InValue(depth)
                    }
                }
                _ => unreachable!(),
            },
            S::Finished => fail!("Finished map tracer cannot handle event"),
        };
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        MapTracer::finish(self)
    }
}

impl EventSink for PrimitiveTracer {
    macros::forward_specialized_to_generic!();

    fn accept(&mut self, event: Event<'_>) -> Result<()> {
        use GenericDataType::*;
        use Strategy as S;

        let (ev_type, ev_strategy) = match event {
            Event::Some | Event::Null => (Null, None),
            Event::Bool(_) => (Bool, None),
            Event::Str(s) => self.get_string_type_and_strategy(s),
            Event::OwnedStr(s) => self.get_string_type_and_strategy(&s),
            Event::U8(_) => (U8, None),
            Event::U16(_) => (U16, None),
            Event::U32(_) => (U32, None),
            Event::U64(_) => (U64, None),
            Event::I8(_) => (I8, None),
            Event::I16(_) => (I16, None),
            Event::I32(_) => (I32, None),
            Event::I64(_) => (I64, None),
            Event::F32(_) => (F32, None),
            Event::F64(_) => (F64, None),
            ev => fail!("Cannot handle event {ev} in primitive tracer"),
        };

        // coercion rules as a table of (this_ty, this_strategy), (ev_ty, ev_strategy)
        (self.item_type, self.strategy) = match (
            (&self.item_type, self.strategy.as_ref()),
            (ev_type, ev_strategy),
        ) {
            ((ty, strategy), (Null, None)) => {
                self.nullable = true;
                (ty.clone(), strategy.cloned())
            }
            ((Null, None), (ev_type, ev_strategy)) => (ev_type, ev_strategy),
            ((Bool, None), (Bool, None)) => (Bool, None),
            ((I8, None), (I8, None)) => (I8, None),
            ((I16, None), (I16, None)) => (I16, None),
            ((I32, None), (I32, None)) => (I32, None),
            ((I64, None), (I64, None)) => (I64, None),
            ((U8, None), (U8, None)) => (U8, None),
            ((U16, None), (U16, None)) => (U16, None),
            ((U32, None), (U32, None)) => (U32, None),
            ((U64, None), (U64, None)) => (U64, None),
            ((F32, None), (F32, None)) => (F32, None),
            ((F64, None), (F64, None)) => (F64, None),
            ((Date64, Some(S::NaiveStrAsDate64)), (Date64, Some(S::NaiveStrAsDate64))) => {
                (Date64, Some(S::NaiveStrAsDate64))
            }
            ((Date64, Some(S::UtcStrAsDate64)), (Date64, Some(S::UtcStrAsDate64))) => {
                (Date64, Some(S::UtcStrAsDate64))
            }
            ((Date64, Some(S::NaiveStrAsDate64)), (Date64, Some(S::UtcStrAsDate64))) => {
                (LargeUtf8, None)
            }
            // incompatible strategies, coerce to string
            ((Date64, Some(S::UtcStrAsDate64)), (Date64, Some(S::NaiveStrAsDate64))) => {
                (LargeUtf8, None)
            }
            (
                (LargeUtf8, None) | (Date64, Some(S::NaiveStrAsDate64) | Some(S::UtcStrAsDate64)),
                (LargeUtf8, None),
            ) => (LargeUtf8, None),
            (
                (LargeUtf8, None),
                (Date64, strategy @ (Some(S::NaiveStrAsDate64) | Some(S::UtcStrAsDate64))),
            ) => {
                if self.seen_samples == 0 {
                    (Date64, strategy)
                } else {
                    (LargeUtf8, None)
                }
            }
            ((ty, None), (ev, None)) if self.options.coerce_numbers => match (ty, ev) {
                // unsigned x unsigned -> u64
                (U8 | U16 | U32 | U64, U8 | U16 | U32 | U64) => (U64, None),
                // signed x signed -> i64
                (I8 | I16 | I32 | I64, I8 | I16 | I32 | I64) => (I64, None),
                // signed x unsigned -> i64
                (I8 | I16 | I32 | I64, U8 | U16 | U32 | U64) => (I64, None),
                // unsigned x signed -> i64
                (U8 | U16 | U32 | U64, I8 | I16 | I32 | I64) => (I64, None),
                // float x float -> f64
                (F32 | F64, F32 | F64) => (F64, None),
                // int x float -> f64
                (I8 | I16 | I32 | I64 | U8 | U16 | U32 | U64, F32 | F64) => (F64, None),
                // float x int -> f64
                (F32 | F64, I8 | I16 | I32 | I64 | U8 | U16 | U32 | U64) => (F64, None),
                (ty, ev) => fail!("Cannot accept event {ev} for tracer of primitive type {ty}"),
            },
            ((this_ty, this_strategy), (ev_ty, ev_strategy)) => {
                fail!("Cannot accept event {ev_ty} with strategy {ev_strategy:?} for tracer of primitive type {this_ty} with strategy {this_strategy:?}")
            }
        };

        self.seen_samples += 1;
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        PrimitiveTracer::finish(self)
    }
}

impl PrimitiveTracer {
    fn get_string_type_and_strategy(&self, s: &str) -> (GenericDataType, Option<Strategy>) {
        if self.options.guess_dates && matches_naive_datetime(s) {
            (GenericDataType::Date64, Some(Strategy::NaiveStrAsDate64))
        } else if self.options.guess_dates && matches_utc_datetime(s) {
            (GenericDataType::Date64, Some(Strategy::UtcStrAsDate64))
        } else {
            (GenericDataType::LargeUtf8, None)
        }
    }
}

mod parsing {
    pub const DIGIT: &[char] = &['0', '1', '2', '3', '4', '5', '6', '7', '8', '9'];

    pub fn match_optional_sign(s: &str) -> Result<&str, &str> {
        Ok(s.strip_prefix(['+', '-']).unwrap_or(s))
    }

    pub fn match_one_or_more_digits(s: &str) -> Result<&str, &str> {
        let mut s = s.strip_prefix(DIGIT).ok_or(s)?;
        while let Some(new_s) = s.strip_prefix(DIGIT) {
            s = new_s;
        }
        Ok(s)
    }

    pub fn match_one_or_two_digits(s: &str) -> Result<&str, &str> {
        let s = s.strip_prefix(DIGIT).ok_or(s)?;
        Ok(s.strip_prefix(DIGIT).unwrap_or(s))
    }

    pub fn match_char(s: &str, c: char) -> Result<&str, &str> {
        s.strip_prefix(c).ok_or(s)
    }

    pub fn matches_naive_datetime_with_sep<'a>(
        s: &'a str,
        sep: &'_ [char],
    ) -> Result<&'a str, &'a str> {
        let s = s.trim();
        let s = match_optional_sign(s)?;
        let s = match_one_or_more_digits(s)?;
        let s = match_char(s, '-')?;
        let s = match_one_or_two_digits(s)?;
        let s = match_char(s, '-')?;
        let s = match_one_or_two_digits(s)?;
        let s = s.strip_prefix(sep).ok_or(s)?;
        let s = match_one_or_two_digits(s)?;
        let s = match_char(s, ':')?;
        let s = match_one_or_two_digits(s)?;
        let s = match_char(s, ':')?;
        let s = match_one_or_two_digits(s)?;

        if let Some(s) = s.strip_prefix('.') {
            match_one_or_more_digits(s)
        } else {
            Ok(s)
        }
    }

    pub fn matches_naive_datetime(s: &str) -> Result<&str, &str> {
        matches_naive_datetime_with_sep(s, &['T'])
    }

    pub fn matches_utc_datetime(s: &str) -> Result<&str, &str> {
        let s = matches_naive_datetime_with_sep(s, &['T', ' '])?;

        if let Some(s) = s.strip_prefix('Z') {
            Ok(s)
        } else if let Some(s) = s.strip_prefix("+0000") {
            Ok(s)
        } else if let Some(s) = s.strip_prefix("+00:00") {
            Ok(s)
        } else {
            Err(s)
        }
    }
}

pub fn matches_naive_datetime(s: &str) -> bool {
    parsing::matches_naive_datetime(s)
        .map(|s| s.is_empty())
        .unwrap_or_default()
}

pub fn matches_utc_datetime(s: &str) -> bool {
    parsing::matches_utc_datetime(s)
        .map(|s| s.is_empty())
        .unwrap_or_default()
}

#[cfg(test)]
mod test_matches_naive_datetime {
    macro_rules! test {
        ($( ( $name:ident, $s:expr, $expected:expr ), )*) => {
            $(
                #[test]
                fn $name() {
                    if $expected {
                        assert_eq!(super::parsing::matches_naive_datetime($s), Ok(""));
                    }
                    assert_eq!(super::matches_naive_datetime($s), $expected);
                }
            )*
        };
    }

    test!(
        (example_chrono_docs_1, "2015-09-18T23:56:04", true),
        (example_chrono_docs_2, "+12345-6-7T7:59:60.5", true),
        (surrounding_space, "   2015-09-18T23:56:04   ", true),
    );
}

#[cfg(test)]
mod test_matches_utc_datetime {
    macro_rules! test {
        ($( ( $name:ident, $s:expr, $expected:expr ), )*) => {
            $(
                #[test]
                fn $name() {
                    if $expected {
                        assert_eq!(super::parsing::matches_utc_datetime($s), Ok(""));
                    }
                    assert_eq!(super::matches_utc_datetime($s), $expected);
                }
            )*
        };
    }

    test!(
        (example_chrono_docs_1, "2012-12-12T12:12:12Z", true),
        (example_chrono_docs_2, "2012-12-12 12:12:12Z", true),
        (example_chrono_docs_3, "2012-12-12 12:12:12+0000", true),
        (example_chrono_docs_4, "2012-12-12 12:12:12+00:00", true),
    );
}
