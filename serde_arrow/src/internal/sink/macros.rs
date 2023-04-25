//! Macros to simplify the implementation of [EventSink][super::EventSink].
//!
//! This module offers macros to only implement the specialized accept methods
//! or the generic methods:
//!
//! - [forward_generic_to_specialized]: implements
//!   [accept][super::EventSink::accept] by forwarding to the individual
//!   `accept_*` methods
//! - [forward_specialized_to_generic]: implements the `accept_*` methods by
//!   forwarding to the corresponding events to
//!   [accept][super::EventSink::accept]
//!
//! It also offers macros to simplify the implementation of the specialized
//! `accept_*` methods:
//!
//! - [accept_start] implements
//!   - [accept_start_sequence][super::EventSink::accept_start_sequence]
//!   - [accept_start_tuple][super::EventSink::accept_start_tuple]
//!   - [accept_start_struct][super::EventSink::accept_start_struct]
//!   - [accept_start_map][super::EventSink::accept_start_map]
//! - [accept_end] implements
//!   - [accept_end_sequence][super::EventSink::accept_end_sequence]
//!   - [accept_end_tuple][super::EventSink::accept_end_tuple]
//!   - [accept_end_struct][super::EventSink::accept_end_struct]
//!   - [accept_end_map][super::EventSink::accept_end_map]
//! - [accept_marker] implements
//!   - [accept_some][super::EventSink::accept_some]
//!   - [accept_variant][super::EventSink::accept_variant]
//!   - [accept_owned_variant][super::EventSink::accept_owned_variant]
//! - [accept_value] implements
//!   - [accept_default][super::EventSink::accept_default]
//!   - [accept_null][super::EventSink::accept_null]
//!   - [accept_u8][super::EventSink::accept_u8]
//!   - [accept_u16][super::EventSink::accept_u16]
//!   - [accept_u32][super::EventSink::accept_u32]
//!   - [accept_u64][super::EventSink::accept_u64]
//!   - [accept_i8][super::EventSink::accept_i8]
//!   - [accept_i16][super::EventSink::accept_i16]
//!   - [accept_i32][super::EventSink::accept_i32]
//!   - [accept_i64][super::EventSink::accept_i64]
//!   - [accept_f32][super::EventSink::accept_f32]
//!   - [accept_f64][super::EventSink::accept_f64]
//!   - [accept_str][super::EventSink::accept_str]
//!   - [accept_owned_str][super::EventSink::accept_owned_str]
//!  
//! The `accept_*` macros must be called as in:
//!
//! ```ignore
//! accept_start!((this, ev, val, next) {
//!     /* implementation */
//! });
//! ```
//!
//! The arguments define names for
//!
//! - `this`: the self parameter
//! - `ev`: an event representing the method being called
//! - `val`: the argument value of the method
//! - `next`: a way to call the same accept method currently being implemented
//!   on an event sink. Should be called with `val`, as in `next(&mut
//!   event_sink, val)`.  
//!

#[allow(unused)]
macro_rules! forward_generic_to_specialized {
    () => {
        fn accept(
            &mut self,
            event: $crate::internal::event::Event<'_>,
        ) -> $crate::internal::error::Result<()> {
            use $crate::internal::event::Event::*;
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
                OwnedStr(val) => self.accept_owned_str(val),
                Variant(name, idx) => self.accept_variant(name, idx),
                OwnedVariant(name, idx) => self.accept_owned_variant(name, idx),
            }
        }
    };
}

#[allow(unused)]
pub(crate) use forward_generic_to_specialized;

#[allow(unused)]
macro_rules! forward_specialized_to_generic {
    () => {
        fn accept_start_sequence(&mut self) -> $crate::internal::error::Result<()> {
            self.accept($crate::internal::event::Event::StartSequence)
        }

        fn accept_end_sequence(&mut self) -> $crate::internal::error::Result<()> {
            self.accept($crate::internal::event::Event::EndSequence)
        }

        fn accept_start_tuple(&mut self) -> $crate::internal::error::Result<()> {
            self.accept($crate::internal::event::Event::StartTuple)
        }

        fn accept_end_tuple(&mut self) -> $crate::internal::error::Result<()> {
            self.accept($crate::internal::event::Event::EndTuple)
        }

        fn accept_start_struct(&mut self) -> $crate::internal::error::Result<()> {
            self.accept($crate::internal::event::Event::StartStruct)
        }

        fn accept_end_struct(&mut self) -> $crate::internal::error::Result<()> {
            self.accept($crate::internal::event::Event::EndStruct)
        }

        fn accept_start_map(&mut self) -> $crate::internal::error::Result<()> {
            self.accept($crate::internal::event::Event::StartMap)
        }

        fn accept_end_map(&mut self) -> $crate::internal::error::Result<()> {
            self.accept($crate::internal::event::Event::EndMap)
        }

        fn accept_item(&mut self) -> $crate::internal::error::Result<()> {
            self.accept($crate::internal::event::Event::Item)
        }

        fn accept_some(&mut self) -> $crate::internal::error::Result<()> {
            self.accept($crate::internal::event::Event::Some)
        }

        fn accept_null(&mut self) -> $crate::internal::error::Result<()> {
            self.accept($crate::internal::event::Event::Null)
        }

        fn accept_default(&mut self) -> $crate::internal::error::Result<()> {
            self.accept($crate::internal::event::Event::Default)
        }

        fn accept_str(&mut self, val: &str) -> $crate::internal::error::Result<()> {
            self.accept($crate::internal::event::Event::Str(val))
        }

        fn accept_owned_str(&mut self, val: String) -> $crate::internal::error::Result<()> {
            self.accept($crate::internal::event::Event::OwnedStr(val))
        }

        fn accept_variant(
            &mut self,
            name: &str,
            idx: usize,
        ) -> $crate::internal::error::Result<()> {
            self.accept($crate::internal::event::Event::Variant(name, idx))
        }

        fn accept_owned_variant(
            &mut self,
            name: String,
            idx: usize,
        ) -> $crate::internal::error::Result<()> {
            self.accept($crate::internal::event::Event::OwnedVariant(name, idx))
        }

        fn accept_bool(&mut self, val: bool) -> $crate::internal::error::Result<()> {
            self.accept($crate::internal::event::Event::Bool(val))
        }

        fn accept_i8(&mut self, val: i8) -> $crate::internal::error::Result<()> {
            self.accept($crate::internal::event::Event::I8(val))
        }

        fn accept_i16(&mut self, val: i16) -> $crate::internal::error::Result<()> {
            self.accept($crate::internal::event::Event::I16(val))
        }

        fn accept_i32(&mut self, val: i32) -> $crate::internal::error::Result<()> {
            self.accept($crate::internal::event::Event::I32(val))
        }

        fn accept_i64(&mut self, val: i64) -> $crate::internal::error::Result<()> {
            self.accept($crate::internal::event::Event::I64(val))
        }

        fn accept_u8(&mut self, val: u8) -> $crate::internal::error::Result<()> {
            self.accept($crate::internal::event::Event::U8(val))
        }

        fn accept_u16(&mut self, val: u16) -> $crate::internal::error::Result<()> {
            self.accept($crate::internal::event::Event::U16(val))
        }

        fn accept_u32(&mut self, val: u32) -> $crate::internal::error::Result<()> {
            self.accept($crate::internal::event::Event::U32(val))
        }

        fn accept_u64(&mut self, val: u64) -> $crate::internal::error::Result<()> {
            self.accept($crate::internal::event::Event::U64(val))
        }

        fn accept_f32(&mut self, val: f32) -> $crate::internal::error::Result<()> {
            self.accept($crate::internal::event::Event::F32(val))
        }

        fn accept_f64(&mut self, val: f64) -> $crate::internal::error::Result<()> {
            self.accept($crate::internal::event::Event::F64(val))
        }
    };
}

#[allow(unused)]
pub(crate) use forward_specialized_to_generic;

#[allow(unused)]
macro_rules! accept_start {
    (($this:ident, $ev:ident, $val:ident, $next:ident) $block:block) => {
        fn accept_start_sequence(&mut self) -> Result<()> {
            let $this = self;
            let $ev = Event::StartSequence;
            let $val = ();
            fn $next<E: EventSink + ?Sized>(next: &mut E, _: ()) -> Result<()> {
                next.accept_start_sequence()
            }

            $block
        }

        fn accept_start_tuple(&mut self) -> Result<()> {
            let $this = self;
            let $ev = Event::StartTuple;
            let $val = ();
            fn $next<E: EventSink + ?Sized>(next: &mut E, _: ()) -> Result<()> {
                next.accept_start_tuple()
            }

            $block
        }

        fn accept_start_struct(&mut self) -> Result<()> {
            let $this = self;
            let $ev = Event::StartStruct;
            let $val = ();
            fn $next<E: EventSink + ?Sized>(next: &mut E, _: ()) -> Result<()> {
                next.accept_start_struct()
            }

            $block
        }

        fn accept_start_map(&mut self) -> Result<()> {
            let $this = self;
            let $ev = Event::StartMap;
            let $val = ();
            fn $next<E: EventSink + ?Sized>(next: &mut E, _: ()) -> Result<()> {
                next.accept_start_map()
            }

            $block
        }
    };
}
#[allow(unused)]
pub(crate) use accept_start;

#[allow(unused)]
macro_rules! accept_end {
    (($this:ident, $ev:ident, $val:ident, $next:ident) $block:block) => {
        fn accept_end_sequence(&mut self) -> Result<()> {
            let $this = self;
            let $ev = Event::EndSequence;
            let $val = ();
            fn $next<E: EventSink + ?Sized>(next: &mut E, _: ()) -> Result<()> {
                next.accept_end_sequence()
            }

            $block
        }

        fn accept_end_tuple(&mut self) -> Result<()> {
            let $this = self;
            let $ev = Event::EndTuple;
            let $val = ();
            fn $next<E: EventSink + ?Sized>(next: &mut E, _: ()) -> Result<()> {
                next.accept_end_tuple()
            }

            $block
        }

        fn accept_end_struct(&mut self) -> Result<()> {
            let $this = self;
            let $ev = Event::EndStruct;
            let $val = ();
            fn $next<E: EventSink + ?Sized>(next: &mut E, _: ()) -> Result<()> {
                next.accept_end_struct()
            }

            $block
        }

        fn accept_end_map(&mut self) -> Result<()> {
            let $this = self;
            let $ev = Event::EndMap;
            let $val = ();
            fn $next<E: EventSink + ?Sized>(next: &mut E, _: ()) -> Result<()> {
                next.accept_end_map()
            }

            $block
        }
    };
}
#[allow(unused)]
pub(crate) use accept_end;

#[allow(unused)]
macro_rules! accept_value {
    (($this:ident, $ev:ident, $val:ident, $next:ident) $block:block) => {
        fn accept_str(&mut self, val: &str) -> Result<()> {
            let $this = self;
            let $ev = Event::Str(val);
            let $val = val;
            fn $next<E: EventSink + ?Sized>(next: &mut E, val: &str) -> Result<()> {
                next.accept_str(val)
            }

            $block
        }

        fn accept_owned_str(&mut self, val: String) -> Result<()> {
            let $this = self;
            let $val = val;
            let $ev = Event::Str(&$val);
            fn $next<E: EventSink + ?Sized>(next: &mut E, val: String) -> Result<()> {
                next.accept_owned_str(val)
            }

            $block
        }

        fn accept_bool(&mut self, val: bool) -> Result<()> {
            let $this = self;
            let $ev = Event::Bool(val);
            let $val = val;
            fn $next<E: EventSink + ?Sized>(next: &mut E, val: bool) -> Result<()> {
                next.accept_bool(val)
            }

            $block
        }

        fn accept_i8(&mut self, val: i8) -> Result<()> {
            let $this = self;
            let $ev = Event::I8(val);
            let $val = val;
            fn $next<E: EventSink + ?Sized>(next: &mut E, val: i8) -> Result<()> {
                next.accept_i8(val)
            }

            $block
        }

        fn accept_i16(&mut self, val: i16) -> Result<()> {
            let $this = self;
            let $ev = Event::I16(val);
            let $val = val;
            fn $next<E: EventSink + ?Sized>(next: &mut E, val: i16) -> Result<()> {
                next.accept_i16(val)
            }

            $block
        }

        fn accept_i32(&mut self, val: i32) -> Result<()> {
            let $this = self;
            let $ev = Event::I32(val);
            let $val = val;
            fn $next<E: EventSink + ?Sized>(next: &mut E, val: i32) -> Result<()> {
                next.accept_i32(val)
            }

            $block
        }

        fn accept_i64(&mut self, val: i64) -> Result<()> {
            let $this = self;
            let $ev = Event::I64(val);
            let $val = val;
            fn $next<E: EventSink + ?Sized>(next: &mut E, val: i64) -> Result<()> {
                next.accept_i64(val)
            }

            $block
        }

        fn accept_u8(&mut self, val: u8) -> Result<()> {
            let $this = self;
            let $ev = Event::U8(val);
            let $val = val;
            fn $next<E: EventSink + ?Sized>(next: &mut E, val: u8) -> Result<()> {
                next.accept_u8(val)
            }

            $block
        }

        fn accept_u16(&mut self, val: u16) -> Result<()> {
            let $this = self;
            let $ev = Event::U16(val);
            let $val = val;
            fn $next<E: EventSink + ?Sized>(next: &mut E, val: u16) -> Result<()> {
                next.accept_u16(val)
            }

            $block
        }

        fn accept_u32(&mut self, val: u32) -> Result<()> {
            let $this = self;
            let $ev = Event::U32(val);
            let $val = val;
            fn $next<E: EventSink + ?Sized>(next: &mut E, val: u32) -> Result<()> {
                next.accept_u32(val)
            }

            $block
        }

        fn accept_u64(&mut self, val: u64) -> Result<()> {
            let $this = self;
            let $ev = Event::U64(val);
            let $val = val;
            fn $next<E: EventSink + ?Sized>(next: &mut E, val: u64) -> Result<()> {
                next.accept_u64(val)
            }

            $block
        }

        fn accept_f32(&mut self, val: f32) -> Result<()> {
            let $this = self;
            let $ev = Event::F32(val);
            let $val = val;
            fn $next<E: EventSink + ?Sized>(next: &mut E, val: f32) -> Result<()> {
                next.accept_f32(val)
            }

            $block
        }

        fn accept_f64(&mut self, val: f64) -> Result<()> {
            let $this = self;
            let $ev = Event::F64(val);
            let $val = val;
            fn $next<E: EventSink + ?Sized>(next: &mut E, val: f64) -> Result<()> {
                next.accept_f64(val)
            }

            $block
        }

        fn accept_null(&mut self) -> Result<()> {
            let $this = self;
            let $ev = Event::Null;
            let $val = ();
            fn $next<E: EventSink + ?Sized>(next: &mut E, _: ()) -> Result<()> {
                next.accept_null()
            }

            $block
        }

        fn accept_default(&mut self) -> Result<()> {
            let $this = self;
            let $ev = Event::Default;
            let $val = ();
            fn $next<E: EventSink + ?Sized>(next: &mut E, _: ()) -> Result<()> {
                next.accept_default()
            }

            $block
        }
    };
}

#[allow(unused)]
pub(crate) use accept_value;

#[allow(unused)]
macro_rules! accept_marker {
    (($this:ident, $ev:ident, $val:ident, $next:ident) $block:block) => {
        fn accept_item(&mut self) -> Result<()> {
            let $this = self;
            let $ev = Event::Item;
            let $val = ();
            fn $next<E: EventSink + ?Sized>(next: &mut E, _val: ()) -> Result<()> {
                next.accept_item()
            }

            $block
        }

        fn accept_some(&mut self) -> Result<()> {
            let $this = self;
            let $ev = Event::Some;
            let $val = ();
            fn $next<E: EventSink + ?Sized>(next: &mut E, _val: ()) -> Result<()> {
                next.accept_some()
            }

            $block
        }

        fn accept_variant(&mut self, name: &str, idx: usize) -> Result<()> {
            let $this = self;
            let $ev = Event::Variant(name, idx);
            let $val = (name, idx);
            fn $next<E: EventSink + ?Sized>(next: &mut E, val: (&str, usize)) -> Result<()> {
                next.accept_variant(val.0, val.1)
            }

            $block
        }

        fn accept_owned_variant(&mut self, name: String, idx: usize) -> Result<()> {
            let $this = self;
            let $val: (String, usize) = (name, idx);
            let $ev = Event::Variant(&$val.0, $val.1);

            fn $next<E: EventSink + ?Sized>(next: &mut E, val: (String, usize)) -> Result<()> {
                next.accept_owned_variant(val.0, val.1)
            }

            $block
        }
    };
}

#[allow(unused)]
pub(crate) use accept_marker;

#[allow(unused)]
macro_rules! fail_on_non_string_primitive {
    ($context:literal) => {
        fn accept_bool(&mut self, _val: bool) -> Result<()> {
            fail!("{} cannot accept Event::Bool [{path}]", $context, path=self.path)
        }
        fn accept_i8(&mut self, _val: i8) -> Result<()> {
            fail!("{} cannot accept Event::I8 [{path}]", $context, path=self.path)
        }
        fn accept_i16(&mut self, _val: i16) -> Result<()> {
            fail!("{} cannot accept Event::I16 [{path}]", $context, path=self.path)
        }
        fn accept_i32(&mut self, _val: i32) -> Result<()> {
            fail!("{} cannot accept Event::I32 [{path}]", $context, path=self.path)
        }
        fn accept_i64(&mut self, _val: i64) -> Result<()> {
            fail!("{} cannot accept Event::I64 [{path}]", $context, path=self.path)
        }
        fn accept_u8(&mut self, _val: u8) -> Result<()> {
            fail!("{} cannot accept Event::U8 [{path}]", $context, path=self.path)
        }
        fn accept_u16(&mut self, _val: u16) -> Result<()> {
            fail!("{} cannot accept Event::U16 [{path}]", $context, path=self.path)
        }
        fn accept_u32(&mut self, _val: u32) -> Result<()> {
            fail!("{} cannot accept Event::U32 [{path}]", $context, path=self.path)
        }
        fn accept_u64(&mut self, _val: u64) -> Result<()> {
            fail!("{} cannot accept Event::U64 [{path}]", $context, path=self.path)
        }
        fn accept_f32(&mut self, _val: f32) -> Result<()> {
            fail!("{} cannot accept Event::F32 [{path}]", $context, path=self.path)
        }
        fn accept_f64(&mut self, _val: f64) -> Result<()> {
            fail!("{} cannot accept Event::F64 [{path}]", $context, path=self.path)
        }
    };
}

#[allow(unused)]
pub(crate) use fail_on_non_string_primitive;
