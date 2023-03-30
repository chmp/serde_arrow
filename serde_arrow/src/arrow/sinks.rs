use std::sync::Arc;

use arrow_array_36::{builder::GenericStringBuilder, OffsetSizeTrait};

use crate::{
    base::Event,
    impls::arrow::array::{
        builder::{BooleanBuilder, PrimitiveBuilder},
        types::{
            Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type,
            UInt32Type, UInt64Type, UInt8Type,
        },
        ArrayRef, NullArray,
    },
    internal::{
        error::{fail, Result},
        generic_sinks::NullArrayBuilder,
        sink::{macros, ArrayBuilder, EventSink},
    },
};

type Ptr<T> = Arc<T>;

impl ArrayBuilder<ArrayRef> for NullArrayBuilder {
    fn build_array(&mut self) -> Result<ArrayRef> {
        if !self.finished {
            fail!("Cannot build unfinished null array");
        }
        Ok(Ptr::new(NullArray::new(self.length)))
    }
}

#[derive(Debug, Default)]
pub struct PrimitiveArrayBuilder<B> {
    array: B,
    finished: bool,
}

macro_rules! impl_primitive_array_builder {
    ($ty:ty, $variant:ident) => {
        impl EventSink for PrimitiveArrayBuilder<$ty> {
            macros::forward_generic_to_specialized!();
            macros::accept_start!((_this, ev, _val, _next) {
                fail!("Cannot handle event {ev} in PrimitiveArrayBuilder<f16>");
            });
            macros::accept_end!((_this, ev, _val, _next) {
                fail!("Cannot handle event {ev} in PrimitiveArrayBuilder<f16>");
            });
            macros::accept_marker!((_this, ev, _val, _next) {
                if !matches!(ev, Event::Some) {
                    fail!("Cannot handle event {ev} in PrimitiveArrayBuilder<f16>");
                }
                Ok(())
            });
            macros::accept_value!((this, ev, _val, _next) {
                match ev {
                    Event::$variant(_) => this.array.append_value(ev.try_into()?),
                    Event::Null => this.array.append_null(),
                    Event::Default => this.array.append_value(Default::default()),
                    ev => fail!("Cannot handle event {ev} in PrimitiveArrayBuilder<f16>"),
                }
                Ok(())
            });

            fn finish(&mut self) -> Result<()> {
                self.finished = true;
                Ok(())
            }
        }

        impl ArrayBuilder<ArrayRef> for PrimitiveArrayBuilder<$ty> {
            fn build_array(&mut self) -> Result<ArrayRef> {
                if !self.finished {
                    fail!(concat!(
                        "Cannot build array from unfinished PrimitiveArrayBuilder<",
                        stringify!($ty),
                        ">"
                    ));
                }
                Ok(Ptr::new(self.array.finish()))
            }
        }
    };
}

impl_primitive_array_builder!(PrimitiveBuilder<Int8Type>, I8);
impl_primitive_array_builder!(PrimitiveBuilder<Int16Type>, I16);
impl_primitive_array_builder!(PrimitiveBuilder<Int32Type>, I32);
impl_primitive_array_builder!(PrimitiveBuilder<Int64Type>, I64);

impl_primitive_array_builder!(PrimitiveBuilder<UInt8Type>, U8);
impl_primitive_array_builder!(PrimitiveBuilder<UInt16Type>, U16);
impl_primitive_array_builder!(PrimitiveBuilder<UInt32Type>, U32);
impl_primitive_array_builder!(PrimitiveBuilder<UInt64Type>, U64);

// impl_primitive_array_builder!(PrimitiveBuilder<Float16Type>, F32);
impl_primitive_array_builder!(PrimitiveBuilder<Float32Type>, F32);
impl_primitive_array_builder!(PrimitiveBuilder<Float64Type>, F64);

impl_primitive_array_builder!(BooleanBuilder, Bool);

#[derive(Debug, Default)]
pub struct Utf8ArrayBuilder<O: OffsetSizeTrait> {
    array: GenericStringBuilder<O>,
    finished: bool,
}

impl<O: OffsetSizeTrait> EventSink for Utf8ArrayBuilder<O> {
    macros::accept_start!((_this, ev, _val, _next) {
        fail!("Utf8ArrayBuilder cannot accept {ev}")
    });
    macros::accept_end!((_this, ev, _val, _next) {
        fail!("Utf8ArrayBuilder cannot accept {ev}")
    });
    macros::accept_marker!((_this, ev, _val, _next) {
        match ev {
            Event::Some => Ok(()),
            _ => fail!("Utf8ArrayBuilder cannot accept {ev}"),
        }
    });

    macros::fail_on_non_string_primitive!("Utf8ArrayBuilder");

    fn accept_str(&mut self, val: &str) -> Result<()> {
        self.array.append_value(val);
        Ok(())
    }

    fn accept_owned_str(&mut self, val: String) -> Result<()> {
        self.array.append_value(val);
        Ok(())
    }

    fn accept_default(&mut self) -> Result<()> {
        self.array.append_value("");
        Ok(())
    }

    fn accept_null(&mut self) -> Result<()> {
        self.array.append_null();
        Ok(())
    }

    fn accept(&mut self, event: Event<'_>) -> Result<()> {
        match event {
            Event::Some => self.accept_some(),
            Event::Default => self.accept_default(),
            Event::Null => self.accept_null(),
            Event::Str(val) => self.accept_str(val),
            Event::OwnedStr(val) => self.accept_owned_str(val),
            ev => fail!("Cannot handle event {ev} in BooleanArrayBuilder"),
        }
    }

    fn finish(&mut self) -> Result<()> {
        self.finished = true;
        Ok(())
    }
}

impl<O: OffsetSizeTrait> ArrayBuilder<ArrayRef> for Utf8ArrayBuilder<O> {
    fn build_array(&mut self) -> Result<ArrayRef> {
        if !self.finished {
            fail!("Cannot build array from unfinished Utf8ArrayBuilder");
        }
        Ok(Ptr::new(self.array.finish()))
    }
}

// TODO: add DictionaryUtf8Builder
