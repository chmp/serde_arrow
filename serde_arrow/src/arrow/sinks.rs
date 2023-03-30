use std::sync::Arc;

use crate::{
    base::Event,
    impls::arrow::{
        array::{
            builder::{BooleanBuilder, GenericStringBuilder, PrimitiveBuilder},
            types::{ArrowPrimitiveType, Float16Type},
            types::{
                Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type,
                UInt32Type, UInt64Type, UInt8Type,
            },
            ArrayRef, NullArray, OffsetSizeTrait, StructArray,
        },
        schema::Field,
    },
    internal::{
        error::{fail, Result},
        generic_sinks::{
            DictionaryUtf8ArrayBuilder, ListArrayBuilder, MapArrayBuilder, NullArrayBuilder,
            PrimitiveBuilders, StructArrayBuilder, TupleStructBuilder, UnionArrayBuilder,
        },
        sink::{macros, ArrayBuilder, DynamicArrayBuilder, EventSink},
    },
};

type Ptr<T> = Arc<T>;

pub struct ArrowPrimitiveBuilders;

impl PrimitiveBuilders for ArrowPrimitiveBuilders {
    type ArrayRef = ArrayRef;

    fn null() -> DynamicArrayBuilder<ArrayRef> {
        DynamicArrayBuilder::new(NullArrayBuilder::new())
    }

    fn bool() -> DynamicArrayBuilder<ArrayRef> {
        DynamicArrayBuilder::new(PrimitiveArrayBuilder::<BooleanBuilder>::default())
    }

    fn u8() -> DynamicArrayBuilder<ArrayRef> {
        DynamicArrayBuilder::new(PrimitiveArrayBuilder::<PrimitiveBuilder<UInt8Type>>::default())
    }

    fn u16() -> DynamicArrayBuilder<ArrayRef> {
        DynamicArrayBuilder::new(PrimitiveArrayBuilder::<PrimitiveBuilder<UInt16Type>>::default())
    }

    fn u32() -> DynamicArrayBuilder<ArrayRef> {
        DynamicArrayBuilder::new(PrimitiveArrayBuilder::<PrimitiveBuilder<UInt32Type>>::default())
    }

    fn u64() -> DynamicArrayBuilder<ArrayRef> {
        DynamicArrayBuilder::new(PrimitiveArrayBuilder::<PrimitiveBuilder<UInt64Type>>::default())
    }

    fn i8() -> DynamicArrayBuilder<ArrayRef> {
        DynamicArrayBuilder::new(PrimitiveArrayBuilder::<PrimitiveBuilder<Int8Type>>::default())
    }

    fn i16() -> DynamicArrayBuilder<ArrayRef> {
        DynamicArrayBuilder::new(PrimitiveArrayBuilder::<PrimitiveBuilder<Int16Type>>::default())
    }

    fn i32() -> DynamicArrayBuilder<ArrayRef> {
        DynamicArrayBuilder::new(PrimitiveArrayBuilder::<PrimitiveBuilder<Int32Type>>::default())
    }

    fn i64() -> DynamicArrayBuilder<ArrayRef> {
        DynamicArrayBuilder::new(PrimitiveArrayBuilder::<PrimitiveBuilder<Int64Type>>::default())
    }

    fn f16() -> DynamicArrayBuilder<ArrayRef> {
        DynamicArrayBuilder::new(PrimitiveArrayBuilder::<PrimitiveBuilder<Float16Type>>::default())
    }

    fn f32() -> DynamicArrayBuilder<ArrayRef> {
        DynamicArrayBuilder::new(PrimitiveArrayBuilder::<PrimitiveBuilder<Float32Type>>::default())
    }

    fn f64() -> DynamicArrayBuilder<ArrayRef> {
        DynamicArrayBuilder::new(PrimitiveArrayBuilder::<PrimitiveBuilder<Float64Type>>::default())
    }

    fn date64() -> DynamicArrayBuilder<Self::ArrayRef> {
        // TODO: check type?
        DynamicArrayBuilder::new(PrimitiveArrayBuilder::<PrimitiveBuilder<Int64Type>>::default())
    }

    fn utf8() -> DynamicArrayBuilder<Self::ArrayRef> {
        DynamicArrayBuilder::new(Utf8ArrayBuilder::<i32>::default())
    }

    fn large_utf8() -> DynamicArrayBuilder<Self::ArrayRef> {
        DynamicArrayBuilder::new(Utf8ArrayBuilder::<i64>::default())
    }
}

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

impl_primitive_array_builder!(PrimitiveBuilder<Float32Type>, F32);
impl_primitive_array_builder!(PrimitiveBuilder<Float64Type>, F64);

impl_primitive_array_builder!(BooleanBuilder, Bool);

impl EventSink for PrimitiveArrayBuilder<PrimitiveBuilder<Float16Type>> {
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
            Event::F32(_) => this.array.append_value(<Float16Type as ArrowPrimitiveType>::Native::from_f32(ev.try_into()?)),
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

impl ArrayBuilder<ArrayRef> for PrimitiveArrayBuilder<PrimitiveBuilder<Float16Type>> {
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

impl<B: ArrayBuilder<ArrayRef>> ArrayBuilder<ArrayRef> for StructArrayBuilder<B> {
    fn build_array(&mut self) -> Result<ArrayRef> {
        if !self.finished {
            fail!("Cannot build array from unfinished StructArrayBuilder");
        }

        let mut data = Vec::new();
        for (i, builder) in self.builders.iter_mut().enumerate() {
            let arr = builder.build_array()?;
            let field = Field::new(
                self.columns[i].to_string(),
                arr.data_type().clone(),
                self.nullable[i],
            );
            data.push((field, arr));
        }
        let array = StructArray::from(data);

        Ok(Arc::new(array))
    }
}

impl<B: ArrayBuilder<ArrayRef>> ArrayBuilder<ArrayRef> for TupleStructBuilder<B> {
    fn build_array(&mut self) -> Result<ArrayRef> {
        if !self.finished {
            fail!("Cannot build array from unfinished TupleStructBuilder");
        }

        let mut data = Vec::new();
        for (i, builder) in self.builders.iter_mut().enumerate() {
            let arr = builder.build_array()?;
            let field = Field::new(i.to_string(), arr.data_type().clone(), self.nullable[i]);
            data.push((field, arr));
        }
        let array = StructArray::from(data);

        Ok(Arc::new(array))
    }
}

impl<B: ArrayBuilder<ArrayRef>> ArrayBuilder<ArrayRef> for UnionArrayBuilder<B> {
    fn build_array(&mut self) -> Result<ArrayRef> {
        if !self.finished {
            fail!("Cannot build array from unfinished UnionArrayBuilder");
        }

        let values: Result<Vec<ArrayRef>> = self
            .field_builders
            .iter_mut()
            .map(|b| b.build_array())
            .collect();
        let values = values?;

        let mut fields = Vec::new();
        for (i, value) in values.iter().enumerate() {
            fields.push(Field::new(
                i.to_string(),
                value.data_type().clone(),
                self.field_nullable[i],
            ));
        }

        todo!()
    }
}

impl<B: ArrayBuilder<ArrayRef>> ArrayBuilder<ArrayRef> for ListArrayBuilder<B, i32> {
    fn build_array(&mut self) -> Result<ArrayRef> {
        fail!("List array construction is currently not supported")
    }
}

impl<B: ArrayBuilder<ArrayRef>> ArrayBuilder<ArrayRef> for ListArrayBuilder<B, i64> {
    fn build_array(&mut self) -> Result<ArrayRef> {
        fail!("List array construction is currently not supported")
    }
}

impl<B: ArrayBuilder<ArrayRef>> ArrayBuilder<ArrayRef> for MapArrayBuilder<B> {
    fn build_array(&mut self) -> Result<ArrayRef> {
        fail!("Map array construction is currently not supported")
    }
}

impl<B: ArrayBuilder<ArrayRef>> ArrayBuilder<ArrayRef> for DictionaryUtf8ArrayBuilder<B> {
    fn build_array(&mut self) -> Result<ArrayRef> {
        fail!("Cannot build dictionary arrays")
    }
}
