use crate::{
    impls::arrow2::{
        array::Array,
        array::{
            BooleanArray, DictionaryArray, DictionaryKey, ListArray, MapArray, MutableBooleanArray,
            MutablePrimitiveArray, MutableUtf8Array, NullArray, PrimitiveArray, StructArray,
            UnionArray, Utf8Array,
        },
        bitmap::Bitmap,
        datatypes::{DataType, Field, IntegerType, UnionMode},
        offset::OffsetsBuffer,
        types::{f16, Offset},
    },
    internal::{
        error::{fail, Result},
        event::Event,
        generic_sinks::{
            DictionaryUtf8ArrayBuilder, ListArrayBuilder, MapArrayBuilder, StructArrayBuilder,
            TupleStructBuilder, UnionArrayBuilder,
        },
        generic_sinks::{NullArrayBuilder, PrimitiveBuilders},
        sink::{macros, ArrayBuilder, DynamicArrayBuilder, EventSink},
    },
};

pub struct Arrow2PrimitiveBuilders;

impl PrimitiveBuilders for Arrow2PrimitiveBuilders {
    type Output = Box<dyn Array>;

    fn null() -> DynamicArrayBuilder<Self::Output> {
        DynamicArrayBuilder::new(NullArrayBuilder::new())
    }

    fn bool() -> DynamicArrayBuilder<Self::Output> {
        DynamicArrayBuilder::new(PrimitiveArrayBuilder::<MutableBooleanArray>::default())
    }

    fn i8() -> DynamicArrayBuilder<Self::Output> {
        DynamicArrayBuilder::new(PrimitiveArrayBuilder::<MutablePrimitiveArray<i8>>::default())
    }

    fn i16() -> DynamicArrayBuilder<Self::Output> {
        DynamicArrayBuilder::new(PrimitiveArrayBuilder::<MutablePrimitiveArray<i16>>::default())
    }

    fn i32() -> DynamicArrayBuilder<Self::Output> {
        DynamicArrayBuilder::new(PrimitiveArrayBuilder::<MutablePrimitiveArray<i32>>::default())
    }

    fn i64() -> DynamicArrayBuilder<Self::Output> {
        DynamicArrayBuilder::new(PrimitiveArrayBuilder::<MutablePrimitiveArray<i64>>::default())
    }

    fn u8() -> DynamicArrayBuilder<Self::Output> {
        DynamicArrayBuilder::new(PrimitiveArrayBuilder::<MutablePrimitiveArray<u8>>::default())
    }

    fn u16() -> DynamicArrayBuilder<Self::Output> {
        DynamicArrayBuilder::new(PrimitiveArrayBuilder::<MutablePrimitiveArray<u16>>::default())
    }

    fn u32() -> DynamicArrayBuilder<Self::Output> {
        DynamicArrayBuilder::new(PrimitiveArrayBuilder::<MutablePrimitiveArray<u32>>::default())
    }

    fn u64() -> DynamicArrayBuilder<Self::Output> {
        DynamicArrayBuilder::new(PrimitiveArrayBuilder::<MutablePrimitiveArray<u64>>::default())
    }

    fn f16() -> DynamicArrayBuilder<Self::Output> {
        DynamicArrayBuilder::new(PrimitiveArrayBuilder::<MutablePrimitiveArray<f16>>::default())
    }

    fn f32() -> DynamicArrayBuilder<Self::Output> {
        DynamicArrayBuilder::new(PrimitiveArrayBuilder::<MutablePrimitiveArray<f32>>::default())
    }

    fn f64() -> DynamicArrayBuilder<Self::Output> {
        DynamicArrayBuilder::new(PrimitiveArrayBuilder::<MutablePrimitiveArray<f64>>::default())
    }

    fn utf8() -> DynamicArrayBuilder<Self::Output> {
        DynamicArrayBuilder::new(Utf8ArrayBuilder::<i32>::default())
    }

    fn large_utf8() -> DynamicArrayBuilder<Self::Output> {
        DynamicArrayBuilder::new(Utf8ArrayBuilder::<i64>::default())
    }

    fn date64() -> DynamicArrayBuilder<Self::Output> {
        // TODO: is this correct? Shouldn't this be a separate type?
        DynamicArrayBuilder::new(PrimitiveArrayBuilder::<MutablePrimitiveArray<i64>>::default())
    }
}

impl<B: ArrayBuilder<Box<dyn Array>>> ArrayBuilder<Box<dyn Array>> for StructArrayBuilder<B> {
    fn build_array(&mut self) -> Result<Box<dyn Array>> {
        if !self.finished {
            fail!("Cannot build array from unfinished StructArrayBuilder");
        }

        let values: Result<Vec<Box<dyn Array>>> =
            self.builders.iter_mut().map(|b| b.build_array()).collect();
        let values = values?;

        let mut fields = Vec::new();
        for (i, column) in self.columns.iter().enumerate() {
            fields.push(Field::new(
                column,
                values[i].data_type().clone(),
                self.nullable[i],
            ));
        }
        let data_type = DataType::Struct(fields);

        Ok(Box::new(StructArray::new(
            data_type,
            values,
            Some(std::mem::take(&mut self.validity).into()),
        )))
    }
}

impl<B: ArrayBuilder<Box<dyn Array>>> ArrayBuilder<Box<dyn Array>> for TupleStructBuilder<B> {
    fn build_array(&mut self) -> Result<Box<dyn Array>> {
        if !self.finished {
            fail!("Cannot build array from unfinished TupleStructBuilder");
        }

        let values: Result<Vec<Box<dyn Array>>> =
            self.builders.iter_mut().map(|b| b.build_array()).collect();
        let values = values?;

        let mut fields = Vec::new();
        for (i, value) in values.iter().enumerate() {
            fields.push(Field::new(
                i.to_string(),
                value.data_type().clone(),
                self.nullable[i],
            ));
        }
        let data_type = DataType::Struct(fields);

        Ok(Box::new(StructArray::new(
            data_type,
            values,
            Some(std::mem::take(&mut self.validity).into()),
        )))
    }
}

impl<B: ArrayBuilder<Box<dyn Array>>> ArrayBuilder<Box<dyn Array>> for UnionArrayBuilder<B> {
    fn build_array(&mut self) -> Result<Box<dyn Array>> {
        if !self.finished {
            fail!("Cannot build array from unfinished UnionArrayBuilder");
        }

        let values: Result<Vec<Box<dyn Array>>> = self
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
        let data_type = DataType::Union(fields, None, UnionMode::Dense);

        Ok(Box::new(UnionArray::new(
            data_type,
            self.field_types.clone().into(),
            values,
            Some(std::mem::take(&mut self.field_offsets).into()),
        )))
    }
}

impl<B: ArrayBuilder<Box<dyn Array>>> ArrayBuilder<Box<dyn Array>> for ListArrayBuilder<B, i32> {
    fn build_array(&mut self) -> Result<Box<dyn Array>> {
        if !self.finished {
            fail!("Cannot build array from unfinished ListArrayBuilder");
        }

        let values = self.builder.build_array()?;
        let array = ListArray::try_new(
            DataType::List(Box::new(Field::new(
                self.item_name.clone(),
                values.data_type().clone(),
                self.nullable,
            ))),
            OffsetsBuffer::try_from(std::mem::take(&mut self.offsets))?,
            values,
            Some(Bitmap::from(std::mem::take(&mut self.validity))),
        )?;
        Ok(Box::new(array))
    }
}

impl<B: ArrayBuilder<Box<dyn Array>>> ArrayBuilder<Box<dyn Array>> for ListArrayBuilder<B, i64> {
    fn build_array(&mut self) -> Result<Box<dyn Array>> {
        if !self.finished {
            fail!("Cannot build array from unfinished ListArrayBuilder");
        }

        let values = self.builder.build_array()?;
        let array = ListArray::try_new(
            DataType::LargeList(Box::new(Field::new(
                self.item_name.clone(),
                values.data_type().clone(),
                self.nullable,
            ))),
            OffsetsBuffer::try_from(std::mem::take(&mut self.offsets))?,
            values,
            Some(Bitmap::from(std::mem::take(&mut self.validity))),
        )?;

        Ok(Box::new(array))
    }
}

impl<B: ArrayBuilder<Box<dyn Array>>> ArrayBuilder<Box<dyn Array>> for MapArrayBuilder<B> {
    fn build_array(&mut self) -> Result<Box<dyn Array>> {
        if !self.finished {
            fail!("Cannot build array from unfinished MapArrayBuilder");
        }

        let keys = self.key_builder.build_array()?;
        let vals = self.val_builder.build_array()?;

        // TODO: fix nullability of different fields
        let entries_type = DataType::Struct(vec![
            Field::new("key", keys.data_type().clone(), false),
            Field::new("value", vals.data_type().clone(), false),
        ]);

        let entries = StructArray::try_new(entries_type.clone(), vec![keys, vals], None)?;
        let entries: Box<dyn Array> = Box::new(entries);

        let map_type = DataType::Map(Box::new(Field::new("entries", entries_type, false)), false);

        let array = MapArray::try_new(
            map_type,
            OffsetsBuffer::try_from(std::mem::take(&mut self.offsets))?,
            entries,
            Some(std::mem::take(&mut self.validity).into()),
        )?;
        Ok(Box::new(array))
    }
}

impl ArrayBuilder<Box<dyn Array>> for NullArrayBuilder {
    fn build_array(&mut self) -> Result<Box<dyn Array>> {
        if !self.finished {
            fail!("Cannot build array from unfinished NullArrayBuilder");
        }
        let res = Box::new(NullArray::new(DataType::Null, self.length));
        *self = Self::new();

        Ok(res)
    }
}

#[derive(Debug, Default)]
pub struct PrimitiveArrayBuilder<B> {
    array: B,
    finished: bool,
}

macro_rules! impl_primitive_array_builder {
    ($ty:ty, $array:ty, $variant:ident) => {
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
                    Event::$variant(_) => this.array.push(Some(ev.try_into()?)),
                    Event::Null => this.array.push(None),
                    Event::Default => this.array.push(Some(Default::default())),
                    ev => fail!("Cannot handle event {ev} in PrimitiveArrayBuilder<f16>"),
                }
                Ok(())
            });

            fn finish(&mut self) -> Result<()> {
                self.finished = true;
                Ok(())
            }
        }

        impl ArrayBuilder<Box<dyn Array>> for PrimitiveArrayBuilder<$ty> {
            fn build_array(&mut self) -> Result<Box<dyn Array>> {
                if !self.finished {
                    fail!(concat!(
                        "Cannot build array from unfinished PrimitiveArrayBuilder<",
                        stringify!($ty),
                        ">"
                    ));
                }
                let array = std::mem::take(&mut self.array);
                Ok(Box::new(<$array>::from(array)))
            }
        }
    };
}

impl_primitive_array_builder!(MutablePrimitiveArray<i8>, PrimitiveArray<_>, I8);
impl_primitive_array_builder!(MutablePrimitiveArray<i16>, PrimitiveArray<_>, I16);
impl_primitive_array_builder!(MutablePrimitiveArray<i32>, PrimitiveArray<_>, I32);
impl_primitive_array_builder!(MutablePrimitiveArray<i64>, PrimitiveArray<_>, I64);

impl_primitive_array_builder!(MutablePrimitiveArray<u8>, PrimitiveArray<_>, U8);
impl_primitive_array_builder!(MutablePrimitiveArray<u16>, PrimitiveArray<_>, U16);
impl_primitive_array_builder!(MutablePrimitiveArray<u32>, PrimitiveArray<_>, U32);
impl_primitive_array_builder!(MutablePrimitiveArray<u64>, PrimitiveArray<_>, U64);

impl_primitive_array_builder!(MutablePrimitiveArray<f16>, PrimitiveArray<_>, F32);
impl_primitive_array_builder!(MutablePrimitiveArray<f32>, PrimitiveArray<_>, F32);
impl_primitive_array_builder!(MutablePrimitiveArray<f64>, PrimitiveArray<_>, F64);

impl_primitive_array_builder!(MutableBooleanArray, BooleanArray, Bool);

#[derive(Debug, Default)]
pub struct Utf8ArrayBuilder<O: Offset> {
    array: MutableUtf8Array<O>,
    finished: bool,
}

impl<O: Offset> EventSink for Utf8ArrayBuilder<O> {
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
        self.array.push(Some(val));
        Ok(())
    }

    fn accept_owned_str(&mut self, val: String) -> Result<()> {
        self.array.push(Some(val));
        Ok(())
    }

    fn accept_default(&mut self) -> Result<()> {
        self.array.push::<String>(Some(String::new()));
        Ok(())
    }

    fn accept_null(&mut self) -> Result<()> {
        self.array.push::<String>(None);
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

impl<O: Offset> ArrayBuilder<Box<dyn Array>> for Utf8ArrayBuilder<O> {
    fn build_array(&mut self) -> Result<Box<dyn Array>> {
        if !self.finished {
            fail!("Cannot build array from unfinished Utf8ArrayBuilder");
        }
        let array = std::mem::take(&mut self.array);
        Ok(Box::new(<Utf8Array<_> as From<_>>::from(array)))
    }
}

impl<B> ArrayBuilder<Box<dyn Array>> for DictionaryUtf8ArrayBuilder<B>
where
    B: ArrayBuilder<Box<dyn Array>>,
{
    fn build_array(&mut self) -> Result<Box<dyn Array>> {
        let values = self.values.build_array()?;
        let mut keys = self.keys.build_array()?;

        fn take<K: DictionaryKey>(
            arr: &mut Box<dyn Array>,
        ) -> Option<(PrimitiveArray<K>, IntegerType)> {
            let arr = arr.as_any_mut().downcast_mut::<PrimitiveArray<K>>()?;
            Some((std::mem::take(arr), K::KEY_TYPE))
        }

        fn build<K: DictionaryKey>(
            data_type: IntegerType,
            key: PrimitiveArray<K>,
            values: Box<dyn Array>,
        ) -> Result<Box<dyn Array>> {
            let values_type = Box::new(values.data_type().clone());
            let data_type = DataType::Dictionary(data_type, values_type, false);
            let arr = DictionaryArray::try_new(data_type, key, values)?;
            Ok(Box::new(arr))
        }

        if let Some((keys, dt)) = take::<u8>(&mut keys) {
            build(dt, keys, values)
        } else if let Some((keys, dt)) = take::<u16>(&mut keys) {
            build(dt, keys, values)
        } else if let Some((keys, dt)) = take::<u32>(&mut keys) {
            build(dt, keys, values)
        } else if let Some((keys, dt)) = take::<u64>(&mut keys) {
            build(dt, keys, values)
        } else if let Some((keys, dt)) = take::<i8>(&mut keys) {
            build(dt, keys, values)
        } else if let Some((keys, dt)) = take::<i16>(&mut keys) {
            build(dt, keys, values)
        } else if let Some((keys, dt)) = take::<i32>(&mut keys) {
            build(dt, keys, values)
        } else if let Some((keys, dt)) = take::<i64>(&mut keys) {
            build(dt, keys, values)
        } else {
            fail!("...")
        }
    }
}
