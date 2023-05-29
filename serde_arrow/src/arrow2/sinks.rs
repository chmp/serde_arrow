use crate::{
    _impl::arrow2::{
        array::Array,
        array::{
            BooleanArray, DictionaryArray, DictionaryKey, ListArray, MapArray, MutableBooleanArray,
            MutablePrimitiveArray, MutableUtf8Array, NullArray, PrimitiveArray, StructArray,
            UnionArray, Utf8Array,
        },
        bitmap::Bitmap,
        datatypes::{DataType, Field, IntegerType},
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
        generic_sinks::{NullArrayBuilder, PrimitiveBuilders, UnknownVariantBuilder},
        schema::GenericField,
        sink::{macros, ArrayBuilder, DynamicArrayBuilder, EventSink},
    },
};

pub struct Arrow2PrimitiveBuilders;

impl PrimitiveBuilders for Arrow2PrimitiveBuilders {
    type Output = Box<dyn Array>;

    fn null(path: String) -> DynamicArrayBuilder<Self::Output> {
        DynamicArrayBuilder::new(NullArrayBuilder::new(path))
    }

    fn bool(path: String) -> DynamicArrayBuilder<Self::Output> {
        DynamicArrayBuilder::new(PrimitiveArrayBuilder::<MutableBooleanArray>::new(path))
    }

    fn i8(path: String) -> DynamicArrayBuilder<Self::Output> {
        DynamicArrayBuilder::new(PrimitiveArrayBuilder::<MutablePrimitiveArray<i8>>::new(
            path,
        ))
    }

    fn i16(path: String) -> DynamicArrayBuilder<Self::Output> {
        DynamicArrayBuilder::new(PrimitiveArrayBuilder::<MutablePrimitiveArray<i16>>::new(
            path,
        ))
    }

    fn i32(path: String) -> DynamicArrayBuilder<Self::Output> {
        DynamicArrayBuilder::new(PrimitiveArrayBuilder::<MutablePrimitiveArray<i32>>::new(
            path,
        ))
    }

    fn i64(path: String) -> DynamicArrayBuilder<Self::Output> {
        DynamicArrayBuilder::new(PrimitiveArrayBuilder::<MutablePrimitiveArray<i64>>::new(
            path,
        ))
    }

    fn u8(path: String) -> DynamicArrayBuilder<Self::Output> {
        DynamicArrayBuilder::new(PrimitiveArrayBuilder::<MutablePrimitiveArray<u8>>::new(
            path,
        ))
    }

    fn u16(path: String) -> DynamicArrayBuilder<Self::Output> {
        DynamicArrayBuilder::new(PrimitiveArrayBuilder::<MutablePrimitiveArray<u16>>::new(
            path,
        ))
    }

    fn u32(path: String) -> DynamicArrayBuilder<Self::Output> {
        DynamicArrayBuilder::new(PrimitiveArrayBuilder::<MutablePrimitiveArray<u32>>::new(
            path,
        ))
    }

    fn u64(path: String) -> DynamicArrayBuilder<Self::Output> {
        DynamicArrayBuilder::new(PrimitiveArrayBuilder::<MutablePrimitiveArray<u64>>::new(
            path,
        ))
    }

    fn f16(path: String) -> DynamicArrayBuilder<Self::Output> {
        DynamicArrayBuilder::new(PrimitiveArrayBuilder::<MutablePrimitiveArray<f16>>::new(
            path,
        ))
    }

    fn f32(path: String) -> DynamicArrayBuilder<Self::Output> {
        DynamicArrayBuilder::new(PrimitiveArrayBuilder::<MutablePrimitiveArray<f32>>::new(
            path,
        ))
    }

    fn f64(path: String) -> DynamicArrayBuilder<Self::Output> {
        DynamicArrayBuilder::new(PrimitiveArrayBuilder::<MutablePrimitiveArray<f64>>::new(
            path,
        ))
    }

    fn utf8(path: String) -> DynamicArrayBuilder<Self::Output> {
        DynamicArrayBuilder::new(Utf8ArrayBuilder::<i32>::new(path))
    }

    fn large_utf8(path: String) -> DynamicArrayBuilder<Self::Output> {
        DynamicArrayBuilder::new(Utf8ArrayBuilder::<i64>::new(path))
    }

    fn date64(path: String) -> DynamicArrayBuilder<Self::Output> {
        let builder = PrimitiveArrayBuilder::<MutablePrimitiveArray<i64>>::new(path);
        let builder = PrimitiveArrayBuilder {
            path: builder.path,
            array: builder.array.to(DataType::Date64),
            finished: builder.finished,
        };
        DynamicArrayBuilder::new(builder)
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

        Ok(Box::new(StructArray::new(
            field_to_datatype(&self.field)?,
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

        Ok(Box::new(StructArray::new(
            field_to_datatype(&self.field)?,
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

        Ok(Box::new(UnionArray::new(
            field_to_datatype(&self.field)?,
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
            field_to_datatype(&self.field)?,
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
            field_to_datatype(&self.field)?,
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

        let dtype = field_to_datatype(&self.field)?;

        let entries_field = match dtype {
            DataType::Map(inner, _) => inner.as_ref().clone(),
            _ => fail!("Invalid data type during struct construction"),
        };

        let entries = StructArray::try_new(entries_field.data_type, vec![keys, vals], None)?;
        let entries: Box<dyn Array> = Box::new(entries);

        let array = MapArray::try_new(
            field_to_datatype(&self.field)?,
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
        *self = Self::new(self.path.clone());

        Ok(res)
    }
}

impl ArrayBuilder<Box<dyn Array>> for UnknownVariantBuilder {
    fn build_array(&mut self) -> Result<Box<dyn Array>> {
        Ok(Box::new(NullArray::new(DataType::Null, 0)))
    }
}

#[derive(Debug, Default)]
pub struct PrimitiveArrayBuilder<B> {
    path: String,
    array: B,
    finished: bool,
}

impl<B: Default> PrimitiveArrayBuilder<B> {
    pub fn new(path: String) -> Self {
        Self {
            path,
            array: B::default(),
            finished: false,
        }
    }
}

macro_rules! impl_primitive_array_builder {
    ($ty:ty, $array:ty, $($variant:ident),*) => {
        impl EventSink for PrimitiveArrayBuilder<$ty> {
            macros::forward_generic_to_specialized!();
            macros::accept_start!((this, ev, _val, _next) {
                fail!(
                    "Cannot handle event {ev} in PrimitiveArrayBuilder<{ty}> [{path}]",
                    ev=ev,
                    ty=stringify!($ty),
                    path=this.path,
                );
            });
            macros::accept_end!((this, ev, _val, _next) {
                fail!(
                    "Cannot handle event {ev} in PrimitiveArrayBuilder<{ty}> [{path}]",
                    ev=ev,
                    ty=stringify!($ty),
                    path=this.path,
                );
            });
            macros::accept_marker!((this, ev, _val, _next) {
                if !matches!(ev, Event::Some) {
                    fail!(
                        "Cannot handle event {ev} in PrimitiveArrayBuilder<{ty}> [{path}]",
                        ev=ev,
                        ty=stringify!($ty),
                        path=this.path,
                    );
                }
                Ok(())
            });
            macros::accept_value!((this, ev, _val, _next) {
                match ev {
                    $(Event::$variant(_) => this.array.push(Some(ev.try_into()?)),)*
                    Event::Null => this.array.push(None),
                    Event::Default => this.array.push(Some(Default::default())),
                    ev => fail!(
                        "Cannot handle event {ev} in PrimitiveArrayBuilder<{ty}> [{path}]",
                        ev=ev,
                        ty=stringify!($ty),
                        path=this.path,
                    ),
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

impl_primitive_array_builder!(
    MutablePrimitiveArray<i8>,
    PrimitiveArray<_>,
    U8,
    I8,
    U16,
    I16,
    U32,
    I32,
    U64,
    I64
);
impl_primitive_array_builder!(
    MutablePrimitiveArray<i16>,
    PrimitiveArray<_>,
    U8,
    I8,
    U16,
    I16,
    U32,
    I32,
    U64,
    I64
);
impl_primitive_array_builder!(
    MutablePrimitiveArray<i32>,
    PrimitiveArray<_>,
    U8,
    I8,
    U16,
    I16,
    U32,
    I32,
    U64,
    I64
);
impl_primitive_array_builder!(
    MutablePrimitiveArray<i64>,
    PrimitiveArray<_>,
    U8,
    I8,
    U16,
    I16,
    U32,
    I32,
    U64,
    I64
);

impl_primitive_array_builder!(
    MutablePrimitiveArray<u8>,
    PrimitiveArray<_>,
    U8,
    I8,
    U16,
    I16,
    U32,
    I32,
    U64,
    I64
);
impl_primitive_array_builder!(
    MutablePrimitiveArray<u16>,
    PrimitiveArray<_>,
    U8,
    I8,
    U16,
    I16,
    U32,
    I32,
    U64,
    I64
);
impl_primitive_array_builder!(
    MutablePrimitiveArray<u32>,
    PrimitiveArray<_>,
    U8,
    I8,
    U16,
    I16,
    U32,
    I32,
    U64,
    I64
);
impl_primitive_array_builder!(
    MutablePrimitiveArray<u64>,
    PrimitiveArray<_>,
    U8,
    I8,
    U16,
    I16,
    U32,
    I32,
    U64,
    I64
);

impl_primitive_array_builder!(MutablePrimitiveArray<f16>, PrimitiveArray<_>, F32);
impl_primitive_array_builder!(MutablePrimitiveArray<f32>, PrimitiveArray<_>, F32);
impl_primitive_array_builder!(MutablePrimitiveArray<f64>, PrimitiveArray<_>, F64);

impl_primitive_array_builder!(MutableBooleanArray, BooleanArray, Bool);

#[derive(Debug, Default)]
pub struct Utf8ArrayBuilder<O: Offset> {
    path: String,
    array: MutableUtf8Array<O>,
    finished: bool,
}

impl<O: Offset> Utf8ArrayBuilder<O> {
    pub fn new(path: String) -> Self {
        Self {
            path,
            array: MutableUtf8Array::<O>::default(),
            finished: true,
        }
    }
}

impl<O: Offset> EventSink for Utf8ArrayBuilder<O> {
    macros::accept_start!((this, ev, _val, _next) {
        fail!("Utf8ArrayBuilder cannot accept {ev} [{path}]", path=this.path)
    });
    macros::accept_end!((this, ev, _val, _next) {
        fail!("Utf8ArrayBuilder cannot accept {ev} [{path}]", path=this.path)
    });
    macros::accept_marker!((this, ev, _val, _next) {
        match ev {
            Event::Some => Ok(()),
            _ => fail!("Utf8ArrayBuilder cannot accept {ev} [{path}]", path=this.path),
        }
    });

    macros::fail_on_non_string_primitive!("Utf8ArrayBuilder");

    fn accept_str(&mut self, val: &str) -> Result<()> {
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
            Event::OwnedStr(val) => self.accept_str(&val),
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

fn field_to_datatype(field: &GenericField) -> Result<DataType> {
    let field: Field = field.try_into()?;
    Ok(field.data_type)
}
