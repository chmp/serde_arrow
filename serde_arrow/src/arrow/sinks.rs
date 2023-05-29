use crate::{
    _impl::arrow::{
        array::{
            Array, ArrayData, ArrowPrimitiveType, BooleanBuilder, GenericStringBuilder, NullArray,
            OffsetSizeTrait, PrimitiveBuilder,
        },
        buffer::Buffer,
        datatypes::{
            DataType, Date64Type, Field, Float16Type, Float32Type, Float64Type, Int16Type,
            Int32Type, Int64Type, Int8Type, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
        },
    },
    internal::{
        bytecode::buffers,
        error::{fail, Result},
        event::Event,
        generic_sinks::{
            DictionaryUtf8ArrayBuilder, ListArrayBuilder, MapArrayBuilder, NullArrayBuilder,
            PrimitiveBuilders, StructArrayBuilder, TupleStructBuilder, UnionArrayBuilder,
            UnknownVariantBuilder,
        },
        schema::GenericField,
        sink::{macros, ArrayBuilder, DynamicArrayBuilder, EventSink},
    },
};

use super::type_support::FieldRef;

pub struct ArrowPrimitiveBuilders;

impl PrimitiveBuilders for ArrowPrimitiveBuilders {
    type Output = ArrayData;

    fn null(path: String) -> DynamicArrayBuilder<Self::Output> {
        DynamicArrayBuilder::new(NullArrayBuilder::new(path))
    }

    fn bool(path: String) -> DynamicArrayBuilder<Self::Output> {
        DynamicArrayBuilder::new(PrimitiveArrayBuilder::<BooleanBuilder>::new(path))
    }

    fn u8(path: String) -> DynamicArrayBuilder<Self::Output> {
        DynamicArrayBuilder::new(PrimitiveArrayBuilder::<PrimitiveBuilder<UInt8Type>>::new(
            path,
        ))
    }

    fn u16(path: String) -> DynamicArrayBuilder<Self::Output> {
        DynamicArrayBuilder::new(PrimitiveArrayBuilder::<PrimitiveBuilder<UInt16Type>>::new(
            path,
        ))
    }

    fn u32(path: String) -> DynamicArrayBuilder<Self::Output> {
        DynamicArrayBuilder::new(PrimitiveArrayBuilder::<PrimitiveBuilder<UInt32Type>>::new(
            path,
        ))
    }

    fn u64(path: String) -> DynamicArrayBuilder<Self::Output> {
        DynamicArrayBuilder::new(PrimitiveArrayBuilder::<PrimitiveBuilder<UInt64Type>>::new(
            path,
        ))
    }

    fn i8(path: String) -> DynamicArrayBuilder<Self::Output> {
        DynamicArrayBuilder::new(PrimitiveArrayBuilder::<PrimitiveBuilder<Int8Type>>::new(
            path,
        ))
    }

    fn i16(path: String) -> DynamicArrayBuilder<Self::Output> {
        DynamicArrayBuilder::new(PrimitiveArrayBuilder::<PrimitiveBuilder<Int16Type>>::new(
            path,
        ))
    }

    fn i32(path: String) -> DynamicArrayBuilder<Self::Output> {
        DynamicArrayBuilder::new(PrimitiveArrayBuilder::<PrimitiveBuilder<Int32Type>>::new(
            path,
        ))
    }

    fn i64(path: String) -> DynamicArrayBuilder<Self::Output> {
        DynamicArrayBuilder::new(PrimitiveArrayBuilder::<PrimitiveBuilder<Int64Type>>::new(
            path,
        ))
    }

    fn f16(path: String) -> DynamicArrayBuilder<Self::Output> {
        DynamicArrayBuilder::new(PrimitiveArrayBuilder::<PrimitiveBuilder<Float16Type>>::new(
            path,
        ))
    }

    fn f32(path: String) -> DynamicArrayBuilder<Self::Output> {
        DynamicArrayBuilder::new(PrimitiveArrayBuilder::<PrimitiveBuilder<Float32Type>>::new(
            path,
        ))
    }

    fn f64(path: String) -> DynamicArrayBuilder<Self::Output> {
        DynamicArrayBuilder::new(PrimitiveArrayBuilder::<PrimitiveBuilder<Float64Type>>::new(
            path,
        ))
    }

    fn date64(path: String) -> DynamicArrayBuilder<Self::Output> {
        DynamicArrayBuilder::new(PrimitiveArrayBuilder::<PrimitiveBuilder<Date64Type>>::new(
            path,
        ))
    }

    fn utf8(path: String) -> DynamicArrayBuilder<Self::Output> {
        DynamicArrayBuilder::new(Utf8ArrayBuilder::<i32>::new(path))
    }

    fn large_utf8(path: String) -> DynamicArrayBuilder<Self::Output> {
        DynamicArrayBuilder::new(Utf8ArrayBuilder::<i64>::new(path))
    }
}

impl ArrayBuilder<ArrayData> for NullArrayBuilder {
    fn build_array(&mut self) -> Result<ArrayData> {
        if !self.finished {
            fail!("Cannot build unfinished null array");
        }
        Ok(NullArray::new(self.length).into_data())
    }
}

impl ArrayBuilder<ArrayData> for UnknownVariantBuilder {
    fn build_array(&mut self) -> Result<ArrayData> {
        Ok(NullArray::new(0).into_data())
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
    ($ty:ty, $($variant:ident),*) => {
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
                    $(Event::$variant(_) => this.array.append_value(ev.try_into()?),)*
                    Event::Null => this.array.append_null(),
                    Event::Default => this.array.append_value(Default::default()),
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

        impl ArrayBuilder<ArrayData> for PrimitiveArrayBuilder<$ty> {
            fn build_array(&mut self) -> Result<ArrayData> {
                if !self.finished {
                    fail!(concat!(
                        "Cannot build array from unfinished PrimitiveArrayBuilder<",
                        stringify!($ty),
                        ">"
                    ));
                }
                Ok(self.array.finish().into_data())
            }
        }
    };
}

impl_primitive_array_builder!(
    PrimitiveBuilder<Int8Type>,
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
    PrimitiveBuilder<Int16Type>,
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
    PrimitiveBuilder<Int32Type>,
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
    PrimitiveBuilder<Int64Type>,
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
    PrimitiveBuilder<UInt8Type>,
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
    PrimitiveBuilder<UInt16Type>,
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
    PrimitiveBuilder<UInt32Type>,
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
    PrimitiveBuilder<UInt64Type>,
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
    PrimitiveBuilder<Date64Type>,
    U8,
    I8,
    U16,
    I16,
    U32,
    I32,
    U64,
    I64
);

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

impl ArrayBuilder<ArrayData> for PrimitiveArrayBuilder<PrimitiveBuilder<Float16Type>> {
    fn build_array(&mut self) -> Result<ArrayData> {
        if !self.finished {
            fail!(concat!(
                "Cannot build array from unfinished PrimitiveArrayBuilder<",
                stringify!($ty),
                ">"
            ));
        }
        Ok(self.array.finish().into_data())
    }
}

#[derive(Debug, Default)]
pub struct Utf8ArrayBuilder<O: OffsetSizeTrait> {
    path: String,
    array: GenericStringBuilder<O>,
    finished: bool,
}

impl<O: OffsetSizeTrait> Utf8ArrayBuilder<O> {
    pub fn new(path: String) -> Self {
        Self {
            path,
            array: Default::default(),
            finished: false,
        }
    }
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
            Event::OwnedStr(val) => self.accept_str(&val),
            ev => fail!("Cannot handle event {ev} in BooleanArrayBuilder"),
        }
    }

    fn finish(&mut self) -> Result<()> {
        self.finished = true;
        Ok(())
    }
}

impl<O: OffsetSizeTrait> ArrayBuilder<ArrayData> for Utf8ArrayBuilder<O> {
    fn build_array(&mut self) -> Result<ArrayData> {
        if !self.finished {
            fail!("Cannot build array from unfinished Utf8ArrayBuilder");
        }
        Ok(self.array.finish().into_data())
    }
}

fn build_struct_array<B>(
    len: usize,
    field: &GenericField,
    builders: &mut [B],
    validity: Option<&mut Vec<bool>>,
) -> Result<ArrayData>
where
    B: ArrayBuilder<ArrayData>,
{
    let validity = if let Some(validity) = validity {
        let validity = std::mem::take(validity);
        Some(build_null_bit_buffer(validity)?)
    } else {
        None
    };

    let mut data = Vec::new();
    for builder in builders {
        data.push(builder.build_array()?);
    }

    Ok(ArrayData::builder(field_to_datatype(field)?)
        .len(len)
        .null_bit_buffer(validity)
        .child_data(data)
        .build()?)
}

impl<B: ArrayBuilder<ArrayData>> ArrayBuilder<ArrayData> for StructArrayBuilder<B> {
    fn build_array(&mut self) -> Result<ArrayData> {
        if !self.finished {
            fail!("Cannot build array from unfinished StructArrayBuilder");
        }

        let len = self.validity.len();
        build_struct_array(
            len,
            &self.field,
            &mut self.builders,
            Some(&mut self.validity),
        )
    }
}

impl<B: ArrayBuilder<ArrayData>> ArrayBuilder<ArrayData> for TupleStructBuilder<B> {
    fn build_array(&mut self) -> Result<ArrayData> {
        if !self.finished {
            fail!("Cannot build array from unfinished TupleStructBuilder");
        }

        let len = self.validity.len();
        build_struct_array(
            len,
            &self.field,
            &mut self.builders,
            Some(&mut self.validity),
        )
    }
}

impl<B: ArrayBuilder<ArrayData>> ArrayBuilder<ArrayData> for UnionArrayBuilder<B> {
    fn build_array(&mut self) -> Result<ArrayData> {
        if !self.finished {
            fail!("Cannot build array from unfinished UnionArrayBuilder");
        }

        let field_types = std::mem::take(&mut self.field_types);
        let field_offsets = std::mem::take(&mut self.field_offsets);

        let values = self
            .field_builders
            .iter_mut()
            .map(|b| b.build_array())
            .collect::<Result<Vec<_>>>()?;

        let len = field_types.len();

        let res = ArrayData::builder(field_to_datatype(&self.field)?)
            .len(len)
            .add_buffer(Buffer::from_vec(field_types))
            .add_buffer(Buffer::from_vec(field_offsets))
            .child_data(values)
            .build()?;

        Ok(res)
    }
}

fn build_null_bit_buffer(validity: Vec<bool>) -> Result<Buffer> {
    let mut buffer = buffers::BitBuffer::default();
    for flag in validity {
        buffer.push(flag)?;
    }
    Ok(Buffer::from(buffer.buffer))
}

fn build_list_array<B: ArrayBuilder<ArrayData>, O: OffsetSizeTrait>(
    this: &mut ListArrayBuilder<B, O>,
) -> Result<ArrayData> {
    let values = this.builder.build_array()?;

    let validity = std::mem::take(&mut this.validity);
    let offsets = std::mem::take(&mut this.offsets);

    let len = validity.len();
    let null_bit_buffer = build_null_bit_buffer(validity)?;
    let offset_buffer = Buffer::from_vec(offsets);

    let array_data_builder = ArrayData::builder(field_to_datatype(&this.field)?)
        .len(len)
        .add_buffer(offset_buffer)
        .add_child_data(values)
        .null_bit_buffer(Some(null_bit_buffer));

    Ok(array_data_builder.build()?)
}

impl<B: ArrayBuilder<ArrayData>> ArrayBuilder<ArrayData> for ListArrayBuilder<B, i32> {
    fn build_array(&mut self) -> Result<ArrayData> {
        if !self.finished {
            fail!("Cannot build array from unfinished ListArrayBuilder");
        }
        build_list_array(self)
    }
}

impl<B: ArrayBuilder<ArrayData>> ArrayBuilder<ArrayData> for ListArrayBuilder<B, i64> {
    fn build_array(&mut self) -> Result<ArrayData> {
        if !self.finished {
            fail!("Cannot build array from unfinished ListArrayBuilder");
        }
        build_list_array(self)
    }
}

impl<B: ArrayBuilder<ArrayData>> ArrayBuilder<ArrayData> for MapArrayBuilder<B> {
    fn build_array(&mut self) -> Result<ArrayData> {
        if !self.finished {
            fail!("Cannot build array from unfinished MapArrayBuilder");
        }

        let keys = self.key_builder.build_array()?;
        let values = self.val_builder.build_array()?;

        let offsets = std::mem::take(&mut self.offsets);
        let offsets = Buffer::from_vec(offsets);

        let validity = std::mem::take(&mut self.validity);
        let len = validity.len();
        let validity = build_null_bit_buffer(validity)?;

        let dtype = field_to_datatype(&self.field)?;

        let inner_field = match &dtype {
            DataType::Map(inner, _) => inner.as_field_ref(),
            _ => fail!("Invalid datatype during map construction"),
        };

        let inner = ArrayData::builder(inner_field.data_type().clone())
            .len(keys.len())
            .child_data(vec![keys, values])
            .build()?;

        let res = ArrayData::builder(dtype)
            .len(len)
            .add_buffer(offsets)
            .add_child_data(inner)
            .null_bit_buffer(Some(validity))
            .build()?;
        Ok(res)
    }
}

impl<B: ArrayBuilder<ArrayData>> ArrayBuilder<ArrayData> for DictionaryUtf8ArrayBuilder<B> {
    fn build_array(&mut self) -> Result<ArrayData> {
        let values = self.values.build_array()?;
        let keys = self.keys.build_array()?;

        let data_type = DataType::Dictionary(
            Box::new(keys.data_type().clone()),
            Box::new(values.data_type().clone()),
        );

        let res = keys
            .into_builder()
            .data_type(data_type)
            .child_data(vec![values])
            .build()?;
        Ok(res)
    }
}

fn field_to_datatype(field: &GenericField) -> Result<DataType> {
    let field: Field = field.try_into()?;
    Ok(field.data_type().clone())
}
