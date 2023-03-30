pub(crate) mod error;
pub(crate) mod event;
pub(crate) mod generic_sinks;
pub(crate) mod generic_sources;
pub(crate) mod schema;
pub(crate) mod sink;
pub(crate) mod source;

use serde::Serialize;

use self::{
    error::{fail, Result},
    generic_sinks::{
        DictionaryUtf8ArrayBuilder, ListArrayBuilder, MapArrayBuilder, NaiveDateTimeStrBuilder,
        PrimitiveBuilders, StructArrayBuilder, TupleStructBuilder, UnionArrayBuilder,
        UtcDateTimeStrBuilder,
    },
    schema::{GenericDataType, GenericField, Tracer, TracingOptions},
    sink::{serialize_into_sink, ArrayBuilder, DynamicArrayBuilder, StripOuterSequenceSink},
};

pub fn serialize_into_fields<T>(items: &T, options: TracingOptions) -> Result<Vec<GenericField>>
where
    T: Serialize + ?Sized,
{
    let tracer = Tracer::new(options);
    let mut tracer = StripOuterSequenceSink::new(tracer);
    serialize_into_sink(&mut tracer, items)?;
    let root = tracer.into_inner().to_field("root")?;

    match root.data_type {
        GenericDataType::Struct => {}
        GenericDataType::Null => fail!("No records found to determine schema"),
        dt => fail!("Unexpected root data type {dt:?}"),
    };

    Ok(root.children)
}

pub fn serialize_into_field<T>(
    items: &T,
    name: &str,
    options: TracingOptions,
) -> Result<GenericField>
where
    T: Serialize + ?Sized,
{
    let tracer = Tracer::new(options);
    let mut tracer = StripOuterSequenceSink::new(tracer);
    serialize_into_sink(&mut tracer, items)?;
    let field = tracer.into_inner().to_field(name)?;
    Ok(field)
}

pub fn serialize_into_arrays<T, Arrow>(
    fields: &[GenericField],
    items: &T,
) -> Result<Vec<Arrow::ArrayRef>>
where
    T: Serialize + ?Sized,
    Arrow: PrimitiveBuilders,
    NaiveDateTimeStrBuilder<DynamicArrayBuilder<Arrow::ArrayRef>>: ArrayBuilder<Arrow::ArrayRef>,
    UtcDateTimeStrBuilder<DynamicArrayBuilder<Arrow::ArrayRef>>: ArrayBuilder<Arrow::ArrayRef>,
    TupleStructBuilder<DynamicArrayBuilder<Arrow::ArrayRef>>: ArrayBuilder<Arrow::ArrayRef>,
    StructArrayBuilder<DynamicArrayBuilder<Arrow::ArrayRef>>: ArrayBuilder<Arrow::ArrayRef>,
    UnionArrayBuilder<DynamicArrayBuilder<Arrow::ArrayRef>>: ArrayBuilder<Arrow::ArrayRef>,
    DictionaryUtf8ArrayBuilder<DynamicArrayBuilder<Arrow::ArrayRef>>: ArrayBuilder<Arrow::ArrayRef>,
    MapArrayBuilder<DynamicArrayBuilder<Arrow::ArrayRef>>: ArrayBuilder<Arrow::ArrayRef>,
    ListArrayBuilder<DynamicArrayBuilder<Arrow::ArrayRef>, i32>: ArrayBuilder<Arrow::ArrayRef>,
    ListArrayBuilder<DynamicArrayBuilder<Arrow::ArrayRef>, i64>: ArrayBuilder<Arrow::ArrayRef>,
{
    let builder = generic_sinks::build_struct_array_builder::<Arrow>(fields)?;
    let mut builder = StripOuterSequenceSink::new(builder);

    serialize_into_sink(&mut builder, items)?;
    builder.into_inner().build_arrays()
}

pub fn serialize_into_array<T, Arrow>(field: &GenericField, items: &T) -> Result<Arrow::ArrayRef>
where
    T: Serialize + ?Sized,
    Arrow: PrimitiveBuilders,
    NaiveDateTimeStrBuilder<DynamicArrayBuilder<Arrow::ArrayRef>>: ArrayBuilder<Arrow::ArrayRef>,
    UtcDateTimeStrBuilder<DynamicArrayBuilder<Arrow::ArrayRef>>: ArrayBuilder<Arrow::ArrayRef>,
    TupleStructBuilder<DynamicArrayBuilder<Arrow::ArrayRef>>: ArrayBuilder<Arrow::ArrayRef>,
    StructArrayBuilder<DynamicArrayBuilder<Arrow::ArrayRef>>: ArrayBuilder<Arrow::ArrayRef>,
    UnionArrayBuilder<DynamicArrayBuilder<Arrow::ArrayRef>>: ArrayBuilder<Arrow::ArrayRef>,
    DictionaryUtf8ArrayBuilder<DynamicArrayBuilder<Arrow::ArrayRef>>: ArrayBuilder<Arrow::ArrayRef>,
    MapArrayBuilder<DynamicArrayBuilder<Arrow::ArrayRef>>: ArrayBuilder<Arrow::ArrayRef>,
    ListArrayBuilder<DynamicArrayBuilder<Arrow::ArrayRef>, i32>: ArrayBuilder<Arrow::ArrayRef>,
    ListArrayBuilder<DynamicArrayBuilder<Arrow::ArrayRef>, i64>: ArrayBuilder<Arrow::ArrayRef>,
{
    let builder = generic_sinks::build_array_builder::<Arrow>(field)?;
    let mut builder = StripOuterSequenceSink::new(builder);

    serialize_into_sink(&mut builder, items).unwrap();
    builder.into_inner().build_array()
}
