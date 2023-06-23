pub mod bytecode;
pub(crate) mod conversions;
pub(crate) mod error;
pub(crate) mod event;
pub(crate) mod generic_sinks;
pub(crate) mod generic_sources;
pub(crate) mod schema;
pub(crate) mod sink;
pub(crate) mod source;

use std::sync::RwLock;

use serde::Serialize;

use self::{
    error::{fail, Result},
    generic_sinks::{
        DictionaryUtf8ArrayBuilder, ListArrayBuilder, MapArrayBuilder, NaiveDateTimeStrBuilder,
        PrimitiveBuilders, StructArrayBuilder, TupleStructBuilder, UnionArrayBuilder,
        UnknownVariantBuilder, UtcDateTimeStrBuilder,
    },
    schema::{GenericDataType, GenericField, Tracer, TracingOptions},
    sink::{
        serialize_into_sink, ArrayBuilder, DynamicArrayBuilder, EventSerializer, EventSink,
        StripOuterSequenceSink,
    },
};

pub static CONFIGURATION: RwLock<Configuration> = RwLock::new(Configuration {
    debug_print_program: false,
    _prevent_construction: (),
});

/// The crate settings can be configured by calling [configure]
#[derive(Default, Clone)]
pub struct Configuration {
    pub(crate) debug_print_program: bool,
    /// A non public member to allow extending the member list as non-breaking
    /// changes
    _prevent_construction: (),
}

/// Change global configuration options
///
/// Note the configuration will be shared by all threads in the current program.
/// Thread-local configurations are not supported at the moment.
///
/// Usage:
///
/// ```
/// serde_arrow::experimental::configure(|c| {
///     // set attributes on c
/// });
/// ```
pub fn configure<F: FnOnce(&mut Configuration)>(f: F) {
    let mut guard = CONFIGURATION.write().unwrap();
    f(&mut guard)
}

pub fn serialize_into_fields<T>(items: &T, options: TracingOptions) -> Result<Vec<GenericField>>
where
    T: Serialize + ?Sized,
{
    let tracer = Tracer::new(String::from("$"), options);
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
    let tracer = Tracer::new(String::from("$"), options);
    let tracer = StripOuterSequenceSink::new(tracer);
    let mut tracer = tracer;
    serialize_into_sink(&mut tracer, items)?;

    let field = tracer.into_inner().to_field(name)?;
    Ok(field)
}

pub struct GenericArrayBuilder<Arrow: PrimitiveBuilders> {
    builder: DynamicArrayBuilder<Arrow::Output>,
    field: GenericField,
}

impl<Arrow> GenericArrayBuilder<Arrow>
where
    Arrow: PrimitiveBuilders,
    NaiveDateTimeStrBuilder<DynamicArrayBuilder<Arrow::Output>>: ArrayBuilder<Arrow::Output>,
    UtcDateTimeStrBuilder<DynamicArrayBuilder<Arrow::Output>>: ArrayBuilder<Arrow::Output>,
    TupleStructBuilder<DynamicArrayBuilder<Arrow::Output>>: ArrayBuilder<Arrow::Output>,
    StructArrayBuilder<DynamicArrayBuilder<Arrow::Output>>: ArrayBuilder<Arrow::Output>,
    UnionArrayBuilder<DynamicArrayBuilder<Arrow::Output>>: ArrayBuilder<Arrow::Output>,
    DictionaryUtf8ArrayBuilder<DynamicArrayBuilder<Arrow::Output>>: ArrayBuilder<Arrow::Output>,
    MapArrayBuilder<DynamicArrayBuilder<Arrow::Output>>: ArrayBuilder<Arrow::Output>,
    ListArrayBuilder<DynamicArrayBuilder<Arrow::Output>, i32>: ArrayBuilder<Arrow::Output>,
    ListArrayBuilder<DynamicArrayBuilder<Arrow::Output>, i64>: ArrayBuilder<Arrow::Output>,
    UnknownVariantBuilder: ArrayBuilder<Arrow::Output>,
{
    pub fn new(field: GenericField) -> Result<Self> {
        Ok(Self {
            builder: generic_sinks::build_array_builder::<Arrow>(String::from("$"), &field)?,
            field,
        })
    }

    pub fn push<T: Serialize + ?Sized>(&mut self, item: &T) -> Result<()> {
        item.serialize(EventSerializer(&mut self.builder))?;
        Ok(())
    }

    pub fn extend<T: Serialize + ?Sized>(&mut self, items: &T) -> Result<()> {
        let mut builder = StripOuterSequenceSink::new(&mut self.builder);
        items.serialize(EventSerializer(&mut builder))?;
        Ok(())
    }

    pub fn build_array(&mut self) -> Result<Arrow::Output> {
        let mut builder =
            generic_sinks::build_array_builder::<Arrow>(String::from("$"), &self.field)?;
        std::mem::swap(&mut builder, &mut self.builder);

        builder.finish()?;
        builder.build_array()
    }
}

pub struct GenericArraysBuilder<Arrow: PrimitiveBuilders> {
    fields: Vec<GenericField>,
    builder: StructArrayBuilder<DynamicArrayBuilder<Arrow::Output>>,
}

impl<Arrow> GenericArraysBuilder<Arrow>
where
    Arrow: PrimitiveBuilders,
    NaiveDateTimeStrBuilder<DynamicArrayBuilder<Arrow::Output>>: ArrayBuilder<Arrow::Output>,
    UtcDateTimeStrBuilder<DynamicArrayBuilder<Arrow::Output>>: ArrayBuilder<Arrow::Output>,
    TupleStructBuilder<DynamicArrayBuilder<Arrow::Output>>: ArrayBuilder<Arrow::Output>,
    StructArrayBuilder<DynamicArrayBuilder<Arrow::Output>>: ArrayBuilder<Arrow::Output>,
    UnionArrayBuilder<DynamicArrayBuilder<Arrow::Output>>: ArrayBuilder<Arrow::Output>,
    DictionaryUtf8ArrayBuilder<DynamicArrayBuilder<Arrow::Output>>: ArrayBuilder<Arrow::Output>,
    MapArrayBuilder<DynamicArrayBuilder<Arrow::Output>>: ArrayBuilder<Arrow::Output>,
    ListArrayBuilder<DynamicArrayBuilder<Arrow::Output>, i32>: ArrayBuilder<Arrow::Output>,
    ListArrayBuilder<DynamicArrayBuilder<Arrow::Output>, i64>: ArrayBuilder<Arrow::Output>,
    UnknownVariantBuilder: ArrayBuilder<Arrow::Output>,
{
    pub fn new(fields: Vec<GenericField>) -> Result<Self> {
        Ok(Self {
            builder: generic_sinks::build_struct_array_builder::<Arrow>(
                String::from("$"),
                &fields,
            )?,
            fields,
        })
    }

    pub fn push<T: Serialize + ?Sized>(&mut self, item: &T) -> Result<()> {
        item.serialize(EventSerializer(&mut self.builder))?;
        Ok(())
    }

    pub fn extend<T: Serialize + ?Sized>(&mut self, items: &T) -> Result<()> {
        let mut builder = StripOuterSequenceSink::new(&mut self.builder);
        items.serialize(EventSerializer(&mut builder))?;
        Ok(())
    }

    pub fn build_arrays(&mut self) -> Result<Vec<Arrow::Output>> {
        let mut builder =
            generic_sinks::build_struct_array_builder::<Arrow>(String::from("$"), &self.fields)?;
        std::mem::swap(&mut builder, &mut self.builder);

        builder.finish()?;
        builder.build_arrays()
    }
}
