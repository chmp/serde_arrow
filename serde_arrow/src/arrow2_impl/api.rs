//! Support for the `arrow2` crate (requires one the `arrow2-*` features)
//!
//! Functions to convert Rust objects into Arrow arrays and back.
//!
use serde::{Deserialize, Serialize};

use crate::{
    _impl::arrow2::{array::Array, datatypes::Field},
    internal::{
        error::Result,
        generic,
        schema::GenericField,
        serialization::{compile_serialization, CompilationOptions, Interpreter},
        sink::serialize_into_sink,
        source::deserialize_from_source,
        tracing::{Tracer, TracingOptions},
    },
};


/// Build arrow2 arrays record by record  (*requires one of the `arrow2-*`
/// features*)
///
/// Example:
///
/// ```rust
/// # fn main() -> serde_arrow::Result<()> {
/// # use serde_arrow::_impl::arrow2 as arrow2;
/// use arrow2::datatypes::{DataType, Field};
/// use serde::Serialize;
/// use serde_arrow::Arrow2Builder;
///
/// ##[derive(Serialize)]
/// struct Record {
///     a: Option<f32>,
///     b: u64,
/// }
///
/// let mut builder = Arrow2Builder::new(&[
///     Field::new("a", DataType::Float32, true),
///     Field::new("b", DataType::UInt64, false),
/// ])?;
///
/// builder.push(&Record { a: Some(1.0), b: 2})?;
/// builder.push(&Record { a: Some(3.0), b: 4})?;
/// builder.push(&Record { a: Some(5.0), b: 5})?;
///
/// builder.extend(&[
///     Record { a: Some(6.0), b: 7},
///     Record { a: Some(8.0), b: 9},
///     Record { a: Some(10.0), b: 11},
/// ])?;
///
/// let arrays = builder.build_arrays()?;
/// #
/// # assert_eq!(arrays.len(), 2);
/// # assert_eq!(arrays[0].len(), 6);
/// # Ok(())
/// # }
/// ```
pub struct Arrow2Builder(generic::GenericBuilder);

impl std::fmt::Debug for Arrow2Builder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Arrow2Builder<...>")
    }
}

impl Arrow2Builder {
    /// Build a new Arrow2Builder for the given fields
    ///
    /// This method may fail when unsupported data types are encountered in the
    /// given fields.
    ///
    pub fn new(fields: &[Field]) -> Result<Self> {
        let fields = fields
            .iter()
            .map(GenericField::try_from)
            .collect::<Result<Vec<_>>>()?;
        Ok(Self(generic::GenericBuilder::new_for_arrays(&fields)?))
    }

    /// Add a single record to the arrays
    ///
    pub fn push<T: Serialize + ?Sized>(&mut self, item: &T) -> Result<()> {
        self.0.push(item)
    }

    /// Add multiple records to the arrays
    ///
    pub fn extend<T: Serialize + ?Sized>(&mut self, items: &T) -> Result<()> {
        self.0.extend(items)
    }

    /// Build the arrays from the rows pushed to far.
    ///
    /// This operation will reset the underlying buffers and start a new batch.
    ///
    pub fn build_arrays(&mut self) -> Result<Vec<Box<dyn Array>>> {
        self.0 .0.build_arrow2_arrays()
    }
}


/// Build arrow2 arrays from the given items  (*requires one of the `arrow2-*`
/// features*)
///
/// `items` should be given in the form a list of records (e.g., a vector of
/// structs).
///
/// To build arrays record by record use [Arrow2Builder].
///
/// ```rust
/// # fn main() -> serde_arrow::Result<()> {
/// use serde::{Serialize, Deserialize};
/// use serde_arrow::schema::{SerdeArrowSchema, TracingOptions};
///
/// ##[derive(Serialize, Deserialize)]
/// struct Record {
///     a: Option<f32>,
///     b: u64,
/// }
///
/// let items = vec![
///     Record { a: Some(1.0), b: 2},
///     // ...
/// ];
///
/// let fields = SerdeArrowSchema::from_type::<Record>(TracingOptions::default())?.
///     to_arrow2_fields()?;
///
/// let arrays = serde_arrow::to_arrow2(&fields, &items)?;
/// #
/// # assert_eq!(arrays.len(), 2);
/// # Ok(())
/// # }
/// ```
///
pub fn to_arrow2<T>(fields: &[Field], items: &T) -> Result<Vec<Box<dyn Array>>>
where
    T: Serialize + ?Sized,
{
    let fields = fields
        .iter()
        .map(GenericField::try_from)
        .collect::<Result<Vec<_>>>()?;

    let program = compile_serialization(&fields, CompilationOptions::default())?;
    let mut interpreter = Interpreter::new(program);
    serialize_into_sink(&mut interpreter, items)?;

    interpreter.build_arrow2_arrays()
}


/// Deserialize items from the given arrow2 arrays  (*requires* one of the
/// `arrow2-*` features)
///
/// The type should be a list of records (e.g., a vector of structs).
///
/// ```rust
/// # fn main() -> serde_arrow::Result<()> {
/// use serde::{Deserialize, Serialize};
/// use serde_arrow::schema::{SerdeArrowSchema, TracingOptions};
///
/// ##[derive(Deserialize, Serialize)]
/// struct Record {
///     a: Option<f32>,
///     b: u64,
/// }
///
/// let fields = SerdeArrowSchema::from_type::<Record>(TracingOptions::default())?
///     .to_arrow2_fields()?;
/// # let items = &[Record { a: Some(1.0), b: 2}];
/// # let arrays = serde_arrow::to_arrow2(&fields, &items).unwrap();
/// #
///
/// // deserialize the records from arrays
/// let items: Vec<Record> = serde_arrow::from_arrow2(&fields, &arrays)?;
/// # Ok(())
/// # }
/// ```
///
pub fn from_arrow2<'de, T, A>(fields: &'de [Field], arrays: &'de [A]) -> Result<T>
where
    T: Deserialize<'de>,
    A: AsRef<dyn Array>,
{
    use crate::internal::{
        common::{BufferExtract, Buffers},
        deserialization,
    };

    let fields = fields
        .iter()
        .map(GenericField::try_from)
        .collect::<Result<Vec<_>>>()?;

    let num_items = arrays
        .iter()
        .map(|a| a.as_ref().len())
        .min()
        .unwrap_or_default();

    let mut buffers = Buffers::new();
    let mut mappings = Vec::new();
    for (field, array) in fields.iter().zip(arrays.iter()) {
        mappings.push(array.as_ref().extract_buffers(field, &mut buffers)?);
    }

    let interpreter = deserialization::compile_deserialization(
        num_items,
        &mappings,
        buffers,
        deserialization::CompilationOptions::default(),
    )?;
    deserialize_from_source(interpreter)
}

/// Determine the schema (as a list of fields) for the given items
#[deprecated = "serde_arrow::arrow2::serialize_into_fields is deprecated. Use serde_arrow::schema::SerdeArrowSchema::from_samples instead"]
pub fn serialize_into_fields<T>(items: &T, options: TracingOptions) -> Result<Vec<Field>>
where
    T: Serialize + ?Sized,
{
    let mut tracer = Tracer::new(String::from("$"), options);
    tracer.trace_samples(items)?;

    let schema = tracer.to_schema()?;
    schema.to_arrow2_fields()
}

/// Renamed to [`serde_arrow::to_arrow2`][crate::to_arrow2]
#[deprecated = "serde_arrow::arrow2::serialize_into_arrays is deprecated. Use serde_arrow::to_arrow2 instead"]
pub fn serialize_into_arrays<T>(fields: &[Field], items: &T) -> Result<Vec<Box<dyn Array>>>
where
    T: Serialize + ?Sized,
{
    crate::to_arrow2(fields, items)
}

/// Renamed to [`serde_arrow::from_arrow2`][crate::from_arrow2]
#[deprecated = "serde_arrow::arrow2::deserialize_from_arrays is deprecated. Use serde_arrow::from_arrow2 instead"]
pub fn deserialize_from_arrays<'de, T, A>(fields: &'de [Field], arrays: &'de [A]) -> Result<T>
where
    T: Deserialize<'de>,
    A: AsRef<dyn Array>,
{
    crate::from_arrow2(fields, arrays)
}


/// Determine the schema of an object that represents a single array
#[deprecated = "serde_arrow::arrow2::serialize_into_field is deprecated. Use serde_arrow::schema::SerdeArrowSchema::from_samples instead"]
pub fn serialize_into_field<T>(items: &T, name: &str, options: TracingOptions) -> Result<Field>
where
    T: Serialize + ?Sized,
{
    let mut tracer = Tracer::new(String::from("$"), options);
    tracer.trace_samples(items)?;
    let field = tracer.to_field(name)?;
    Field::try_from(&field)
}

/// Serialize a sequence of objects representing a single array into an array
#[deprecated = "serde_arrow::arrow2::serialize_into_array is deprecated. Use serde_arrow::to_arrow2 instead"]
pub fn serialize_into_array<T>(field: &Field, items: &T) -> Result<Box<dyn Array>>
where
    T: Serialize + ?Sized,
{
    let field: GenericField = field.try_into()?;

    let program = compile_serialization(
        std::slice::from_ref(&field),
        CompilationOptions::default().wrap_with_struct(false),
    )?;
    let mut interpreter = Interpreter::new(program);
    serialize_into_sink(&mut interpreter, items)?;
    interpreter.build_arrow2_array()
}

/// Deserialize a sequence of objects from a single array
#[deprecated = "serde_arrow::arrow2::deserialize_from_array is deprecated. Use serde_arrow::from_arrow2 instead"]
pub fn deserialize_from_array<'de, T, A>(field: &'de Field, array: &'de A) -> Result<T>
where
    T: Deserialize<'de>,
    A: AsRef<dyn Array> + 'de + ?Sized,
{
    generic::deserialize_from_array(field, array.as_ref())
}

/// Build a single array item by item
#[deprecated = "serde_arrow::arrow2::ArrayBuilder is deprecated. Use serde_arrow::Arrow2Builder instead"]
pub struct ArrayBuilder(generic::GenericBuilder);

#[allow(deprecated)]
impl ArrayBuilder {
    /// Construct a new build for the given field
    pub fn new(field: &Field) -> Result<Self> {
        Ok(Self(generic::GenericBuilder::new_for_array(
            GenericField::try_from(field)?,
        )?))
    }

    /// Add a single item to the arrays
    pub fn push<T: Serialize + ?Sized>(&mut self, item: &T) -> Result<()> {
        self.0.push(item)
    }

    /// Add multiple items to the arrays
    pub fn extend<T: Serialize + ?Sized>(&mut self, items: &T) -> Result<()> {
        self.0.extend(items)
    }

    /// Build the array from the rows pushed to far.
    pub fn build_array(&mut self) -> Result<Box<dyn Array>> {
        self.0 .0.build_arrow2_array()
    }
}
