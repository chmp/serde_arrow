#![deny(missing_docs)]
use serde::{Deserialize, Serialize};

use crate::{
    _impl::arrow::{
        array::{Array, ArrayRef},
        datatypes::Field,
    },
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

/// Deserialize items from arrow arrays (*requires one of the `arrow-*`
/// features*)
///
/// The type should be a list of records (e.g., a vector of structs).
///
/// ```rust
/// use serde::{Deserialize, Serialize};
/// use serde_arrow::{
///     arrow::{
///         deserialize_from_arrays,
///         serialize_into_arrays,
///         serialize_into_fields,
///     },
///     schema::TracingOptions,
/// };
///
/// ##[derive(Deserialize, Serialize)]
/// struct Record {
///     a: Option<f32>,
///     b: u64,
/// }
///
/// // provide an example record to get the field information
/// let fields = serialize_into_fields(
///     &[Record { a: Some(1.0), b: 2}],
///     TracingOptions::default(),
/// ).unwrap();
/// # let items = &[Record { a: Some(1.0), b: 2}];
/// # let arrays = serialize_into_arrays(&fields, &items).unwrap();
/// #
///
/// // deserialize the records from arrays
/// let items: Vec<Record> = deserialize_from_arrays(&fields, &arrays).unwrap();
/// ```
///
pub fn from_arrow<'de, T, A>(fields: &'de [Field], arrays: &'de [A]) -> Result<T>
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
#[deprecated = "serialize_into_fields is deprecated. Use serde_arrow::schema::SerdeArrowSchema::from_samples instead"]
pub fn serialize_into_fields<T>(items: &T, options: TracingOptions) -> Result<Vec<Field>>
where
    T: Serialize + ?Sized,
{
    let mut tracer = Tracer::new(String::from("$"), options);
    tracer.trace_samples(items)?;

    let schema = tracer.to_schema()?;
    schema.to_arrow_fields()
}

/// Determine the schema of an object that represents a single array
#[deprecated = "serialize_into_field is deprecated. Use serde_arrow::to_arrow with serde_arrow::utils::Items instead"]
pub fn serialize_into_field<T>(items: &T, name: &str, options: TracingOptions) -> Result<Field>
where
    T: Serialize + ?Sized,
{
    let mut tracer = Tracer::new(String::from("$"), options);
    tracer.trace_samples(items)?;

    let field = tracer.to_field(name)?;
    Field::try_from(&field)
}

/// Build arrays from the given items
#[deprecated = "serialize_into_arrays is deprecated. Use serde_arrow::to_arrow instead"]
pub fn serialize_into_arrays<T: Serialize + ?Sized>(
    fields: &[Field],
    items: &T,
) -> Result<Vec<ArrayRef>> {
    to_arrow(fields, items)
}

/// Build arrow arrays from the given items  (*requires one of the `arrow-*`
/// features*))
///
/// `items` should be given in the form a list of records (e.g., a vector of
/// structs).
///
/// Example:
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
/// let fields = SerdeArrowSchema::from_type::<Record>(TracingOptions::default())?.to_arrow_fields()?;
/// let arrays = serde_arrow::to_arrow(&fields, &items)?;
///
/// assert_eq!(arrays.len(), 2);
/// # Ok(())
/// # }
/// ```
///
pub fn to_arrow<T: Serialize + ?Sized>(fields: &[Field], items: &T) -> Result<Vec<ArrayRef>> {
    let fields = fields
        .iter()
        .map(GenericField::try_from)
        .collect::<Result<Vec<_>>>()?;

    let program = compile_serialization(&fields, CompilationOptions::default())?;
    let mut interpreter = Interpreter::new(program);
    serialize_into_sink(&mut interpreter, items)?;
    interpreter.build_arrow_arrays()
}

/// Deserialize a type from the given arrays
#[deprecated = "deserialize_from_arrays is deprecated. Use serde_arrow::from_arrow instead"]
pub fn deserialize_from_arrays<'de, T, A>(fields: &'de [Field], arrays: &'de [A]) -> Result<T>
where
    T: Deserialize<'de>,
    A: AsRef<dyn Array>,
{
    from_arrow(fields, arrays)
}

/// Serialize an object that represents a single array into an array
#[deprecated = "serialize_into_array is deprecated. Use serde_arrow::arrow::ArrayBuilder instead"]
pub fn serialize_into_array<T>(field: &Field, items: &T) -> Result<ArrayRef>
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
    interpreter.build_arrow_array()
}

/// Deserialize a sequence of objects from a single array
#[deprecated = "deserialize_from_array is deprecated"]
pub fn deserialize_from_array<'de, T, A>(field: &'de Field, array: &'de A) -> Result<T>
where
    T: Deserialize<'de>,
    A: AsRef<dyn Array> + 'de + ?Sized,
{
    generic::deserialize_from_array(field, array.as_ref())
}

/// Build a single array item by item
///
/// Example:
///
/// ```rust
/// # use serde_arrow::_impl::arrow as arrow;
/// use arrow::datatypes::{Field, DataType};
/// use serde_arrow::arrow::ArrayBuilder;
///
/// let field = Field::new("value", DataType::Int64, false);
/// let mut builder = ArrayBuilder::new(&field).unwrap();
///
/// builder.push(&-1_i64).unwrap();
/// builder.push(&2_i64).unwrap();
/// builder.push(&-3_i64).unwrap();
///
/// builder.extend(&[4_i64, -5, 6]).unwrap();
///
/// let array = builder.build_array().unwrap();
/// assert_eq!(array.len(), 6);
/// ```
#[deprecated = "serde_arrow::arrow::ArrayBuilder is deprecated. Use serde_arrow::ArrowBuilder instead"]
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
    pub fn build_array(&mut self) -> Result<ArrayRef> {
        self.0 .0.build_arrow_array()
    }
}

/// Build arrow arrays record by record
///
/// Example:
///
/// ```rust
/// # use serde_arrow::_impl::arrow as arrow;
/// use arrow::datatypes::{DataType, Field};
/// use serde::Serialize;
/// use serde_arrow::ArrowBuilder;
///
/// ##[derive(Serialize)]
/// struct Record {
///     a: Option<f32>,
///     b: u64,
/// }

/// let fields = vec![
///     Field::new("a", DataType::Float32, true),
///     Field::new("b", DataType::UInt64, false),
/// ];
/// let mut builder = ArrowBuilder::new(&fields).unwrap();
///
/// builder.push(&Record { a: Some(1.0), b: 2}).unwrap();
/// builder.push(&Record { a: Some(3.0), b: 4}).unwrap();
/// builder.push(&Record { a: Some(5.0), b: 5}).unwrap();
///
/// builder.extend(&[
///     Record { a: Some(6.0), b: 7},
///     Record { a: Some(8.0), b: 9},
///     Record { a: Some(10.0), b: 11},
/// ]).unwrap();
///
/// let arrays = builder.build_arrays().unwrap();
///
/// assert_eq!(arrays.len(), 2);
/// assert_eq!(arrays[0].len(), 6);
/// ```
pub struct ArrowBuilder(generic::GenericBuilder);

impl std::fmt::Debug for ArrowBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ArrowBuilder<...>")
    }
}

impl ArrowBuilder {
    /// Build a new ArrowBuilder for the given fields
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
    pub fn build_arrays(&mut self) -> Result<Vec<ArrayRef>> {
        self.0 .0.build_arrow_arrays()
    }
}
