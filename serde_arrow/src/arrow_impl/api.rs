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

/// Build arrow arrays record by record (*requires one of the `arrow-*`
/// features*)
///
/// The given items should be records (e.g., structs). To serialize items
/// encoding single values consider the [`Items`][crate::utils::Items] and
/// [`Item`][crate::utils::Item] wrappers.
///
/// Example:
///
/// ```rust
/// # fn main() -> serde_arrow::Result<()> {
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
///
/// let mut builder = ArrowBuilder::new(&[
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

/// Build arrow arrays from the given items  (*requires one of the `arrow-*`
/// features*))
///
/// `items` should be given in the form a list of records (e.g., a vector of
/// structs). To serialize items encoding single values consider the
/// [`Items`][crate::utils::Items] wrapper.
///
/// To build arrays record by record use [`ArrowBuilder`].
///
/// Example:
///
/// ```rust
/// # fn main() -> serde_arrow::Result<()> {
/// # use serde_arrow::_impl::arrow;
/// use arrow::datatypes::Field;
/// use serde::{Serialize, Deserialize};
/// use serde_arrow::schema::{SchemaLike, TracingOptions};
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
/// let fields = Vec::<Field>::from_type::<Record>(TracingOptions::default())?;
/// let arrays = serde_arrow::to_arrow(&fields, &items)?;
/// #
/// # assert_eq!(arrays.len(), 2);
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

/// Deserialize items from arrow arrays (*requires one of the `arrow-*`
/// features*)
///
/// The type should be a list of records (e.g., a vector of structs). To
/// deserialize items encoding single values consider the
/// [`Items`][crate::utils::Items] wrapper.
///
/// ```rust
/// # fn main() -> serde_arrow::Result<()> {
/// # use serde_arrow::_impl::arrow;
/// use arrow::datatypes::Field;
/// use serde::{Deserialize, Serialize};
/// use serde_arrow::schema::{SchemaLike, TracingOptions};
///
/// ##[derive(Deserialize, Serialize)]
/// struct Record {
///     a: Option<f32>,
///     b: u64,
/// }
///
/// let fields = Vec::<Field>::from_type::<Record>(TracingOptions::default())?;
/// # let items = &[Record { a: Some(1.0), b: 2}];
/// # let arrays = serde_arrow::to_arrow(&fields, &items)?;
/// #
/// let items: Vec<Record> = serde_arrow::from_arrow(&fields, &arrays)?;
/// # Ok(())
/// # }
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

/// Replaced by
/// [`SchemaLike::from_samples`][crate::schema::SchemaLike::from_samples]
/// (*[example][serialize_into_fields]*)
///
/// ```rust
/// # fn main() -> serde_arrow::Result<()> {
/// # use serde_arrow::_impl::arrow;
/// use arrow::datatypes::Field;
/// use serde::Serialize;
/// use serde_arrow::schema::{SchemaLike, TracingOptions};
///
/// ##[derive(Serialize)]
/// struct Record {
///     a: u32,
///     b: f32,
/// }
///
/// let samples = [Record { a: 1, b: 2.0 }, /* ... */ ];
/// let fields = Vec::<Field>::from_samples(&samples, TracingOptions::default())?;
/// #
/// # drop(fields);
/// # Ok(())
/// # }
/// ```
#[deprecated = "serialize_into_fields is deprecated. Use serde_arrow::schema::SchemaLike::from_samples instead"]
pub fn serialize_into_fields<T>(items: &T, options: TracingOptions) -> Result<Vec<Field>>
where
    T: Serialize + ?Sized,
{
    let mut tracer = Tracer::new(String::from("$"), options);
    tracer.trace_samples(items)?;

    let schema = tracer.to_schema()?;
    schema.to_arrow_fields()
}

/// Replaced by
/// [`SchemaLike::from_samples`][crate::schema::SchemaLike::from_samples] and
/// [`Items`][crate::utils::Items] (*[example][serialize_into_field]*)
///
/// ```rust
/// # fn main() -> serde_arrow::Result<()> {
/// # use serde_arrow::_impl::arrow;
/// use arrow::datatypes::Field;
/// use serde_arrow::{
///     schema::{SchemaLike, TracingOptions},
///     utils::Items,
/// };
///
/// let samples: Vec<u32> = vec![1, 2, 3, /* ... */ ];
/// let fields = Vec::<Field>::from_samples(&Items(&samples), TracingOptions::default())?;
/// #
/// # drop(fields);
/// # Ok(())
/// # }
/// ```
#[deprecated = "serialize_into_field is deprecated. Use serde_arrow::schema::SchemaLike with serde_arrow::utils::Items instead"]
pub fn serialize_into_field<T>(items: &T, name: &str, options: TracingOptions) -> Result<Field>
where
    T: Serialize + ?Sized,
{
    let mut tracer = Tracer::new(String::from("$"), options);
    tracer.trace_samples(items)?;

    let field = tracer.to_field(name)?;
    Field::try_from(&field)
}

/// Renamed to [`serde_arrow::to_arrow`][crate::to_arrow]
#[deprecated = "serialize_into_arrays is deprecated. Use serde_arrow::to_arrow instead"]
pub fn serialize_into_arrays<T: Serialize + ?Sized>(
    fields: &[Field],
    items: &T,
) -> Result<Vec<ArrayRef>> {
    crate::to_arrow(fields, items)
}

/// Renamed to [`serde_arrow::from_arrow`][crate::from_arrow]
#[deprecated = "deserialize_from_arrays is deprecated. Use serde_arrow::from_arrow instead"]
pub fn deserialize_from_arrays<'de, T, A>(fields: &'de [Field], arrays: &'de [A]) -> Result<T>
where
    T: Deserialize<'de>,
    A: AsRef<dyn Array>,
{
    crate::from_arrow(fields, arrays)
}

/// Replaced by [`serde_arrow::to_arrow`][crate::to_arrow] and
/// [`Items`][crate::utils::Items] (*[example][serialize_into_array]*)
///
/// ```rust
/// # fn main() -> serde_arrow::Result<()> {
/// # use serde_arrow::_impl::arrow::datatypes::Field;
/// # use serde_arrow::schema::{SchemaLike, TracingOptions};
/// use serde_arrow::utils::Items;
///
/// let samples: Vec<u32> = vec![1, 2, 3, /* ... */ ];
/// # let fields = Vec::<Field>::from_samples(&Items(&samples), TracingOptions::default())?;
/// let arrays = serde_arrow::to_arrow(&fields, &Items(&samples))?;
/// #
/// # Ok(())
/// # }
/// ```
#[deprecated = "serialize_into_array is deprecated. Use serde_arrow::to_arrow with serde_arrow::utils::Items instead"]
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

/// Replaced by [`serde_arrow::to_arrow`][crate::from_arrow] and
/// [`Items`][crate::utils::Items] (*[example][deserialize_from_array]*)
///
/// ```rust
/// # fn main() -> serde_arrow::Result<()> {
/// # use serde_arrow::schema::{SerdeArrowSchema, SchemaLike, TracingOptions};
/// # let samples: Vec<u32> = vec![1, 2, 3, /* ... */ ];
/// # let fields = SerdeArrowSchema::from_samples(&Items(&samples), TracingOptions::default())?
/// #     .to_arrow_fields()?;
/// # let arrays = serde_arrow::to_arrow(&fields, &Items(&samples))?;
/// #
/// use serde_arrow::utils::Items;
///
/// let Items(items): Items<Vec<u32>> = serde_arrow::from_arrow(&fields, &arrays)?;
/// #
/// # drop(items);
/// # Ok(())
/// # }
/// ```
#[deprecated = "deserialize_from_array is deprecated. Use serde_arrow::from_arrow instead"]
pub fn deserialize_from_array<'de, T, A>(field: &'de Field, array: &'de A) -> Result<T>
where
    T: Deserialize<'de>,
    A: AsRef<dyn Array> + 'de + ?Sized,
{
    generic::deserialize_from_array(field, array.as_ref())
}

/// Replaced by [`ArrowBuilder`][crate::ArrowBuilder] and
/// [`Items`][crate::utils::Items] / [`Item`][crate::utils::Item] (*[example][ArrayBuilder]*)
///
/// ```rust
/// # fn main() -> serde_arrow::Result<()> {
/// # use serde_arrow::_impl::arrow;
/// use arrow::datatypes::{DataType, Field};
/// use serde_arrow::{ArrowBuilder, utils::{Items, Item}};
///
/// let mut builder = ArrowBuilder::new(&[
///     Field::new("item", DataType::UInt8, false),
/// ])?;
///
/// builder.push(&Item(0))?;
/// builder.push(&Item(1))?;
/// builder.push(&Item(2))?;
///
/// builder.extend(&Items(&[3, 4, 5]))?;
///
/// let arrays = builder.build_arrays()?;
/// # drop(arrays);
/// # Ok(())
/// # }
/// ```
#[deprecated = "serde_arrow::arrow::ArrayBuilder is deprecated. Use serde_arrow::ArrowBuilder with serde_arrow::utils::Items instead"]
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
