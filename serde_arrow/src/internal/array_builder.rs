use serde::Serialize;

use marrow::array::Array;

use crate::internal::{
    error::Result, schema::SerdeArrowSchema, serialization::OuterSequenceBuilder,
};

/// Construct arrays by pushing individual records
///
/// It can be constructed via
///
/// - [`ArrayBuilder::new`]
#[cfg_attr(has_arrow, doc = r"- [`ArrayBuilder::from_arrow`]")]
#[cfg_attr(has_arrow2, doc = r"- [`ArrayBuilder::from_arrow2`]")]
///
#[cfg_attr(
    any(has_arrow, has_arrow2),
    doc = r"It supports array construction via"
)]
#[cfg_attr(any(has_arrow, has_arrow2), doc = r"")]
#[cfg_attr(has_arrow, doc = r"- [`ArrayBuilder::to_record_batch`]")]
#[cfg_attr(has_arrow, doc = r"- [`ArrayBuilder::to_arrow`]")]
#[cfg_attr(has_arrow2, doc = r"- [`ArrayBuilder::to_arrow2`]")]
///
/// Usage:
///
/// ```rust
/// # #[cfg(has_arrow)]
/// # fn main() -> serde_arrow::_impl::PanicOnError<()> {
/// # use serde_arrow::_impl::docs::defs::{Record, example_records};
/// # use serde_arrow::schema::{TracingOptions, SchemaLike};
/// # use serde::Serialize;
/// # let items = example_records();
/// # let item = items[0].clone();
/// # let fields = Vec::<serde_arrow::_impl::arrow::datatypes::FieldRef>::from_type::<Record>(TracingOptions::default())?;
/// use serde_arrow::ArrayBuilder;
/// let mut builder = ArrayBuilder::from_arrow(&fields)?;
///
/// // push multiple items
/// builder.extend(&items)?;
///
/// // push a single items
/// builder.push(&item)?;
///
/// // build the arrays
/// let arrays = builder.to_arrow()?;
/// #
/// # Ok(()) }
/// # #[cfg(not(has_arrow))]
/// # fn main() {}
/// ```
pub struct ArrayBuilder {
    pub(crate) builder: OuterSequenceBuilder,
    #[allow(unused)]
    pub(crate) schema: SerdeArrowSchema,
}

impl ArrayBuilder {
    /// Construct an array builder from an [`SerdeArrowSchema`]
    pub fn new(schema: SerdeArrowSchema) -> Result<Self> {
        Ok(Self {
            builder: OuterSequenceBuilder::new(&schema)?,
            schema,
        })
    }
}

impl std::fmt::Debug for ArrayBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ArrayBuilder {{ .. }}")
    }
}

impl ArrayBuilder {
    /// Add a single record to the arrays
    ///
    pub fn push<T: Serialize>(&mut self, item: T) -> Result<()> {
        self.builder.push(item)
    }

    /// Add multiple records to the arrays
    ///
    pub fn extend<T: Serialize>(&mut self, items: T) -> Result<()> {
        self.builder.extend(items)
    }

    pub(crate) fn build_arrays(&mut self) -> Result<Vec<Array>> {
        let mut arrays = Vec::new();
        for field in self.builder.take_records()? {
            arrays.push(field.into_array()?);
        }
        Ok(arrays)
    }
}

impl std::convert::AsRef<ArrayBuilder> for ArrayBuilder {
    fn as_ref(&self) -> &ArrayBuilder {
        self
    }
}

impl std::convert::AsMut<ArrayBuilder> for ArrayBuilder {
    fn as_mut(&mut self) -> &mut ArrayBuilder {
        self
    }
}

#[allow(unused)]
const _: () = {
    trait AssertSendSync: Send + Sync {}
    impl AssertSendSync for ArrayBuilder {}
};
