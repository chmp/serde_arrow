use serde::Serialize;

use marrow::{
    array::Array,
    datatypes::{Field, FieldMeta},
};

use crate::internal::{
    error::Result, schema::SerdeArrowSchema, serialization::OuterSequenceBuilder,
};

/// Construct arrays by pushing individual records
///
/// It can be constructed via
///
/// - [`ArrayBuilder::new`]
/// - [`ArrayBuilder::from_marrow`]
#[cfg_attr(has_arrow, doc = r"- [`ArrayBuilder::from_arrow`]")]
#[cfg_attr(has_arrow2, doc = r"- [`ArrayBuilder::from_arrow2`]")]
///
/// It supports array construction via
/// - [`ArrayBuilder::to_marrow`]
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
}

impl ArrayBuilder {
    /// Construct an array builder from an [`SerdeArrowSchema`]
    pub fn new(schema: SerdeArrowSchema) -> Result<Self> {
        Self::from_marrow_vec(schema.fields)
    }

    pub(crate) fn from_marrow_vec(fields: Vec<Field>) -> Result<Self> {
        Ok(Self {
            builder: OuterSequenceBuilder::new(fields)?,
        })
    }

    pub fn reserve(&mut self, additional: usize) {
        self.builder.reserve(additional);
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

    pub(crate) fn take(&mut self) -> Self {
        Self {
            builder: OuterSequenceBuilder(self.builder.0.take_self()),
        }
    }

    #[inline]
    pub(crate) fn into_arrays_and_field_metas(self) -> Result<(Vec<Array>, Vec<FieldMeta>)> {
        let mut arrays = Vec::with_capacity(self.builder.num_fields());
        let mut metas = Vec::with_capacity(self.builder.num_fields());
        for builder in self.builder.0.fields {
            let (array, meta) = builder.into_array_and_field_meta()?;
            arrays.push(array);
            metas.push(meta);
        }
        Ok((arrays, metas))
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
