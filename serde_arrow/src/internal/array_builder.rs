use serde::Serialize;

use crate::internal::{
    error::Result,
    serialization::OuterSequenceBuilder,
};

/// Construct arrays by pushing individual items
pub struct ArrayBuilder(pub(crate) OuterSequenceBuilder);

impl ArrayBuilder {
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
