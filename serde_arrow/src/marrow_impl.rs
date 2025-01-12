use marrow::{array::Array, datatypes::Field, view::View};
use serde::{Deserialize, Serialize};

use crate::internal::{
    array_builder::ArrayBuilder,
    deserializer::Deserializer,
    error::Result,
    serializer::Serializer,
    schema::SerdeArrowSchema
};

/// TODO
pub fn to_marrow<T: Serialize>(fields: &[Field], items: T) -> Result<Vec<Array>> {
    let builder = ArrayBuilder::from_marrow(fields)?;
    items.serialize(Serializer::new(builder))?.into_inner().to_marrow()
}

/// TODO
pub fn from_marrow<'de, T>(fields: &[Field], views: &'de [View]) -> Result<T>
where   
    T: Deserialize<'de>,
{
    T::deserialize(Deserializer::from_marrow(fields, views)?)
}

impl ArrayBuilder {
    /// TODO
    pub fn from_marrow(fields: &[Field]) -> Result<Self> {
        ArrayBuilder::new(SerdeArrowSchema { fields: fields.to_vec() })
    }

    /// TODO
    pub fn to_marrow(&mut self) -> Result<Vec<Array>> {
        self.build_arrays()
    }
}

impl<'de> Deserializer<'de> {
    /// TODO
    pub fn from_marrow(fields: &[Field], views: &'de [View]) -> Result<Self> {
        Self::new(fields, views.to_vec())
    }
}
