//! Serialize and deserialize a field split into

use std::collections::HashMap;

use serde::ser::SerializeStruct;

use crate::{internal::schema::GenericField, schema::STRATEGY_KEY};

impl serde::Serialize for GenericField {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let non_strategy_metadata = self
            .metadata
            .iter()
            .filter(|(key, _)| *key != STRATEGY_KEY)
            .collect::<HashMap<_, _>>();

        let mut num_fields = 2;
        if !non_strategy_metadata.is_empty() {
            num_fields += 1;
        }
        if self.strategy.is_some() {
            num_fields += 1;
        }
        if self.nullable {
            num_fields += 1;
        }
        if !self.children.is_empty() {
            num_fields += 1;
        }

        let mut s = serializer.serialize_struct("Field", num_fields)?;
        s.serialize_field("name", &self.name)?;
        s.serialize_field("data_type", &self.data_type)?;

        if self.nullable {
            s.serialize_field("nullable", &self.nullable)?;
        }
        if !non_strategy_metadata.is_empty() {
            s.serialize_field("metadata", &non_strategy_metadata)?;
        }
        if let Some(strategy) = self.strategy.as_ref() {
            s.serialize_field("strategy", strategy)?;
        }
        if !self.children.is_empty() {
            s.serialize_field("children", &self.children)?;
        }
        s.end()
    }
}
