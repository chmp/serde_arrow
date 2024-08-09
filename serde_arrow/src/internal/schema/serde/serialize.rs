//! Serialize and deserialize a field split into

use std::collections::HashMap;

use serde::ser::{SerializeSeq, SerializeStruct};

use crate::internal::{
    arrow::{DataType, Field},
    schema::{SerdeArrowSchema, STRATEGY_KEY},
};

impl serde::Serialize for SerdeArrowSchema {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut s = serializer.serialize_struct("SerdeArrowSchema", 1)?;
        s.serialize_field("fields", &PrettyFields(&self.fields))?;
        s.end()
    }
}

/// A wrapper around fields to serialize into a more compact format
pub struct PrettyFields<'a>(pub &'a [Field]);

impl<'a> serde::Serialize for PrettyFields<'a> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut s = serializer.serialize_seq(Some(self.0.len()))?;
        for field in self.0 {
            s.serialize_element(&PrettyField(field))?;
        }

        s.end()
    }
}

/// A wrapper around a single field to serialize into a more compact format
pub struct PrettyField<'a>(pub &'a Field);

impl<'a> serde::Serialize for PrettyField<'a> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let non_strategy_metadata = self
            .0
            .metadata
            .iter()
            .filter(|(key, _)| *key != STRATEGY_KEY)
            .collect::<HashMap<_, _>>();

        let mut num_fields = 2;
        if !non_strategy_metadata.is_empty() {
            num_fields += 1;
        }
        if self.0.metadata.contains_key(STRATEGY_KEY) {
            num_fields += 1;
        }
        if self.0.nullable {
            num_fields += 1;
        }
        if is_data_type_with_children(&self.0.data_type) {
            num_fields += 1;
        }

        let mut s = serializer.serialize_struct("Field", num_fields)?;
        s.serialize_field("name", &self.0.name)?;
        s.serialize_field("data_type", &PrettyFieldDataType(&self.0.data_type))?;

        if self.0.nullable {
            s.serialize_field("nullable", &self.0.nullable)?;
        }
        if !non_strategy_metadata.is_empty() {
            s.serialize_field("metadata", &non_strategy_metadata)?;
        }
        if let Some(strategy) = self.0.metadata.get(STRATEGY_KEY) {
            s.serialize_field("strategy", strategy)?;
        }
        if is_data_type_with_children(&self.0.data_type) {
            s.serialize_field("children", &PrettyFieldChildren(&self.0.data_type))?;
        }
        s.end()
    }
}

struct PrettyFieldDataType<'a>(pub &'a DataType);

impl<'a> serde::Serialize for PrettyFieldDataType<'a> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use DataType as T;
        match self.0 {
            T::Null => "Null".serialize(serializer),
            T::Boolean => "Bool".serialize(serializer),
            T::Int8 => "I8".serialize(serializer),
            T::Int16 => "I16".serialize(serializer),
            T::Int32 => "I32".serialize(serializer),
            T::Int64 => "I64".serialize(serializer),
            T::UInt8 => "U8".serialize(serializer),
            T::UInt16 => "U16".serialize(serializer),
            T::UInt32 => "U32".serialize(serializer),
            T::UInt64 => "U64".serialize(serializer),
            T::Float16 => "F16".serialize(serializer),
            T::Float32 => "F32".serialize(serializer),
            T::Float64 => "F64".serialize(serializer),
            T::Utf8 => "Utf8".serialize(serializer),
            T::LargeUtf8 => "LargeUtf8".serialize(serializer),
            T::Binary => "Binary".serialize(serializer),
            T::LargeBinary => "LargeBinary".serialize(serializer),
            T::Date32 => "Date32".serialize(serializer),
            T::Date64 => "Date64".serialize(serializer),
            T::Decimal128(precision, scale) => {
                format!("Decimal128({precision}, {scale})").serialize(serializer)
            }
            T::Duration(unit) => format!("Duration({unit})").serialize(serializer),
            T::Time32(unit) => format!("Time32({unit})").serialize(serializer),
            T::Time64(unit) => format!("Time64({unit})").serialize(serializer),
            T::Timestamp(unit, tz) => format!("Timestamp({unit}, {tz:?})").serialize(serializer),
            T::FixedSizeBinary(n) => format!("FixedSizeBinary({n})").serialize(serializer),
            T::FixedSizeList(_, n) => format!("FixedSizeList({n})").serialize(serializer),
            T::Struct(_) => "Struct".serialize(serializer),
            T::Map(_, _) => "Map".serialize(serializer),
            T::Union(_, _) => "Union".serialize(serializer),
            T::Dictionary(_, _, _) => "Dictionary".serialize(serializer),
            T::LargeList(_) => "LargeList".serialize(serializer),
            T::List(_) => "List".serialize(serializer),
        }
    }
}

struct PrettyFieldChildren<'a>(pub &'a DataType);

impl<'a> serde::Serialize for PrettyFieldChildren<'a> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use DataType as T;

        match self.0 {
            T::FixedSizeList(entry, _)
            | T::Map(entry, _)
            | T::LargeList(entry)
            | T::List(entry) => {
                let mut s = serializer.serialize_seq(Some(1))?;
                s.serialize_element(&PrettyField(entry.as_ref()))?;
                s.end()
            }
            T::Struct(fields) => {
                let mut s = serializer.serialize_seq(Some(fields.len()))?;
                for field in fields {
                    s.serialize_element(&PrettyField(field))?;
                }
                s.end()
            }
            T::Union(fields, _) => {
                let mut s = serializer.serialize_seq(Some(fields.len()))?;
                for (_, field) in fields {
                    s.serialize_element(&PrettyField(field))?;
                }
                s.end()
            }
            T::Dictionary(key, value, _) => {
                let mut s = serializer.serialize_seq(Some(2))?;
                s.serialize_element(&DictionaryField("key", key))?;
                s.serialize_element(&DictionaryField("value", value))?;
                s.end()
            }
            _ => serializer.serialize_seq(Some(0))?.end(),
        }
    }
}

struct DictionaryField<'a>(&'a str, &'a DataType);

impl<'a> serde::Serialize for DictionaryField<'a> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut s = serializer.serialize_struct("Field", 2)?;
        s.serialize_field("name", self.0)?;
        s.serialize_field("data_type", &PrettyFieldDataType(self.1))?;
        s.end()
    }
}

fn is_data_type_with_children(data_type: &DataType) -> bool {
    use DataType as T;
    matches!(
        data_type,
        T::FixedSizeList(_, _)
            | T::Struct(_)
            | T::Map(_, _)
            | T::Union(_, _)
            | T::Dictionary(_, _, _)
            | T::LargeList(_)
            | T::List(_)
    )
}
