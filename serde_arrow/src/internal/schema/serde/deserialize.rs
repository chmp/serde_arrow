use std::collections::HashMap;

use serde::{de::Visitor, Deserialize};

use crate::internal::{
    arrow::{DataType, Field},
    error::{fail, Error, Result},
    schema::{SerdeArrowSchema, Strategy, STRATEGY_KEY},
};

// A custom impl of untagged-enum repr with better error messages
impl<'de> serde::Deserialize<'de> for SerdeArrowSchema {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct VisitorImpl;

        impl<'de> Visitor<'de> for VisitorImpl {
            type Value = SerdeArrowSchema;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(f, "a sequence of fields or a struct with key 'fields' containing a sequence of fields")
            }

            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> Result<Self::Value, A::Error> {
                use serde::de::Error;

                let mut fields = Vec::new();
                while let Some(item) = seq.next_element::<CustomField>()? {
                    fields.push(item.into_field().map_err(A::Error::custom)?);
                }

                Ok(SerdeArrowSchema { fields })
            }

            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut map: A,
            ) -> Result<Self::Value, A::Error> {
                use serde::de::Error;

                let mut fields = None;

                while let Some(key) = map.next_key::<String>()? {
                    if key == "fields" {
                        fields = Some(map.next_value::<Vec<CustomField>>()?);
                    } else {
                        map.next_value::<serde::de::IgnoredAny>()?;
                    }
                }

                let Some(fields) = fields else {
                    return Err(A::Error::custom("missing field `fields`"));
                };

                let mut converted_fields = Vec::new();
                for field in fields {
                    converted_fields.push(field.into_field().map_err(A::Error::custom)?);
                }

                Ok(SerdeArrowSchema {
                    fields: converted_fields,
                })
            }
        }

        deserializer.deserialize_any(VisitorImpl)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct CustomField {
    name: String,
    data_type: ArrowOrCustomDataType,
    #[serde(default)]
    nullable: bool,
    #[serde(default)]
    strategy: Option<Strategy>,
    #[serde(default)]
    children: Vec<CustomField>,
    #[serde(default)]
    metadata: HashMap<String, String>,
}

impl CustomField {
    pub fn into_field(self) -> Result<Field> {
        match self.data_type {
            ArrowOrCustomDataType::Arrow(data_type) => {
                if !self.children.is_empty() {
                    fail!("Cannot use children with an arrow data type");
                }

                let metadata = merge_strategy_with_metadata(self.metadata, self.strategy)?;
                Ok(Field {
                    name: self.name,
                    nullable: self.nullable,
                    data_type,
                    metadata,
                })
            }
            ArrowOrCustomDataType::Custom(data_type) => {
                todo!()
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum ArrowOrCustomDataType {
    Arrow(DataType),
    Custom(String),
}

impl ArrowOrCustomDataType {
    pub fn into_data_type(self, children: Vec<CustomField>) -> Result<Self> {
        todo!()
    }
}

impl<'de> serde::Deserialize<'de> for ArrowOrCustomDataType {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        struct VisitorImpl;

        impl<'de> Visitor<'de> for VisitorImpl {
            type Value = ArrowOrCustomDataType;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(f, "string or DataType variant")
            }

            fn visit_newtype_struct<D: serde::Deserializer<'de>>(
                self,
                deserializer: D,
            ) -> Result<Self::Value, D::Error> {
                ArrowOrCustomDataType::deserialize(deserializer)
            }

            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
                Ok(ArrowOrCustomDataType::Custom(v.to_string()))
            }

            fn visit_enum<A: serde::de::EnumAccess<'de>>(
                self,
                data: A,
            ) -> Result<Self::Value, A::Error> {
                let field = DataType::deserialize(EnumDeserializer(data))?;
                Ok(ArrowOrCustomDataType::Arrow(field))
            }
        }

        deserializer.deserialize_any(VisitorImpl)
    }
}

/// A helper to deserialize from an `EnumAccess` object directly
struct EnumDeserializer<A>(A);

impl<'de, A: serde::de::EnumAccess<'de>> serde::de::Deserializer<'de> for EnumDeserializer<A> {
    type Error = A::Error;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_enum(self.0)
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf option unit unit_struct newtype_struct seq tuple
        tuple_struct map struct enum identifier ignored_any
    }
}

pub fn merge_strategy_with_metadata(
    mut metadata: HashMap<String, String>,
    strategy: Option<Strategy>,
) -> Result<HashMap<String, String>> {
    if metadata.contains_key(STRATEGY_KEY) && strategy.is_some() {
        fail!("Duplicate strategy: metadata map contains {STRATEGY_KEY} and strategy given");
    }
    if let Some(strategy) = strategy {
        metadata.insert(STRATEGY_KEY.to_owned(), strategy.to_string());
    }
    Ok(metadata)
}

#[test]
fn test_split_strategy_from_metadata_with_metadata() {
    use crate::internal::testing::hash_map;

    let metadata: HashMap<String, String> = hash_map!(
        "key1" => "value1",
        "key2" => "value2",
    );
    let strategy: Option<Strategy> = Some(Strategy::TupleAsStruct);

    let expected: HashMap<String, String> = hash_map!(
        "SERDE_ARROW:strategy" => "TupleAsStruct",
        "key1" => "value1",
        "key2" => "value2",
    );

    let actual = merge_strategy_with_metadata(metadata, strategy).unwrap();
    assert_eq!(actual, expected);
}

#[test]
fn test_split_strategy_from_metadata_without_metadata() {
    use crate::internal::testing::hash_map;

    let metadata: HashMap<String, String> = hash_map!(
        "key1" => "value1",
        "key2" => "value2",
    );
    let strategy: Option<Strategy> = None;

    let expected: HashMap<String, String> = hash_map!(
        "key1" => "value1",
        "key2" => "value2",
    );

    let actual = merge_strategy_with_metadata(metadata, strategy).unwrap();
    assert_eq!(actual, expected);
}
