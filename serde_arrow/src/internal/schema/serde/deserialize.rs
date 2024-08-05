use std::collections::HashMap;

use serde::de::Visitor;

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
                while let Some(item) = seq.next_element::<ArrowOrCustomField>()? {
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
                        fields = Some(map.next_value::<Vec<ArrowOrCustomField>>()?);
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

pub enum ArrowOrCustomField {
    Arrow(Field),
    Custom(CustomField),
}

impl ArrowOrCustomField {
    pub fn into_field(self) -> Result<Field> {
        let field = match self {
            ArrowOrCustomField::Arrow(field) => return Ok(field),
            ArrowOrCustomField::Custom(field) => field,
        };

        todo!()
    }
}

impl<'de> serde::Deserialize<'de> for ArrowOrCustomField {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        todo!()
    }
}

pub struct CustomField {
    name: String,
    data_type: ArrowOrCustomDataType,
    strategy: Option<Strategy>,
    children: Vec<CustomField>,
    metadata: HashMap<String, String>,
}

pub enum ArrowOrCustomDataType {
    Arrow(DataType),
    Custom(String),
}

impl ArrowOrCustomDataType {
    pub fn into_data_type(self, children: Vec<CustomField>) -> Result<Self> {
        todo!()
    }
}
