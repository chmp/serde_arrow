//! Deserialization of SchemaLike objects with explicit support to deserialize
//! from arrow-rs types 

use std::{collections::HashMap, str::FromStr};

use serde::Deserialize;

use crate::internal::{error::{Error, Result}, schema::{GenericField, GenericDataType, Strategy, SerdeArrowSchema}};

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ArrowField {
    name: String,
    data_type: ArrowDataType,
    nullable: bool,
    metadata: HashMap<String, String>,
}


#[derive(Debug, Clone, PartialEq, Deserialize)]
pub enum ArrowDataType {
    Null,
    Bool,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float16,
    Float32,
    Float64,
    Utf8,
    LargeUtf8,
    Struct(Vec<ArrowField>),
    List(Box<ArrowField>),
    LargeList(Box<ArrowField>),
}

impl ArrowDataType {
    pub fn into_generic(self) -> Result<(GenericDataType, Vec<GenericField>)> {
        let (data_type, children) = match self {
            ArrowDataType::Null => (GenericDataType::Null, vec![]),
            ArrowDataType::Bool => (GenericDataType::Bool, vec![]),
            ArrowDataType::Int8 => (GenericDataType::I8, vec![]),
            ArrowDataType::Int16 => (GenericDataType::I16, vec![]),
            ArrowDataType::Int32 => (GenericDataType::I32, vec![]),
            ArrowDataType::Int64 => (GenericDataType::I64, vec![]),
            ArrowDataType::UInt8 => (GenericDataType::U8, vec![]),
            ArrowDataType::UInt16 => (GenericDataType::U16, vec![]),
            ArrowDataType::UInt32 => (GenericDataType::U32, vec![]),
            ArrowDataType::UInt64 => (GenericDataType::U64, vec![]),
            ArrowDataType::Float16 => (GenericDataType::F16, vec![]),
            ArrowDataType::Float32 => (GenericDataType::F32, vec![]),
            ArrowDataType::Float64 => (GenericDataType::F64, vec![]),
            ArrowDataType::Utf8 => (GenericDataType::Utf8, vec![]),
            ArrowDataType::LargeUtf8 => (GenericDataType::LargeUtf8, vec![]),
            ArrowDataType::Struct(fields) => (GenericDataType::Struct, fields),
            ArrowDataType::List(field) => (GenericDataType::List, vec![*field]),
            ArrowDataType::LargeList(field) => (GenericDataType::LargeList, vec![*field]),
        };
        let children = children.into_iter().map(|field| GenericField::try_from(field)).collect::<Result<Vec<_>>>()?;
        Ok((data_type, children))
    }
}

impl TryFrom<ArrowField> for GenericField {
    type Error = Error;

    fn try_from(value: ArrowField) -> Result<Self> {
        let (data_type, children) = value.data_type.into_generic()?;

        Ok(GenericField {
            name: value.name,
            nullable: value.nullable,
            data_type,
            children: children,
            // TODO: Fix the strategy
            strategy: None,
        })
    }
}


#[derive(Debug)]
enum GenericOrArrowDataType {
    Generic(GenericDataType),
    Arrow(ArrowDataType),
}

impl<'de> Deserialize<'de> for GenericOrArrowDataType {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;

        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = GenericOrArrowDataType;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(f, "string or DataType variant")
            }

            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
                match GenericDataType::from_str(v) {
                    Ok(res) => Ok(GenericOrArrowDataType::Generic(res)),
                    Err(err) => Err(E::custom(err.to_string())),
                }
            }

            fn visit_enum<A: serde::de::EnumAccess<'de>>(self, data: A) -> Result<Self::Value, A::Error> {
                let field = ArrowDataType::deserialize(EnumDeserializer(data))?;
                Ok(GenericOrArrowDataType::Arrow(field))
            }
        }

        deserializer.deserialize_any(Visitor)
    }
}

struct EnumDeserializer<A>(A);

impl<'de, A: serde::de::EnumAccess<'de>> serde::de::Deserializer<'de> for EnumDeserializer<A> {
    type Error = A::Error;
    
    fn deserialize_any<V: serde::de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_enum(self.0)
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf option unit unit_struct newtype_struct seq tuple
        tuple_struct map struct enum identifier ignored_any
    }
}  

#[derive(Debug)]
struct NativeOrArrowField(GenericField);

impl<'de> Deserialize<'de> for NativeOrArrowField {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;

        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = NativeOrArrowField;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(f, "a struct with keys 'name', 'data_type', ...")
            }

            fn visit_map<A: serde::de::MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                use serde::de::Error;

                let mut name = None;
                let mut nullable = None;
                let mut strategy = None;
                let mut data_type = None;
                let mut children = None;

                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "name" => {
                            name = Some(map.next_value::<String>()?);
                        }
                        "nullable" => {
                            nullable = Some(map.next_value::<bool>()?);
                        }
                        "strategy" => {
                            strategy = Some(map.next_value::<Option<Strategy>>()?);
                        }
                        "data_type" => {
                            data_type = Some(map.next_value::<GenericOrArrowDataType>()?);
                        }
                        "children" => {
                            children = Some(map.next_value::<Vec<GenericField>>()?);
                        }
                        _ => {
                            map.next_value::<serde::de::IgnoredAny>()?;
                        },
                    }
                }
                
                let data_type = data_type.ok_or_else(|| A::Error::custom("missing field `data_type`"))?;
                let (data_type, children) = match data_type {
                    GenericOrArrowDataType::Generic(data_type) => (data_type, children.unwrap_or_default()),
                    GenericOrArrowDataType::Arrow(data_type) => {
                        if children.is_some() {
                            return Err(A::Error::custom("cannot mix `children` with arrow-rs-style data types"));
                        }
                        data_type.into_generic().map_err(|err| A::Error::custom(err.to_string()))?
                    },
                };

                let field = GenericField {
                    name: name.ok_or_else(|| A::Error::custom("missing field `name`"))?,
                    data_type,
                    children,
                    nullable: nullable.unwrap_or_default(),
                    strategy: strategy.flatten(),
                };
                Ok(NativeOrArrowField(field))
            }
        }

        deserializer.deserialize_map(Visitor)
    }
}

impl<'de> serde::Deserialize<'de> for SerdeArrowSchema {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;

        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = SerdeArrowSchema;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(f, "a sequence of fields or a struct with key 'fields' containing a sequence of fields")
            }

            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> Result<Self::Value, A::Error> {
                let mut fields = Vec::new();

                while let Some(item) = seq.next_element::<NativeOrArrowField>()? {
                    fields.push(item.0);
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
                        let fields_data = map.next_value::<Vec<NativeOrArrowField>>()?;
                        fields = Some(fields_data.into_iter().map(|item| item.0).collect::<Vec<_>>());
                    } else {
                        map.next_value::<serde::de::IgnoredAny>()?;
                    }
                }

                let Some(fields) = fields else {
                    return Err(A::Error::custom("missing field `fields`"));
                };

                Ok(SerdeArrowSchema { fields })
            }
        }

        deserializer.deserialize_any(Visitor)
    }
}