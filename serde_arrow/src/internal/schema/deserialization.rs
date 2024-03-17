//! Deserialization of SchemaLike objects with explicit support to deserialize
//! from arrow-rs types

use std::{collections::HashMap, str::FromStr};

use serde::Deserialize;

use crate::internal::{
    error::{fail, Error, Result},
    schema::{GenericDataType, GenericField, GenericTimeUnit, SerdeArrowSchema, Strategy},
};

use super::STRATEGY_KEY;

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ArrowField {
    name: String,
    data_type: ArrowDataType,
    nullable: bool,
    metadata: HashMap<String, String>,
}

impl ArrowField {
    pub fn new(name: &str, data_type: ArrowDataType, nullable: bool) -> Self {
        Self {
            name: name.to_string(),
            data_type,
            nullable,
            metadata: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub enum ArrowTimeUnit {
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

impl From<ArrowTimeUnit> for GenericTimeUnit {
    fn from(value: ArrowTimeUnit) -> Self {
        match value {
            ArrowTimeUnit::Second => Self::Second,
            ArrowTimeUnit::Millisecond => Self::Millisecond,
            ArrowTimeUnit::Microsecond => Self::Microsecond,
            ArrowTimeUnit::Nanosecond => Self::Nanosecond,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub enum ArrowUnionMode {
    Sparse,
    Dense,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub enum ArrowDataType {
    Null,
    Boolean,
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
    Date32,
    Date64,
    Time64(ArrowTimeUnit),
    Struct(Vec<ArrowField>),
    List(Box<ArrowField>),
    LargeList(Box<ArrowField>),
    Map(Box<ArrowField>),
    // TODO:
    // Union,
    Dictionary(Box<ArrowDataType>, Box<ArrowDataType>),
    Decimal128(u8, i8),
    Timestamp(ArrowTimeUnit, Option<String>),
    Union(Vec<(i8, ArrowField)>, ArrowUnionMode),
}

impl ArrowDataType {
    pub fn into_generic(self) -> Result<(GenericDataType, Vec<GenericField>)> {
        use GenericDataType as T;

        let (data_type, children) = match self {
            Self::Null => (T::Null, vec![]),
            Self::Boolean => (T::Bool, vec![]),
            Self::Int8 => (T::I8, vec![]),
            Self::Int16 => (T::I16, vec![]),
            Self::Int32 => (T::I32, vec![]),
            Self::Int64 => (T::I64, vec![]),
            Self::UInt8 => (T::U8, vec![]),
            Self::UInt16 => (T::U16, vec![]),
            Self::UInt32 => (T::U32, vec![]),
            Self::UInt64 => (T::U64, vec![]),
            Self::Float16 => (T::F16, vec![]),
            Self::Float32 => (T::F32, vec![]),
            Self::Float64 => (T::F64, vec![]),
            Self::Utf8 => (T::Utf8, vec![]),
            Self::LargeUtf8 => (T::LargeUtf8, vec![]),
            Self::Date32 => (T::Date32, vec![]),
            Self::Date64 => (T::Date64, vec![]),
            Self::Time64(unit) => (T::Time64(unit.into()), vec![]),
            Self::Decimal128(precision, scale) => (T::Decimal128(precision, scale), vec![]),
            Self::Struct(fields) => (T::Struct, fields),
            Self::List(field) => (T::List, vec![*field]),
            Self::LargeList(field) => (T::LargeList, vec![*field]),
            Self::Map(field) => (T::Map, vec![*field]),
            Self::Dictionary(key, value) => (
                T::Map,
                vec![
                    ArrowField::new("", *key, false),
                    ArrowField::new("", *value, false),
                ],
            ),
            Self::Timestamp(unit, timezone) => (T::Timestamp(unit.into(), timezone), vec![]),
            Self::Union(variants, mode) => {
                let mut children = Vec::new();

                if !matches!(mode, ArrowUnionMode::Dense) {
                    fail!("Only dense unions are supported at the moment");
                }

                for (pos, (idx, variant)) in variants.into_iter().enumerate() {
                    if pos as i8 != idx {
                        fail!("Union types with explicit field indices are not supported");
                    }
                    children.push(variant);
                }

                (T::Union, children)
            }
        };
        let children = children
            .into_iter()
            .map(GenericField::try_from)
            .collect::<Result<Vec<_>>>()?;
        Ok((data_type, children))
    }
}

impl TryFrom<ArrowField> for GenericField {
    type Error = Error;

    fn try_from(value: ArrowField) -> Result<Self> {
        let (data_type, children) = value.data_type.into_generic()?;

        let strategy = if let Some(strategy) = value.metadata.get(STRATEGY_KEY) {
            Some(strategy.parse()?)
        } else {
            None
        };

        Ok(GenericField {
            name: value.name,
            nullable: value.nullable,
            data_type,
            children,
            strategy,
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

            fn visit_enum<A: serde::de::EnumAccess<'de>>(
                self,
                data: A,
            ) -> Result<Self::Value, A::Error> {
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

    fn deserialize_any<V: serde::de::Visitor<'de>>(
        self,
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
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

            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut map: A,
            ) -> Result<Self::Value, A::Error> {
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
                        }
                    }
                }

                let data_type =
                    data_type.ok_or_else(|| A::Error::custom("missing field `data_type`"))?;
                let (data_type, children) = match data_type {
                    GenericOrArrowDataType::Generic(data_type) => {
                        (data_type, children.unwrap_or_default())
                    }
                    GenericOrArrowDataType::Arrow(data_type) => {
                        if children.is_some() {
                            return Err(A::Error::custom(
                                "cannot mix `children` with arrow-rs-style data types",
                            ));
                        }
                        data_type
                            .into_generic()
                            .map_err(|err| A::Error::custom(err.to_string()))?
                    }
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
                        fields = Some(
                            fields_data
                                .into_iter()
                                .map(|item| item.0)
                                .collect::<Vec<_>>(),
                        );
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
