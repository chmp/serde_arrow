use std::{collections::HashMap, str::FromStr};

use marrow::datatypes::{DataType, Field, TimeUnit, UnionMode};
use serde::{de::Visitor, Deserialize};

use crate::internal::{
    error::{fail, Result},
    schema::{validate_field, SerdeArrowSchema, Strategy, STRATEGY_KEY},
    utils::dsl::Term,
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
struct CustomField {
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
    fn into_field(self) -> Result<Field> {
        let mut children = Vec::new();
        for child in self.children {
            children.push(child.into_field()?);
        }

        let data_type = self.data_type.into_data_type(children)?;
        let metadata = merge_strategy_with_metadata(self.metadata, self.strategy)?;

        let nullable = match &data_type {
            DataType::Null => true,
            _ => self.nullable,
        };

        let field = Field {
            name: self.name,
            nullable,
            data_type,
            metadata,
        };
        validate_field(&field)?;
        Ok(field)
    }
}

#[derive(Debug, Clone)]
enum ArrowOrCustomDataType {
    Arrow(DataType),
    Custom(String),
}

impl ArrowOrCustomDataType {
    fn into_data_type(self, children: Vec<Field>) -> Result<DataType> {
        match self {
            Self::Custom(data_type) => build_data_type(data_type, children),
            Self::Arrow(data_type) => {
                if !children.is_empty() {
                    fail!("Cannot use children with an arrow data type");
                }
                Ok(data_type)
            }
        }
    }
}

fn build_data_type(data_type: String, children: Vec<Field>) -> Result<DataType> {
    use DataType as T;

    let res = match Term::from_str(&data_type)?.as_call()? {
        ("Null", []) => T::Null,
        ("Bool" | "Boolean", []) => T::Boolean,
        ("Utf8", []) => T::Utf8,
        ("LargeUtf8", []) => T::LargeUtf8,
        ("Utf8View", []) => T::Utf8View,
        ("U8" | "UInt8", []) => T::UInt8,
        ("U16" | "UInt16", []) => T::UInt16,
        ("U32" | "UInt32", []) => T::UInt32,
        ("U64" | "UInt64", []) => T::UInt64,
        ("I8" | "Int8", []) => T::Int8,
        ("I16" | "Int16", []) => T::Int16,
        ("I32" | "Int32", []) => T::Int32,
        ("I64" | "Int64", []) => T::Int64,
        ("F16" | "Float16", []) => T::Float16,
        ("F32" | "Float32", []) => T::Float32,
        ("F64" | "Float64", []) => T::Float64,
        ("Date32", []) => T::Date32,
        ("Date64", []) => T::Date64,
        ("Binary", []) => T::Binary,
        ("LargeBinary", []) => T::LargeBinary,
        ("FixedSizeBinary", [n]) => T::FixedSizeBinary(n.as_ident()?.parse()?),
        ("BinaryView", []) => T::BinaryView,
        ("Timestamp", [unit, timezone]) => {
            let unit: TimeUnit = unit.as_ident()?.parse()?;
            let timezone = timezone
                .as_option()?
                .map(|term| term.as_string())
                .transpose()?;
            T::Timestamp(unit, timezone.map(|s| s.to_owned()))
        }
        ("Time32", [unit]) => T::Time32(unit.as_ident()?.parse()?),
        ("Time64", [unit]) => T::Time64(unit.as_ident()?.parse()?),
        ("Duration", [unit]) => T::Duration(unit.as_ident()?.parse()?),
        ("Decimal128", [precision, scale]) => {
            T::Decimal128(precision.as_ident()?.parse()?, scale.as_ident()?.parse()?)
        }
        ("Struct", []) => T::Struct(children),
        ("List", []) => {
            let Ok([child]) = <[_; 1]>::try_from(children) else {
                fail!("Invalid children for List: expected one child");
            };
            T::List(Box::new(child))
        }
        ("LargeList", []) => {
            let Ok([child]) = <[_; 1]>::try_from(children) else {
                fail!("Invalid children for List: expected one child");
            };
            T::LargeList(Box::new(child))
        }
        ("FixedSizeList", [n]) => {
            let Ok([child]) = <[_; 1]>::try_from(children) else {
                fail!("Invalid children for LargeList: expected one child");
            };
            T::FixedSizeList(Box::new(child), n.as_ident()?.parse()?)
        }
        ("Dictionary", []) => {
            let Ok([key, value]) = <[_; 2]>::try_from(children) else {
                fail!("Invalid children for Dictionary: expected two children");
            };
            T::Dictionary(Box::new(key.data_type), Box::new(value.data_type))
        }
        ("Map", []) => {
            let Ok([child]) = <[_; 1]>::try_from(children) else {
                fail!("Invalid children for Map: expected one child");
            };
            T::Map(Box::new(child), false)
        }
        ("Union", []) => {
            let mut children_with_type_ids = Vec::new();
            for (idx, child) in children.into_iter().enumerate() {
                children_with_type_ids.push((idx.try_into()?, child));
            }
            T::Union(children_with_type_ids, UnionMode::Dense)
        }
        _ => fail!("invalid data type {data_type}"),
    };
    Ok(res)
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

#[test]
fn data_type_serialization_from_short_repr() {
    fn deserialize_data_type(s: &str) -> DataType {
        use crate::internal::schema::SchemaLike;
        let schema = SerdeArrowSchema::from_value(serde_json::json!([
            {"name": "item", "data_type": s},
        ]))
        .unwrap();
        schema.fields[0].data_type.clone()
    }

    assert_eq!(deserialize_data_type("I8"), DataType::Int8);
    assert_eq!(deserialize_data_type("Int8"), DataType::Int8);
    assert_eq!(deserialize_data_type("I16"), DataType::Int16);
    assert_eq!(deserialize_data_type("Int16"), DataType::Int16);
    assert_eq!(deserialize_data_type("I32"), DataType::Int32);
    assert_eq!(deserialize_data_type("Int32"), DataType::Int32);
    assert_eq!(deserialize_data_type("I64"), DataType::Int64);
    assert_eq!(deserialize_data_type("Int64"), DataType::Int64);

    assert_eq!(deserialize_data_type("U8"), DataType::UInt8);
    assert_eq!(deserialize_data_type("UInt8"), DataType::UInt8);
    assert_eq!(deserialize_data_type("U16"), DataType::UInt16);
    assert_eq!(deserialize_data_type("UInt16"), DataType::UInt16);
    assert_eq!(deserialize_data_type("U32"), DataType::UInt32);
    assert_eq!(deserialize_data_type("UInt32"), DataType::UInt32);
    assert_eq!(deserialize_data_type("U64"), DataType::UInt64);
    assert_eq!(deserialize_data_type("UInt64"), DataType::UInt64);

    assert_eq!(deserialize_data_type("F16"), DataType::Float16);
    assert_eq!(deserialize_data_type("Float16"), DataType::Float16);
    assert_eq!(deserialize_data_type("F32"), DataType::Float32);
    assert_eq!(deserialize_data_type("Float32"), DataType::Float32);
    assert_eq!(deserialize_data_type("F64"), DataType::Float64);
    assert_eq!(deserialize_data_type("Float64"), DataType::Float64);

    assert_eq!(deserialize_data_type("Utf8"), DataType::Utf8);
    assert_eq!(deserialize_data_type("LargeUtf8"), DataType::LargeUtf8);
    assert_eq!(deserialize_data_type("Utf8View"), DataType::Utf8View);

    assert_eq!(deserialize_data_type("Binary"), DataType::Binary);
    assert_eq!(deserialize_data_type("LargeBinary"), DataType::LargeBinary);
    assert_eq!(deserialize_data_type("BinaryView"), DataType::BinaryView);
    assert_eq!(
        deserialize_data_type("FixedSizeBinary(16)"),
        DataType::FixedSizeBinary(16)
    );
}
