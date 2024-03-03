//! Deserialization of SchemaLike objects with explicit support to deserialize
//! from arrow-rs types 

use std::collections::HashMap;

use serde::Deserialize;

use crate::internal::{error::{Error, Result}, schema::{GenericField, GenericDataType}};

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