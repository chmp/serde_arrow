use std::iter;

use arrow2::datatypes::{DataType, Field};

use crate::{
    base::{error::fail, Event, EventSink},
    generic::schema::{FieldBuilder, PrimitiveType, Strategy, Tracer, STRATEGY_KEY},
    Result,
};

/// Make sure the field is configured correctly if a strategy is used
///
pub fn check_strategy(field: &Field) -> Result<()> {
    let strategy_str = match field.metadata.get(STRATEGY_KEY) {
        Some(strategy_str) => strategy_str,
        None => return Ok(()),
    };

    match strategy_str.parse::<Strategy>()? {
        Strategy::UtcDateTimeStr | Strategy::NaiveDateTimeStr => {
            if !matches!(field.data_type, DataType::Date64) {
                fail!(
                    "Invalid strategy for field {name}: {strategy_str} expects the data type Date64, found: {dt:?}",
                    name = field.name,
                    dt = field.data_type,
                );
            }
        }
    }

    Ok(())
}

pub struct TracedSchema {
    tracer: Tracer,
}

impl EventSink for TracedSchema {
    fn accept(&mut self, event: Event<'_>) -> Result<()> {
        self.tracer.accept(event)
    }
}

impl TracedSchema {
    pub fn new() -> Self {
        Self {
            tracer: Tracer::new(),
        }
    }

    pub fn to_fields(&self) -> Result<Vec<Field>> {
        let field = self.tracer.to_field("root")?;

        let field = match field.data_type {
            DataType::Null => return Ok(Vec::new()),
            DataType::LargeList(field) | DataType::List(field) => field,
            dt => fail!("Cannot handle data type {dt:?}"),
        };

        let field = match field.data_type {
            DataType::Null => Vec::new(),
            DataType::Struct(fields) => fields,
            dt => fail!("Cannot handle data type {dt:?}"),
        };
        Ok(field)
    }
}

impl FieldBuilder<Field> for Tracer {
    fn to_field(&self, name: &str) -> Result<Field> {
        type T = PrimitiveType;
        type D = DataType;

        match self {
            Self::Unknown(tracer) => Ok(Field::new(name, DataType::Null, tracer.nullable)),
            Self::Primitive(tracer) => match tracer.item_type {
                T::Unknown => Ok(Field::new(name, D::Null, tracer.nullable)),
                T::Bool => Ok(Field::new(name, D::Boolean, tracer.nullable)),
                T::Str => Ok(Field::new(name, D::LargeUtf8, tracer.nullable)),
                T::I8 => Ok(Field::new(name, D::Int8, tracer.nullable)),
                T::I16 => Ok(Field::new(name, D::Int16, tracer.nullable)),
                T::I32 => Ok(Field::new(name, D::Int32, tracer.nullable)),
                T::I64 => Ok(Field::new(name, D::Int64, tracer.nullable)),
                T::U8 => Ok(Field::new(name, D::UInt8, tracer.nullable)),
                T::U16 => Ok(Field::new(name, D::UInt16, tracer.nullable)),
                T::U32 => Ok(Field::new(name, D::UInt32, tracer.nullable)),
                T::U64 => Ok(Field::new(name, D::UInt64, tracer.nullable)),
                T::F32 => Ok(Field::new(name, D::Float32, tracer.nullable)),
                T::F64 => Ok(Field::new(name, D::Float64, tracer.nullable)),
            },
            Self::List(tracer) => {
                let item_type = tracer.item_tracer.to_field("element")?;
                let item_type = Box::new(item_type);
                Ok(Field::new(
                    name,
                    DataType::LargeList(item_type),
                    tracer.nullable,
                ))
            }
            Self::Struct(tracer) => {
                let mut fields = Vec::new();
                for (tracer, name) in iter::zip(&tracer.field_tracers, &tracer.field_names) {
                    fields.push(tracer.to_field(name)?);
                }
                Ok(Field::new(name, DataType::Struct(fields), tracer.nullable))
            }
        }
    }
}
