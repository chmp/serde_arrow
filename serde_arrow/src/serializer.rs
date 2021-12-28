use std::{collections::HashMap, convert::TryFrom, sync::Arc};

use arrow::{datatypes::Schema as ArrowSchema, record_batch::RecordBatch};
use serde::Serialize;

use crate::{
    array_builder::ArrayBuilder,
    error,
    schema::Schema,
    util::outer_structure::{OuterSerializer, RecordBuilder},
    Error, Result,
};

/// Convert a sequence of records into an Arrow RecordBatch
///
pub fn to_record_batch<T>(value: &T, schema: &Schema) -> Result<RecordBatch>
where
    T: serde::Serialize + ?Sized,
{
    let record_serializer = RecordSerializer::new(schema)?;
    let mut serializer = OuterSerializer::new(record_serializer)?;
    value.serialize(&mut serializer)?;

    let mut record_serializer = serializer.into_inner();
    let batch = record_serializer.build()?;
    Ok(batch)
}

pub struct RecordSerializer<'a> {
    schema: &'a Schema,
    builders: HashMap<String, ArrayBuilder>,
}

impl<'a> RecordSerializer<'a> {
    pub fn new(schema: &'a Schema) -> Result<Self> {
        let mut builders = HashMap::new();

        for field in schema.fields() {
            let data_type = schema
                .data_type(field)
                .ok_or_else(|| error!("no known data type for {}", field))?;
            let builder = ArrayBuilder::new(data_type)?;
            builders.insert(field.to_owned(), builder);
        }

        Ok(Self { schema, builders })
    }

    pub fn build(&mut self) -> Result<RecordBatch> {
        let mut fields = Vec::new();

        for field in self.schema.fields() {
            let field = self
                .builders
                .get_mut(field)
                .expect("Invalid state")
                .build()?;
            fields.push(field);
        }

        let schema = Arc::new(ArrowSchema::try_from(self.schema.clone())?);
        let res = RecordBatch::try_new(schema, fields)?;
        Ok(res)
    }
}

impl<'a> RecordBuilder for RecordSerializer<'a> {
    fn start(&mut self) -> Result<()> {
        Ok(())
    }

    fn field<T: Serialize + ?Sized>(&mut self, key: &str, value: &T) -> Result<()> {
        let builder = self
            .builders
            .get_mut(key)
            .ok_or_else(|| Error::Custom(format!("Unknown field {}", key)))?;
        value.serialize(builder)?;
        Ok(())
    }

    fn end(&mut self) -> Result<()> {
        Ok(())
    }
}
