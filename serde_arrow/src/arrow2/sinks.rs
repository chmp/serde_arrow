use arrow2::{
    array::Array,
    array::{ListArray, StructArray},
    bitmap::Bitmap,
    buffer::Buffer,
    datatypes::{DataType, Field},
};

use crate::{
    base::error::fail,
    generic::{
        chrono::{NaiveDateTimeStrBuilder, UtcDateTimeStrBuilder},
        schema::{Strategy, STRATEGY_KEY},
        sinks::{
            ArrayBuilder, DynamicArrayBuilder, ListArrayBuilder, RecordsBuilder,
            StructArrayBuilder, StructArrayBuilderState,
        },
    },
    Result,
};

use arrow2::{
    array::{BooleanArray, MutableBooleanArray, MutablePrimitiveArray, PrimitiveArray},
    types::NativeType,
};

use crate::{
    base::{Event, EventSink},
    Error,
};

use super::schema::check_strategy;

pub fn build_records_builder(fields: &[Field]) -> Result<RecordsBuilder<Box<dyn Array>>> {
    let mut columns = Vec::new();
    let mut builders = Vec::new();

    for field in fields {
        builders.push(build_dynamic_array_builder(field)?);
        columns.push(field.name.to_owned());
    }

    RecordsBuilder::new(columns, builders)
}

pub fn build_dynamic_array_builder(field: &Field) -> Result<DynamicArrayBuilder<Box<dyn Array>>> {
    check_strategy(field)?;

    match field.data_type() {
        DataType::Boolean => Ok(DynamicArrayBuilder::new(BooleanArrayBuilder::new())),
        DataType::Int8 => Ok(DynamicArrayBuilder::new(PrimitiveArrayBuilder::<i8>::new())),
        DataType::Int16 => Ok(DynamicArrayBuilder::new(PrimitiveArrayBuilder::<i16>::new())),
        DataType::Int32 => Ok(DynamicArrayBuilder::new(PrimitiveArrayBuilder::<i32>::new())),
        DataType::Int64 => Ok(DynamicArrayBuilder::new(PrimitiveArrayBuilder::<i64>::new())),
        DataType::UInt8 => Ok(DynamicArrayBuilder::new(PrimitiveArrayBuilder::<u8>::new())),
        DataType::UInt16 => Ok(DynamicArrayBuilder::new(PrimitiveArrayBuilder::<u16>::new())),
        DataType::UInt32 => Ok(DynamicArrayBuilder::new(PrimitiveArrayBuilder::<u32>::new())),
        DataType::UInt64 => Ok(DynamicArrayBuilder::new(PrimitiveArrayBuilder::<u64>::new())),
        DataType::Float32 => Ok(DynamicArrayBuilder::new(PrimitiveArrayBuilder::<f32>::new())),
        DataType::Float64 => Ok(DynamicArrayBuilder::new(PrimitiveArrayBuilder::<f64>::new())),
        DataType::Date64 => {
            if let Some(strategy) = field.metadata.get(STRATEGY_KEY) {
                let strategy: Strategy = strategy.parse()?;
                match strategy {
                    Strategy::NaiveDateTimeStr => Ok(DynamicArrayBuilder::new(
                        NaiveDateTimeStrBuilder(PrimitiveArrayBuilder::<i64>::new()),
                    )),
                    Strategy::UtcDateTimeStr => Ok(DynamicArrayBuilder::new(
                        UtcDateTimeStrBuilder(PrimitiveArrayBuilder::<i64>::new()),
                    )),
                }
            } else {
                // TODO: is this correct?
                Ok(DynamicArrayBuilder::new(PrimitiveArrayBuilder::<i64>::new()))
            }
        }
        DataType::Struct(fields) => {
            let mut columns = Vec::new();
            let mut builders = Vec::new();
            let mut nullable = Vec::new();

            for field in fields {
                columns.push(field.name.to_owned());
                builders.push(build_dynamic_array_builder(field)?);
                nullable.push(field.is_nullable);
            }

            let builder = StructArrayBuilder::new(columns, nullable, builders);
            Ok(DynamicArrayBuilder::new(builder))
        }
        DataType::List(field) | DataType::LargeList(field) => {
            let values = build_dynamic_array_builder(field.as_ref())?;
            let builder = ListArrayBuilder::new(values, field.name.to_owned(), field.is_nullable);
            Ok(DynamicArrayBuilder::new(builder))
        }
        _ => fail!(
            "Cannot build sink for {} with type {:?}",
            field.name,
            field.data_type
        ),
    }
}

impl<B: ArrayBuilder<Box<dyn Array>>> ArrayBuilder<Box<dyn Array>> for StructArrayBuilder<B> {
    fn box_into_array(self: Box<Self>) -> Result<Box<dyn Array>> {
        (*self).into_array()
    }

    fn into_array(self) -> Result<Box<dyn Array>>
    where
        Self: Sized,
    {
        if !matches!(self.state, StructArrayBuilderState::Start) {
            fail!("Invalid state at array construction");
        }

        let values: Result<Vec<Box<dyn Array>>> =
            self.builders.into_iter().map(|b| b.into_array()).collect();
        let values = values?;

        let mut fields = Vec::new();
        for (i, column) in self.columns.into_iter().enumerate() {
            fields.push(Field::new(
                column,
                values[i].data_type().clone(),
                self.nullable[i],
            ));
        }
        let data_type = DataType::Struct(fields);

        Ok(Box::new(StructArray::new(data_type, values, None)))
    }
}

#[derive(Debug, Default)]
pub struct PrimitiveArrayBuilder<T: NativeType + for<'a> TryFrom<Event<'a>, Error = Error>> {
    array: MutablePrimitiveArray<T>,
}

impl<T: NativeType + for<'a> TryFrom<Event<'a>, Error = Error>> PrimitiveArrayBuilder<T> {
    pub fn new() -> Self {
        Self::default()
    }
}

impl<T: NativeType + for<'a> TryFrom<Event<'a>, Error = Error>> EventSink
    for PrimitiveArrayBuilder<T>
{
    fn accept(&mut self, event: Event<'_>) -> Result<()> {
        self.array.push(event.into_option()?);
        Ok(())
    }
}

impl<T> ArrayBuilder<Box<dyn Array>> for PrimitiveArrayBuilder<T>
where
    T: NativeType + for<'a> TryFrom<Event<'a>, Error = Error>,
{
    fn box_into_array(self: Box<Self>) -> Result<Box<dyn Array>> {
        (*self).into_array()
    }

    fn into_array(self) -> Result<Box<dyn Array>>
    where
        Self: Sized,
    {
        Ok(Box::new(PrimitiveArray::from(self.array)))
    }
}

#[derive(Debug, Default)]
pub struct BooleanArrayBuilder {
    array: MutableBooleanArray,
}

impl BooleanArrayBuilder {
    pub fn new() -> Self {
        Self::default()
    }
}

impl EventSink for BooleanArrayBuilder {
    fn accept(&mut self, event: Event<'_>) -> Result<()> {
        self.array.push(event.into_option()?);
        Ok(())
    }
}

impl ArrayBuilder<Box<dyn Array>> for BooleanArrayBuilder {
    fn box_into_array(self: Box<Self>) -> Result<Box<dyn Array>> {
        (*self).into_array()
    }

    fn into_array(self) -> Result<Box<dyn Array>> {
        Ok(Box::new(BooleanArray::from(self.array)))
    }
}

impl<B: ArrayBuilder<Box<dyn Array>>> ArrayBuilder<Box<dyn Array>> for ListArrayBuilder<B> {
    fn box_into_array(self: Box<Self>) -> Result<Box<dyn Array>> {
        (*self).into_array()
    }

    fn into_array(self) -> Result<Box<dyn Array>>
    where
        Self: Sized,
    {
        let values = self.builder.into_array()?;
        let array = ListArray::try_new(
            DataType::LargeList(Box::new(Field::new(
                self.item_name,
                values.data_type().clone(),
                self.nullable,
            ))),
            Buffer::from(self.offsets),
            values,
            Some(Bitmap::from(self.validity)),
        )?;
        Ok(Box::new(array))
    }
}
