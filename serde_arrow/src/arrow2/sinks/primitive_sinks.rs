use arrow2::{array::Array, types::NativeType, array::{MutablePrimitiveArray, MutableBooleanArray, PrimitiveArray, BooleanArray}};

use crate::{event::{Event, EventSink}, Error, Result};

use super::base::ArrayBuilder;

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

impl<T: NativeType + for<'a> TryFrom<Event<'a>, Error = Error>> ArrayBuilder
    for PrimitiveArrayBuilder<T>
{
    fn box_into_array(self: Box<Self>) -> Result<Box<dyn Array>> {
        (*self).into_array()
    }

    fn into_array(self) -> Result<Box<dyn Array>> where Self: Sized {
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

impl ArrayBuilder for BooleanArrayBuilder {
    fn box_into_array(self: Box<Self>) -> Result<Box<dyn Array>> {
        (*self).into_array()
    }

    fn into_array(self) -> Result<Box<dyn Array>> {
        Ok(Box::new(BooleanArray::from(self.array)))
    }
}