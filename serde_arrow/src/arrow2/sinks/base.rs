use arrow2::array::Array;

use crate::{
    event::{Event, EventSink},
    Result,
};

pub trait ArrayBuilder: EventSink {
    fn box_into_array(self: Box<Self>) -> Result<Box<dyn Array>>;
    fn into_array(self) -> Result<Box<dyn Array>>
    where
        Self: Sized;
}

pub struct DynamicArrayBuilder {
    builder: Box<dyn ArrayBuilder>,
}

impl DynamicArrayBuilder {
    pub fn new<B: ArrayBuilder + 'static>(builder: B) -> Self {
        Self {
            builder: Box::new(builder),
        }
    }
}

impl EventSink for DynamicArrayBuilder {
    fn accept(&mut self, event: Event<'_>) -> Result<()> {
        self.builder.accept(event)
    }
}

impl ArrayBuilder for DynamicArrayBuilder {
    fn box_into_array(self: Box<Self>) -> Result<Box<dyn Array>> {
        self.builder.box_into_array()
    }

    fn into_array(self) -> Result<Box<dyn Array>> {
        self.builder.box_into_array()
    }
}

impl From<Box<dyn ArrayBuilder>> for DynamicArrayBuilder {
    fn from(builder: Box<dyn ArrayBuilder>) -> Self {
        Self { builder }
    }
}
