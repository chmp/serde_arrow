use arrow2::{
    array::{Array, BooleanArray, PrimitiveArray},
    types::NativeType,
};

use crate::{
    event::{Event, EventSource},
    Result,
};

pub struct PrimitiveEventSource<'a, T: Into<Event<'static>> + NativeType> {
    array: &'a PrimitiveArray<T>,
    next: usize,
}

impl<'a, T: Into<Event<'static>> + NativeType> PrimitiveEventSource<'a, T> {
    pub fn new(array: &'a PrimitiveArray<T>) -> Self {
        Self { array, next: 0 }
    }
}

impl<'a, T: Into<Event<'static>> + NativeType> EventSource<'a> for PrimitiveEventSource<'a, T> {
    fn next(&mut self) -> Result<Option<Event<'a>>> {
        let ev = if self.next >= self.array.len() {
            return Ok(None);
        } else if !self.array.is_valid(self.next) {
            Event::Null
        } else {
            self.array.value(self.next).into()
        };
        self.next += 1;
        Ok(Some(ev))
    }
}

pub struct BooleanEventSource<'a> {
    array: &'a BooleanArray,
    next: usize,
}

impl<'a> BooleanEventSource<'a> {
    pub fn new(array: &'a BooleanArray) -> Self {
        Self { array, next: 0 }
    }
}

impl<'a> EventSource<'a> for BooleanEventSource<'a> {
    fn next(&mut self) -> Result<Option<Event<'a>>> {
        let ev = if self.next >= self.array.len() {
            return Ok(None);
        } else if !self.array.is_valid(self.next) {
            Event::Null
        } else {
            self.array.value(self.next).into()
        };
        self.next += 1;
        Ok(Some(ev))
    }
}
