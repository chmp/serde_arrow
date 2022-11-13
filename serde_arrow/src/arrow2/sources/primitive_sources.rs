use arrow2::{
    array::{Array, BooleanArray, PrimitiveArray},
    types::NativeType,
};
use chrono::{NaiveDateTime, TimeZone, Utc};

use crate::{
    error,
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

    pub fn from_array(array: &'a dyn Array) -> Result<Self> {
        Ok(Self::new(
            array
                .as_any()
                .downcast_ref::<PrimitiveArray<T>>()
                .ok_or_else(|| error!("Mismatched type"))?,
        ))
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

pub struct NaiveDateTimeStrSource<S>(pub S);

impl<'a, S: EventSource<'a>> EventSource<'a> for NaiveDateTimeStrSource<S> {
    fn next(&mut self) -> Result<Option<Event<'a>>> {
        match self.0.next()? {
            Some(Event::I64(val)) => {
                let val = NaiveDateTime::from_timestamp(val / 1000, (val % 1000) as u32 * 100_000);
                // NOTE: chrono documents that Debug, not Display, can be parsed
                Ok(Some(format!("{:?}", val).into()))
            }
            ev => Ok(ev),
        }
    }
}

pub struct DateTimeStrSource<S>(pub S);

impl<'a, S: EventSource<'a>> EventSource<'a> for DateTimeStrSource<S> {
    fn next(&mut self) -> Result<Option<Event<'a>>> {
        match self.0.next()? {
            Some(Event::I64(val)) => {
                let val = Utc.timestamp(val / 1000, (val % 1000) as u32 * 100_000);
                // NOTE: chrono documents that Debug, not Display, can be parsed
                Ok(Some(format!("{:?}", val).into()))
            }
            ev => Ok(ev),
        }
    }
}
