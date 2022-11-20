use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};

use crate::{
    base::{Event, EventSink, EventSource},
    Result,
};

use super::sinks::ArrayBuilder;

#[derive(Debug)]
pub struct NaiveDateTimeStrBuilder<B>(pub B);

impl<B: EventSink> EventSink for NaiveDateTimeStrBuilder<B> {
    fn accept(&mut self, event: Event<'_>) -> Result<()> {
        self.0.accept(match event.to_self() {
            Event::Str(s) => Event::I64(s.parse::<NaiveDateTime>()?.timestamp_millis()),
            ev => ev,
        })
    }
}

impl<A, B: ArrayBuilder<A>> ArrayBuilder<A> for NaiveDateTimeStrBuilder<B> {
    fn box_into_array(self: Box<Self>) -> Result<A> {
        (*self).into_array()
    }

    fn into_array(self) -> Result<A> {
        self.0.into_array()
    }
}

#[derive(Debug)]
pub struct UtcDateTimeStrBuilder<B>(pub B);

impl<B: EventSink> EventSink for UtcDateTimeStrBuilder<B> {
    fn accept(&mut self, event: Event<'_>) -> Result<()> {
        self.0.accept(match event.to_self() {
            Event::Str(s) => Event::I64(s.parse::<DateTime<Utc>>()?.timestamp_millis()),
            ev => ev,
        })
    }
}

impl<A, B: ArrayBuilder<A>> ArrayBuilder<A> for UtcDateTimeStrBuilder<B> {
    fn box_into_array(self: Box<Self>) -> Result<A> {
        (*self).into_array()
    }

    fn into_array(self) -> Result<A> {
        self.0.into_array()
    }
}

pub struct NaiveDateTimeStrSource<S>(pub S);

impl<'a, S: EventSource<'a>> EventSource<'a> for NaiveDateTimeStrSource<S> {
    fn next(&mut self) -> Result<Option<Event<'a>>> {
        match self.0.next()? {
            Some(Event::I64(val)) => {
                // TODO: update with chrono 0.5
                #[allow(deprecated)]
                let val = NaiveDateTime::from_timestamp(val / 1000, (val % 1000) as u32 * 100_000);
                // NOTE: chrono documents that Debug, not Display, can be parsed
                Ok(Some(format!("{:?}", val).into()))
            }
            ev => Ok(ev),
        }
    }
}

pub struct UtcDateTimeStrSource<S>(pub S);

impl<'a, S: EventSource<'a>> EventSource<'a> for UtcDateTimeStrSource<S> {
    fn next(&mut self) -> Result<Option<Event<'a>>> {
        match self.0.next()? {
            Some(Event::I64(val)) => {
                // TODO: update with chrono 0.5
                #[allow(deprecated)]
                let val = Utc.timestamp(val / 1000, (val % 1000) as u32 * 100_000);
                // NOTE: chrono documents that Debug, not Display, can be parsed
                Ok(Some(format!("{:?}", val).into()))
            }
            ev => Ok(ev),
        }
    }
}
