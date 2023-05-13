use chrono::{DateTime, NaiveDateTime, Utc};

use crate::internal::{
    error::Result,
    event::Event,
    sink::{macros, ArrayBuilder, EventSink},
};

#[derive(Debug)]
pub struct NaiveDateTimeStrBuilder<B>(pub B);

impl<B: EventSink> EventSink for NaiveDateTimeStrBuilder<B> {
    macros::forward_specialized_to_generic!();

    fn accept(&mut self, event: Event<'_>) -> Result<()> {
        self.0.accept(match event.to_self() {
            Event::Str(s) => Event::I64(s.parse::<NaiveDateTime>()?.timestamp_millis()),
            ev => ev,
        })
    }

    fn finish(&mut self) -> Result<()> {
        self.0.finish()
    }
}

impl<A, B: ArrayBuilder<A>> ArrayBuilder<A> for NaiveDateTimeStrBuilder<B> {
    fn build_array(&mut self) -> Result<A> {
        self.0.build_array()
    }
}

#[derive(Debug)]
pub struct UtcDateTimeStrBuilder<B>(pub B);

impl<B: EventSink> EventSink for UtcDateTimeStrBuilder<B> {
    macros::forward_specialized_to_generic!();

    fn accept(&mut self, event: Event<'_>) -> Result<()> {
        self.0.accept(match event.to_self() {
            Event::Str(s) => Event::I64(s.parse::<DateTime<Utc>>()?.timestamp_millis()),
            ev => ev,
        })
    }

    fn finish(&mut self) -> Result<()> {
        self.0.finish()
    }
}

impl<A, B: ArrayBuilder<A>> ArrayBuilder<A> for UtcDateTimeStrBuilder<B> {
    fn build_array(&mut self) -> Result<A> {
        self.0.build_array()
    }
}
