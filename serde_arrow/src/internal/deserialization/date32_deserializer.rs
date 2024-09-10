use chrono::{Duration, NaiveDate, NaiveDateTime};
use serde::de::Visitor;

use crate::internal::{
    arrow::BitsWithOffset,
    error::{set_default, try_, Context, ContextSupport, Error, Result},
    utils::Mut,
};

use super::{simple_deserializer::SimpleDeserializer, utils::ArrayBufferIterator};

pub struct Date32Deserializer<'a> {
    path: String,
    array: ArrayBufferIterator<'a, i32>,
}

impl<'a> Date32Deserializer<'a> {
    pub fn new(path: String, buffer: &'a [i32], validity: Option<BitsWithOffset<'a>>) -> Self {
        Self {
            path,
            array: ArrayBufferIterator::new(buffer, validity),
        }
    }

    pub fn get_string_repr(&self, ts: i32) -> Result<String> {
        const UNIX_EPOCH: NaiveDate = NaiveDateTime::UNIX_EPOCH.date();
        #[allow(deprecated)]
        let delta = Duration::days(ts as i64);
        let date = UNIX_EPOCH + delta;
        Ok(date.to_string())
    }
}

impl<'de> Context for Date32Deserializer<'de> {
    fn annotate(&self, annotations: &mut std::collections::BTreeMap<String, String>) {
        set_default(annotations, "field", &self.path);
        set_default(annotations, "data_type", "Date32");
    }
}

impl<'de> SimpleDeserializer<'de> for Date32Deserializer<'de> {
    fn deserialize_any<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        try_(|| {
            if self.array.peek_next()? {
                self.deserialize_i32(visitor)
            } else {
                self.array.consume_next();
                visitor.visit_none()
            }
        })
        .ctx(self)
    }

    fn deserialize_option<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        try_(|| {
            if self.array.peek_next()? {
                visitor.visit_some(Mut(self))
            } else {
                self.array.consume_next();
                visitor.visit_none::<Error>()
            }
        })
        .ctx(self)
    }

    fn deserialize_i32<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        try_(|| visitor.visit_i32(self.array.next_required()?)).ctx(self)
    }

    fn deserialize_str<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        try_(|| self.deserialize_string(visitor)).ctx(self)
    }

    fn deserialize_string<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        try_(|| {
            let ts = self.array.next_required()?;
            visitor.visit_string(self.get_string_repr(ts)?)
        })
        .ctx(self)
    }
}
