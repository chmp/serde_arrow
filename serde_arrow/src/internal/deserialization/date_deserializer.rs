use chrono::{Datelike, Duration, NaiveDate, NaiveDateTime};
use marrow::view::PrimitiveView;
use serde::de::Visitor;

use crate::internal::{
    error::{fail, set_default, try_, Context, ContextSupport, Error, ErrorKind, Result},
    utils::array_view_ext::ViewAccess,
};

use super::random_access_deserializer::RandomAccessDeserializer;

pub trait DatePrimitive:
    TryInto<i32> + TryInto<i64> + Copy + std::fmt::Display + std::ops::Div<Self, Output = Self>
{
    const DATA_TYPE_NAME: &'static str;
    const DAY_TO_VALUE_FACTOR: Self;
    const BITS: usize;
}

impl DatePrimitive for i32 {
    const DATA_TYPE_NAME: &'static str = "Date32";
    const DAY_TO_VALUE_FACTOR: Self = 1;
    const BITS: usize = 32;
}

impl DatePrimitive for i64 {
    const DATA_TYPE_NAME: &'static str = "Date64";
    const DAY_TO_VALUE_FACTOR: Self = 86_400_000;
    const BITS: usize = 64;
}

pub struct DateDeserializer<'a, I: DatePrimitive> {
    path: String,
    view: PrimitiveView<'a, I>,
}

impl<'a, I: DatePrimitive> DateDeserializer<'a, I> {
    pub fn new(path: String, view: PrimitiveView<'a, I>) -> Self {
        Self { path, view }
    }

    pub fn get_string_repr(&self, ts: I) -> Result<String> {
        let ts = (ts / I::DAY_TO_VALUE_FACTOR)
            .try_into()
            .map_err(|_| Error::new(ErrorKind::Custom, format!("Cannot convert {ts} to i64")))?;

        #[allow(deprecated)]
        const UNIX_EPOCH: NaiveDate = NaiveDateTime::UNIX_EPOCH.date();
        #[allow(deprecated)]
        let delta = Duration::days(ts);
        let date = UNIX_EPOCH + delta;

        // special handling of negative dates:
        //
        // - jiff expects 6 digits years in this case
        // - chrono allows an arbitrary number of digits, when prefixed with a sign
        //
        // https://github.com/chronotope/chrono/blob/05a6ce68cf18a01274cef211b080a7170c7c1a1f/src/format/parse.rs#L368
        if date.year() < 0 {
            Ok(format!(
                "-{positive_year:06}-{month:02}-{day:02}",
                positive_year = -date.year(),
                month = date.month(),
                day = date.day(),
            ))
        } else {
            Ok(date.to_string())
        }
    }
}

impl<I: DatePrimitive> Context for DateDeserializer<'_, I> {
    fn annotate(&self, annotations: &mut std::collections::BTreeMap<String, String>) {
        set_default(annotations, "field", &self.path);
        set_default(annotations, "data_type", I::DATA_TYPE_NAME);
    }
}

impl<'de, I: DatePrimitive> RandomAccessDeserializer<'de> for DateDeserializer<'de, I> {
    fn is_some(&self, idx: usize) -> Result<bool> {
        self.view.is_some(idx)
    }

    fn deserialize_any_some<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        if I::BITS == 32 {
            self.deserialize_i32(visitor, idx)
        } else {
            self.deserialize_i64(visitor, idx)
        }
    }

    fn deserialize_i32<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        try_(|| {
            let val = self.view.get_required(idx)?;
            let Ok(val) = (*val).try_into() else {
                fail!("Cannot convert {val} to i32");
            };
            visitor.visit_i32(val)
        })
        .ctx(self)
    }

    fn deserialize_i64<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        try_(|| {
            let val = self.view.get_required(idx)?;
            let Ok(val) = (*val).try_into() else {
                fail!("Cannot convert {val} to i64");
            };
            visitor.visit_i64(val)
        })
        .ctx(self)
    }

    fn deserialize_str<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        try_(|| self.deserialize_string(visitor, idx)).ctx(self)
    }

    fn deserialize_string<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        try_(|| {
            let ts = self.view.get_required(idx)?;
            visitor.visit_string(self.get_string_repr(*ts)?)
        })
        .ctx(self)
    }

    fn deserialize_bytes<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        try_(|| self.deserialize_byte_buf(visitor, idx)).ctx(self)
    }

    fn deserialize_byte_buf<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        try_(|| {
            let ts = self.view.get_required(idx)?;
            visitor.visit_byte_buf(self.get_string_repr(*ts)?.into_bytes())
        })
        .ctx(self)
    }
}
