use std::collections::{BTreeMap, HashMap};

use marrow::{
    array::{Array, PrimitiveArray},
    datatypes::FieldMeta,
    types::MonthDayNanoInterval,
};
use serde::{Serialize, Serializer};

use crate::internal::{
    chrono,
    error::{fail, set_default, Context, ContextSupport, Error, Result},
    serialization::utils::impl_serializer,
    utils::{
        array_ext::{ArrayExt, ScalarArrayExt},
        value,
    },
};

use super::array_builder::ArrayBuilder;

const FIELDS: [&str; 3] = ["months", "days", "nanoseconds"];

/// Build `Interval(MonthDayNano)` arrays from span strings or from structs with the fields
/// `months`, `days` and `nanoseconds`
#[derive(Debug, Clone)]
pub struct IntervalBuilder {
    pub name: String,
    array: PrimitiveArray<MonthDayNanoInterval>,
    metadata: HashMap<String, String>,
    /// The fields of the struct or map currently being serialized, ordered as `FIELDS`
    fields: [Option<i64>; 3],
    /// The field of the next map value
    next_field: Option<usize>,
}

impl IntervalBuilder {
    pub fn new(name: String, is_nullable: bool, metadata: HashMap<String, String>) -> Self {
        Self {
            name,
            array: PrimitiveArray::new(is_nullable),
            metadata,
            fields: [None; 3],
            next_field: None,
        }
    }

    pub fn take(&mut self) -> ArrayBuilder {
        ArrayBuilder::Interval(Self {
            name: self.name.clone(),
            metadata: self.metadata.clone(),
            array: self.array.take(),
            fields: [None; 3],
            next_field: None,
        })
    }

    pub fn is_nullable(&self) -> bool {
        self.array.is_nullable()
    }

    pub fn into_array_and_field_meta(self) -> Result<(Array, FieldMeta)> {
        let meta = FieldMeta {
            name: self.name,
            metadata: self.metadata,
            nullable: self.array.is_nullable(),
        };
        Ok((Array::MonthDayNanoInterval(self.array), meta))
    }

    pub fn reserve(&mut self, additional: usize) {
        self.array.reserve(additional);
    }

    pub fn serialize_default_value(&mut self) -> Result<()> {
        self.array.push_scalar_default().ctx(self)
    }

    pub fn serialize_value<V: Serialize>(&mut self, value: V) -> Result<()> {
        value.serialize(&mut *self).ctx(self)
    }

    /// Finish the struct or map started with `serialize_struct` or `serialize_map`
    pub fn end(&mut self) -> Result<()> {
        let mut values = [0_i64; 3];
        for ((name, field), value) in FIELDS.iter().zip(self.fields).zip(&mut values) {
            let Some(field) = field else {
                fail!("missing field {name:?}");
            };
            *value = field;
        }
        let [months, days, nanoseconds] = values;
        self.array.push_scalar_value(MonthDayNanoInterval {
            months: convert("months", months)?,
            days: convert("days", days)?,
            nanoseconds,
        })
    }

    fn start_fields(&mut self) {
        self.fields = [None; 3];
        self.next_field = None;
    }

    fn set_field<T: Serialize + ?Sized>(&mut self, key: &str, value: &T) -> Result<()> {
        let Some((_, field)) = FIELDS
            .iter()
            .zip(&mut self.fields)
            .find(|(name, _)| **name == key)
        else {
            fail!("unknown field {key:?}, expected one of {FIELDS:?}");
        };
        if field.is_some() {
            fail!("duplicate field {key:?}");
        }
        *field = Some(value::transmute(value)?);
        Ok(())
    }
}

fn convert(name: &str, value: i64) -> Result<i32> {
    match i32::try_from(value) {
        Ok(value) => Ok(value),
        Err(_) => fail!("{name} value {value} is out of range for Interval(MonthDayNano)"),
    }
}

impl Context for IntervalBuilder {
    fn annotate(&self, annotations: &mut BTreeMap<String, String>) {
        set_default(annotations, "field", &self.name);
        set_default(annotations, "data_type", "Interval(MonthDayNano)");
    }
}

impl<'a> Serializer for &'a mut IntervalBuilder {
    impl_serializer!(
        'a, IntervalBuilder;
        override serialize_none,
        override serialize_str,
        override serialize_struct,
        override serialize_map,
    );

    fn serialize_none(self) -> Result<()> {
        self.array.push_scalar_none()
    }

    fn serialize_str(self, v: &str) -> Result<()> {
        let value = chrono::parse_span(v)?.to_arrow_interval()?;
        self.array.push_scalar_value(value)
    }

    fn serialize_struct(self, _: &'static str, _: usize) -> Result<Self::SerializeStruct> {
        self.start_fields();
        Ok(Self::SerializeStruct::Interval(self))
    }

    fn serialize_map(self, _: Option<usize>) -> Result<Self::SerializeMap> {
        self.start_fields();
        Ok(Self::SerializeMap::Interval(self))
    }
}

impl serde::ser::SerializeStruct for &mut IntervalBuilder {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<()> {
        self.set_field(key, value)
    }

    fn end(self) -> Result<()> {
        IntervalBuilder::end(self)
    }
}

impl serde::ser::SerializeMap for &mut IntervalBuilder {
    type Ok = ();
    type Error = Error;

    fn serialize_key<T: ?Sized + Serialize>(&mut self, key: &T) -> Result<()> {
        let key: String = value::transmute(key)?;
        let Some(idx) = FIELDS.iter().position(|name| *name == key) else {
            fail!("unknown field {key:?}, expected one of {FIELDS:?}");
        };
        self.next_field = Some(idx);
        Ok(())
    }

    fn serialize_value<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<()> {
        let Some(key) = self.next_field.take().and_then(|idx| FIELDS.get(idx)) else {
            fail!("serialize_value called without a preceding key");
        };
        self.set_field(key, value)
    }

    fn end(self) -> Result<()> {
        IntervalBuilder::end(self)
    }
}
