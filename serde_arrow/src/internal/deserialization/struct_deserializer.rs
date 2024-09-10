use serde::de::{
    value::StrDeserializer, DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor,
};

use crate::internal::{
    arrow::BitsWithOffset,
    error::{fail, set_default, try_, Context, ContextSupport, Error, Result},
    utils::Mut,
};

use super::{
    array_deserializer::ArrayDeserializer, simple_deserializer::SimpleDeserializer,
    utils::bitset_is_set,
};

pub struct StructDeserializer<'a> {
    pub path: String,
    pub fields: Vec<(String, ArrayDeserializer<'a>)>,
    pub validity: Option<BitsWithOffset<'a>>,
    pub next: (usize, usize),
    pub len: usize,
}

impl<'a> StructDeserializer<'a> {
    pub fn new(
        path: String,
        fields: Vec<(String, ArrayDeserializer<'a>)>,
        validity: Option<BitsWithOffset<'a>>,
        len: usize,
    ) -> Self {
        Self {
            path,
            fields,
            validity,
            len,
            next: (0, 0),
        }
    }

    pub fn peek_next(&self) -> Result<bool> {
        if self.next.0 >= self.len {
            fail!("Exhausted StructDeserializer");
        }
        if let Some(validity) = &self.validity {
            Ok(bitset_is_set(validity, self.next.0)?)
        } else {
            Ok(true)
        }
    }

    pub fn consume_next(&mut self) {
        self.next = (self.next.0 + 1, 0)
    }
}

impl<'de> Context for StructDeserializer<'de> {
    fn annotate(&self, annotations: &mut std::collections::BTreeMap<String, String>) {
        set_default(annotations, "field", &self.path);
        set_default(annotations, "data_type", "Struct(..)");
    }
}

impl<'de> SimpleDeserializer<'de> for StructDeserializer<'de> {
    fn deserialize_any<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        try_(|| {
            if self.peek_next()? {
                visitor.visit_map(&mut *self)
            } else {
                self.consume_next();
                for (_, field) in &mut self.fields {
                    field.deserialize_ignored_any(IgnoredAny)?;
                }
                visitor.visit_none()
            }
        })
        .ctx(self)
    }

    fn deserialize_option<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        try_(|| {
            if self.peek_next()? {
                visitor.visit_some(Mut(&mut *self))
            } else {
                self.consume_next();
                for (_, field) in &mut self.fields {
                    field.deserialize_ignored_any(IgnoredAny)?;
                }
                visitor.visit_none()
            }
        })
        .ctx(self)
    }

    fn deserialize_map<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        try_(|| visitor.visit_map(&mut *self)).ctx(self)
    }

    fn deserialize_struct<V: Visitor<'de>>(
        &mut self,
        _: &'static str,
        _: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        try_(|| visitor.visit_map(&mut *self)).ctx(self)
    }

    fn deserialize_tuple<V: Visitor<'de>>(&mut self, _: usize, visitor: V) -> Result<V::Value> {
        try_(|| {
            let res = visitor.visit_seq(&mut *self)?;

            // tuples do not consume the sequence until none is raised
            self.consume_next();
            Ok(res)
        })
        .ctx(self)
    }

    fn deserialize_tuple_struct<V: Visitor<'de>>(
        &mut self,
        _: &'static str,
        _: usize,
        visitor: V,
    ) -> Result<V::Value> {
        try_(|| {
            let res = visitor.visit_seq(&mut *self)?;

            // tuples do not consume the sequence until none is raised
            self.consume_next();
            Ok(res)
        })
        .ctx(self)
    }
}

impl<'de> MapAccess<'de> for StructDeserializer<'de> {
    type Error = Error;

    fn next_key_seed<K: DeserializeSeed<'de>>(&mut self, seed: K) -> Result<Option<K::Value>> {
        let (item, field) = self.next;
        if item >= self.len {
            fail!("Exhausted StructDeserializer");
        }
        if field >= self.fields.len() {
            self.next = (item + 1, 0);
            return Ok(None);
        }

        let key = seed.deserialize(StrDeserializer::<Error>::new(&self.fields[field].0))?;
        Ok(Some(key))
    }

    fn next_value_seed<V: DeserializeSeed<'de>>(&mut self, seed: V) -> Result<V::Value> {
        let (item, field) = self.next;
        self.next = (item, field + 1);

        seed.deserialize(Mut(&mut self.fields[field].1))
    }
}

impl<'de> SeqAccess<'de> for StructDeserializer<'de> {
    type Error = Error;

    fn next_element_seed<T: DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> Result<Option<T::Value>, Self::Error> {
        let (item, field) = self.next;
        if item >= self.len {
            fail!("Exhausted StructDeserializer");
        }
        if field >= self.fields.len() {
            self.next = (item + 1, 0);
            return Ok(None);
        }

        let res = seed.deserialize(Mut(&mut self.fields[field].1))?;
        self.next = (item, field + 1);

        Ok(Some(res))
    }
}
