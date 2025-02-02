use marrow::view::BitsWithOffset;
use serde::de::{SeqAccess, Visitor};

use crate::internal::{
    error::{fail, set_default, try_, Context, ContextSupport, Error, Result},
    utils::{Mut, NamedType, Offset},
};

use super::{
    array_deserializer::ArrayDeserializer,
    random_access_deserializer::RandomAccessDeserializer,
    simple_deserializer::SimpleDeserializer,
    utils::{bitset_is_set, check_supported_list_layout},
};

pub struct ListDeserializer<'a, O: Offset> {
    pub path: String,
    pub item: Box<ArrayDeserializer<'a>>,
    pub offsets: &'a [O],
    pub validity: Option<BitsWithOffset<'a>>,
    pub next: (usize, usize),
}

impl<'a, O: Offset> ListDeserializer<'a, O> {
    pub fn new(
        path: String,
        mut item: ArrayDeserializer<'a>,
        offsets: &'a [O],
        validity: Option<BitsWithOffset<'a>>,
    ) -> Result<Self> {
        check_supported_list_layout(validity, offsets)?;
        item.skip(offsets[0].try_into_usize()?)?;

        Ok(Self {
            path,
            item: Box::new(item),
            offsets,
            validity,
            next: (0, 0),
        })
    }

    pub fn peek_next(&self) -> Result<bool> {
        if self.next.0 + 1 >= self.offsets.len() {
            fail!("Exhausted deserializer")
        }
        if let Some(validity) = &self.validity {
            Ok(bitset_is_set(validity, self.next.0)?)
        } else {
            Ok(true)
        }
    }

    pub fn consume_next(&mut self) {
        self.next = (self.next.0 + 1, 0);
    }
}

impl<O: NamedType + Offset> Context for ListDeserializer<'_, O> {
    fn annotate(&self, annotations: &mut std::collections::BTreeMap<String, String>) {
        set_default(annotations, "filed", &self.path);
        set_default(
            annotations,
            "data_type",
            match O::NAME {
                "i32" => "List(..)",
                "i64" => "LargeList(..)",
                _ => "<unknown>",
            },
        );
    }
}

impl<'a, O: NamedType + Offset> SimpleDeserializer<'a> for ListDeserializer<'a, O> {
    fn deserialize_any<V: Visitor<'a>>(&mut self, visitor: V) -> Result<V::Value> {
        try_(|| {
            if self.peek_next()? {
                self.deserialize_seq(visitor)
            } else {
                self.consume_next();
                visitor.visit_none::<Error>()
            }
        })
        .ctx(self)
    }

    fn deserialize_option<V: Visitor<'a>>(&mut self, visitor: V) -> Result<V::Value> {
        try_(|| {
            if self.peek_next()? {
                visitor.visit_some(Mut(&mut *self))
            } else {
                self.consume_next();
                visitor.visit_none::<Error>()
            }
        })
        .ctx(self)
    }

    fn deserialize_seq<V: Visitor<'a>>(&mut self, visitor: V) -> Result<V::Value> {
        try_(|| visitor.visit_seq(&mut *self)).ctx(self)
    }

    fn deserialize_bytes<V: Visitor<'a>>(&mut self, visitor: V) -> Result<V::Value> {
        try_(|| visitor.visit_seq(&mut *self)).ctx(self)
    }

    fn deserialize_byte_buf<V: Visitor<'a>>(&mut self, visitor: V) -> Result<V::Value> {
        try_(|| visitor.visit_seq(&mut *self)).ctx(self)
    }
}

impl<'de, O: NamedType + Offset> SeqAccess<'de> for ListDeserializer<'de, O> {
    type Error = Error;

    fn next_element_seed<T: serde::de::DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> Result<Option<T::Value>> {
        let (item, offset) = self.next;
        if item + 1 >= self.offsets.len() {
            return Ok(None);
        }
        let end = self.offsets[item + 1].try_into_usize()?;
        let start = self.offsets[item].try_into_usize()?;

        if offset >= end - start {
            self.next = (item + 1, 0);
            return Ok(None);
        }
        self.next = (item, offset + 1);

        let item = seed.deserialize(Mut(self.item.as_mut()))?;
        Ok(Some(item))
    }
}

impl<'de, O: Offset + NamedType> RandomAccessDeserializer<'de> for ListDeserializer<'de, O> {}
