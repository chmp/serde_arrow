use serde::de::{SeqAccess, Visitor};

use crate::internal::{
    arrow::BitsWithOffset,
    error::{fail, Context, ContextSupport, Error, Result},
    utils::{btree_map, Mut, NamedType, Offset},
};

use super::{
    array_deserializer::ArrayDeserializer,
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
        item: ArrayDeserializer<'a>,
        offsets: &'a [O],
        validity: Option<BitsWithOffset<'a>>,
    ) -> Result<Self> {
        check_supported_list_layout(validity, offsets)?;

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
            fail!("Exhausted ListDeserializer")
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

impl<'a, O: NamedType + Offset> Context for ListDeserializer<'a, O> {
    fn annotations(&self) -> std::collections::BTreeMap<String, String> {
        let data_type = match O::NAME {
            "i32" => "List(..)",
            "i64" => "LargeList(..)",
            _ => "<unknown>",
        };
        btree_map!("field" => self.path.clone(), "data_type" => data_type)
    }
}

impl<'a, O: NamedType + Offset> SimpleDeserializer<'a> for ListDeserializer<'a, O> {
    fn deserialize_any<V: Visitor<'a>>(&mut self, visitor: V) -> Result<V::Value> {
        if self.peek_next().ctx(self)? {
            self.deserialize_seq(visitor)
        } else {
            self.consume_next();
            visitor.visit_none::<Error>().ctx(self)
        }
    }

    fn deserialize_option<V: Visitor<'a>>(&mut self, visitor: V) -> Result<V::Value> {
        if self.peek_next().ctx(self)? {
            visitor.visit_some(Mut(self))
        } else {
            self.consume_next();
            visitor.visit_none::<Error>().ctx(self)
        }
    }

    fn deserialize_seq<V: Visitor<'a>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_seq(self)
    }

    fn deserialize_bytes<V: Visitor<'a>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_seq(self)
    }

    fn deserialize_byte_buf<V: Visitor<'a>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_seq(self)
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
        let end = self.offsets[item + 1].try_into_usize().ctx(self)?;
        let start = self.offsets[item].try_into_usize().ctx(self)?;

        if offset >= end - start {
            self.next = (item + 1, 0);
            return Ok(None);
        }
        self.next = (item, offset + 1);

        let item = seed.deserialize(Mut(self.item.as_mut()))?;
        Ok(Some(item))
    }
}
