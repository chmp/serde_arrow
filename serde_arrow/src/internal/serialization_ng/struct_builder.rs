use std::collections::BTreeMap;

use serde::{ser::SerializeStruct, Serialize, Serializer};

use crate::{internal::error::fail, Error, Result};

use super::{array_builder::ArrayBuilder, not_implemented::NotImplemented};

#[derive(Debug, Clone)]
pub struct StructBuilder {
    pub named_fields: Vec<(String, ArrayBuilder)>,
    pub cached_names: Vec<Option<(*const u8, usize)>>,
    pub seen: Vec<bool>,
    pub next: usize,
    pub index: BTreeMap<String, usize>,
}

impl StructBuilder {
    pub fn new(named_fields: Vec<(String, ArrayBuilder)>) -> Result<Self> {
        let mut index = BTreeMap::new();
        let cached_names = vec![None; named_fields.len()];
        let seen = vec![false; named_fields.len()];
        let next = 0;

        for (idx, (name, _)) in named_fields.iter().enumerate() {
            if index.contains_key(name) {
                fail!("Duplicate field {name}");
            }
            index.insert(name.to_owned(), idx);
        }

        Ok(Self {
            named_fields,
            cached_names,
            seen,
            next,
            index,
        })
    }
}

impl<'a> serde::Serializer for &'a mut StructBuilder {
    type Error = Error;
    type Ok = ();

    type SerializeMap = NotImplemented;
    type SerializeSeq = NotImplemented;
    type SerializeStruct = &'a mut StructBuilder;
    type SerializeTuple = NotImplemented;
    type SerializeStructVariant = NotImplemented;
    type SerializeTupleStruct = NotImplemented;
    type SerializeTupleVariant = NotImplemented;

    fn serialize_unit(self) -> Result<()> {
        fail!("Serializer::serialize_unit is not implemented")
    }

    fn serialize_none(self) -> Result<()> {
        fail!("Serializer::serialize_none is not implemented")
    }

    fn serialize_some<T: Serialize + ?Sized>(self, _: &T) -> Result<()> {
        fail!("Serializer::serialize_some is not implemented")
    }

    fn serialize_bool(self, _: bool) -> Result<()> {
        fail!("Serializer::serialize_bool is not implemented")
    }

    fn serialize_char(self, _: char) -> Result<()> {
        fail!("Serializer::serialize_char is not implemented")
    }

    fn serialize_u8(self, _: u8) -> Result<()> {
        fail!("Serializer::serialize_u8 is not implemented")
    }

    fn serialize_u16(self, _: u16) -> Result<()> {
        fail!("Serializer::serialize_u16 is not implemented")
    }

    fn serialize_u32(self, _: u32) -> Result<()> {
        fail!("Serializer::serialize_u32 is not implemented")
    }

    fn serialize_u64(self, _: u64) -> Result<()> {
        fail!("Serializer::serialize_u64 is not implemented")
    }

    fn serialize_i8(self, _: i8) -> Result<()> {
        fail!("Serializer::serialize_i8 is not implemented")
    }

    fn serialize_i16(self, _: i16) -> Result<()> {
        fail!("Serializer::serialize_i16 is not implemented")
    }

    fn serialize_i32(self, _: i32) -> Result<()> {
        fail!("Serializer::serialize_i32 is not implemented")
    }

    fn serialize_i64(self, _: i64) -> Result<()> {
        fail!("Serializer::serialize_i64 is not implemented")
    }

    fn serialize_f32(self, _: f32) -> Result<()> {
        fail!("Serializer::serialize_f32 is not implemented")
    }

    fn serialize_f64(self, _: f64) -> Result<()> {
        fail!("Serializer::serialize_f64 is not implemented")
    }

    fn serialize_bytes(self, _: &[u8]) -> Result<()> {
        fail!("Serializer::serialize_bytes is not implemented")
    }

    fn serialize_str(self, _: &str) -> Result<()> {
        fail!("Serializer::serialize_str is not implemented")
    }

    fn serialize_map(self, _: Option<usize>) -> Result<Self::SerializeMap> {
        Ok(NotImplemented)
    }

    fn serialize_seq(self, _: Option<usize>) -> Result<Self::SerializeSeq> {
        Ok(NotImplemented)
    }

    fn serialize_struct(self, _: &'static str, _: usize) -> Result<Self::SerializeStruct> {
        Ok(&mut *self)
    }

    fn serialize_tuple(self, _: usize) -> Result<Self::SerializeTuple> {
        Ok(NotImplemented)
    }

    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        value: &T,
    ) -> Result<()> {
        value.serialize(&mut *self)
    }

    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<()> {
        fail!("Serializer::serialize_newtype_variant is not implemented")
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<()> {
        fail!("Serializer::serialize_unit_struct is not implemented")
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
    ) -> Result<()> {
        fail!("Serializer::serialize_unit_variant is not implemented")
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        Ok(NotImplemented)
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        Ok(NotImplemented)
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        Ok(NotImplemented)
    }
}

impl<'a> SerializeStruct for &'a mut StructBuilder {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error> {
        let fast_key = (key.as_ptr(), key.len());
        let idx = if self.next < self.cached_names.len()
            && Some(fast_key) == self.cached_names[self.next]
        {
            self.next
        } else {
            let Some(&idx) = self.index.get(key) else {
                // ignore unknown fields
                return Ok(());
            };

            if self.cached_names[idx].is_none() {
                self.cached_names[idx] = Some(fast_key);
            }
            idx
        };

        if self.seen[idx] {
            fail!("Duplicate field {key}");
        }

        value.serialize(&mut self.named_fields[idx].1)?;
        self.seen[idx] = true;
        self.next = idx + 1;

        Ok(())
    }

    fn end(self) -> Result<()> {
        for (idx, seen) in self.seen.iter_mut().enumerate() {
            if !*seen {
                (&mut self.named_fields[idx].1).serialize_none()?;
            }
            *seen = false;
        }

        self.next = 0;

        Ok(())
    }
}
