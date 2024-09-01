use std::collections::BTreeMap;

use serde::Serialize;

use crate::internal::{
    arrow::{Array, FixedSizeBinaryArray},
    error::{fail, Context, ContextSupport, Result},
    utils::{
        array_ext::{ArrayExt, CountArray, SeqArrayExt},
        btree_map, Mut,
    },
};

use super::{array_builder::ArrayBuilder, simple_serializer::SimpleSerializer};

#[derive(Debug, Clone)]

pub struct FixedSizeBinaryBuilder {
    pub path: String,
    pub seq: CountArray,
    pub buffer: Vec<u8>,
    pub current_n: usize,
    pub n: usize,
}

impl FixedSizeBinaryBuilder {
    pub fn new(path: String, n: usize, is_nullable: bool) -> Self {
        Self {
            path,
            seq: CountArray::new(is_nullable),
            buffer: Vec::new(),
            n,
            current_n: 0,
        }
    }

    pub fn take(&mut self) -> ArrayBuilder {
        ArrayBuilder::FixedSizeBinary(Self {
            path: self.path.clone(),
            seq: self.seq.take(),
            buffer: std::mem::take(&mut self.buffer),
            current_n: std::mem::take(&mut self.current_n),
            n: self.n,
        })
    }

    pub fn is_nullable(&self) -> bool {
        self.seq.validity.is_some()
    }

    pub fn into_array(self) -> Result<Array> {
        Ok(Array::FixedSizeBinary(FixedSizeBinaryArray {
            n: self.n.try_into()?,
            validity: self.seq.validity,
            data: self.buffer,
        }))
    }
}

impl FixedSizeBinaryBuilder {
    fn start(&mut self) -> Result<()> {
        self.current_n = 0;
        self.seq.start_seq()
    }

    fn element<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        let mut u8_serializer = U8Serializer(0);
        value.serialize(Mut(&mut u8_serializer))?;

        self.buffer.push(u8_serializer.0);
        self.current_n += 1;

        Ok(())
    }

    fn end(&mut self) -> Result<()> {
        if self.current_n != self.n {
            fail!(
                "Invalid number of elements for fixed size binary: got {actual}, expected {expected}",
                actual = self.current_n,
                expected = self.n,
            );
        }
        self.seq.end_seq()
    }
}

impl Context for FixedSizeBinaryBuilder {
    fn annotations(&self) -> BTreeMap<String, String> {
        btree_map!("field" => self.path.clone())
    }
}

impl SimpleSerializer for FixedSizeBinaryBuilder {
    fn serialize_default(&mut self) -> Result<()> {
        self.seq.push_seq_default().ctx(self)?;
        for _ in 0..self.n {
            self.buffer.push(0);
        }
        Ok(())
    }

    fn serialize_none(&mut self) -> Result<()> {
        self.seq.push_seq_none().ctx(self)?;
        for _ in 0..self.n {
            self.buffer.push(0);
        }
        Ok(())
    }

    fn serialize_seq_start(&mut self, _: Option<usize>) -> Result<()> {
        self.start().ctx(self)
    }

    fn serialize_seq_element<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        self.element(value).ctx(self)
    }

    fn serialize_seq_end(&mut self) -> Result<()> {
        self.end().ctx(self)
    }

    fn serialize_tuple_start(&mut self, _: usize) -> Result<()> {
        self.start().ctx(self)
    }

    fn serialize_tuple_element<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        self.element(value).ctx(self)
    }

    fn serialize_tuple_end(&mut self) -> Result<()> {
        self.end().ctx(self)
    }

    fn serialize_tuple_struct_start(&mut self, _: &'static str, _: usize) -> Result<()> {
        self.start().ctx(self)
    }

    fn serialize_tuple_struct_field<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        self.element(value).ctx(self)
    }

    fn serialize_tuple_struct_end(&mut self) -> Result<()> {
        self.end().ctx(self)
    }

    fn serialize_bytes(&mut self, v: &[u8]) -> Result<()> {
        if v.len() != self.n {
            fail!(
                in self,
                "Invalid number of elements for fixed size binary: got {actual}, expected {expected}",
                actual = v.len(),
                expected = self.n,
            );
        }

        self.seq.start_seq().ctx(self)?;
        self.buffer.extend(v);
        self.seq.end_seq().ctx(self)
    }
}

struct U8Serializer(u8);

impl Context for U8Serializer {
    fn annotations(&self) -> BTreeMap<String, String> {
        btree_map!()
    }
}

impl SimpleSerializer for U8Serializer {
    fn serialize_u8(&mut self, v: u8) -> Result<()> {
        self.0 = v;
        Ok(())
    }

    fn serialize_u16(&mut self, v: u16) -> Result<()> {
        self.serialize_u8(v.try_into()?)
    }

    fn serialize_u32(&mut self, v: u32) -> Result<()> {
        self.serialize_u8(v.try_into()?)
    }

    fn serialize_u64(&mut self, v: u64) -> Result<()> {
        self.serialize_u8(v.try_into()?)
    }

    fn serialize_i8(&mut self, v: i8) -> Result<()> {
        self.serialize_u8(v.try_into()?)
    }

    fn serialize_i16(&mut self, v: i16) -> Result<()> {
        self.serialize_u8(v.try_into()?)
    }

    fn serialize_i32(&mut self, v: i32) -> Result<()> {
        self.serialize_u8(v.try_into()?)
    }

    fn serialize_i64(&mut self, v: i64) -> Result<()> {
        self.serialize_u8(v.try_into()?)
    }
}
