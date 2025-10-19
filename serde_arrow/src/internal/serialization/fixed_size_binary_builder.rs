use std::collections::BTreeMap;

use marrow::array::{Array, FixedSizeBinaryArray};
use serde::Serialize;

use crate::internal::{
    error::{fail, set_default, try_, Context, ContextSupport, Error, Result},
    serialization::utils::impl_serializer,
    utils::array_ext::{ArrayExt, CountArray, SeqArrayExt},
};

use super::{array_builder::ArrayBuilder, binary_builder::U8Serializer};

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

    pub fn reserve(&mut self, additional: usize) {
        self.seq.reserve(additional);
        self.buffer.reserve(additional * self.n);
    }

    pub fn serialize_default_value(&mut self) -> Result<()> {
        try_(|| {
            self.seq.push_seq_default()?;
            for _ in 0..self.n {
                self.buffer.push(0);
            }
            Ok(())
        })
        .ctx(self)
    }
}

impl FixedSizeBinaryBuilder {
    fn start(&mut self) -> Result<()> {
        self.current_n = 0;
        self.seq.start_seq()
    }

    fn element<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        let mut u8_serializer = U8Serializer(0);
        value.serialize(&mut u8_serializer)?;

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
    fn annotate(&self, annotations: &mut BTreeMap<String, String>) {
        set_default(annotations, "field", &self.path);
        set_default(annotations, "data_type", "FixedSizeBinary(..)");
    }
}

impl<'a> serde::Serializer for &'a mut FixedSizeBinaryBuilder {
    impl_serializer!(
        'a, FixedSizeBinaryBuilder;
        override serialize_none,
        override serialize_seq,
        override serialize_tuple,
        override serialize_bytes,
        override serialize_str,
    );

    fn serialize_none(self) -> Result<()> {
        try_(|| {
            self.seq.push_seq_none()?;
            for _ in 0..self.n {
                self.buffer.push(0);
            }
            Ok(())
        })
        .ctx(self)
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq> {
        // TOOD: fix reservation
        self.start().ctx(self)?;
        Ok(super::utils::SerializeSeq::FixedSizeBinary(self))
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple> {
        // TOOD: fix reservation
        self.start().ctx(self)?;
        Ok(Self::SerializeTuple::FixedSizeBinary(self))
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<()> {
        try_(|| {
            if v.len() != self.n {
                fail!(
                    in self,
                    "Invalid number of elements for fixed size binary: got {actual}, expected {expected}",
                    actual = v.len(),
                    expected = self.n,
                );
            }

            self.seq.start_seq()?;
            self.buffer.extend(v);
            self.seq.end_seq()
        }).ctx(self)
    }

    fn serialize_str(self, v: &str) -> Result<()> {
        serde::Serializer::serialize_bytes(self, v.as_bytes())
    }
}

impl serde::ser::SerializeSeq for &mut FixedSizeBinaryBuilder {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<()> {
        self.element(value).ctx(*self)
    }

    fn end(self) -> Result<()> {
        FixedSizeBinaryBuilder::end(&mut *self).ctx(self)
    }
}

impl serde::ser::SerializeTuple for &mut FixedSizeBinaryBuilder {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<()> {
        self.element(value).ctx(*self)
    }

    fn end(self) -> Result<()> {
        FixedSizeBinaryBuilder::end(&mut *self).ctx(self)
    }
}
