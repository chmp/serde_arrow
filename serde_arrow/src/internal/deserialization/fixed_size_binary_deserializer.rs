use marrow::view::FixedSizeBinaryView;
use serde::de::Visitor;

use crate::internal::error::{
    fail, set_default, try_, Context, ContextSupport, Error, ErrorKind, Result,
};

use super::{
    random_access_deserializer::RandomAccessDeserializer,
    utils::{bitset_is_set, U8SliceDeserializer},
};

pub struct FixedSizeBinaryDeserializer<'a> {
    pub path: String,
    pub view: FixedSizeBinaryView<'a>,
    pub len: usize,
    pub n: usize,
}

impl<'a> FixedSizeBinaryDeserializer<'a> {
    pub fn new(path: String, view: FixedSizeBinaryView<'a>) -> Result<Self> {
        let n = usize::try_from(view.n)?;
        if view.data.len() % n != 0 {
            fail!(
                concat!(
                    "Invalid FixedSizeBinary array: Data of len {len} is not ",
                    "evenly divisible into chunks of size {n}",
                ),
                len = view.data.len(),
                n = n,
            );
        }

        Ok(Self {
            path,
            len: view.data.len() / n,
            view,
            n,
        })
    }

    pub fn get(&self, idx: usize) -> Result<Option<&'a [u8]>> {
        if idx >= self.len {
            fail!("Out of bounds access")
        }
        if let Some(validity) = &self.view.validity {
            if !bitset_is_set(validity, idx)? {
                return Ok(None);
            }
        }
        let start = idx * self.n;
        let end = (idx + 1) * self.n;
        Ok(Some(&self.view.data[start..end]))
    }

    pub fn get_required(&self, idx: usize) -> Result<&'a [u8]> {
        let Some(s) = self.get(idx)? else {
            return Err(Error::new(
                ErrorKind::NullabilityViolation { field: None },
                "Required value is not defined".into(),
            ));
        };
        Ok(s)
    }
}

impl Context for FixedSizeBinaryDeserializer<'_> {
    fn annotate(&self, annotations: &mut std::collections::BTreeMap<String, String>) {
        set_default(annotations, "field", &self.path);
        set_default(
            annotations,
            "data_type",
            format!("FixedSizeBinary({n})", n = self.n),
        );
    }
}

impl<'de> RandomAccessDeserializer<'de> for FixedSizeBinaryDeserializer<'de> {
    fn is_some(&self, idx: usize) -> Result<bool> {
        Ok(self.get(idx)?.is_some())
    }

    fn deserialize_any_some<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        self.deserialize_bytes(visitor, idx)
    }

    fn deserialize_bytes<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        try_(|| visitor.visit_borrowed_bytes(self.get_required(idx)?)).ctx(self)
    }

    fn deserialize_byte_buf<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        try_(|| visitor.visit_borrowed_bytes(self.get_required(idx)?)).ctx(self)
    }

    fn deserialize_seq<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        try_(|| visitor.visit_seq(U8SliceDeserializer::new(self.get_required(idx)?))).ctx(self)
    }
}
