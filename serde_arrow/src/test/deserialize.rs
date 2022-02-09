#![allow(unused)]
use crate::{error, fail, Error, Result};

use std::marker::PhantomData;

use arrow::record_batch::RecordBatch;
use serde::de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Serialize};

#[ignore]
#[test]
fn example() -> Result<()> {
    let original = &[
        Example { int8: 0, int32: 21 },
        Example { int8: 1, int32: 42 },
    ];
    let schema = crate::trace_schema(&original)?;
    let record_batch = crate::to_record_batch(&original, &schema)?;
    let round_tripped = from_record_batch::<Vec<Example>>(&record_batch)?;

    assert_eq!(round_tripped, original);

    Ok(())
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
struct Example {
    int8: i8,
    int32: i32,
}

fn from_record_batch<'de, T: Deserialize<'de>>(record_batch: &'de RecordBatch) -> Result<T> {
    let mut deserializer = Deserializer::from_record_batch(&record_batch);
    let res = T::deserialize(&mut deserializer)?;

    if !matches!(deserializer.state, DeserializeState::End) {
        fail!("Input not fully consumed");
    }

    Ok(res)
}

impl<'de> Deserializer<'de> {
    fn from_record_batch(record_batch: &'de RecordBatch) -> Self {
        let fields = record_batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().to_owned())
            .collect();
        Self {
            record_batch: record_batch.clone(),
            state: DeserializeState::Start,
            fields,
            _phantom: PhantomData,
        }
    }
}

pub struct Deserializer<'de> {
    record_batch: RecordBatch,
    state: DeserializeState,
    fields: Vec<String>,
    _phantom: PhantomData<&'de ()>,
}

enum DeserializeState {
    Start,
    StartRow(usize),
    Value(usize, usize),
    EndRow(usize),
    End,
}

impl<'de, 'a> de::Deserializer<'de> for &'a mut Deserializer<'de> {
    type Error = Error;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        fail!("Not implemented deserialize_any");
    }

    fn deserialize_bool<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        fail!("Not implemented deserialize_bool");
    }

    fn deserialize_i8<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        fail!("Not implemented deserialize_i8");
    }

    fn deserialize_i16<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        fail!("Not implemented deserialize_i16");
    }

    fn deserialize_i32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        fail!("Not implemented deserialize_i32");
    }

    fn deserialize_i64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        fail!("Not implemented deserialize_i64");
    }

    fn deserialize_u8<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        fail!("Not implemented deserialize_u8");
    }

    fn deserialize_u16<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        fail!("Not implemented deserialize_u16");
    }

    fn deserialize_u32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        fail!("Not implemented deserialize_u32");
    }

    fn deserialize_u64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        fail!("Not implemented deserialize_u64");
    }

    // Float parsing is stupidly hard.
    fn deserialize_f32<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value> {
        fail!("Not implemented deserialize_f32");
    }

    // Float parsing is stupidly hard.
    fn deserialize_f64<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value> {
        fail!("Not implemented deserialize_f64");
    }

    fn deserialize_char<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value> {
        fail!("Not implemented deserialize_char");
    }

    fn deserialize_str<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        fail!("Not implemented deserialize_str");
    }

    fn deserialize_string<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        fail!("Not implemented deserialize_string");
    }

    fn deserialize_bytes<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value> {
        fail!("Not implemented deserialize_bytes");
    }

    fn deserialize_byte_buf<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value> {
        fail!("Not implemented deserialize_byte_buf")
    }

    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        fail!("Not implemented deserialize_option");
    }

    fn deserialize_unit<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        fail!("Not implemented deserialize_unit");
    }

    // Unit struct means a named value containing no data.
    fn deserialize_unit_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value> {
        fail!("Not implemented deserialize_unit_struct");
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value> {
        fail!("Not implemented deserialize_newtype_struct");
    }

    fn deserialize_seq<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        if !matches!(self.state, DeserializeState::Start) {
            fail!("Can only read sequences in the outer struct");
        }
        self.state = DeserializeState::StartRow(0);
        let res = visitor.visit_seq(&mut *self)?;

        // TODO: check correct state

        self.state = DeserializeState::End;

        Ok(res)
    }

    fn deserialize_tuple<V: Visitor<'de>>(self, _len: usize, visitor: V) -> Result<V::Value> {
        fail!("Not implemented deserialize_tuple");
    }

    fn deserialize_tuple_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value> {
        fail!("Not implemented deserialize_tuple_struct");
    }

    fn deserialize_map<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        let row = match self.state {
            DeserializeState::StartRow(row) => row,
            _ => fail!("Invalid state"),
        };

        self.state = DeserializeState::Value(row, 0);
        let res = visitor.visit_map(&mut *self)?;

        // TODO: check state
        self.state = DeserializeState::EndRow(row);

        Ok(res)
    }

    fn deserialize_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        self.deserialize_map(visitor)
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        fail!("Not implemented deserialize_enum");
    }

    fn deserialize_identifier<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        fail!("Not implemented deserialize_identifier");
    }

    fn deserialize_ignored_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        fail!("Not implemented deserialize_ignored_any");
    }
}

impl<'de, 'a> SeqAccess<'de> for &'a mut Deserializer<'de> {
    type Error = Error;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>>
    where
        T: DeserializeSeed<'de>,
    {
        let idx = match self.state {
            DeserializeState::StartRow(idx) => idx,
            _ => fail!("invalid state"),
        };

        if idx >= self.record_batch.num_rows() {
            return Ok(None);
        }

        let res = seed.deserialize(&mut **self)?;
        self.state = DeserializeState::StartRow(idx + 1);

        Ok(Some(res))
    }
}

impl<'de, 'a> MapAccess<'de> for &'a mut Deserializer<'de> {
    type Error = Error;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>>
    where
        K: DeserializeSeed<'de>,
    {
        let (row, col) = match self.state {
            DeserializeState::Value(row, col) => (row, col),
            _ => fail!("Invalid state"),
        };

        let mut name_de = FieldNameDeserializer {
            field: Some(self.fields[col].clone()),
            _phantom: PhantomData,
        };
        let key = seed.deserialize(&mut name_de)?;
        self.state = DeserializeState::Value(row, col + 1);

        Ok(Some(key))
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value>
    where
        V: DeserializeSeed<'de>,
    {
        fail!("Not implemented: next_value_seed")
    }
}

struct FieldNameDeserializer<'de> {
    field: Option<String>,
    _phantom: PhantomData<&'de ()>,
}

impl<'de, 'a> de::Deserializer<'de> for &'a mut FieldNameDeserializer<'de> {
    type Error = Error;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.deserialize_string(visitor)
    }

    fn deserialize_str<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.deserialize_string(visitor)
    }

    fn deserialize_identifier<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.deserialize_string(visitor)
    }

    fn deserialize_string<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        let name = self
            .field
            .take()
            .ok_or_else(|| error!("Invalid state for FieldNameDeserializer"))?;
        visitor.visit_string(name)
    }

    fn deserialize_bool<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        fail!("Not implemented deserialize_bool");
    }

    fn deserialize_i8<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        fail!("Not implemented deserialize_i8");
    }

    fn deserialize_i16<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        fail!("Not implemented deserialize_i16");
    }

    fn deserialize_i32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        fail!("Not implemented deserialize_i32");
    }

    fn deserialize_i64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        fail!("Not implemented deserialize_i64");
    }

    fn deserialize_u8<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        fail!("Not implemented deserialize_u8");
    }

    fn deserialize_u16<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        fail!("Not implemented deserialize_u16");
    }

    fn deserialize_u32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        fail!("Not implemented deserialize_u32");
    }

    fn deserialize_u64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        fail!("Not implemented deserialize_u64");
    }

    // Float parsing is stupidly hard.
    fn deserialize_f32<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value> {
        fail!("Not implemented deserialize_f32");
    }

    // Float parsing is stupidly hard.
    fn deserialize_f64<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value> {
        fail!("Not implemented deserialize_f64");
    }

    fn deserialize_char<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value> {
        fail!("Not implemented deserialize_char");
    }

    fn deserialize_bytes<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value> {
        fail!("Not implemented deserialize_bytes");
    }

    fn deserialize_byte_buf<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value> {
        fail!("Not implemented deserialize_byte_buf")
    }

    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        fail!("Not implemented deserialize_option");
    }

    fn deserialize_unit<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        fail!("Not implemented deserialize_unit");
    }

    // Unit struct means a named value containing no data.
    fn deserialize_unit_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value> {
        fail!("Not implemented deserialize_unit_struct");
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value> {
        fail!("Not implemented deserialize_newtype_struct");
    }

    fn deserialize_seq<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        fail!("Not implemented deserialize_enum");
    }

    fn deserialize_tuple<V: Visitor<'de>>(self, _len: usize, visitor: V) -> Result<V::Value> {
        fail!("Not implemented deserialize_tuple");
    }

    fn deserialize_tuple_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value> {
        fail!("Not implemented deserialize_tuple_struct");
    }

    fn deserialize_map<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        fail!("Not implemented deserialize_enum");
    }

    fn deserialize_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        fail!("Not implemented deserialize_enum");
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        fail!("Not implemented deserialize_enum");
    }

    fn deserialize_ignored_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        fail!("Not implemented deserialize_ignored_any");
    }
}
