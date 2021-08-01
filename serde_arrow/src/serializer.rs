use std::{collections::HashMap, sync::Arc};

use arrow::{datatypes::Schema, record_batch::RecordBatch};
use serde::{
    ser::{self, Impossible},
    Serialize,
};

use crate::{
    array_builder::ArrayBuilder, fail, util::string_extractor::StringExtractor, Error, Result,
};

pub fn to_record_batch<T>(value: &T, schema: Schema) -> Result<RecordBatch>
where
    T: serde::Serialize + ?Sized,
{
    let mut serializer = OuterSerializer::new(schema)?;
    value.serialize(&mut serializer)?;
    let batch = serializer.build()?;
    Ok(batch)
}

macro_rules! unsupported {
    ($name:ident, $ty:ty) => {
        fn $name(self, _v: $ty) -> Result<()> {
            return Err(Error::Custom(String::from("Not supported")));
        }
    };
}

enum State {
    Start,
    OuterSequence,
    Item,
    Field(String),
    End,
}

pub struct OuterSerializer {
    state: State,
    schema: Schema,
    builders: HashMap<String, ArrayBuilder>,
}

impl OuterSerializer {
    pub fn new(schema: Schema) -> Result<Self> {
        let mut builders = HashMap::new();

        for field in schema.fields() {
            let builder = ArrayBuilder::new(field.data_type())?;
            builders.insert(field.name().to_owned(), builder);
        }

        let res = OuterSerializer {
            state: State::Start,
            schema,
            builders,
        };
        Ok(res)
    }

    pub fn build(&mut self) -> Result<RecordBatch> {
        let mut fields = Vec::new();

        for field in self.schema.fields() {
            let name = field.name();
            let field = self
                .builders
                .get_mut(name)
                .expect("Invalid state")
                .build()?;
            fields.push(field);
        }

        let schema = Arc::new(self.schema.clone());
        let res = RecordBatch::try_new(schema, fields)?;
        Ok(res)
    }
}

impl<'a> ser::Serializer for &'a mut OuterSerializer {
    type Ok = ();
    type Error = Error;

    type SerializeSeq = Self;
    type SerializeStruct = Self;
    type SerializeTuple = Impossible<Self::Ok, Self::Error>;
    type SerializeTupleStruct = Impossible<Self::Ok, Self::Error>;
    type SerializeTupleVariant = Impossible<Self::Ok, Self::Error>;
    type SerializeMap = Self;
    type SerializeStructVariant = Impossible<Self::Ok, Self::Error>;

    unsupported!(serialize_bool, bool);
    unsupported!(serialize_i8, i8);
    unsupported!(serialize_i16, i16);
    unsupported!(serialize_i32, i32);
    unsupported!(serialize_i64, i64);
    unsupported!(serialize_u8, u8);
    unsupported!(serialize_u16, u16);
    unsupported!(serialize_u32, u32);
    unsupported!(serialize_u64, u64);
    unsupported!(serialize_f32, f32);
    unsupported!(serialize_f64, f64);
    unsupported!(serialize_char, char);
    unsupported!(serialize_str, &str);
    unsupported!(serialize_bytes, &[u8]);

    fn serialize_none(self) -> Result<()> {
        fail!("Not supported");
    }

    fn serialize_some<T>(self, _value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        fail!("Not supported");
    }

    fn serialize_unit(self) -> Result<()> {
        fail!("Not supported");
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<()> {
        fail!("Not supported");
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
    ) -> Result<()> {
        fail!("Not supported");
    }

    fn serialize_newtype_struct<T>(self, _name: &'static str, _value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        fail!("Not supported");
    }

    fn serialize_newtype_variant<T>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        fail!("Not supported");
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq> {
        self.state = State::OuterSequence;
        Ok(self)
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple> {
        fail!("Not supported");
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        fail!("Not supported");
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        fail!("Not supported");
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap> {
        Ok(self)
    }

    fn serialize_struct(self, _name: &'static str, _len: usize) -> Result<Self::SerializeStruct> {
        Ok(self)
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        fail!("Not supported");
    }
}

impl<'a> ser::SerializeSeq for &'a mut OuterSerializer {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        self.state = State::Item;
        value.serialize(&mut **self)?;
        self.state = State::OuterSequence;
        Ok(())
    }

    fn end(self) -> Result<()> {
        self.state = State::End;
        Ok(())
    }
}

impl<'a> ser::SerializeStruct for &'a mut OuterSerializer {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        let builder = self
            .builders
            .get_mut(key)
            .ok_or_else(|| Error::Custom(format!("Unknown field {}", key)))?;
        value.serialize(builder)?;
        Ok(())
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl<'a> ser::SerializeMap for &'a mut OuterSerializer {
    type Ok = ();
    type Error = Error;

    fn serialize_key<T: Serialize + ?Sized>(&mut self, key: &T) -> Result<(), Self::Error> {
        if !matches!(self.state, State::Item) {
            fail!("Cannot enter a map field outside of an item");
        }

        let key = key.serialize(StringExtractor)?;
        self.state = State::Field(key);

        Ok(())
    }

    fn serialize_value<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Self::Error> {
        let field = match &self.state {
            State::Field(field) => field.to_owned(),
            _ => fail!("Cannot enter a map field outside of a sequence"),
        };

        let builder = self
            .builders
            .get_mut(&field)
            .ok_or_else(|| Error::Custom(format!("Unknown field {}", &field)))?;
        value.serialize(builder)?;

        self.state = State::Item;
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}
