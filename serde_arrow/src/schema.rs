use std::{
    collections::{HashMap, HashSet},
    convert::TryFrom,
};

use arrow::datatypes::{DataType as ArrowType, Field, Schema as ArrowSchema};
use serde::{
    ser::{self, Impossible},
    Serialize,
};

use crate::{fail, util::string_extractor::StringExtractor, Error, Result};

/// Try to determine the schema from the existing records
///
/// This function inspects the individual records and tries to determine the
/// data types of each field. For some fields no data type can be determined,
/// e.g., for options if all entries are missing. In this case, the data type
/// has to be overwritten manually via [Schema::set_data_type]. Further fields may
/// erroneously be detected as non-nullable. In this case, the nullability can
/// be overwritten with [Schema::set_nullable].
///
/// For most types, it is sufficient to trace a small number of records to
/// accurately determine the schema. One option is to use only subset of the
/// full data set.
///
/// The traced schema can be converted into an Arrow Schema via `TryFrom`:
///
/// ```
/// # use std::convert::TryFrom;
/// # use serde_arrow::{Schema, DataType};
/// // Create a new TracedSchema
/// let mut schema = Schema::new();
/// schema.add_field("col1", Some(DataType::I64), true);
/// schema.add_field("col2", Some(DataType::I64), false);
/// ```
///
pub fn trace_schema<T>(value: &T) -> Result<Schema>
where
    T: serde::Serialize + ?Sized,
{
    let mut tracer = Tracer::new();
    value.serialize(&mut tracer)?;
    Ok(tracer.schema)
}

#[derive(Debug, Clone)]
pub enum DataType {
    Bool,
    I8,
    I16,
    I32,
    I64,
    U8,
    U16,
    U32,
    U64,
    F32,
    F64,
    /// A date time as a RFC 3339 string with time zone (requires chrono, mapped
    /// to Arrow's Date64)
    DateTimeStr,
    /// A date time as a RFC 3339 string without a time zone (requires chrono,
    /// mapped to Arrow's Date64)
    NaiveDateTimeStr,
    /// A date time as non-leap milliseconds since the epoch (mapped to Arrow's Date64)
    DateTimeMilliseconds,
    /// A string (mapped to Arrow's UTF8)
    Str,
    /// a raw arrow data type
    Arrow(ArrowType),
}

impl std::convert::TryFrom<&DataType> for ArrowType {
    type Error = Error;

    fn try_from(value: &DataType) -> Result<Self, Self::Error> {
        match value {
            DataType::Bool => Ok(ArrowType::Boolean),
            DataType::I8 => Ok(ArrowType::Int8),
            DataType::I16 => Ok(ArrowType::Int16),
            DataType::I32 => Ok(ArrowType::Int32),
            DataType::I64 => Ok(ArrowType::Int64),
            DataType::U8 => Ok(ArrowType::UInt8),
            DataType::U16 => Ok(ArrowType::UInt16),
            DataType::U32 => Ok(ArrowType::UInt32),
            DataType::U64 => Ok(ArrowType::UInt64),
            DataType::F32 => Ok(ArrowType::Float32),
            DataType::F64 => Ok(ArrowType::Float64),
            DataType::DateTimeStr | DataType::NaiveDateTimeStr | DataType::DateTimeMilliseconds => {
                Ok(ArrowType::Date64)
            }
            DataType::Str => Ok(ArrowType::Utf8),
            DataType::Arrow(res) => Ok(res.clone()),
        }
    }
}

impl From<ArrowType> for DataType {
    fn from(value: ArrowType) -> Self {
        Self::Arrow(value)
    }
}

impl From<&ArrowType> for DataType {
    fn from(value: &ArrowType) -> Self {
        value.clone().into()
    }
}

#[derive(Default, Debug, Clone)]
pub struct Schema {
    fields: Vec<String>,
    seen_fields: HashSet<String>,
    data_type: HashMap<String, DataType>,
    nullable: HashSet<String>,
}

impl Schema {
    pub fn new() -> Self {
        Self::default()
    }

    fn build_arrow_schema(&self) -> Result<ArrowSchema> {
        let mut fields = Vec::new();

        for field in &self.fields {
            let data_type = self
                .data_type
                .get(field)
                .ok_or_else(|| Error::Custom(format!("No data type detected for {}", field)))?;
            let nullable = self.nullable.contains(field);

            let field = Field::new(field, ArrowType::try_from(data_type)?, nullable);
            fields.push(field);
        }

        let schema = ArrowSchema::new(fields);
        Ok(schema)
    }

    /// Get the name of the detected fields
    ///
    pub fn fields(&self) -> &[String] {
        self.fields.as_slice()
    }

    /// Check whether the given field was found
    ///
    pub fn exists(&self, field: &str) -> bool {
        self.seen_fields.contains(field)
    }

    /// Get the data type of a field
    ///
    /// For some fields no data type can be determined, e.g., for options if all
    /// entries are missing. In this case, this function will return `None`.
    /// This function also returns `None` for non-existing fields.
    ///
    pub fn data_type(&self, field: &str) -> Option<&DataType> {
        self.data_type.get(field)
    }

    /// Check whether the filed is nullable
    ///
    /// This function returns `false` for non-existing fields.
    ///
    pub fn is_nullable(&self, field: &str) -> bool {
        self.nullable.contains(field)
    }

    /// Add a new field
    ///
    /// This function fails if the field already exists.
    ///
    pub fn add_field(
        &mut self,
        field: &str,
        data_type: Option<DataType>,
        nullable: bool,
    ) -> Result<()> {
        if self.seen_fields.contains(field) {
            fail!("Duplicate field {}", field);
        }

        self.seen_fields.insert(field.to_owned());
        self.fields.push(field.to_owned());

        if let Some(data_type) = data_type {
            self.data_type.insert(field.to_owned(), data_type);
        }

        if nullable {
            self.nullable.insert(field.to_owned());
        }

        Ok(())
    }

    /// Set or overwrite the data type of a field
    ///
    pub fn set_data_type(&mut self, field: &str, data_type: DataType) -> Result<()> {
        if !self.seen_fields.contains(field) {
            fail!("Cannot set data type for unknown field {}", field);
        }
        self.data_type.insert(field.to_owned(), data_type);
        Ok(())
    }

    /// Mark a field as nullable or not
    ///
    pub fn set_nullable(&mut self, field: &str, nullable: bool) -> Result<()> {
        if !self.seen_fields.contains(field) {
            fail!("Cannot set data type for unknown field {}", field);
        }
        if nullable {
            self.nullable.insert(field.to_owned());
        } else {
            self.nullable.remove(field);
        }
        Ok(())
    }
}

impl std::convert::TryFrom<Schema> for ArrowSchema {
    type Error = Error;

    fn try_from(value: Schema) -> Result<Self, Self::Error> {
        value.build_arrow_schema()
    }
}

impl std::convert::TryFrom<ArrowSchema> for Schema {
    type Error = Error;

    fn try_from(value: ArrowSchema) -> Result<Self> {
        let mut res = Schema::new();

        for field in value.fields() {
            res.add_field(
                field.name(),
                Some(DataType::from(field.data_type())),
                field.is_nullable(),
            )?;
        }

        Ok(res)
    }
}

enum State {
    /// The tracer has not observed any events
    Start,
    /// The outer sequence has been closed
    End,
    /// The tracer is in the outer sequence and waits for the next record
    OuterSequence,
    /// The tracer is currently processing this event
    Field(String),
}

struct Tracer {
    schema: Schema,
    state: State,
}

impl Tracer {
    fn new() -> Self {
        Self {
            schema: Schema::new(),
            state: State::Start,
        }
    }

    fn add_field(&mut self, name: &str, nullable: bool, data_type: Option<DataType>) {
        if !self.schema.seen_fields.contains(name) {
            self.schema.fields.push(name.to_owned());
            self.schema.seen_fields.insert(name.to_owned());
        }

        if nullable {
            self.schema.nullable.insert(name.to_owned());
        }

        if let Some(data_type) = data_type {
            if !self.schema.data_type.contains_key(name) {
                self.schema.data_type.insert(name.to_owned(), data_type);
            }
            // TODO: check that the data type did not change
        }
    }
}

macro_rules! unsupported {
    ($name:ident) => {
        fn $name(self) -> Result<Self::Ok> {
            return Err(Error::Custom(format!(
                "{} not supported for in schema tracing",
                stringify!($name)
            )));
        }
    };
    ($name:ident, $($ty:ty),*) => {
        fn $name(self, $(_: $ty),*) -> Result<Self::Ok> {
            return Err(Error::Custom(format!(
                "{} not supported for in schema tracing",
                stringify!($name)
            )));
        }
    };
}

/// Serialize the outer structure (sequence + records)
impl<'a> ser::Serializer for &'a mut Tracer {
    type Ok = ();
    type Error = Error;

    type SerializeSeq = Self;
    type SerializeStruct = Self;
    type SerializeMap = Self;
    type SerializeTuple = Impossible<Self::Ok, Self::Error>;
    type SerializeTupleStruct = Impossible<Self::Ok, Self::Error>;
    type SerializeTupleVariant = Impossible<Self::Ok, Self::Error>;
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
    unsupported!(serialize_none);

    fn serialize_some<T: ?Sized + Serialize>(self, _: &T) -> Result<()> {
        fail!("serialize_unit  not supported in schema tracing");
    }

    fn serialize_unit(self) -> Result<()> {
        fail!("serialize_unit  not supported in schema tracing");
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<()> {
        fail!("serialize_unit_struct not supported in schema tracing");
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
    ) -> Result<()> {
        fail!("serialize_unit_variant not supported in schema tracing");
    }

    fn serialize_newtype_struct<T>(self, _name: &'static str, _value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        fail!("serialize_newtype_struct not supported in schema tracing");
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
        fail!("serialize_newtype_variant not supported in schema tracing");
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq> {
        self.state = State::OuterSequence;
        Ok(self)
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple> {
        fail!("serialize_tuple not supported in schema tracing");
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        fail!("serialize_tuple_struct not supported in schema tracing");
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        fail!("serialize_tuple_variant not supported in schema tracing");
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
        fail!("serialize_struct_variant not supported in schema tracing");
    }
}

impl<'a> ser::SerializeSeq for &'a mut Tracer {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)?;
        Ok(())
    }

    fn end(self) -> Result<()> {
        self.state = State::End;
        Ok(())
    }
}

impl<'a> ser::SerializeStruct for &'a mut Tracer {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        if !matches!(self.state, State::OuterSequence) {
            fail!("Cannot enter field outside of a sequence");
        }

        self.state = State::Field(key.to_owned());

        let (nullable, data_type) = value.serialize(FieldTracer)?;
        self.add_field(key, nullable, data_type);

        self.state = State::OuterSequence;
        Ok(())
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl<'a> ser::SerializeMap for &'a mut Tracer {
    type Ok = ();
    type Error = Error;

    fn serialize_key<T: ?Sized>(&mut self, key: &T) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
    {
        if !matches!(self.state, State::OuterSequence) {
            fail!("Cannot enter a map field outside of a sequence");
        }

        let key = key.serialize(StringExtractor)?;
        self.state = State::Field(key);

        Ok(())
    }

    fn serialize_value<T: ?Sized>(&mut self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
    {
        let field = match &self.state {
            State::Field(field) => field.to_owned(),
            _ => fail!("Cannot enter a map field outside of a sequence"),
        };

        let (nullable, data_type) = value.serialize(FieldTracer)?;
        self.add_field(&field, nullable, data_type);

        self.state = State::OuterSequence;

        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

struct FieldTracer;

impl<'a> ser::Serializer for FieldTracer {
    type Ok = (bool, Option<DataType>);
    type Error = Error;

    type SerializeSeq = Impossible<Self::Ok, Self::Error>;
    type SerializeStruct = Impossible<Self::Ok, Self::Error>;
    type SerializeMap = Impossible<Self::Ok, Self::Error>;
    type SerializeTuple = Impossible<Self::Ok, Self::Error>;
    type SerializeTupleStruct = Impossible<Self::Ok, Self::Error>;
    type SerializeTupleVariant = Impossible<Self::Ok, Self::Error>;
    type SerializeStructVariant = Impossible<Self::Ok, Self::Error>;

    fn serialize_bool(self, _: bool) -> Result<Self::Ok> {
        Ok((false, Some(DataType::Bool)))
    }

    fn serialize_i8(self, _: i8) -> Result<Self::Ok> {
        Ok((false, Some(DataType::I8)))
    }

    fn serialize_i16(self, _: i16) -> Result<Self::Ok> {
        Ok((false, Some(DataType::I16)))
    }

    fn serialize_i32(self, _: i32) -> Result<Self::Ok> {
        Ok((false, Some(DataType::I32)))
    }

    fn serialize_i64(self, _: i64) -> Result<Self::Ok> {
        Ok((false, Some(DataType::I64)))
    }

    fn serialize_u8(self, _: u8) -> Result<Self::Ok> {
        Ok((false, Some(DataType::U8)))
    }

    fn serialize_u16(self, _: u16) -> Result<Self::Ok> {
        Ok((false, Some(DataType::U16)))
    }

    fn serialize_u32(self, _: u32) -> Result<Self::Ok> {
        Ok((false, Some(DataType::U32)))
    }

    fn serialize_u64(self, _: u64) -> Result<Self::Ok> {
        Ok((false, Some(DataType::U64)))
    }

    fn serialize_f32(self, _: f32) -> Result<Self::Ok> {
        Ok((false, Some(DataType::F32)))
    }

    fn serialize_f64(self, _: f64) -> Result<Self::Ok> {
        Ok((false, Some(DataType::F64)))
    }

    unsupported!(serialize_char, char);

    fn serialize_str(self, _: &str) -> Result<Self::Ok> {
        Ok((false, Some(DataType::Str)))
    }

    unsupported!(serialize_bytes, &[u8]);

    fn serialize_none(self) -> Result<Self::Ok> {
        Ok((true, None))
    }

    fn serialize_some<T>(self, value: &T) -> Result<Self::Ok>
    where
        T: ?Sized + Serialize,
    {
        let (_, data_type) = value.serialize(self)?;
        Ok((true, data_type))
    }

    unsupported!(serialize_unit);
    unsupported!(serialize_unit_struct, &'static str);
    unsupported!(serialize_unit_variant, &'static str, u32, &'static str);

    fn serialize_newtype_struct<T>(self, _name: &'static str, _value: &T) -> Result<Self::Ok>
    where
        T: ?Sized + Serialize,
    {
        fail!("serialize_newtype_struct not supported in schema tracing");
    }

    fn serialize_newtype_variant<T>(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: &T,
    ) -> Result<Self::Ok>
    where
        T: ?Sized + Serialize,
    {
        fail!("serialize_newtype_variant not supported in schema tracing");
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq> {
        fail!("serialize_seq not supported in schema tracing");
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple> {
        fail!("serialize_tuple not supported in schema tracing");
    }

    fn serialize_tuple_struct(
        self,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        fail!("serialize_tuple_struct not supported in schema tracing");
    }

    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        fail!("serialize_tuple_variant not supported in schema tracing");
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap> {
        fail!("serialize_map not supported in schema tracing");
    }

    fn serialize_struct(self, _name: &'static str, _len: usize) -> Result<Self::SerializeStruct> {
        fail!("serialize_struct not supported in schema tracing");
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        fail!("serialize_struct_variant not supported in schema tracing");
    }
}
