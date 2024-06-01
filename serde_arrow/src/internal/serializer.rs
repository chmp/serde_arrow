use serde::Serialize;

use crate::internal::{
    array_builder::ArrayBuilder,
    error::{fail, Error, Result},
};

/// Wrap an [`ArrayBuilder`] with as a Serializer
///
/// To support serialization, the wrapped object must implement
/// `AsMut<ArrayBuilder>`. This requirement is covered in particular by mutable
/// references to `ArrayBuilder`s
///
/// Usage:
///
/// ```rust
/// # #[cfg(has_arrow)]
/// # fn main() -> serde_arrow::_impl::PanicOnError<()> {
/// # use serde_arrow::_impl::docs::defs::{Record, example_records};
/// # use serde_arrow::schema::{TracingOptions, SchemaLike};
/// # use serde::Serialize;
/// # let items = example_records();
/// # let fields = Vec::<serde_arrow::_impl::arrow::datatypes::FieldRef>::from_type::<Record>(TracingOptions::default())?;
/// use serde_arrow::{ArrayBuilder, Serializer};
///
/// let mut builder = ArrayBuilder::from_arrow(&fields)?;
///
/// items.serialize(Serializer::new(&mut builder))?;
///
/// let arrays = builder.to_arrow()?;
/// #
/// # Ok(()) }
/// # #[cfg(not(has_arrow))]
/// # fn main() {}
/// ```
///
pub struct Serializer<A>(A);

impl<A> Serializer<A> {
    /// Construct a new serializer from an array builder
    pub fn new(inner: A) -> Self {
        Self(inner)
    }

    /// Extract the wrapped array builder
    pub fn into_inner(self) -> A {
        self.0
    }
}

pub struct CollectionSerializer<A>(A);

impl<A: AsMut<ArrayBuilder>> serde::ser::Serializer for Serializer<A> {
    type Error = Error;
    type Ok = ();

    type SerializeSeq = CollectionSerializer<A>;
    type SerializeTuple = CollectionSerializer<A>;
    type SerializeMap = CollectionSerializer<A>;
    type SerializeStruct = CollectionSerializer<A>;
    type SerializeStructVariant = CollectionSerializer<A>;
    type SerializeTupleStruct = CollectionSerializer<A>;
    type SerializeTupleVariant = CollectionSerializer<A>;

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq> {
        Ok(CollectionSerializer(self.0))
    }

    fn serialize_tuple(self, _: usize) -> Result<Self::SerializeTuple> {
        Ok(CollectionSerializer(self.0))
    }

    fn serialize_tuple_struct(
        self,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        Ok(CollectionSerializer(self.0))
    }

    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        Ok(CollectionSerializer(self.0))
    }

    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        value: &T,
    ) -> Result<()> {
        value.serialize(self)
    }

    fn serialize_bool(self, _: bool) -> Result<()> {
        fail!("not supported")
    }

    fn serialize_bytes(self, _: &[u8]) -> Result<()> {
        fail!("not supported")
    }

    fn serialize_char(self, _: char) -> Result<()> {
        fail!("not supported")
    }

    fn serialize_f32(self, _: f32) -> Result<()> {
        fail!("not supported")
    }

    fn serialize_f64(self, _: f64) -> Result<()> {
        fail!("not supported")
    }

    fn serialize_i128(self, _: i128) -> Result<(), Self::Error> {
        fail!("not supported")
    }

    fn serialize_i16(self, _: i16) -> Result<()> {
        fail!("not supported")
    }

    fn serialize_i32(self, _: i32) -> Result<()> {
        fail!("not supported")
    }

    fn serialize_i64(self, _: i64) -> Result<()> {
        fail!("not supported")
    }

    fn serialize_i8(self, _: i8) -> Result<()> {
        fail!("not supported")
    }

    fn serialize_map(self, _: Option<usize>) -> Result<Self::SerializeMap> {
        fail!("not supported")
    }

    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: &T,
    ) -> Result<()> {
        fail!("not supported")
    }

    fn serialize_none(self) -> Result<()> {
        fail!("not supported")
    }

    fn serialize_some<T: Serialize + ?Sized>(self, _: &T) -> Result<()> {
        fail!("not supported")
    }

    fn serialize_str(self, _: &str) -> Result<()> {
        fail!("not supported")
    }

    fn serialize_struct(self, _: &'static str, _: usize) -> Result<Self::SerializeStruct> {
        fail!("not supported")
    }

    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeStructVariant> {
        fail!("not supported")
    }

    fn serialize_u128(self, _: u128) -> Result<(), Self::Error> {
        fail!("not supported")
    }

    fn serialize_u16(self, _: u16) -> Result<()> {
        fail!("not supported")
    }

    fn serialize_u32(self, _: u32) -> Result<()> {
        fail!("not supported")
    }

    fn serialize_u64(self, _: u64) -> Result<()> {
        fail!("not supported")
    }

    fn serialize_u8(self, _: u8) -> Result<()> {
        fail!("not supported")
    }

    fn serialize_unit(self) -> Result<()> {
        fail!("not supported")
    }

    fn serialize_unit_struct(self, _: &'static str) -> Result<()> {
        fail!("not supported")
    }

    fn serialize_unit_variant(self, _: &'static str, _: u32, _: &'static str) -> Result<()> {
        fail!("not supported")
    }

    fn is_human_readable(&self) -> bool {
        false
    }
}

impl<A: AsMut<ArrayBuilder>> serde::ser::SerializeSeq for CollectionSerializer<A> {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        self.0.as_mut().push(value)
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl<A: AsMut<ArrayBuilder>> serde::ser::SerializeTuple for CollectionSerializer<A> {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        self.0.as_mut().push(value)
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl<A: AsMut<ArrayBuilder>> serde::ser::SerializeTupleStruct for CollectionSerializer<A> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        self.0.as_mut().push(value)
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl<A: AsMut<ArrayBuilder>> serde::ser::SerializeTupleVariant for CollectionSerializer<A> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        self.0.as_mut().push(value)
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl<A: AsMut<ArrayBuilder>> serde::ser::SerializeMap for CollectionSerializer<A> {
    type Ok = ();
    type Error = Error;

    fn serialize_key<T: Serialize + ?Sized>(&mut self, _: &T) -> Result<()> {
        fail!("not supported")
    }

    fn serialize_value<T: Serialize + ?Sized>(&mut self, _: &T) -> Result<()> {
        fail!("not supported")
    }

    fn end(self) -> Result<()> {
        fail!("not supported")
    }
}

impl<A: AsMut<ArrayBuilder>> serde::ser::SerializeStruct for CollectionSerializer<A> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T: ?Sized>(&mut self, _: &'static str, _: &T) -> Result<()> {
        fail!("not supported")
    }

    fn end(self) -> Result<()> {
        fail!("not supported")
    }
}

impl<A: AsMut<ArrayBuilder>> serde::ser::SerializeStructVariant for CollectionSerializer<A> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T: Serialize + ?Sized>(&mut self, _: &'static str, _: &T) -> Result<()> {
        fail!("Not supported")
    }

    fn end(self) -> Result<()> {
        fail!("not supported")
    }
}
