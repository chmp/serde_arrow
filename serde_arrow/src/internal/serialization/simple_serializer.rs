use serde::{
    ser::{
        SerializeMap, SerializeSeq, SerializeStruct, SerializeStructVariant, SerializeTuple,
        SerializeTupleStruct, SerializeTupleVariant,
    },
    Serialize, Serializer,
};

use crate::internal::{
    error::{fail, Error, Result},
    utils::Mut,
};

use super::ArrayBuilder;

/// A simplified serialization trait with default implementations raising an
/// error
///
/// This trait deviates from the serde model of returning new serializers for
/// (struct, tuple, seq, ..). The only exceptions are struct or tuple variants.
/// As these two cases can be sped up by returning the child serializer in the
/// start call.
///
#[allow(unused_variables)]
pub trait SimpleSerializer: Sized {
    fn name(&self) -> &str;

    // TODO: remove default
    fn annotate_error(&self, err: Error) -> Error {
        err
    }

    fn serialize_default(&mut self) -> Result<()> {
        fail!("serialize_default is not supported for {}", self.name());
    }

    fn serialize_unit(&mut self) -> Result<()> {
        self.serialize_none()
    }

    fn serialize_none(&mut self) -> Result<()> {
        fail!(
            "serialize_unit/serialize_none is not supported for {}",
            self.name()
        );
    }

    fn serialize_some<V: serde::Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        value.serialize(Mut(self))
    }

    fn serialize_bool(&mut self, v: bool) -> Result<()> {
        fail!("serialize_bool is not implemented for {}", self.name())
    }

    fn serialize_char(&mut self, v: char) -> Result<()> {
        fail!("serialize_char is not implemented for {}", self.name())
    }

    fn serialize_u8(&mut self, v: u8) -> Result<()> {
        fail!("serialize_u8 is not implemented for {}", self.name())
    }

    fn serialize_u16(&mut self, v: u16) -> Result<()> {
        fail!("serialize_u16 is not implemented for {}", self.name())
    }

    fn serialize_u32(&mut self, v: u32) -> Result<()> {
        fail!("serialize_u32 is not implemented for {}", self.name())
    }

    fn serialize_u64(&mut self, v: u64) -> Result<()> {
        fail!("serialize_u64 is not implemented for {}", self.name())
    }

    fn serialize_i8(&mut self, v: i8) -> Result<()> {
        fail!("serialize_i8 is not implemented for {}", self.name())
    }

    fn serialize_i16(&mut self, v: i16) -> Result<()> {
        fail!("serialize_i16 is not implemented for {}", self.name())
    }

    fn serialize_i32(&mut self, v: i32) -> Result<()> {
        fail!("serialize_i32 is not implemented for {}", self.name())
    }

    fn serialize_i64(&mut self, v: i64) -> Result<()> {
        fail!("serialize_i64 is not implemented for {}", self.name())
    }

    fn serialize_f32(&mut self, v: f32) -> Result<()> {
        fail!("serialize_f32 is not implemented for {}", self.name())
    }

    fn serialize_f64(&mut self, v: f64) -> Result<()> {
        fail!("serialize_f64 is not implemented for {}", self.name())
    }

    fn serialize_bytes(&mut self, v: &[u8]) -> Result<()> {
        fail!("serialize_bytes is not implemented for {}", self.name())
    }

    fn serialize_str(&mut self, v: &str) -> Result<()> {
        fail!("serialize_str is not implemented for {}", self.name())
    }

    fn serialize_newtype_struct<V: Serialize + ?Sized>(
        &mut self,
        name: &'static str,
        value: &V,
    ) -> Result<()> {
        value.serialize(Mut(self))
    }

    fn serialize_newtype_variant<V: Serialize + ?Sized>(
        &mut self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        value: &V,
    ) -> Result<()> {
        fail!(
            "serialize_newtype_variant is not implemented for {}",
            self.name()
        )
    }

    fn serialize_unit_struct(&mut self, name: &'static str) -> Result<()> {
        fail!(
            "serialize_unit_struct is not implemented for {}",
            self.name()
        )
    }

    fn serialize_unit_variant(
        &mut self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
    ) -> Result<()> {
        fail!(
            "serialize_unit_variant is not implemented for {}",
            self.name()
        )
    }

    fn serialize_map_start(&mut self, len: Option<usize>) -> Result<()> {
        fail!("serialize_map_start is not implemented for {}", self.name())
    }

    fn serialize_map_key<V: Serialize + ?Sized>(&mut self, key: &V) -> Result<()> {
        fail!("serialize_map_key is not implemented for {}", self.name());
    }

    fn serialize_map_value<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        fail!("serialize_map_value is not implemented for {}", self.name())
    }

    fn serialize_map_end(&mut self) -> Result<()> {
        fail!("serialize_map_end is not implemented for {}", self.name())
    }

    fn serialize_seq_start(&mut self, len: Option<usize>) -> Result<()> {
        fail!("serialize_seq_start is not implemented for {}", self.name())
    }

    fn serialize_seq_element<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        fail!(
            "serialize_seq_element is not implemented for {}",
            self.name()
        );
    }

    fn serialize_seq_end(&mut self) -> Result<()> {
        fail!("serialize_seq_end is not implemented for {}", self.name());
    }

    fn serialize_struct_start(&mut self, name: &'static str, len: usize) -> Result<()> {
        fail!(
            "serialize_start_start is not implemented for {}",
            self.name()
        )
    }

    fn serialize_struct_field<V: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &V,
    ) -> Result<()> {
        fail!(
            "serialize_struct_field is not implemented for {}",
            self.name()
        );
    }

    fn serialize_struct_end(&mut self) -> Result<()> {
        fail!(
            "serialize_struct_end is not implemented for {}",
            self.name()
        );
    }

    fn serialize_tuple_start(&mut self, len: usize) -> Result<()> {
        fail!(
            "serialize_tuple_start is not implemented for {}",
            self.name()
        )
    }

    fn serialize_tuple_element<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        fail!(
            "serialize_tuple_element is not implemented for {}",
            self.name()
        );
    }

    fn serialize_tuple_end(&mut self) -> Result<()> {
        fail!("serialize_tuple_end is not implemented for {}", self.name())
    }

    fn serialize_tuple_struct_start(&mut self, name: &'static str, len: usize) -> Result<()> {
        fail!(
            "serialize_tuple_struct_start is not implemented for {}",
            self.name()
        )
    }

    fn serialize_tuple_struct_field<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        fail!(
            "serialize_tuple_struct_field is not implemented for {}",
            self.name()
        );
    }

    fn serialize_tuple_struct_end(&mut self) -> Result<()> {
        fail!(
            "serialize_tuple_struct_end is not implemented for {}",
            self.name()
        );
    }

    fn serialize_struct_variant_start<'this>(
        &'this mut self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<&'this mut ArrayBuilder> {
        fail!(
            "serialize_struct_variant_start is not implemented for {}",
            self.name()
        )
    }

    fn serialize_tuple_variant_start<'this>(
        &'this mut self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<&'this mut ArrayBuilder> {
        fail!(
            "serialize_tuple_variant_start is not implemented for {}",
            self.name()
        )
    }
}

impl<'a, T: SimpleSerializer> Serializer for Mut<'a, T> {
    type Error = Error;
    type Ok = ();

    type SerializeMap = Mut<'a, T>;
    type SerializeSeq = Mut<'a, T>;
    type SerializeStruct = Mut<'a, T>;
    type SerializeTuple = Mut<'a, T>;
    type SerializeTupleStruct = Mut<'a, T>;
    type SerializeStructVariant = Mut<'a, ArrayBuilder>;
    type SerializeTupleVariant = Mut<'a, ArrayBuilder>;

    fn serialize_unit(self) -> Result<()> {
        self.0
            .serialize_unit()
            .map_err(|err| self.0.annotate_error(err))
    }

    fn serialize_none(self) -> Result<()> {
        self.0
            .serialize_none()
            .map_err(|err| self.0.annotate_error(err))
    }

    fn serialize_some<V: Serialize + ?Sized>(self, value: &V) -> Result<()> {
        self.0
            .serialize_some(value)
            .map_err(|err| self.0.annotate_error(err))
    }

    fn serialize_bool(self, v: bool) -> Result<()> {
        self.0
            .serialize_bool(v)
            .map_err(|err| self.0.annotate_error(err))
    }

    fn serialize_char(self, v: char) -> Result<()> {
        self.0
            .serialize_char(v)
            .map_err(|err| self.0.annotate_error(err))
    }

    fn serialize_u8(self, v: u8) -> Result<()> {
        self.0
            .serialize_u8(v)
            .map_err(|err| self.0.annotate_error(err))
    }

    fn serialize_u16(self, v: u16) -> Result<()> {
        self.0
            .serialize_u16(v)
            .map_err(|err| self.0.annotate_error(err))
    }

    fn serialize_u32(self, v: u32) -> Result<()> {
        self.0
            .serialize_u32(v)
            .map_err(|err| self.0.annotate_error(err))
    }

    fn serialize_u64(self, v: u64) -> Result<()> {
        self.0
            .serialize_u64(v)
            .map_err(|err| self.0.annotate_error(err))
    }

    fn serialize_i8(self, v: i8) -> Result<()> {
        self.0
            .serialize_i8(v)
            .map_err(|err| self.0.annotate_error(err))
    }

    fn serialize_i16(self, v: i16) -> Result<()> {
        self.0
            .serialize_i16(v)
            .map_err(|err| self.0.annotate_error(err))
    }

    fn serialize_i32(self, v: i32) -> Result<()> {
        self.0
            .serialize_i32(v)
            .map_err(|err| self.0.annotate_error(err))
    }

    fn serialize_i64(self, v: i64) -> Result<()> {
        self.0
            .serialize_i64(v)
            .map_err(|err| self.0.annotate_error(err))
    }

    fn serialize_f32(self, v: f32) -> Result<()> {
        self.0
            .serialize_f32(v)
            .map_err(|err| self.0.annotate_error(err))
    }

    fn serialize_f64(self, v: f64) -> Result<()> {
        self.0
            .serialize_f64(v)
            .map_err(|err| self.0.annotate_error(err))
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<()> {
        self.0
            .serialize_bytes(v)
            .map_err(|err| self.0.annotate_error(err))
    }

    fn serialize_str(self, v: &str) -> Result<()> {
        self.0
            .serialize_str(v)
            .map_err(|err| self.0.annotate_error(err))
    }

    fn serialize_newtype_struct<V: Serialize + ?Sized>(
        self,
        name: &'static str,
        value: &V,
    ) -> Result<()> {
        self.0
            .serialize_newtype_struct(name, value)
            .map_err(|err| self.0.annotate_error(err))
    }

    fn serialize_newtype_variant<V: Serialize + ?Sized>(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        value: &V,
    ) -> Result<()> {
        self.0
            .serialize_newtype_variant(name, variant_index, variant, value)
            .map_err(|err| self.0.annotate_error(err))
    }

    fn serialize_unit_struct(self, name: &'static str) -> Result<()> {
        self.0
            .serialize_unit_struct(name)
            .map_err(|err| self.0.annotate_error(err))
    }

    fn serialize_unit_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
    ) -> Result<()> {
        self.0
            .serialize_unit_variant(name, variant_index, variant)
            .map_err(|err| self.0.annotate_error(err))
    }

    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap> {
        self.0
            .serialize_map_start(len)
            .map_err(|err| self.0.annotate_error(err))?;
        Ok(Mut(&mut *self.0))
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq> {
        self.0
            .serialize_seq_start(len)
            .map_err(|err| self.0.annotate_error(err))?;
        Ok(Mut(&mut *self.0))
    }

    fn serialize_struct(self, name: &'static str, len: usize) -> Result<Self::SerializeStruct> {
        self.0
            .serialize_struct_start(name, len)
            .map_err(|err| self.0.annotate_error(err))?;
        Ok(Mut(&mut *self.0))
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple> {
        self.0
            .serialize_tuple_start(len)
            .map_err(|err| self.0.annotate_error(err))?;
        Ok(Mut(&mut *self.0))
    }

    fn serialize_tuple_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        self.0
            .serialize_tuple_struct_start(name, len)
            .map_err(|err| self.0.annotate_error(err))?;
        Ok(Mut(&mut *self.0))
    }

    fn serialize_struct_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        // cannot borrow self immutably, as the result will keep self.0 borrowed mutably
        // TODO: figure out how to remove this hack
        let annotations_error = self.0.annotate_error(Error::empty());
        match self
            .0
            .serialize_struct_variant_start(name, variant_index, variant, len)
        {
            Ok(variant_builder) => Ok(Mut(variant_builder)),
            Err(err) => Err(merge_annotations(err, annotations_error)),
        }
    }

    fn serialize_tuple_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        // cannot borrow self immutably, as the result will keep self.0 borrowed mutably
        // TODO: figure out how to remove this hack
        let annotations_error = self.0.annotate_error(Error::empty());
        match self
            .0
            .serialize_tuple_variant_start(name, variant_index, variant, len)
        {
            Ok(variant_builder) => Ok(Mut(variant_builder)),
            Err(err) => Err(merge_annotations(err, annotations_error)),
        }
    }
}

pub fn merge_annotations(err: Error, annotations_err: Error) -> Error {
    let Error::Annotated(annotations_err) = annotations_err else {
        return err;
    };
    err.annotate_unannotated(|annotations| {
        *annotations = annotations_err.annotations;
    })
}

impl<'a, T: SimpleSerializer> SerializeMap for Mut<'a, T> {
    type Ok = ();
    type Error = Error;

    fn serialize_key<V: Serialize + ?Sized>(&mut self, key: &V) -> Result<()> {
        self.0
            .serialize_map_key(key)
            .map_err(|err| self.0.annotate_error(err))
    }

    fn serialize_value<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        self.0
            .serialize_map_value(value)
            .map_err(|err| self.0.annotate_error(err))
    }

    fn end(self) -> Result<()> {
        self.0
            .serialize_map_end()
            .map_err(|err| self.0.annotate_error(err))
    }
}

impl<'a, T: SimpleSerializer> SerializeSeq for Mut<'a, T> {
    type Ok = ();
    type Error = Error;

    fn serialize_element<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        self.0
            .serialize_seq_element(value)
            .map_err(|err| self.0.annotate_error(err))
    }

    fn end(self) -> Result<()> {
        self.0
            .serialize_seq_end()
            .map_err(|err| self.0.annotate_error(err))
    }
}

impl<'a, T: SimpleSerializer> SerializeStruct for Mut<'a, T> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<V: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &V,
    ) -> Result<()> {
        self.0
            .serialize_struct_field(key, value)
            .map_err(|err| self.0.annotate_error(err))
    }

    fn end(self) -> Result<()> {
        self.0
            .serialize_struct_end()
            .map_err(|err| self.0.annotate_error(err))
    }
}

impl<'a, T: SimpleSerializer> SerializeTuple for Mut<'a, T> {
    type Ok = ();
    type Error = Error;

    fn serialize_element<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        self.0
            .serialize_tuple_element(value)
            .map_err(|err| self.0.annotate_error(err))
    }

    fn end(self) -> Result<()> {
        self.0
            .serialize_tuple_end()
            .map_err(|err| self.0.annotate_error(err))
    }
}

impl<'a, T: SimpleSerializer> SerializeTupleStruct for Mut<'a, T> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        self.0
            .serialize_tuple_struct_field(value)
            .map_err(|err| self.0.annotate_error(err))
    }

    fn end(self) -> Result<()> {
        self.0
            .serialize_tuple_struct_end()
            .map_err(|err| self.0.annotate_error(err))
    }
}

impl<'a, T: SimpleSerializer> SerializeStructVariant for Mut<'a, T> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<V: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &V,
    ) -> Result<()> {
        self.0
            .serialize_struct_field(key, value)
            .map_err(|err| self.0.annotate_error(err))
    }

    fn end(self) -> Result<()> {
        self.0
            .serialize_struct_end()
            .map_err(|err| self.0.annotate_error(err))
    }
}

impl<'a, T: SimpleSerializer> SerializeTupleVariant for Mut<'a, T> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        self.0
            .serialize_tuple_struct_field(value)
            .map_err(|err| self.0.annotate_error(err))
    }

    fn end(self) -> Result<()> {
        self.0
            .serialize_tuple_struct_end()
            .map_err(|err| self.0.annotate_error(err))
    }
}
