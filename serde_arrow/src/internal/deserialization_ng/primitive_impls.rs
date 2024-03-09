use serde::de::Visitor;

use crate::Result;

use super::{
    array_deserializer::ArrayDeserializer,
    primitive_deserializer::{Primitive, PrimitiveDeserializer},
    simple_deserializer::SimpleDeserializer,
};

macro_rules! implement_integer_into {
    () => {
        fn into_i8(&self) -> Result<i8> {
            Ok((*self).try_into()?)
        }

        fn into_i16(&self) -> Result<i16> {
            Ok((*self).try_into()?)
        }

        fn into_i32(&self) -> Result<i32> {
            Ok((*self).try_into()?)
        }

        fn into_i64(&self) -> Result<i64> {
            Ok((*self).try_into()?)
        }

        fn into_u8(&self) -> Result<u8> {
            Ok((*self).try_into()?)
        }

        fn into_u16(&self) -> Result<u16> {
            Ok((*self).try_into()?)
        }

        fn into_u32(&self) -> Result<u32> {
            Ok((*self).try_into()?)
        }

        fn into_u64(&self) -> Result<u64> {
            Ok((*self).try_into()?)
        }
    };
}

impl Primitive for i8 {
    fn build_array_deserializer<'a>(
        deser: PrimitiveDeserializer<'a, Self>,
    ) -> ArrayDeserializer<'a> {
        ArrayDeserializer::I8(deser)
    }

    fn deserialize_any<'de, S: SimpleDeserializer<'de>, V: Visitor<'de>>(
        deser: &mut S,
        visitor: V,
    ) -> Result<V::Value> {
        deser.deserialize_i8(visitor)
    }

    implement_integer_into!();
}

impl Primitive for i16 {
    fn build_array_deserializer<'a>(
        deser: PrimitiveDeserializer<'a, Self>,
    ) -> ArrayDeserializer<'a> {
        ArrayDeserializer::I16(deser)
    }

    fn deserialize_any<'de, S: SimpleDeserializer<'de>, V: Visitor<'de>>(
        deser: &mut S,
        visitor: V,
    ) -> Result<V::Value> {
        deser.deserialize_i16(visitor)
    }

    implement_integer_into!();
}

impl Primitive for i32 {
    fn build_array_deserializer<'a>(
        deser: PrimitiveDeserializer<'a, Self>,
    ) -> ArrayDeserializer<'a> {
        ArrayDeserializer::I32(deser)
    }

    fn deserialize_any<'de, S: SimpleDeserializer<'de>, V: Visitor<'de>>(
        deser: &mut S,
        visitor: V,
    ) -> Result<V::Value> {
        deser.deserialize_i32(visitor)
    }

    implement_integer_into!();
}

impl Primitive for i64 {
    fn build_array_deserializer<'a>(
        deser: PrimitiveDeserializer<'a, Self>,
    ) -> ArrayDeserializer<'a> {
        ArrayDeserializer::I64(deser)
    }

    fn deserialize_any<'de, S: SimpleDeserializer<'de>, V: Visitor<'de>>(
        deser: &mut S,
        visitor: V,
    ) -> Result<V::Value> {
        deser.deserialize_i64(visitor)
    }

    implement_integer_into!();
}

impl Primitive for u8 {
    fn build_array_deserializer<'a>(
        deser: PrimitiveDeserializer<'a, Self>,
    ) -> ArrayDeserializer<'a> {
        ArrayDeserializer::U8(deser)
    }

    fn deserialize_any<'de, S: SimpleDeserializer<'de>, V: Visitor<'de>>(
        deser: &mut S,
        visitor: V,
    ) -> Result<V::Value> {
        deser.deserialize_u8(visitor)
    }

    implement_integer_into!();
}

impl Primitive for u16 {
    fn build_array_deserializer<'a>(
        deser: PrimitiveDeserializer<'a, Self>,
    ) -> ArrayDeserializer<'a> {
        ArrayDeserializer::U16(deser)
    }

    fn deserialize_any<'de, S: SimpleDeserializer<'de>, V: Visitor<'de>>(
        deser: &mut S,
        visitor: V,
    ) -> Result<V::Value> {
        deser.deserialize_u16(visitor)
    }

    implement_integer_into!();
}

impl Primitive for u32 {
    fn build_array_deserializer<'a>(
        deser: PrimitiveDeserializer<'a, Self>,
    ) -> ArrayDeserializer<'a> {
        ArrayDeserializer::U32(deser)
    }

    fn deserialize_any<'de, S: SimpleDeserializer<'de>, V: Visitor<'de>>(
        deser: &mut S,
        visitor: V,
    ) -> Result<V::Value> {
        deser.deserialize_u32(visitor)
    }

    implement_integer_into!();
}

impl Primitive for u64 {
    fn build_array_deserializer<'a>(
        deser: PrimitiveDeserializer<'a, Self>,
    ) -> ArrayDeserializer<'a> {
        ArrayDeserializer::U64(deser)
    }

    fn deserialize_any<'de, S: SimpleDeserializer<'de>, V: Visitor<'de>>(
        deser: &mut S,
        visitor: V,
    ) -> Result<V::Value> {
        deser.deserialize_u64(visitor)
    }

    implement_integer_into!();
}
