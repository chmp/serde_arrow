use serde::de::Visitor;

use crate::internal::error::Result;

use super::integer_deserializer::Integer;

macro_rules! implement_integer_into {
    () => {
        fn into_i8(self) -> Result<i8> {
            Ok(self.try_into()?)
        }

        fn into_i16(self) -> Result<i16> {
            Ok(self.try_into()?)
        }

        fn into_i32(self) -> Result<i32> {
            Ok(self.try_into()?)
        }

        fn into_i64(self) -> Result<i64> {
            Ok(self.try_into()?)
        }

        fn into_u8(self) -> Result<u8> {
            Ok(self.try_into()?)
        }

        fn into_u16(self) -> Result<u16> {
            Ok(self.try_into()?)
        }

        fn into_u32(self) -> Result<u32> {
            Ok(self.try_into()?)
        }

        fn into_u64(self) -> Result<u64> {
            Ok(self.try_into()?)
        }

        fn into_bool(self) -> Result<bool> {
            Ok(self != 0)
        }
    };
}

impl Integer for i8 {
    fn deserialize_any_at<
        'de,
        S: super::random_access_deserializer::RandomAccessDeserializer<'de>,
        V: Visitor<'de>,
    >(
        deser: &S,
        visitor: V,
        idx: usize,
    ) -> Result<V::Value> {
        deser.deserialize_i8(visitor, idx)
    }

    implement_integer_into!();
}

impl Integer for i16 {
    fn deserialize_any_at<
        'de,
        S: super::random_access_deserializer::RandomAccessDeserializer<'de>,
        V: Visitor<'de>,
    >(
        deser: &S,
        visitor: V,
        idx: usize,
    ) -> Result<V::Value> {
        deser.deserialize_i16(visitor, idx)
    }

    implement_integer_into!();
}

impl Integer for i32 {
    fn deserialize_any_at<
        'de,
        S: super::random_access_deserializer::RandomAccessDeserializer<'de>,
        V: Visitor<'de>,
    >(
        deser: &S,
        visitor: V,
        idx: usize,
    ) -> Result<V::Value> {
        deser.deserialize_i32(visitor, idx)
    }

    implement_integer_into!();
}

impl Integer for i64 {
    fn deserialize_any_at<
        'de,
        S: super::random_access_deserializer::RandomAccessDeserializer<'de>,
        V: Visitor<'de>,
    >(
        deser: &S,
        visitor: V,
        idx: usize,
    ) -> Result<V::Value> {
        deser.deserialize_i64(visitor, idx)
    }

    implement_integer_into!();
}

impl Integer for u8 {
    fn deserialize_any_at<
        'de,
        S: super::random_access_deserializer::RandomAccessDeserializer<'de>,
        V: Visitor<'de>,
    >(
        deser: &S,
        visitor: V,
        idx: usize,
    ) -> Result<V::Value> {
        deser.deserialize_u8(visitor, idx)
    }

    implement_integer_into!();
}

impl Integer for u16 {
    fn deserialize_any_at<
        'de,
        S: super::random_access_deserializer::RandomAccessDeserializer<'de>,
        V: Visitor<'de>,
    >(
        deser: &S,
        visitor: V,
        idx: usize,
    ) -> Result<V::Value> {
        deser.deserialize_u16(visitor, idx)
    }

    implement_integer_into!();
}

impl Integer for u32 {
    fn deserialize_any_at<
        'de,
        S: super::random_access_deserializer::RandomAccessDeserializer<'de>,
        V: Visitor<'de>,
    >(
        deser: &S,
        visitor: V,
        idx: usize,
    ) -> Result<V::Value> {
        deser.deserialize_u32(visitor, idx)
    }

    implement_integer_into!();
}

impl Integer for u64 {
    fn deserialize_any_at<
        'de,
        S: super::random_access_deserializer::RandomAccessDeserializer<'de>,
        V: Visitor<'de>,
    >(
        deser: &S,
        visitor: V,
        idx: usize,
    ) -> Result<V::Value> {
        deser.deserialize_u64(visitor, idx)
    }

    implement_integer_into!();
}
