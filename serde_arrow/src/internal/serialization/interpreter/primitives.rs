use crate::internal::{
    common::{DictionaryIndex, DictionaryValue},
    conversions::{ToBytes, WrappedF16, WrappedF32, WrappedF64},
    decimal,
    error::Result,
    serialization::{bytecode::PushDecimal128, compiler::Structure},
};

use super::super::bytecode::{
    PushBool, PushDate64FromNaiveStr, PushDate64FromUtcStr, PushDictionary, PushF16, PushF32,
    PushF64, PushI16, PushI32, PushI64, PushI8, PushLargeUtf8, PushNull, PushU16, PushU32, PushU64,
    PushU8, PushUtf8,
};
use super::{Instruction, SerializationContext};

impl Instruction for PushNull {
    const NAME: &'static str = "PushNull";
    const EXPECTED: &'static [&'static str] = &["Null"];

    fn accept_null(
        &self,
        _structure: &Structure,
        context: &mut SerializationContext,
    ) -> Result<usize> {
        context.buffers.u0[self.idx].push(());
        Ok(self.next)
    }
}

impl Instruction for PushUtf8 {
    const NAME: &'static str = "PushUtf8";
    const EXPECTED: &'static [&'static str] = &["Str"];

    fn accept_str(
        &self,
        _structure: &Structure,
        context: &mut SerializationContext,
        val: &str,
    ) -> Result<usize> {
        context.buffers.u8[self.buffer].extend(val.as_bytes().iter().copied());
        context.buffers.u32_offsets[self.offsets].push(val.len())?;
        Ok(self.next)
    }
}

impl Instruction for PushLargeUtf8 {
    const NAME: &'static str = "PushLargeUtf8";
    const EXPECTED: &'static [&'static str] = &["Str"];

    fn accept_str(
        &self,
        _structure: &Structure,
        context: &mut SerializationContext,
        val: &str,
    ) -> Result<usize> {
        context.buffers.u8[self.buffer].extend(val.as_bytes().iter().copied());
        context.buffers.u64_offsets[self.offsets].push(val.len())?;
        Ok(self.next)
    }
}

impl Instruction for PushDate64FromNaiveStr {
    const NAME: &'static str = "PushDate64FromNaiveStr";
    const EXPECTED: &'static [&'static str] = &["Str"];

    fn accept_str(
        &self,
        _structure: &Structure,
        context: &mut SerializationContext,
        val: &str,
    ) -> Result<usize> {
        use chrono::NaiveDateTime;

        context.buffers.u64[self.idx]
            .push(val.parse::<NaiveDateTime>()?.timestamp_millis().to_bytes());
        Ok(self.next)
    }
}

impl Instruction for PushDate64FromUtcStr {
    const NAME: &'static str = "PushDate64FromUtcStr";
    const EXPECTED: &'static [&'static str] = &["Str"];

    fn accept_str(
        &self,
        _structure: &Structure,
        context: &mut SerializationContext,
        val: &str,
    ) -> Result<usize> {
        use chrono::{DateTime, Utc};

        context.buffers.u64[self.idx]
            .push(val.parse::<DateTime<Utc>>()?.timestamp_millis().to_bytes());
        Ok(self.next)
    }
}

impl Instruction for PushDictionary {
    const NAME: &'static str = "PushDictionary";
    const EXPECTED: &'static [&'static str] = &["Str"];

    fn accept_str(
        &self,
        _structure: &Structure,
        context: &mut SerializationContext,
        val: &str,
    ) -> Result<usize> {
        use {DictionaryIndex as I, DictionaryValue as V};

        let idx = if context.dictionaries[self.dictionary].contains_key(val) {
            context.dictionaries[self.dictionary][val]
        } else {
            match self.values {
                V::Utf8 { buffer, offsets } => {
                    context.buffers.u8[buffer].extend(val.as_bytes().iter().copied());
                    context.buffers.u32_offsets[offsets].push(val.len())?;
                }
                V::LargeUtf8 { buffer, offsets } => {
                    context.buffers.u8[buffer].extend(val.as_bytes().iter().copied());
                    context.buffers.u64_offsets[offsets].push(val.len())?;
                }
            }

            let idx = context.dictionaries[self.dictionary].len();
            context.dictionaries[self.dictionary].insert(val.to_string(), idx);
            idx
        };

        match self.indices {
            I::U8(indices) => context.buffers.u8[indices].push(idx.try_into()?),
            I::U16(indices) => context.buffers.u16[indices].push(idx.try_into()?),
            I::U32(indices) => context.buffers.u32[indices].push(idx.try_into()?),
            I::U64(indices) => context.buffers.u64[indices].push(idx.try_into()?),
            I::I8(indices) => context.buffers.u8[indices].push(i8::try_from(idx)?.to_bytes()),
            I::I16(indices) => context.buffers.u16[indices].push(u16::try_from(idx)?.to_bytes()),
            I::I32(indices) => context.buffers.u32[indices].push(u32::try_from(idx)?.to_bytes()),
            I::I64(indices) => context.buffers.u64[indices].push(u64::try_from(idx)?.to_bytes()),
        }
        Ok(self.next)
    }
}

macro_rules! impl_primitive_instruction {
    (
        $(
            $name:ident($val_type:ty, $builder:ident) {
                $($func:ident($ty:ty),)*
            },
        )*
    ) => {
        $(
            impl Instruction for $name {
                const NAME: &'static str = stringify!($name);
                const EXPECTED: &'static [&'static str] = &[$(stringify!($ty)),*];

                $(
                    fn $func(&self, _structure: &Structure, context: &mut SerializationContext, val: $ty) -> Result<usize> {
                        let val = <$val_type>::try_from(val)?;
                        context.buffers.$builder[self.idx].push(ToBytes::to_bytes(val));
                        Ok(self.next)
                    }
                )*
            }
        )*
    };
}

impl_primitive_instruction!(
    PushU8(u8, u8) {
        accept_u8(u8),
        accept_u16(u16),
        accept_u32(u32),
        accept_u64(u64),
        accept_i8(i8),
        accept_i16(i16),
        accept_i32(i32),
        accept_i64(i64),
    },
    PushU16(u16, u16) {
        accept_u8(u8),
        accept_u16(u16),
        accept_u32(u32),
        accept_u64(u64),
        accept_i8(i8),
        accept_i16(i16),
        accept_i32(i32),
        accept_i64(i64),
    },
    PushU32(u32, u32) {
        accept_u8(u8),
        accept_u16(u16),
        accept_u32(u32),
        accept_u64(u64),
        accept_i8(i8),
        accept_i16(i16),
        accept_i32(i32),
        accept_i64(i64),
    },
    PushU64(u64, u64) {
        accept_u8(u8),
        accept_u16(u16),
        accept_u32(u32),
        accept_u64(u64),
        accept_i8(i8),
        accept_i16(i16),
        accept_i32(i32),
        accept_i64(i64),
    },
    PushI8(i8, u8) {
        accept_u8(u8),
        accept_u16(u16),
        accept_u32(u32),
        accept_u64(u64),
        accept_i8(i8),
        accept_i16(i16),
        accept_i32(i32),
        accept_i64(i64),
    },
    PushI16(i16, u16) {
        accept_u8(u8),
        accept_u16(u16),
        accept_u32(u32),
        accept_u64(u64),
        accept_i8(i8),
        accept_i16(i16),
        accept_i32(i32),
        accept_i64(i64),
    },
    PushI32(i32, u32) {
        accept_u8(u8),
        accept_u16(u16),
        accept_u32(u32),
        accept_u64(u64),
        accept_i8(i8),
        accept_i16(i16),
        accept_i32(i32),
        accept_i64(i64),
    },
    PushI64(i64, u64) {
        accept_u8(u8),
        accept_u16(u16),
        accept_u32(u32),
        accept_u64(u64),
        accept_i8(i8),
        accept_i16(i16),
        accept_i32(i32),
        accept_i64(i64),
    },
    PushF16(WrappedF16, u16) {
        accept_f32(f32),
        accept_f64(f64),
    },
    PushF32(WrappedF32, u32) {
        accept_f32(f32),
        accept_f64(f64),
    },
    PushF64(WrappedF64, u64) {
        accept_f32(f32),
        accept_f64(f64),
        accept_u8(u8),
        accept_u16(u16),
        accept_u32(u32),
        accept_u64(u64),
        accept_i8(i8),
        accept_i16(i16),
        accept_i32(i32),
        accept_i64(i64),
    },
);

impl Instruction for PushBool {
    const NAME: &'static str = "PushBool";
    const EXPECTED: &'static [&'static str] = &["Bool"];

    fn accept_bool(
        &self,
        _structure: &Structure,
        context: &mut SerializationContext,
        val: bool,
    ) -> Result<usize> {
        context.buffers.u1[self.idx].push(val);
        Ok(self.next)
    }
}

impl Instruction for PushDecimal128 {
    const NAME: &'static str = "PushDecimal128";
    const EXPECTED: &'static [&'static str] = &["F32", "F64", "Str"];

    fn accept_f32(
        &self,
        _: &Structure,
        context: &mut SerializationContext,
        val: f32,
    ) -> Result<usize> {
        let val = (val * self.f32_factor) as i128;
        context.buffers.u128[self.idx].push(ToBytes::to_bytes(val));
        Ok(self.next)
    }

    fn accept_f64(
        &self,
        _: &Structure,
        context: &mut SerializationContext,
        val: f64,
    ) -> Result<usize> {
        let val = (val * self.f64_factor) as i128;
        context.buffers.u128[self.idx].push(ToBytes::to_bytes(val));
        Ok(self.next)
    }

    fn accept_str(
        &self,
        _: &Structure,
        context: &mut SerializationContext,
        val: &str,
    ) -> Result<usize> {
        let mut buffer = [0; decimal::BUFFER_SIZE_I128];
        let val = self.parser.parse_decimal128(&mut buffer, val.as_bytes())?;
        context.buffers.u128[self.idx].push(ToBytes::to_bytes(val));
        Ok(self.next)
    }
}
