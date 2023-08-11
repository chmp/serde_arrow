use crate::internal::{
    conversions::ToBytes,
    error::{fail, Result},
    serialization::compiler::Structure,
};

use super::super::bytecode::{
    dispatch_bytecode, Bytecode, OptionMarker, Panic, ProgramEnd, UnionEnd, Variant,
};
use super::{Instruction, MutableBuffers};

impl Instruction for Panic {
    const NAME: &'static str = "Panic";
    const EXPECTED: &'static [&'static str] = &[];

    fn accept_start_sequence(&self, _: &Structure, _: &mut MutableBuffers) -> Result<usize> {
        fail!("{}", self.message);
    }

    fn accept_end_sequence(&self, _: &Structure, _: &mut MutableBuffers) -> Result<usize> {
        fail!("{}", self.message);
    }

    fn accept_start_tuple(&self, _: &Structure, _: &mut MutableBuffers) -> Result<usize> {
        fail!("{}", self.message);
    }

    fn accept_end_tuple(&self, _: &Structure, _: &mut MutableBuffers) -> Result<usize> {
        fail!("{}", self.message);
    }

    fn accept_start_struct(&self, _: &Structure, _: &mut MutableBuffers) -> Result<usize> {
        fail!("{}", self.message);
    }

    fn accept_end_struct(&self, _: &Structure, _: &mut MutableBuffers) -> Result<usize> {
        fail!("{}", self.message);
    }

    fn accept_start_map(&self, _: &Structure, _: &mut MutableBuffers) -> Result<usize> {
        fail!("{}", self.message);
    }

    fn accept_end_map(&self, _: &Structure, _: &mut MutableBuffers) -> Result<usize> {
        fail!("{}", self.message);
    }

    fn accept_item(&self, _: &Structure, _: &mut MutableBuffers) -> Result<usize> {
        fail!("{}", self.message);
    }

    fn accept_some(&self, _: &Structure, _: &mut MutableBuffers) -> Result<usize> {
        fail!("{}", self.message);
    }

    fn accept_default(&self, _: &Structure, _: &mut MutableBuffers) -> Result<usize> {
        fail!("{}", self.message);
    }

    fn accept_variant(
        &self,
        _: &Structure,
        _: &mut MutableBuffers,
        _: &str,
        _: usize,
    ) -> Result<usize> {
        fail!("{}", self.message);
    }

    fn accept_null(&self, _: &Structure, _: &mut MutableBuffers) -> Result<usize> {
        fail!("{}", self.message);
    }

    fn accept_bool(&self, _: &Structure, _: &mut MutableBuffers, _: bool) -> Result<usize> {
        fail!("{}", self.message);
    }

    fn accept_u8(&self, _: &Structure, _: &mut MutableBuffers, _: u8) -> Result<usize> {
        fail!("{}", self.message);
    }

    fn accept_u16(&self, _: &Structure, _: &mut MutableBuffers, _: u16) -> Result<usize> {
        fail!("{}", self.message);
    }

    fn accept_u32(&self, _: &Structure, _: &mut MutableBuffers, _: u32) -> Result<usize> {
        fail!("{}", self.message);
    }

    fn accept_u64(&self, _: &Structure, _: &mut MutableBuffers, _: u64) -> Result<usize> {
        fail!("{}", self.message);
    }

    fn accept_i8(&self, _: &Structure, _: &mut MutableBuffers, _: i8) -> Result<usize> {
        fail!("{}", self.message);
    }

    fn accept_i16(&self, _: &Structure, _: &mut MutableBuffers, _: i16) -> Result<usize> {
        fail!("{}", self.message);
    }

    fn accept_i32(&self, _: &Structure, _: &mut MutableBuffers, _: i32) -> Result<usize> {
        fail!("{}", self.message);
    }

    fn accept_i64(&self, _: &Structure, _: &mut MutableBuffers, _: i64) -> Result<usize> {
        fail!("{}", self.message);
    }

    fn accept_f32(&self, _: &Structure, _: &mut MutableBuffers, _: f32) -> Result<usize> {
        fail!("{}", self.message);
    }

    fn accept_f64(&self, _: &Structure, _: &mut MutableBuffers, _: f64) -> Result<usize> {
        fail!("{}", self.message);
    }

    fn accept_str(&self, _: &Structure, _: &mut MutableBuffers, _: &str) -> Result<usize> {
        fail!("{}", self.message);
    }
}

impl Instruction for ProgramEnd {
    const NAME: &'static str = "ProgramEnd";
    const EXPECTED: &'static [&'static str] = &[];
}

macro_rules! option_marker_handle {
    ($name:ident$(, $($val:ident: $ty:ty),*)?) => {
        fn $name(&self, structure: &Structure, buffers: &mut MutableBuffers $(, $($val: $ty),*)?) -> Result<usize> {
            buffers.u1[self.validity].push(true);
            dispatch_bytecode!(&structure.program[self.next], instr => instr.$name(structure, buffers $(, $($val),*)?))
        }
    };
}

/// Handle optionality markers (null / some)
///
/// The basic strategy is to keep this instruction active until any event but
/// `Some` is encountered. If a `Null `event is encountered store a missing
/// value and continue with the next field / item. If any other value is
/// encountered, call the next instruction inline.
///
impl Instruction for OptionMarker {
    const NAME: &'static str = "OptionMarker";
    const EXPECTED: &'static [&'static str] = &[
        "Some",
        "Null",
        "StartSequence",
        "EndSequence",
        "StartTuple",
        "EndTuple",
        "StartStruct",
        "EndStruct",
        "Item",
        "Default",
        "Variant",
        "Bool",
        "U8",
        "U16",
        "U32",
        "U64",
        "I8",
        "I16",
        "I32",
        "I64",
        "F32",
        "F64",
        "Str",
    ];

    fn accept_some(&self, _structure: &Structure, _buffers: &mut MutableBuffers) -> Result<usize> {
        Ok(self.self_pos)
    }

    fn accept_null(&self, structure: &Structure, buffers: &mut MutableBuffers) -> Result<usize> {
        apply_null(structure, buffers, self.null_definition)?;
        Ok(self.if_none)
    }

    option_marker_handle!(accept_start_sequence);
    option_marker_handle!(accept_end_sequence);
    option_marker_handle!(accept_start_tuple);
    option_marker_handle!(accept_end_tuple);
    option_marker_handle!(accept_start_struct);
    option_marker_handle!(accept_end_struct);
    option_marker_handle!(accept_start_map);
    option_marker_handle!(accept_end_map);
    option_marker_handle!(accept_item);
    option_marker_handle!(accept_default);
    option_marker_handle!(accept_variant, name: &str, idx: usize);
    option_marker_handle!(accept_bool, val: bool);
    option_marker_handle!(accept_u8, val: u8);
    option_marker_handle!(accept_u16, val: u16);
    option_marker_handle!(accept_u32, val: u32);
    option_marker_handle!(accept_u64, val: u64);
    option_marker_handle!(accept_i8, val: i8);
    option_marker_handle!(accept_i16, val: i16);
    option_marker_handle!(accept_i32, val: i32);
    option_marker_handle!(accept_i64, val: i64);
    option_marker_handle!(accept_f32, val: f32);
    option_marker_handle!(accept_f64, val: f64);
    option_marker_handle!(accept_str, val: &str);
}

impl Instruction for Variant {
    const NAME: &'static str = "Variant";
    const EXPECTED: &'static [&'static str] = &["Variant"];

    fn accept_variant(
        &self,
        structure: &Structure,
        buffers: &mut MutableBuffers,
        _name: &str,
        idx: usize,
    ) -> Result<usize> {
        if idx < structure.unions[self.union_idx].fields.len() {
            buffers.u8[self.type_idx].push(i8::try_from(idx)?.to_bytes());
            Ok(structure.unions[self.union_idx].fields[idx])
        } else {
            fail!(
                concat!(
                    "Serialization failed: an unknown variant with index {child_idx} for field was ",
                    "encountered. To fix this error, ensure all variants are seen during ",
                    "schema tracing or add the relevant variants manually to the traced fields.",
                ),
                child_idx = idx,
            )
        }
    }
}

impl Instruction for UnionEnd {
    const NAME: &'static str = "UnionEnd";
    const EXPECTED: &'static [&'static str] = &[];
}

macro_rules! apply_null {
    ($structure:expr, $buffers:expr, $null_definition:expr, $name:ident) => {
        for &idx in &$structure.nulls[$null_definition].$name {
            $buffers.$name[idx].push(Default::default());
        }
    };
}

pub fn apply_null(
    structure: &Structure,
    buffers: &mut MutableBuffers,
    null_definition: usize,
) -> Result<()> {
    apply_null!(structure, buffers, null_definition, u0);
    apply_null!(structure, buffers, null_definition, u1);
    apply_null!(structure, buffers, null_definition, u8);
    apply_null!(structure, buffers, null_definition, u16);
    apply_null!(structure, buffers, null_definition, u32);
    apply_null!(structure, buffers, null_definition, u64);

    for &idx in &structure.nulls[null_definition].u32_offsets {
        buffers.u32_offsets[idx].push_current_items();
    }
    for &idx in &structure.nulls[null_definition].u64_offsets {
        buffers.u64_offsets[idx].push_current_items();
    }

    Ok(())
}
