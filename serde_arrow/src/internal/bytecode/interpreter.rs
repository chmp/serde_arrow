use std::collections::HashSet;

use crate::internal::{
    bytecode::{
        buffers::{
            BitBuffer, NullBuffer, OffsetBuilder, PrimitiveBuffer, StringBuffer, StringDictonary,
        },
        compiler::{
            dispatch_bytecode, BufferCounts, Bytecode, DictionaryIndex, DictionaryValue,
            LargeListEnd, LargeListItem, LargeListStart, ListEnd, ListItem, ListStart, MapEnd,
            MapItem, MapStart, OptionMarker, OuterRecordEnd, OuterRecordField, OuterRecordStart,
            OuterSequenceEnd, OuterSequenceItem, OuterSequenceStart, Program, ProgramEnd, PushBool,
            PushDate64FromNaiveStr, PushDate64FromUtcStr, PushDictionary, PushF32, PushF64,
            PushI16, PushI32, PushI64, PushI8, PushLargeUtf8, PushNull, PushU16, PushU32, PushU64,
            PushU8, PushUtf8, StructEnd, StructField, StructItem, StructStart, Structure,
            TupleStructEnd, TupleStructItem, TupleStructStart, UnionEnd, Variant,
        },
    },
    conversions::ToBytes,
    error::{self, fail, Result},
    sink::macros,
    sink::EventSink,
};

use super::compiler::Panic;

pub struct Interpreter {
    pub program_counter: usize,
    pub structure: Structure,
    pub buffers: Buffers,
}

#[derive(Debug, Clone)]
pub struct Buffers {
    pub bool: Vec<BitBuffer>,
    pub null: Vec<NullBuffer>,
    pub u8: Vec<PrimitiveBuffer<u8>>,
    pub u16: Vec<PrimitiveBuffer<u16>>,
    pub u32: Vec<PrimitiveBuffer<u32>>,
    pub u64: Vec<PrimitiveBuffer<u64>>,
    pub utf8: Vec<StringBuffer<i32>>,
    pub large_utf8: Vec<StringBuffer<i64>>,
    pub validity: Vec<BitBuffer>,
    pub offset: Vec<OffsetBuilder<i32>>,
    pub large_offset: Vec<OffsetBuilder<i64>>,
    pub dictionaries: Vec<StringDictonary<i32>>,
    pub large_dictionaries: Vec<StringDictonary<i64>>,
    /// markers for which struct fields have been seen
    pub seen: Vec<HashSet<String>>,
}

impl Buffers {
    pub fn from_counts(counts: &BufferCounts) -> Self {
        Self {
            null: vec![Default::default(); counts.num_null],
            u8: vec![Default::default(); counts.num_u8],
            u16: vec![Default::default(); counts.num_u16],
            u32: vec![Default::default(); counts.num_u32],
            u64: vec![Default::default(); counts.num_u64],
            bool: vec![Default::default(); counts.num_bool],
            utf8: vec![Default::default(); counts.num_utf8],
            large_utf8: vec![Default::default(); counts.num_large_utf8],
            validity: vec![Default::default(); counts.num_validity],
            offset: vec![Default::default(); counts.num_offsets],
            large_offset: vec![Default::default(); counts.num_large_offsets],
            dictionaries: vec![Default::default(); counts.num_dictionaries],
            large_dictionaries: vec![Default::default(); counts.num_large_dictionaries],
            seen: vec![Default::default(); counts.num_seen],
        }
    }
}

impl Interpreter {
    pub fn new(program: Program) -> Self {
        Self {
            program_counter: 0,
            structure: program.structure,
            buffers: Buffers::from_counts(&program.buffers),
        }
    }
}

// TODO: use custom trait to improve error message
#[allow(unused_variables)]
trait Instruction: std::fmt::Debug {
    fn accept_start_sequence(&self, structure: &Structure, buffers: &mut Buffers) -> Result<usize> {
        fail!("{self:?} cannot accept StartSequence");
    }

    fn accept_end_sequence(&self, structure: &Structure, buffers: &mut Buffers) -> Result<usize> {
        fail!("{self:?} cannot accept EndSequence");
    }

    fn accept_start_tuple(&self, structure: &Structure, buffers: &mut Buffers) -> Result<usize> {
        fail!("{self:?} cannot accept StartTuple");
    }

    fn accept_end_tuple(&self, structure: &Structure, buffers: &mut Buffers) -> Result<usize> {
        fail!("{self:?} cannot accept EndTuple");
    }

    fn accept_start_struct(&self, structure: &Structure, buffers: &mut Buffers) -> Result<usize> {
        fail!("{self:?} cannot accept StartStructure");
    }

    fn accept_end_struct(&self, structure: &Structure, buffers: &mut Buffers) -> Result<usize> {
        fail!("{self:?} cannot accept EndStructure");
    }

    fn accept_start_map(&self, structure: &Structure, buffers: &mut Buffers) -> Result<usize> {
        fail!("{self:?} cannot accept StartMap");
    }

    fn accept_end_map(&self, structure: &Structure, buffers: &mut Buffers) -> Result<usize> {
        fail!("{self:?} cannot accept EndMap");
    }

    fn accept_item(&self, structure: &Structure, buffers: &mut Buffers) -> Result<usize> {
        fail!("{self:?} cannot accept Item");
    }

    fn accept_some(&self, structure: &Structure, buffers: &mut Buffers) -> Result<usize> {
        fail!("{self:?} cannot accept Some")
    }

    fn accept_default(&self, structure: &Structure, buffers: &mut Buffers) -> Result<usize> {
        fail!("{self:?} cannot accept Default")
    }

    fn accept_variant(
        &self,
        structure: &Structure,
        buffers: &mut Buffers,
        name: &str,
        idx: usize,
    ) -> Result<usize> {
        fail!("{self:?} cannot accept Variant({name:?}, {idx}")
    }

    fn accept_null(&self, structure: &Structure, buffers: &mut Buffers) -> Result<usize> {
        fail!("{self:?} cannot accept Null")
    }

    fn accept_bool(
        &self,
        structure: &Structure,
        buffers: &mut Buffers,
        val: bool,
    ) -> Result<usize> {
        fail!("{self:?} cannot accept Bool({val})");
    }

    fn accept_u8(&self, structure: &Structure, buffers: &mut Buffers, val: u8) -> Result<usize> {
        fail!("{self:?} cannot accept U8({val})");
    }

    fn accept_u16(&self, structure: &Structure, buffers: &mut Buffers, val: u16) -> Result<usize> {
        fail!("{self:?} cannot accept U16({val})");
    }

    fn accept_u32(&self, structure: &Structure, buffers: &mut Buffers, val: u32) -> Result<usize> {
        fail!("{self:?} cannot accept U32({val})");
    }

    fn accept_u64(&self, structure: &Structure, buffers: &mut Buffers, val: u64) -> Result<usize> {
        fail!("{self:?} cannot accept U64({val})");
    }

    fn accept_i8(&self, structure: &Structure, buffers: &mut Buffers, val: i8) -> Result<usize> {
        fail!("{self:?} cannot accept I8({val})");
    }

    fn accept_i16(&self, structure: &Structure, buffers: &mut Buffers, val: i16) -> Result<usize> {
        fail!("{self:?} cannot accept I16({val})");
    }

    fn accept_i32(&self, structure: &Structure, buffers: &mut Buffers, val: i32) -> Result<usize> {
        fail!("{self:?} cannot accept I32({val})");
    }

    fn accept_i64(&self, structure: &Structure, buffers: &mut Buffers, val: i64) -> Result<usize> {
        fail!("{self:?} cannot accept I64({val})");
    }

    fn accept_f32(&self, structure: &Structure, buffers: &mut Buffers, val: f32) -> Result<usize> {
        fail!("{self:?} cannot accept F32({val})");
    }

    fn accept_f64(&self, structure: &Structure, buffers: &mut Buffers, val: f64) -> Result<usize> {
        fail!("{self:?} cannot accept F64({val})");
    }

    fn accept_str(&self, structure: &Structure, buffers: &mut Buffers, val: &str) -> Result<usize> {
        fail!("{self:?} cannot accept Str({val:?})")
    }
}

impl Instruction for Panic {}

impl Instruction for MapEnd {
    fn accept_item(&self, structure: &Structure, buffers: &mut Buffers) -> Result<usize> {
        buffers.offset[self.offsets].inc_current_items()?;
        Ok(structure.maps[self.map_idx].key)
    }

    fn accept_end_map(&self, structure: &Structure, buffers: &mut Buffers) -> Result<usize> {
        buffers.offset[self.offsets].push_current_items();
        Ok(structure.maps[self.map_idx].r#return)
    }
}

impl Instruction for OuterSequenceStart {
    fn accept_start_sequence(
        &self,
        _structure: &Structure,
        _buffers: &mut Buffers,
    ) -> Result<usize> {
        Ok(self.next)
    }

    fn accept_start_tuple(&self, _structure: &Structure, _buffers: &mut Buffers) -> Result<usize> {
        Ok(self.next)
    }
}

impl Instruction for ProgramEnd {}

impl Instruction for OuterSequenceItem {
    fn accept_end_sequence(&self, structure: &Structure, _buffers: &mut Buffers) -> Result<usize> {
        Ok(structure.large_lists[self.list_idx].r#return)
    }

    fn accept_item(&self, _structure: &Structure, _buffers: &mut Buffers) -> Result<usize> {
        Ok(self.next)
    }

    fn accept_end_tuple(&self, structure: &Structure, _buffers: &mut Buffers) -> Result<usize> {
        Ok(structure.large_lists[self.list_idx].r#return)
    }
}
impl Instruction for OuterSequenceEnd {
    fn accept_end_sequence(&self, _structure: &Structure, _buffers: &mut Buffers) -> Result<usize> {
        Ok(self.next)
    }

    fn accept_item(&self, structure: &Structure, _buffers: &mut Buffers) -> Result<usize> {
        Ok(structure.large_lists[self.list_idx].item)
    }

    fn accept_end_tuple(&self, _structure: &Structure, _buffers: &mut Buffers) -> Result<usize> {
        Ok(self.next)
    }
}

impl Instruction for OuterRecordStart {
    fn accept_start_struct(&self, _structure: &Structure, _buffers: &mut Buffers) -> Result<usize> {
        Ok(self.next)
    }

    fn accept_start_map(&self, structure: &Structure, buffers: &mut Buffers) -> Result<usize> {
        self.accept_start_struct(structure, buffers)
    }
}
impl Instruction for OuterRecordField {
    fn accept_end_struct(&self, structure: &Structure, _buffers: &mut Buffers) -> Result<usize> {
        Ok(structure.structs[self.struct_idx].r#return)
    }

    fn accept_end_map(&self, structure: &Structure, buffers: &mut Buffers) -> Result<usize> {
        self.accept_end_struct(structure, buffers)
    }

    fn accept_str(
        &self,
        structure: &Structure,
        _buffers: &mut Buffers,
        val: &str,
    ) -> Result<usize> {
        if self.field_name == val {
            Ok(self.next)
        } else {
            let Some(&next) = structure.structs[self.struct_idx].fields.get(val) else {
                fail!("Cannot find field {val} in struct {idx}", idx=self.struct_idx);
            };
            Ok(next)
        }
    }
}

impl Instruction for OuterRecordEnd {
    fn accept_end_struct(&self, _structure: &Structure, _buffers: &mut Buffers) -> Result<usize> {
        Ok(self.next)
    }

    fn accept_end_map(&self, structure: &Structure, buffers: &mut Buffers) -> Result<usize> {
        self.accept_end_struct(structure, buffers)
    }

    fn accept_str(
        &self,
        structure: &Structure,
        _buffers: &mut Buffers,
        val: &str,
    ) -> Result<usize> {
        let Some(&next) = structure.structs[self.struct_idx].fields.get(val) else {
            fail!("cannot find field {val:?} in struct {idx}", idx=self.struct_idx);
        };
        Ok(next)
    }
}

impl Instruction for LargeListStart {
    fn accept_start_sequence(
        &self,
        _structure: &Structure,
        _buffers: &mut Buffers,
    ) -> Result<usize> {
        Ok(self.next)
    }

    fn accept_start_tuple(&self, _structure: &Structure, _buffers: &mut Buffers) -> Result<usize> {
        Ok(self.next)
    }
}

impl Instruction for LargeListItem {
    fn accept_end_sequence(&self, structure: &Structure, buffers: &mut Buffers) -> Result<usize> {
        buffers.large_offset[self.offsets].push_current_items();
        Ok(structure.large_lists[self.list_idx].r#return)
    }

    fn accept_item(&self, _structure: &Structure, buffers: &mut Buffers) -> Result<usize> {
        buffers.large_offset[self.offsets].inc_current_items()?;
        Ok(self.next)
    }

    fn accept_end_tuple(&self, structure: &Structure, buffers: &mut Buffers) -> Result<usize> {
        buffers.large_offset[self.offsets].push_current_items();
        Ok(structure.large_lists[self.list_idx].r#return)
    }
}

impl Instruction for LargeListEnd {
    fn accept_end_sequence(&self, _structure: &Structure, buffers: &mut Buffers) -> Result<usize> {
        buffers.large_offset[self.offsets].push_current_items();
        Ok(self.next)
    }

    fn accept_item(&self, structure: &Structure, buffers: &mut Buffers) -> Result<usize> {
        buffers.large_offset[self.offsets].inc_current_items()?;
        Ok(structure.large_lists[self.list_idx].item)
    }

    fn accept_end_tuple(&self, _structure: &Structure, buffers: &mut Buffers) -> Result<usize> {
        buffers.large_offset[self.offsets].push_current_items();
        Ok(self.next)
    }
}

impl Instruction for ListStart {
    fn accept_start_sequence(
        &self,
        _structure: &Structure,
        _buffers: &mut Buffers,
    ) -> Result<usize> {
        Ok(self.next)
    }

    fn accept_start_tuple(&self, _structure: &Structure, _buffers: &mut Buffers) -> Result<usize> {
        Ok(self.next)
    }
}

impl Instruction for ListItem {
    fn accept_end_sequence(&self, structure: &Structure, buffers: &mut Buffers) -> Result<usize> {
        buffers.offset[self.offsets].push_current_items();
        Ok(structure.lists[self.list_idx].r#return)
    }

    fn accept_item(&self, _structure: &Structure, buffers: &mut Buffers) -> Result<usize> {
        buffers.offset[self.offsets].inc_current_items()?;
        Ok(self.next)
    }

    fn accept_end_tuple(&self, structure: &Structure, buffers: &mut Buffers) -> Result<usize> {
        buffers.offset[self.offsets].push_current_items();
        Ok(structure.lists[self.list_idx].r#return)
    }
}

impl Instruction for ListEnd {
    fn accept_end_sequence(&self, _structure: &Structure, buffers: &mut Buffers) -> Result<usize> {
        buffers.offset[self.offsets].push_current_items();
        Ok(self.next)
    }

    fn accept_item(&self, structure: &Structure, buffers: &mut Buffers) -> Result<usize> {
        buffers.offset[self.offsets].inc_current_items()?;
        Ok(structure.lists[self.list_idx].item)
    }

    fn accept_end_tuple(&self, _structure: &Structure, buffers: &mut Buffers) -> Result<usize> {
        buffers.offset[self.offsets].push_current_items();
        Ok(self.next)
    }
}

fn struct_end(
    structure: &Structure,
    buffers: &mut Buffers,
    struct_idx: usize,
    seen: usize,
) -> Result<()> {
    for (name, validity) in &structure.structs[struct_idx].validities {
        if !buffers.seen[seen].contains(name) {
            let validity = validity
                .ok_or_else(|| error::error!("missing non-nullable field {name} in struct"))?;
            apply_null(structure, buffers, validity)?;
        }
    }
    buffers.seen[seen].clear();

    Ok(())
}

impl Instruction for StructStart {
    fn accept_start_struct(&self, _structure: &Structure, buffers: &mut Buffers) -> Result<usize> {
        buffers.seen[self.seen].clear();
        Ok(self.next)
    }

    fn accept_start_map(&self, structure: &Structure, buffers: &mut Buffers) -> Result<usize> {
        self.accept_start_struct(structure, buffers)
    }
}

impl Instruction for StructField {
    fn accept_end_struct(&self, structure: &Structure, buffers: &mut Buffers) -> Result<usize> {
        struct_end(structure, buffers, self.struct_idx, self.seen)?;
        Ok(structure.structs[self.struct_idx].r#return)
    }

    fn accept_end_map(&self, structure: &Structure, buffers: &mut Buffers) -> Result<usize> {
        self.accept_end_struct(structure, buffers)
    }

    fn accept_str(&self, structure: &Structure, buffers: &mut Buffers, val: &str) -> Result<usize> {
        if self.field_name == val {
            buffers.seen[self.seen].insert(val.to_owned());
            Ok(self.next)
        } else {
            let Some(&next) = structure.structs[self.struct_idx].fields.get(val) else {
                fail!("Cannot find field {val} in struct {idx}", idx=self.struct_idx);
            };
            buffers.seen[self.seen].insert(val.to_owned());
            Ok(next)
        }
    }
}

impl Instruction for StructEnd {
    fn accept_end_struct(&self, structure: &Structure, buffers: &mut Buffers) -> Result<usize> {
        struct_end(structure, buffers, self.struct_idx, self.seen)?;
        Ok(self.next)
    }

    fn accept_end_map(&self, structure: &Structure, buffers: &mut Buffers) -> Result<usize> {
        self.accept_end_struct(structure, buffers)
    }

    fn accept_str(&self, structure: &Structure, buffers: &mut Buffers, val: &str) -> Result<usize> {
        let Some(&next) = structure.structs[self.struct_idx].fields.get(val) else {
            fail!("cannot find field {val:?} in struct {idx}", idx=self.struct_idx);
        };
        buffers.seen[self.seen].insert(val.to_owned());
        Ok(next)
    }

    // relevant for maps serialized as structs
    fn accept_item(&self, structure: &Structure, _buffers: &mut Buffers) -> Result<usize> {
        Ok(structure.structs[self.struct_idx].item)
    }
}

impl Instruction for StructItem {
    fn accept_item(&self, _structure: &Structure, _buffers: &mut Buffers) -> Result<usize> {
        Ok(self.next)
    }

    fn accept_end_map(&self, structure: &Structure, buffers: &mut Buffers) -> Result<usize> {
        struct_end(structure, buffers, self.struct_idx, self.seen)?;
        Ok(structure.structs[self.struct_idx].r#return)
    }
}

impl Instruction for MapStart {
    fn accept_start_map(&self, _structure: &Structure, _buffers: &mut Buffers) -> Result<usize> {
        Ok(self.next)
    }
}

impl Instruction for MapItem {
    fn accept_item(&self, _structure: &Structure, buffers: &mut Buffers) -> Result<usize> {
        buffers.offset[self.offsets].inc_current_items()?;
        Ok(self.next)
    }

    fn accept_end_map(&self, structure: &Structure, buffers: &mut Buffers) -> Result<usize> {
        buffers.offset[self.offsets].push_current_items();
        Ok(structure.maps[self.map_idx].r#return)
    }
}

impl Instruction for TupleStructStart {
    fn accept_start_tuple(&self, _structure: &Structure, _buffers: &mut Buffers) -> Result<usize> {
        Ok(self.next)
    }
}

impl Instruction for TupleStructItem {
    fn accept_item(&self, _structure: &Structure, _buffers: &mut Buffers) -> Result<usize> {
        Ok(self.next)
    }
}

impl Instruction for TupleStructEnd {
    fn accept_end_tuple(&self, _structure: &Structure, _buffers: &mut Buffers) -> Result<usize> {
        Ok(self.next)
    }
}

macro_rules! option_marker_handle {
    ($name:ident$(, $($val:ident: $ty:ty),*)?) => {
        fn $name(&self, structure: &Structure, buffers: &mut Buffers $(, $($val: $ty),*)?) -> Result<usize> {
            buffers.validity[self.validity].push(true)?;
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
    fn accept_some(&self, _structure: &Structure, _buffers: &mut Buffers) -> Result<usize> {
        Ok(self.self_pos)
    }

    fn accept_null(&self, structure: &Structure, buffers: &mut Buffers) -> Result<usize> {
        apply_null(structure, buffers, self.validity)?;
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
    fn accept_variant(
        &self,
        structure: &Structure,
        buffers: &mut Buffers,
        _name: &str,
        idx: usize,
    ) -> Result<usize> {
        if idx < structure.unions[self.union_idx].fields.len() {
            buffers.u8[self.type_idx].push(i8::try_from(idx)?.to_bytes())?;
            Ok(structure.unions[self.union_idx].fields[idx])
        } else {
            fail!(
                concat!(
                    "Serialization failed: an unknown variant with index {child_idx} for field was ",
                    "encountered. To fix this error, sure all variants are seen during ",
                    "schema tracing or add the relevant variants manually to the traced fields.",
                ),
                child_idx = idx,
            )
        }
    }
}

impl Instruction for PushUtf8 {
    fn accept_str(
        &self,
        _structure: &Structure,
        buffers: &mut Buffers,
        val: &str,
    ) -> Result<usize> {
        buffers.utf8[self.idx].push(val)?;
        Ok(self.next)
    }
}

impl Instruction for PushLargeUtf8 {
    fn accept_str(
        &self,
        _structure: &Structure,
        buffers: &mut Buffers,
        val: &str,
    ) -> Result<usize> {
        buffers.large_utf8[self.idx].push(val)?;
        Ok(self.next)
    }
}

impl Instruction for PushDate64FromNaiveStr {
    fn accept_str(
        &self,
        _structure: &Structure,
        buffers: &mut Buffers,
        val: &str,
    ) -> Result<usize> {
        use chrono::NaiveDateTime;

        buffers.u64[self.idx].push(val.parse::<NaiveDateTime>()?.timestamp_millis().to_bytes())?;
        Ok(self.next)
    }
}

impl Instruction for PushDate64FromUtcStr {
    fn accept_str(
        &self,
        _structure: &Structure,
        buffers: &mut Buffers,
        val: &str,
    ) -> Result<usize> {
        use chrono::{DateTime, Utc};

        buffers.u64[self.idx].push(val.parse::<DateTime<Utc>>()?.timestamp_millis().to_bytes())?;
        Ok(self.next)
    }
}

impl Instruction for PushDictionary {
    fn accept_str(
        &self,
        _structure: &Structure,
        buffers: &mut Buffers,
        val: &str,
    ) -> Result<usize> {
        use {DictionaryIndex as I, DictionaryValue as V};
        let idx = match self.values {
            V::Utf8(dict) => buffers.dictionaries[dict].push(val)?,
            V::LargeUtf8(dict) => buffers.large_dictionaries[dict].push(val)?,
        };
        match self.indices {
            I::U8(indices) => buffers.u8[indices].push(idx.try_into()?)?,
            I::U16(indices) => buffers.u16[indices].push(idx.try_into()?)?,
            I::U32(indices) => buffers.u32[indices].push(idx.try_into()?)?,
            I::U64(indices) => buffers.u64[indices].push(idx.try_into()?)?,
            I::I8(indices) => buffers.u8[indices].push(i8::try_from(idx)?.to_bytes())?,
            I::I16(indices) => buffers.u16[indices].push(u16::try_from(idx)?.to_bytes())?,
            I::I32(indices) => buffers.u32[indices].push(u32::try_from(idx)?.to_bytes())?,
            I::I64(indices) => buffers.u64[indices].push(u64::try_from(idx)?.to_bytes())?,
        }
        Ok(self.next)
    }
}

impl Instruction for UnionEnd {}

impl Instruction for PushNull {
    fn accept_null(&self, _structure: &Structure, buffers: &mut Buffers) -> Result<usize> {
        buffers.null[self.idx].push(())?;
        Ok(self.next)
    }
}

macro_rules! impl_primitive_instruction {
    (
        $name:ident($val_type:ty, $builder:ident) {
            $($func:ident($ty:ty),)*
        }
    ) => {
        impl Instruction for $name {
            $(
                fn $func(&self, _structure: &Structure, buffers: &mut Buffers, val: $ty) -> Result<usize> {
                    let val = <$val_type>::try_from(val)?;
                    buffers.$builder[self.idx].push(ToBytes::to_bytes(val))?;
                    Ok(self.next)
                }
            )*
        }
    };
}

impl_primitive_instruction!(PushU8(u8, u8) {
    accept_u8(u8),
    accept_u16(u16),
    accept_u32(u32),
    accept_u64(u64),
    accept_i8(i8),
    accept_i16(i16),
    accept_i32(i32),
    accept_i64(i64),
});

impl_primitive_instruction!(PushU16(u16, u16) {
    accept_u8(u8),
    accept_u16(u16),
    accept_u32(u32),
    accept_u64(u64),
    accept_i8(i8),
    accept_i16(i16),
    accept_i32(i32),
    accept_i64(i64),
});

impl_primitive_instruction!(PushU32(u32, u32) {
    accept_u8(u8),
    accept_u16(u16),
    accept_u32(u32),
    accept_u64(u64),
    accept_i8(i8),
    accept_i16(i16),
    accept_i32(i32),
    accept_i64(i64),
});

impl_primitive_instruction!(PushU64(u64, u64) {
    accept_u8(u8),
    accept_u16(u16),
    accept_u32(u32),
    accept_u64(u64),
    accept_i8(i8),
    accept_i16(i16),
    accept_i32(i32),
    accept_i64(i64),
});

impl_primitive_instruction!(PushI8(i8, u8) {
    accept_u8(u8),
    accept_u16(u16),
    accept_u32(u32),
    accept_u64(u64),
    accept_i8(i8),
    accept_i16(i16),
    accept_i32(i32),
    accept_i64(i64),
});

impl_primitive_instruction!(PushI16(i16, u16) {
    accept_u8(u8),
    accept_u16(u16),
    accept_u32(u32),
    accept_u64(u64),
    accept_i8(i8),
    accept_i16(i16),
    accept_i32(i32),
    accept_i64(i64),
});

impl_primitive_instruction!(PushI32(i32, u32) {
    accept_u8(u8),
    accept_u16(u16),
    accept_u32(u32),
    accept_u64(u64),
    accept_i8(i8),
    accept_i16(i16),
    accept_i32(i32),
    accept_i64(i64),
});

impl_primitive_instruction!(PushI64(i64, u64) {
    accept_u8(u8),
    accept_u16(u16),
    accept_u32(u32),
    accept_u64(u64),
    accept_i8(i8),
    accept_i16(i16),
    accept_i32(i32),
    accept_i64(i64),
});

impl_primitive_instruction!(PushF32(f32, u32) {
    accept_f32(f32),
});

impl_primitive_instruction!(PushF64(f64, u64) {
    accept_f64(f64),
});

impl Instruction for PushBool {
    fn accept_bool(
        &self,
        _structure: &Structure,
        buffers: &mut Buffers,
        val: bool,
    ) -> Result<usize> {
        buffers.bool[self.idx].push(val)?;
        Ok(self.next)
    }
}

macro_rules! dispatch_instruction {
    ($this:expr, $method:ident) => {
        {
            $this.program_counter = dispatch_bytecode!(
                &$this.structure.program[$this.program_counter],
                instr => instr.$method(&$this.structure, &mut $this.buffers)?
            );
            Ok(())
        }
    };
    ($this:expr, $method:ident, $($val:expr),*) => {
        {
            $this.program_counter = dispatch_bytecode!(
                &$this.structure.program[$this.program_counter],
                instr => instr.$method(&$this.structure, &mut $this.buffers, $($val),*)?
            );
            Ok(())
        }
    };
}

#[allow(clippy::match_single_binding)]
impl EventSink for Interpreter {
    macros::forward_generic_to_specialized!();

    fn accept_bool(&mut self, val: bool) -> Result<()> {
        dispatch_instruction!(self, accept_bool, val)
    }

    fn accept_u8(&mut self, val: u8) -> Result<()> {
        dispatch_instruction!(self, accept_u8, val)
    }

    fn accept_u16(&mut self, val: u16) -> Result<()> {
        dispatch_instruction!(self, accept_u16, val)
    }

    fn accept_u32(&mut self, val: u32) -> Result<()> {
        dispatch_instruction!(self, accept_u32, val)
    }

    fn accept_u64(&mut self, val: u64) -> Result<()> {
        dispatch_instruction!(self, accept_u64, val)
    }

    fn accept_i8(&mut self, val: i8) -> Result<()> {
        dispatch_instruction!(self, accept_i8, val)
    }

    fn accept_i16(&mut self, val: i16) -> Result<()> {
        dispatch_instruction!(self, accept_i16, val)
    }

    fn accept_i32(&mut self, val: i32) -> Result<()> {
        dispatch_instruction!(self, accept_i32, val)
    }

    fn accept_i64(&mut self, val: i64) -> Result<()> {
        dispatch_instruction!(self, accept_i64, val)
    }

    fn accept_f32(&mut self, val: f32) -> Result<()> {
        dispatch_instruction!(self, accept_f32, val)
    }

    fn accept_f64(&mut self, val: f64) -> Result<()> {
        dispatch_instruction!(self, accept_f64, val)
    }

    fn accept_start_sequence(&mut self) -> crate::Result<()> {
        dispatch_instruction!(self, accept_start_sequence)
    }

    fn accept_end_sequence(&mut self) -> crate::Result<()> {
        dispatch_instruction!(self, accept_end_sequence)
    }

    fn accept_start_struct(&mut self) -> crate::Result<()> {
        dispatch_instruction!(self, accept_start_struct)
    }

    fn accept_end_struct(&mut self) -> crate::Result<()> {
        dispatch_instruction!(self, accept_end_struct)
    }

    fn accept_item(&mut self) -> Result<()> {
        dispatch_instruction!(self, accept_item)
    }

    fn accept_start_tuple(&mut self) -> Result<()> {
        dispatch_instruction!(self, accept_start_tuple)
    }

    fn accept_end_tuple(&mut self) -> Result<()> {
        dispatch_instruction!(self, accept_end_tuple)
    }

    fn accept_start_map(&mut self) -> Result<()> {
        dispatch_instruction!(self, accept_start_map)
    }

    fn accept_end_map(&mut self) -> Result<()> {
        dispatch_instruction!(self, accept_end_map)
    }

    fn accept_some(&mut self) -> Result<()> {
        dispatch_instruction!(self, accept_some)
    }

    fn accept_null(&mut self) -> Result<()> {
        dispatch_instruction!(self, accept_null)
    }
    fn accept_default(&mut self) -> Result<()> {
        dispatch_instruction!(self, accept_default)
    }

    fn accept_str(&mut self, val: &str) -> Result<()> {
        dispatch_instruction!(self, accept_str, val)
    }

    fn accept_variant(&mut self, name: &str, idx: usize) -> Result<()> {
        dispatch_instruction!(self, accept_variant, name, idx)
    }

    fn finish(&mut self) -> Result<()> {
        Ok(())
    }
}

macro_rules! apply_null {
    ($structure:expr, $buffers:expr, $validity:expr, $name:ident) => {
        for &idx in &$structure.nulls[$validity].$name {
            $buffers.$name[idx].push(Default::default())?;
        }
    };
}

fn apply_null(structure: &Structure, buffers: &mut Buffers, validity: usize) -> Result<()> {
    apply_null!(structure, buffers, validity, null);
    apply_null!(structure, buffers, validity, bool);
    apply_null!(structure, buffers, validity, u8);
    apply_null!(structure, buffers, validity, u16);
    apply_null!(structure, buffers, validity, u32);
    apply_null!(structure, buffers, validity, u64);
    apply_null!(structure, buffers, validity, utf8);
    apply_null!(structure, buffers, validity, large_utf8);
    apply_null!(structure, buffers, validity, validity);

    for &idx in &structure.nulls[validity].large_offsets {
        buffers.large_offset[idx].push_current_items();
    }

    Ok(())
}
