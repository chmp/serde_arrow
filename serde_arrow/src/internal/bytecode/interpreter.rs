use super::{
    buffers::{
        BitBuffer, NullBuffer, OffsetBuilder, PrimitiveBuffer, StringBuffer, StringDictonary,
    },
    compiler::{
        BufferCounts, Bytecode, DictionaryIndices, DictionaryValue, LargeListEnd, LargeListItem,
        LargeListStart, MapItem, MapStart, OptionMarker, OuterRecordEnd, OuterRecordField,
        OuterRecordStart, OuterSequenceEnd, OuterSequenceItem, OuterSequenceStart, Program,
        ProgramEnd, StructEnd, StructField, StructItem, StructStart, Structure, TupleStructEnd,
        TupleStructItem, TupleStructStart, Variant,
    },
};

use crate::{
    _impl::bytecode::compiler::{dispatch_bytecode, MapEnd},
    internal::{
        error::{fail, Result},
        event::Event,
        sink::macros,
        sink::EventSink,
    },
};

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
    pub i8: Vec<PrimitiveBuffer<i8>>,
    pub i16: Vec<PrimitiveBuffer<i16>>,
    pub i32: Vec<PrimitiveBuffer<i32>>,
    pub i64: Vec<PrimitiveBuffer<i64>>,
    pub f32: Vec<PrimitiveBuffer<f32>>,
    pub f64: Vec<PrimitiveBuffer<f64>>,
    pub utf8: Vec<StringBuffer<i32>>,
    pub large_utf8: Vec<StringBuffer<i64>>,
    pub validity: Vec<BitBuffer>,
    pub offset: Vec<OffsetBuilder<i32>>,
    pub large_offset: Vec<OffsetBuilder<i64>>,
    pub dictionaries: Vec<StringDictonary<i32>>,
    pub large_dictionaries: Vec<StringDictonary<i64>>,
}

impl Buffers {
    pub fn from_counts(counts: &BufferCounts) -> Self {
        Self {
            null: vec![Default::default(); counts.num_null],
            u8: vec![Default::default(); counts.num_u8],
            u16: vec![Default::default(); counts.num_u16],
            u32: vec![Default::default(); counts.num_u32],
            u64: vec![Default::default(); counts.num_u64],
            i8: vec![Default::default(); counts.num_i8],
            i16: vec![Default::default(); counts.num_i16],
            i32: vec![Default::default(); counts.num_i32],
            i64: vec![Default::default(); counts.num_i64],
            f32: vec![Default::default(); counts.num_f32],
            f64: vec![Default::default(); counts.num_f64],
            bool: vec![Default::default(); counts.num_bool],
            utf8: vec![Default::default(); counts.num_utf8],
            large_utf8: vec![Default::default(); counts.num_large_utf8],
            validity: vec![Default::default(); counts.num_validity],
            offset: vec![Default::default(); counts.num_offsets],
            large_offset: vec![Default::default(); counts.num_large_offsets],
            dictionaries: vec![Default::default(); counts.num_dictionaries],
            large_dictionaries: vec![Default::default(); counts.num_large_dictionaries],
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

    fn accept_str(&self, structure: &Structure, buffers: &mut Buffers, val: &str) -> Result<usize> {
        fail!("{self:?} cannot accept Str({val:?})")
    }
}

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

impl Instruction for StructStart {
    fn accept_start_struct(&self, _structure: &Structure, _buffers: &mut Buffers) -> Result<usize> {
        Ok(self.next)
    }

    fn accept_start_map(&self, structure: &Structure, buffers: &mut Buffers) -> Result<usize> {
        self.accept_start_struct(structure, buffers)
    }
}

impl Instruction for StructField {
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

impl Instruction for StructEnd {
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

impl Instruction for StructItem {
    fn accept_item(&self, _structure: &Structure, _buffers: &mut Buffers) -> Result<usize> {
        Ok(self.next)
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
impl Instruction for OptionMarker {}

impl Instruction for Variant {}

macro_rules! dispatch_instruction {
    ($this:expr, $method:ident) => {
        {
            $this.program_counter = dispatch_bytecode!(
                &$this.structure.program[$this.program_counter].1,
                instr => instr.$method(&$this.structure, &mut $this.buffers)?,
                instr => fail!("Cannot accept EndSequence in {instr:?}"),
            );
            Ok(())
        }
    };
}

macro_rules! accept_primitive {
    ($func:ident, $ty:ty, $(($builder:ident, $variant:ident),)*) => {
        fn $func(&mut self, val: $ty) -> crate::Result<()> {
            match &self.structure.program[self.program_counter] {
                $(
                    &(next, Bytecode::$variant(array_idx)) => {
                        self.buffers.$builder[array_idx].push(val.try_into()?)?;
                        self.program_counter = next;
                    }
                )*
                instr => fail!("Cannot accept {} in {instr:?}", stringify!($ty)),
            }
            Ok(())
        }
    };
}

#[allow(clippy::match_single_binding)]
impl EventSink for Interpreter {
    macros::forward_generic_to_specialized!();

    accept_primitive!(
        accept_u8,
        u8,
        (u8, PushU8),
        (u16, PushU16),
        (u32, PushU32),
        (u64, PushU64),
        (i8, PushI8),
        (i16, PushI16),
        (i32, PushI32),
        (i64, PushI64),
    );
    accept_primitive!(
        accept_u16,
        u16,
        (u8, PushU8),
        (u16, PushU16),
        (u32, PushU32),
        (u64, PushU64),
        (i8, PushI8),
        (i16, PushI16),
        (i32, PushI32),
        (i64, PushI64),
    );
    accept_primitive!(
        accept_u32,
        u32,
        (u8, PushU8),
        (u16, PushU16),
        (u32, PushU32),
        (u64, PushU64),
        (i8, PushI8),
        (i16, PushI16),
        (i32, PushI32),
        (i64, PushI64),
    );
    accept_primitive!(
        accept_u64,
        u64,
        (u8, PushU8),
        (u16, PushU16),
        (u32, PushU32),
        (u64, PushU64),
        (i8, PushI8),
        (i16, PushI16),
        (i32, PushI32),
        (i64, PushI64),
    );
    accept_primitive!(
        accept_i8,
        i8,
        (u8, PushU8),
        (u16, PushU16),
        (u32, PushU32),
        (u64, PushU64),
        (i8, PushI8),
        (i16, PushI16),
        (i32, PushI32),
        (i64, PushI64),
    );
    accept_primitive!(
        accept_i16,
        i16,
        (u8, PushU8),
        (u16, PushU16),
        (u32, PushU32),
        (u64, PushU64),
        (i8, PushI8),
        (i16, PushI16),
        (i32, PushI32),
        (i64, PushI64),
    );
    accept_primitive!(
        accept_i32,
        i32,
        (u8, PushU8),
        (u16, PushU16),
        (u32, PushU32),
        (u64, PushU64),
        (i8, PushI8),
        (i16, PushI16),
        (i32, PushI32),
        (i64, PushI64),
    );
    accept_primitive!(
        accept_i64,
        i64,
        (u8, PushU8),
        (u16, PushU16),
        (u32, PushU32),
        (u64, PushU64),
        (i8, PushI8),
        (i16, PushI16),
        (i32, PushI32),
        (i64, PushI64),
    );
    accept_primitive!(accept_f32, f32, (f32, PushF32),);
    accept_primitive!(accept_f64, f64, (f64, PushF64),);
    accept_primitive!(accept_bool, bool, (bool, PushBool),);

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
        use Bytecode as B;
        self.program_counter = match &self.structure.program[self.program_counter] {
            (_, B::Option(instr)) => {
                self.buffers.validity[instr.validity].push(true)?;
                instr.next
            }
            // Todo: handle EndMap
            instr => fail!("Cannot accept Some in {instr:?}"),
        };
        Ok(())
    }

    fn accept_null(&mut self) -> Result<()> {
        use Bytecode as B;
        self.program_counter = match &self.structure.program[self.program_counter] {
            (_, B::Option(instr)) => {
                apply_null(&self.structure, &mut self.buffers, instr.validity)?;
                instr.if_none
            }
            &(next, B::PushNull(idx)) => {
                self.buffers.null[idx].push(())?;
                next
            }
            // Todo: handle EndMap
            instr => fail!("Cannot accept Null in {instr:?}"),
        };
        Ok(())
    }
    fn accept_default(&mut self) -> Result<()> {
        match &self.structure.program[self.program_counter] {
            instr => fail!("Cannot accept Default in {instr:?}"),
        }
    }

    fn accept_str(&mut self, val: &str) -> Result<()> {
        use Bytecode as B;
        self.program_counter = match &self.structure.program[self.program_counter] {
            (_, B::OuterRecordEnd(instr)) => {
                instr.accept_str(&self.structure, &mut self.buffers, val)?
            }
            (_, B::StructEnd(instr)) => {
                instr.accept_str(&self.structure, &mut self.buffers, val)?
            }
            (_, B::StructField(instr)) => {
                instr.accept_str(&self.structure, &mut self.buffers, val)?
            }
            (_, B::OuterRecordField(instr)) => {
                instr.accept_str(&self.structure, &mut self.buffers, val)?
            }
            &(next, B::PushUTF8(idx)) => {
                self.buffers.utf8[idx].push(val)?;
                next
            }
            &(next, B::PushLargeUTF8(idx)) => {
                self.buffers.large_utf8[idx].push(val)?;
                next
            }
            &(next, B::PushDate64FromNaiveStr(idx)) => {
                use chrono::NaiveDateTime;

                self.buffers.i64[idx].push(val.parse::<NaiveDateTime>()?.timestamp_millis())?;
                next
            }
            &(next, B::PushDate64FromUtcStr(idx)) => {
                use chrono::{DateTime, Utc};

                self.buffers.i64[idx].push(val.parse::<DateTime<Utc>>()?.timestamp_millis())?;
                next
            }
            &(next, B::PushDictionary(dictionary, indices)) => {
                use {DictionaryIndices as I, DictionaryValue as V};
                let idx = match dictionary {
                    V::Utf8(dict) => self.buffers.dictionaries[dict].push(val)?,
                    V::LargeUtf8(dict) => self.buffers.large_dictionaries[dict].push(val)?,
                };
                match indices {
                    I::U8(indices) => self.buffers.u8[indices].push(idx.try_into()?)?,
                    I::U16(indices) => self.buffers.u16[indices].push(idx.try_into()?)?,
                    I::U32(indices) => self.buffers.u32[indices].push(idx.try_into()?)?,
                    I::U64(indices) => self.buffers.u64[indices].push(idx.try_into()?)?,
                    I::I8(indices) => self.buffers.i8[indices].push(idx.try_into()?)?,
                    I::I16(indices) => self.buffers.i16[indices].push(idx.try_into()?)?,
                    I::I32(indices) => self.buffers.i32[indices].push(idx.try_into()?)?,
                    I::I64(indices) => self.buffers.i64[indices].push(idx.try_into()?)?,
                }
                next
            }
            instr => fail!("Cannot accept {ev} in {instr:?}", ev = Event::Str(val)),
        };
        Ok(())
    }

    fn accept_variant(&mut self, _name: &str, idx: usize) -> Result<()> {
        use Bytecode as B;
        self.program_counter = match &self.structure.program[self.program_counter] {
            (_, B::Variant(instr)) => {
                self.buffers.i8[instr.type_idx].push(idx.try_into()?)?;
                self.structure.unions[instr.union_idx].fields[idx]
            }
            // TODO: improve error message
            instr => fail!("Cannot accept Variant in {instr:?}"),
        };
        Ok(())
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
    apply_null!(structure, buffers, validity, i8);
    apply_null!(structure, buffers, validity, i16);
    apply_null!(structure, buffers, validity, i32);
    apply_null!(structure, buffers, validity, i64);
    apply_null!(structure, buffers, validity, f32);
    apply_null!(structure, buffers, validity, f64);
    apply_null!(structure, buffers, validity, utf8);
    apply_null!(structure, buffers, validity, large_utf8);
    apply_null!(structure, buffers, validity, validity);

    for &idx in &structure.nulls[validity].large_offsets {
        buffers.large_offset[idx].push_current_items();
    }

    Ok(())
}

#[cfg(test)]
mod test {
    use serde::Serialize;

    use crate::{
        _impl::bytecode::{compile_serialization, compiler::CompilationOptions, Interpreter},
        base::serialize_into_sink,
        internal::serialize_into_fields,
    };

    fn serialize_simple<T: Serialize + ?Sized>(items: &T) {
        let fields = serialize_into_fields(items, Default::default()).unwrap();
        let program = compile_serialization(&fields, CompilationOptions::default()).unwrap();
        let mut interpreter = Interpreter::new(program);

        serialize_into_sink(&mut interpreter, items).unwrap();
    }

    #[test]
    fn empty() {
        #[derive(Serialize)]
        struct Item {}

        serialize_simple(&[Item {}]);
    }

    #[test]
    fn primitives() {
        #[derive(Serialize)]
        struct Item {
            a: u64,
            b: f32,
        }

        serialize_simple(&[Item { a: 0, b: 21.0 }, Item { a: 10, b: 42.0 }]);
    }

    #[test]
    fn nested() {
        #[derive(Serialize)]
        struct Item {
            a: u64,
            b: f32,
            c: Child,
        }

        #[derive(Serialize)]
        struct Child {
            first: String,
            second: bool,
        }

        serialize_simple(&[
            Item {
                a: 0,
                b: 21.0,
                c: Child {
                    first: "foo".into(),
                    second: true,
                },
            },
            Item {
                a: 10,
                b: 42.0,
                c: Child {
                    first: "bar".into(),
                    second: false,
                },
            },
        ]);
    }
}
