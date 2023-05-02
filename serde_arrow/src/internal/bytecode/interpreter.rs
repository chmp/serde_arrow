use super::{
    buffers::{
        BitBuffer, NullBuffer, OffsetBuilder, PrimitiveBuffer, StringBuffer, StringDictonary,
    },
    compiler::{
        ArrayMapping, Bytecode, ListDefinition, NullDefinition, Program, StructDefinition,
        UnionDefinition,
    },
};

use crate::internal::{
    error::{fail, Result},
    sink::macros,
    sink::EventSink,
};

pub struct Interpreter {
    pub program_counter: usize,
    pub program: Vec<(usize, Bytecode)>,
    pub structs: Vec<StructDefinition>,
    pub lists: Vec<ListDefinition>,
    pub unions: Vec<UnionDefinition>,
    pub nulls: Vec<NullDefinition>,
    pub array_mapping: Vec<ArrayMapping>,
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
    pub large_dictionaries: Vec<StringDictonary<i64>>,
}

impl Interpreter {
    pub fn new(program: Program) -> Self {
        let mut instructions = Vec::with_capacity(program.program.len());
        for (pos, instr) in program.program.into_iter().enumerate() {
            let dst = program
                .next_instruction
                .get(&pos)
                .copied()
                .unwrap_or(pos + 1);
            instructions.push((dst, instr));
        }

        Self {
            program: instructions,
            structs: program.structs,
            lists: program.large_lists,
            unions: program.unions,
            nulls: program.nulls,
            array_mapping: program.array_mapping,
            program_counter: 0,
            buffers: Buffers {
                null: vec![Default::default(); program.num_null],
                u8: vec![Default::default(); program.num_u8],
                u16: vec![Default::default(); program.num_u16],
                u32: vec![Default::default(); program.num_u32],
                u64: vec![Default::default(); program.num_u64],
                i8: vec![Default::default(); program.num_i8],
                i16: vec![Default::default(); program.num_i16],
                i32: vec![Default::default(); program.num_i32],
                i64: vec![Default::default(); program.num_i64],
                f32: vec![Default::default(); program.num_f32],
                f64: vec![Default::default(); program.num_f64],
                bool: vec![Default::default(); program.num_bool],
                utf8: vec![Default::default(); program.num_utf8],
                large_utf8: vec![Default::default(); program.num_large_utf8],
                validity: vec![Default::default(); program.num_validity],
                offset: vec![Default::default(); program.num_offsets],
                large_offset: vec![Default::default(); program.num_large_offsets],
                large_dictionaries: vec![Default::default(); program.num_large_dictionaries],
            },
        }
    }
}

macro_rules! accept_primitive {
    ($func:ident, $variant:ident, $builder:ident, $ty:ty) => {
        fn $func(&mut self, val: $ty) -> crate::Result<()> {
            match &self.program[self.program_counter] {
                &(next, Bytecode::$variant(array_idx)) => {
                    self.buffers.$builder[array_idx].push(val)?;
                    self.program_counter = next;
                    Ok(())
                }
                instr => fail!("Cannot accept {} in {instr:?}", stringify!($ty)),
            }
        }
    };
}

#[allow(clippy::match_single_binding)]
impl EventSink for Interpreter {
    macros::forward_generic_to_specialized!();

    accept_primitive!(accept_u8, PushU8, u8, u8);
    accept_primitive!(accept_u16, PushU16, u16, u16);
    accept_primitive!(accept_u32, PushU32, u32, u32);
    accept_primitive!(accept_u64, PushU64, u64, u64);
    accept_primitive!(accept_i8, PushI8, i8, i8);
    accept_primitive!(accept_i16, PushI16, i16, i16);
    accept_primitive!(accept_i32, PushI32, i32, i32);
    accept_primitive!(accept_i64, PushI64, i64, i64);
    accept_primitive!(accept_f32, PushF32, f32, f32);
    accept_primitive!(accept_f64, PushF64, f64, f64);
    accept_primitive!(accept_bool, PushBool, bool, bool);

    fn accept_start_sequence(&mut self) -> crate::Result<()> {
        // TOOD: add new offset
        use Bytecode as B;
        match &self.program[self.program_counter] {
            &(next, B::LargeListStart | B::OuterSequenceStart) => {
                self.program_counter = next;
                Ok(())
            }
            instr => fail!("Cannot accept StartSequence in {instr:?}"),
        }
    }

    fn accept_end_sequence(&mut self) -> crate::Result<()> {
        use Bytecode as B;
        match &self.program[self.program_counter] {
            &(next, B::LargeListEnd(_, offsets)) => {
                self.buffers.large_offset[offsets].push_current_items();
                self.program_counter = next;
            }
            &(next, B::OuterSequenceEnd(_)) => {
                self.program_counter = next;
            }
            &(_, B::LargeListItem(idx, offsets)) => {
                self.buffers.large_offset[offsets].push_current_items();
                self.program_counter = self.lists[idx].r#return;
            }
            &(_, B::OuterSequenceItem(idx)) => {
                self.program_counter = self.lists[idx].r#return;
            }
            instr => fail!("Cannot accept EndSequence in {instr:?}"),
        }
        Ok(())
    }

    fn accept_start_struct(&mut self) -> crate::Result<()> {
        use Bytecode as B;
        match &self.program[self.program_counter] {
            &(next, B::StructStart | B::OuterRecordStart) => {
                self.program_counter = next;
            }
            instr => fail!("Cannot accept StartStruct in {instr:?}"),
        }
        Ok(())
    }

    fn accept_end_struct(&mut self) -> crate::Result<()> {
        use Bytecode as B;
        match &self.program[self.program_counter] {
            &(next, B::StructEnd | B::OuterRecordEnd) => {
                self.program_counter = next;
            }
            instr => fail!("Cannot accept EndStruct in {instr:?}"),
        }
        Ok(())
    }

    fn accept_item(&mut self) -> Result<()> {
        // TODO: increment the count
        use Bytecode as B;
        match &self.program[self.program_counter] {
            &(next, B::LargeListItem(_, offsets)) => {
                self.buffers.large_offset[offsets].inc_current_items()?;
                self.program_counter = next;
            }
            &(_, B::LargeListEnd(idx, offsets)) => {
                self.buffers.large_offset[offsets].inc_current_items()?;
                self.program_counter = self.lists[idx].item;
            }
            &(_, B::OuterSequenceEnd(idx)) => {
                self.program_counter = self.lists[idx].item;
            }
            &(next, B::OuterSequenceItem(_) | B::TupleStructItem) => {
                self.program_counter = next;
            }
            instr => fail!("Cannot accept Item in {instr:?}"),
        }
        Ok(())
    }

    fn accept_start_tuple(&mut self) -> Result<()> {
        // TOOD: add new offset
        use Bytecode as B;
        match &self.program[self.program_counter] {
            &(next, B::LargeListStart | B::OuterSequenceStart | B::TupleStructStart) => {
                self.program_counter = next;
            }
            instr => fail!("Cannot accept StartTuple in {instr:?}"),
        }
        Ok(())
    }

    fn accept_end_tuple(&mut self) -> Result<()> {
        use Bytecode as B;
        match &self.program[self.program_counter] {
            &(next, B::LargeListEnd(_, offsets)) => {
                self.buffers.large_offset[offsets].push_current_items();
                self.program_counter = next;
            }
            &(_, B::LargeListItem(idx, offsets)) => {
                self.buffers.large_offset[offsets].push_current_items();
                self.program_counter = self.lists[idx].r#return;
            }
            &(_, B::OuterSequenceItem(idx)) => {
                self.program_counter = self.lists[idx].r#return;
            }
            &(next, B::OuterSequenceEnd(_) | B::TupleStructEnd) => {
                self.program_counter = next;
            }
            instr => fail!("Cannot accept EndTuple in {instr:?}"),
        }
        Ok(())
    }

    fn accept_start_map(&mut self) -> Result<()> {
        match &self.program[self.program_counter] {
            instr => fail!("Cannot accept StartMap in {instr:?}"),
        }
    }

    fn accept_end_map(&mut self) -> Result<()> {
        match &self.program[self.program_counter] {
            instr => fail!("Cannot accept EndMap in {instr:?}"),
        }
    }

    fn accept_some(&mut self) -> Result<()> {
        use Bytecode as B;
        match &self.program[self.program_counter] {
            &(next, B::Option(_, validity)) => {
                self.buffers.validity[validity].push(true)?;
                self.program_counter = next;
            }
            instr => fail!("Cannot accept Some in {instr:?}"),
        }
        Ok(())
    }

    fn accept_null(&mut self) -> Result<()> {
        use Bytecode as B;
        match &self.program[self.program_counter] {
            &(_, B::Option(if_none, validity)) => {
                self.apply_null(validity)?;
                self.program_counter = if_none;
            }
            &(next, B::PushNull(idx)) => {
                self.buffers.null[idx].push(())?;
                self.program_counter = next;
            }
            instr => fail!("Cannot accept Null in {instr:?}"),
        }
        Ok(())
    }
    fn accept_default(&mut self) -> Result<()> {
        match &self.program[self.program_counter] {
            instr => fail!("Cannot accept Default in {instr:?}"),
        }
    }

    fn accept_str(&mut self, val: &str) -> Result<()> {
        use Bytecode as B;
        match &self.program[self.program_counter] {
            // TODO: implement fallback for unordered structs
            (next, B::StructField(_, name) | B::OuterRecordField(_, name)) if name == val => {
                self.program_counter = *next;
            }
            &(next, B::PushUTF8(idx)) => {
                self.buffers.utf8[idx].push(val)?;
                self.program_counter = next;
            }
            &(next, B::PushLargeUTF8(idx)) => {
                self.buffers.large_utf8[idx].push(val)?;
                self.program_counter = next;
            }
            &(next, B::PushDate64FromNaiveStr(idx)) => {
                use chrono::NaiveDateTime;

                self.buffers.i64[idx].push(val.parse::<NaiveDateTime>()?.timestamp_millis())?;
                self.program_counter = next;
            }
            &(next, B::PushDate64FromUtcStr(idx)) => {
                use chrono::{DateTime, Utc};

                self.buffers.i64[idx].push(val.parse::<DateTime<Utc>>()?.timestamp_millis())?;
                self.program_counter = next;
            }
            &(next, B::PushDictionaryU32LargeUTF8(dictionary, indices)) => {
                let idx = self.buffers.large_dictionaries[dictionary].push(val)?;
                self.buffers.u32[indices].push(idx.try_into()?)?;

                self.program_counter = next;
            }
            instr => fail!("Cannot accept Str in {instr:?}"),
        }
        Ok(())
    }

    fn accept_variant(&mut self, _name: &str, idx: usize) -> Result<()> {
        use Bytecode as B;
        match &self.program[self.program_counter] {
            &(_, B::Variant(union_idx, types)) => {
                // TODO: improve error message
                self.buffers.i8[types].push(idx.try_into()?)?;
                self.program_counter = self.unions[union_idx].fields[idx];
            }
            instr => fail!("Cannot accept Variant in {instr:?}"),
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        Ok(())
    }
}

macro_rules! apply_null {
    ($this:expr, $validity:expr, $name:ident) => {
        for &idx in &$this.nulls[$validity].$name {
            $this.buffers.$name[idx].push(Default::default())?;
        }
    };
}

impl Interpreter {
    fn apply_null(&mut self, validity: usize) -> Result<()> {
        apply_null!(self, validity, null);
        apply_null!(self, validity, bool);
        apply_null!(self, validity, u8);
        apply_null!(self, validity, u16);
        apply_null!(self, validity, u32);
        apply_null!(self, validity, u64);
        apply_null!(self, validity, i8);
        apply_null!(self, validity, i16);
        apply_null!(self, validity, i32);
        apply_null!(self, validity, i64);
        apply_null!(self, validity, f32);
        apply_null!(self, validity, f64);
        apply_null!(self, validity, utf8);
        apply_null!(self, validity, large_utf8);
        apply_null!(self, validity, validity);

        for &idx in &self.nulls[validity].large_offsets {
            self.buffers.large_offset[idx].push_current_items();
        }

        Ok(())
    }
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
