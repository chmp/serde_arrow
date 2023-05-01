use super::{
    buffers::{BitBuffer, OffsetBuilder, PrimitiveBuffer, StringBuffer},
    compiler::{ArrayMapping, Bytecode, ListDefinition, NullDefinition, Program, StructDefinition},
};

use crate::internal::{
    error::{fail, Result},
    sink::macros,
    sink::EventSink,
};

pub struct Interpreter {
    pub program_counter: usize,
    pub program: Vec<Bytecode>,
    pub structs: Vec<StructDefinition>,
    pub lists: Vec<ListDefinition>,
    pub nulls: Vec<NullDefinition>,
    pub array_mapping: Vec<ArrayMapping>,
    pub buffers: Buffers,
}

pub struct Buffers {
    pub bool: Vec<BitBuffer>,
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
}

impl Interpreter {
    pub fn new(program: Program) -> Self {
        Self {
            program: program.program,
            structs: program.structs,
            lists: program.large_lists,
            nulls: program.nulls,
            array_mapping: program.array_mapping,
            program_counter: 0,
            buffers: Buffers {
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
            },
        }
    }
}

macro_rules! accept_primitive {
    ($func:ident, $variant:ident, $builder:ident, $ty:ty) => {
        fn $func(&mut self, val: $ty) -> crate::Result<()> {
            match &self.program[self.program_counter] {
                Bytecode::$variant(array_idx) => {
                    self.buffers.$builder[*array_idx].push(val)?;
                    self.program_counter += 1;
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
            B::LargeListStart(_) | B::OuterSequenceStart(_) => {
                self.program_counter += 1;
                Ok(())
            }
            instr => fail!("Cannot accept StartSequence in {instr:?}"),
        }
    }

    fn accept_end_sequence(&mut self) -> crate::Result<()> {
        use Bytecode as B;
        match &self.program[self.program_counter] {
            B::LargeListEnd(_) | B::OuterSequenceEnd(_) => {
                self.program_counter += 1;
            }
            &B::LargeListItem(idx) | &B::OuterSequenceItem(idx) => {
                self.program_counter = self.lists[idx].r#return;
            }
            instr => fail!("Cannot accept EndSequence in {instr:?}"),
        }
        Ok(())
    }

    fn accept_start_struct(&mut self) -> crate::Result<()> {
        use Bytecode as B;
        match &self.program[self.program_counter] {
            B::StructStart(_) | B::OuterRecordStart(_) => {
                self.program_counter += 1;
            }
            instr => fail!("Cannot accept StartStruct in {instr:?}"),
        }
        Ok(())
    }

    fn accept_end_struct(&mut self) -> crate::Result<()> {
        use Bytecode as B;
        match &self.program[self.program_counter] {
            B::StructEnd(_) | B::OuterRecordEnd(_) => {
                self.program_counter += 1;
            }
            instr => fail!("Cannot accept EndStruct in {instr:?}"),
        }
        Ok(())
    }

    fn accept_item(&mut self) -> Result<()> {
        // TODO: increment the count
        use Bytecode as B;
        match &self.program[self.program_counter] {
            B::LargeListItem(_) | B::OuterSequenceItem(_) => {
                self.program_counter += 1;
            }
            &B::LargeListEnd(idx) | &B::OuterSequenceEnd(idx) => {
                self.program_counter = self.lists[idx].item;
            }
            instr => fail!("Cannot accept Item in {instr:?}"),
        }
        Ok(())
    }

    fn accept_start_tuple(&mut self) -> Result<()> {
        // TOOD: add new offset
        use Bytecode as B;
        match &self.program[self.program_counter] {
            B::LargeListStart(_) | B::OuterSequenceStart(_) => {
                self.program_counter += 1;
            }
            instr => fail!("Cannot accept StartTuple in {instr:?}"),
        }
        Ok(())
    }

    fn accept_end_tuple(&mut self) -> Result<()> {
        use Bytecode as B;
        match &self.program[self.program_counter] {
            B::LargeListEnd(_) | B::OuterSequenceEnd(_) => {
                self.program_counter += 1;
            }
            &B::LargeListItem(idx) | &B::OuterSequenceItem(idx) => {
                self.program_counter = self.lists[idx].r#return;
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
            &B::Option(_, validity) => {
                self.buffers.validity[validity].push(true)?;
                self.program_counter += 1;
                Ok(())
            }
            instr => fail!("Cannot accept Some in {instr:?}"),
        }
    }

    fn accept_null(&mut self) -> Result<()> {
        use Bytecode as B;
        match &self.program[self.program_counter] {
            &B::Option(if_none, validity) => {
                self.apply_null(validity)?;
                self.program_counter = if_none;
                Ok(())
            }
            instr => fail!("Cannot accept Null in {instr:?}"),
        }
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
            B::StructField(_, name) | B::OuterRecordField(_, name) if name == val => {
                self.program_counter += 1;
            }
            &B::PushUTF8(idx) => {
                self.buffers.utf8[idx].push(val)?;
                self.program_counter += 1;
            }
            &B::PushLargeUTF8(idx) => {
                self.buffers.large_utf8[idx].push(val)?;
                self.program_counter += 1;
            }
            instr => fail!("Cannot accept Str in {instr:?}"),
        }
        Ok(())
    }

    fn accept_owned_str(&mut self, val: String) -> Result<()> {
        use Bytecode as B;
        match &self.program[self.program_counter] {
            // TODO: implement fallback for unordered structs
            B::StructField(_, name) | B::OuterRecordField(_, name) if name == &val => {
                self.program_counter += 1;
            }
            &B::PushUTF8(array_idx) => {
                self.buffers.utf8[array_idx].push(&val)?;
                self.program_counter += 1;
            }
            &B::PushLargeUTF8(array_idx) => {
                self.buffers.large_utf8[array_idx].push(&val)?;
                self.program_counter += 1;
            }
            instr => fail!("Cannot accept OwnedStr in {instr:?}"),
        }
        Ok(())
    }

    fn accept_variant(&mut self, _name: &str, _idx: usize) -> Result<()> {
        match &self.program[self.program_counter] {
            instr => fail!("Cannot accept Variant in {instr:?}"),
        }
    }

    fn accept_owned_variant(&mut self, _name: String, _idx: usize) -> Result<()> {
        match &self.program[self.program_counter] {
            instr => fail!("Cannot accept OwnedVariant in {instr:?}"),
        }
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
