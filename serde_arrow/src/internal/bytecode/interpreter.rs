use super::{
    buffers::{BoolBuffer, PrimitiveBuffer, StringBuffer},
    compiler::{Bytecode, ListDefinition, Program, StructDefinition},
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
    pub bool: Vec<BoolBuffer>,
    pub utf8: Vec<StringBuffer<i32>>,
    pub large_utf8: Vec<StringBuffer<i64>>,
}

impl Interpreter {
    pub fn new(program: Program) -> Self {
        Self {
            program: program.program,
            structs: program.structs,
            lists: program.lists,
            program_counter: 0,
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
        }
    }
}

macro_rules! accept_primitive {
    ($func:ident, $variant:ident, $builder:ident, $ty:ty) => {
        fn $func(&mut self, val: $ty) -> crate::Result<()> {
            match &self.program[self.program_counter] {
                Bytecode::$variant(array_idx) => {
                    self.$builder[*array_idx].push(val)?;
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
        match &self.program[self.program_counter] {
            &Bytecode::ListStart(_list_idx) => {
                self.program_counter += 1;
                Ok(())
            }
            instr => fail!("Cannot accept StartSequence in {instr:?}"),
        }
    }

    fn accept_end_sequence(&mut self) -> crate::Result<()> {
        match &self.program[self.program_counter] {
            &Bytecode::ListEnd(_list_idx) => {
                self.program_counter += 1;
                Ok(())
            }
            &Bytecode::ListItem(list_idx) => {
                self.program_counter = self.lists[list_idx].r#return;
                Ok(())
            }
            instr => fail!("Cannot accept EndSequence in {instr:?}"),
        }
    }

    fn accept_start_struct(&mut self) -> crate::Result<()> {
        match &self.program[self.program_counter] {
            &Bytecode::StructStart(_struct_idx) => {
                self.program_counter += 1;
                Ok(())
            }
            instr => fail!("Cannot accept StartStruct in {instr:?}"),
        }
    }

    fn accept_end_struct(&mut self) -> crate::Result<()> {
        match &self.program[self.program_counter] {
            &Bytecode::StructEnd(_struct_idx) => {
                self.program_counter += 1;
                Ok(())
            }
            instr => fail!("Cannot accept EndStruct in {instr:?}"),
        }
    }

    fn accept_item(&mut self) -> Result<()> {
        match &self.program[self.program_counter] {
            &Bytecode::ListItem(_list_idx) => {
                self.program_counter += 1;
                Ok(())
            }
            &Bytecode::ListEnd(list_idx) => {
                self.program_counter = self.lists[list_idx].item;
                Ok(())
            }
            instr => fail!("Cannot accept Item in {instr:?}"),
        }
    }

    fn accept_start_tuple(&mut self) -> Result<()> {
        match &self.program[self.program_counter] {
            Bytecode::ListStart(_list_idx) => {
                self.program_counter += 1;
                Ok(())
            }
            instr => fail!("Cannot accept StartTuple in {instr:?}"),
        }
    }

    fn accept_end_tuple(&mut self) -> Result<()> {
        match &self.program[self.program_counter] {
            Bytecode::ListEnd(_list_idx) => {
                self.program_counter += 1;
                Ok(())
            }
            &Bytecode::ListItem(list_idx) => {
                self.program_counter = self.lists[list_idx].r#return;
                Ok(())
            }
            instr => fail!("Cannot accept EndTuple in {instr:?}"),
        }
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
        match &self.program[self.program_counter] {
            Bytecode::Option(_) => {
                self.program_counter += 1;
                Ok(())
            }
            instr => fail!("Cannot accept Some in {instr:?}"),
        }
    }

    fn accept_null(&mut self) -> Result<()> {
        match &self.program[self.program_counter] {
            &Bytecode::Option(if_none) => {
                // TODO: fix this. How to do this generically?
                self.large_utf8[0].push_null()?;
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
        match &self.program[self.program_counter] {
            Bytecode::StructField(_struct_idx, name) if name == val => {
                self.program_counter += 1;
                Ok(())
            }
            &Bytecode::PushUTF8(array_idx) => {
                self.utf8[array_idx].push(val)?;
                self.program_counter += 1;
                Ok(())
            }
            &Bytecode::PushLargeUTF8(array_idx) => {
                self.large_utf8[array_idx].push(val)?;
                self.program_counter += 1;
                Ok(())
            }
            instr => fail!("Cannot accept Str in {instr:?}"),
        }
    }

    fn accept_owned_str(&mut self, val: String) -> Result<()> {
        match &self.program[self.program_counter] {
            Bytecode::StructField(_struct_idx, name) if name == &val => {
                self.program_counter += 1;
                Ok(())
            }
            &Bytecode::PushUTF8(array_idx) => {
                self.utf8[array_idx].push(&val)?;
                self.program_counter += 1;
                Ok(())
            }
            &Bytecode::PushLargeUTF8(array_idx) => {
                self.large_utf8[array_idx].push(&val)?;
                self.program_counter += 1;
                Ok(())
            }
            instr => fail!("Cannot accept OwnedStr in {instr:?}"),
        }
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
