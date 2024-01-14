mod misc;
mod primitives;
mod sequences;
mod structures;

use std::collections::HashMap;

use crate::internal::{
    common::MutableBuffers,
    error::{fail, Result},
    event::Event,
    serialization::{
        bit_set::BitSet,
        bytecode::{dispatch_bytecode, Bytecode},
        compiler::{BufferCounts, Program, Structure},
    },
    sink::EventSink,
};

pub struct Interpreter {
    pub program_counter: usize,
    pub structure: Structure,
    pub context: SerializationContext,
}

#[derive(Debug, Clone)]
pub struct SerializationContext {
    pub buffers: MutableBuffers,
    /// markers for which struct fields have been seen
    pub seen: Vec<BitSet>,
    /// mappings from strings to indices for dictionaries
    pub dictionaries: Vec<HashMap<String, usize>>,
}

impl SerializationContext {
    pub fn from_counts(counts: &BufferCounts) -> Self {
        Self {
            buffers: MutableBuffers {
                u0: vec![Default::default(); counts.num_u0],
                u1: vec![Default::default(); counts.num_u1],
                u8: vec![Default::default(); counts.num_u8],
                u16: vec![Default::default(); counts.num_u16],
                u32: vec![Default::default(); counts.num_u32],
                u64: vec![Default::default(); counts.num_u64],
                u128: vec![Default::default(); counts.num_u128],
                u32_offsets: vec![Default::default(); counts.num_u32_offsets],
                u64_offsets: vec![Default::default(); counts.num_u64_offsets],
            },
            seen: vec![Default::default(); counts.num_seen],
            dictionaries: vec![Default::default(); counts.num_dictionaries],
        }
    }

    pub fn reset(&mut self) {
        self.buffers.reset();
        self.seen.iter_mut().for_each(|b| b.clear());
        self.dictionaries.iter_mut().for_each(|b| b.clear());
    }
}

impl Interpreter {
    pub fn new(program: Program) -> Self {
        Self {
            program_counter: 0,
            structure: program.structure,
            context: SerializationContext::from_counts(&program.buffers),
        }
    }
}

// TODO: use custom trait to improve error message
#[allow(unused_variables)]
trait Instruction: std::fmt::Debug {
    const NAME: &'static str;
    const EXPECTED: &'static [&'static str];

    fn accept_start_sequence(
        &self,
        structure: &Structure,
        context: &mut SerializationContext,
    ) -> Result<usize> {
        fail!(
            "{name} cannot accept StartSequence, expected {expected:?}",
            name = Self::NAME,
            expected = Self::EXPECTED
        );
    }

    fn accept_end_sequence(
        &self,
        structure: &Structure,
        context: &mut SerializationContext,
    ) -> Result<usize> {
        fail!(
            "{name} cannot accept EndSequence, expected {expected:?}",
            name = Self::NAME,
            expected = Self::EXPECTED
        );
    }

    fn accept_start_tuple(
        &self,
        structure: &Structure,
        context: &mut SerializationContext,
    ) -> Result<usize> {
        fail!(
            "{name} cannot accept StartTuple, expected {expected:?}",
            name = Self::NAME,
            expected = Self::EXPECTED
        );
    }

    fn accept_end_tuple(
        &self,
        structure: &Structure,
        context: &mut SerializationContext,
    ) -> Result<usize> {
        fail!(
            "{name} cannot accept EndTuple, expected {expected:?}",
            name = Self::NAME,
            expected = Self::EXPECTED
        );
    }

    fn accept_start_struct(
        &self,
        structure: &Structure,
        context: &mut SerializationContext,
    ) -> Result<usize> {
        fail!(
            "{name} cannot accept StartStructure, expected {expected:?}",
            name = Self::NAME,
            expected = Self::EXPECTED
        );
    }

    fn accept_end_struct(
        &self,
        structure: &Structure,
        context: &mut SerializationContext,
    ) -> Result<usize> {
        fail!(
            "{name} cannot accept EndStructure, expected {expected:?}",
            name = Self::NAME,
            expected = Self::EXPECTED
        );
    }

    fn accept_start_map(
        &self,
        structure: &Structure,
        context: &mut SerializationContext,
    ) -> Result<usize> {
        fail!(
            "{name} cannot accept StartMap, expected {expected:?}",
            name = Self::NAME,
            expected = Self::EXPECTED
        );
    }

    fn accept_end_map(
        &self,
        structure: &Structure,
        context: &mut SerializationContext,
    ) -> Result<usize> {
        fail!(
            "{name} cannot accept EndMap, expected {expected:?}",
            name = Self::NAME,
            expected = Self::EXPECTED
        );
    }

    fn accept_item(
        &self,
        structure: &Structure,
        context: &mut SerializationContext,
    ) -> Result<usize> {
        fail!(
            "{name} cannot accept Item, expected {expected:?}",
            name = Self::NAME,
            expected = Self::EXPECTED
        );
    }

    fn accept_some(
        &self,
        structure: &Structure,
        context: &mut SerializationContext,
    ) -> Result<usize> {
        fail!(
            "{name} cannot accept Some, expected {expected:?}",
            name = Self::NAME,
            expected = Self::EXPECTED
        );
    }

    fn accept_default(
        &self,
        structure: &Structure,
        context: &mut SerializationContext,
    ) -> Result<usize> {
        fail!(
            "{name} cannot accept Default, expected {expected:?}",
            name = Self::NAME,
            expected = Self::EXPECTED
        );
    }

    fn accept_variant(
        &self,
        structure: &Structure,
        context: &mut SerializationContext,
        name: &str,
        idx: usize,
    ) -> Result<usize> {
        fail!("{name} cannot accept Variant({name:?}, {idx}")
    }

    fn accept_null(
        &self,
        structure: &Structure,
        context: &mut SerializationContext,
    ) -> Result<usize> {
        fail!(
            "{name} cannot accept Null, expected {expected:?}",
            name = Self::NAME,
            expected = Self::EXPECTED
        );
    }

    fn accept_bool(
        &self,
        structure: &Structure,
        context: &mut SerializationContext,
        val: bool,
    ) -> Result<usize> {
        fail!(
            "{name} cannot accept Bool({val}), expected {expected:?}",
            name = Self::NAME,
            expected = Self::EXPECTED
        );
    }

    fn accept_u8(
        &self,
        structure: &Structure,
        context: &mut SerializationContext,
        val: u8,
    ) -> Result<usize> {
        fail!(
            "{name} cannot accept U8({val}), expected {expected:?}",
            name = Self::NAME,
            expected = Self::EXPECTED
        );
    }

    fn accept_u16(
        &self,
        structure: &Structure,
        context: &mut SerializationContext,
        val: u16,
    ) -> Result<usize> {
        fail!(
            "{name} cannot accept U16({val}), expected {expected:?}",
            name = Self::NAME,
            expected = Self::EXPECTED
        );
    }

    fn accept_u32(
        &self,
        structure: &Structure,
        context: &mut SerializationContext,
        val: u32,
    ) -> Result<usize> {
        fail!(
            "{name} cannot accept U32({val}), expected {expected:?}",
            name = Self::NAME,
            expected = Self::EXPECTED
        );
    }

    fn accept_u64(
        &self,
        structure: &Structure,
        context: &mut SerializationContext,
        val: u64,
    ) -> Result<usize> {
        fail!(
            "{name} cannot accept U64({val}), expected {expected:?}",
            name = Self::NAME,
            expected = Self::EXPECTED
        );
    }

    fn accept_i8(
        &self,
        structure: &Structure,
        context: &mut SerializationContext,
        val: i8,
    ) -> Result<usize> {
        fail!(
            "{name} cannot accept I8({val}), expected {expected:?}",
            name = Self::NAME,
            expected = Self::EXPECTED
        );
    }

    fn accept_i16(
        &self,
        structure: &Structure,
        context: &mut SerializationContext,
        val: i16,
    ) -> Result<usize> {
        fail!(
            "{name} cannot accept I16({val}), expected {expected:?}",
            name = Self::NAME,
            expected = Self::EXPECTED
        );
    }

    fn accept_i32(
        &self,
        structure: &Structure,
        context: &mut SerializationContext,
        val: i32,
    ) -> Result<usize> {
        fail!(
            "{name} cannot accept I32({val}), expected {expected:?}",
            name = Self::NAME,
            expected = Self::EXPECTED
        );
    }

    fn accept_i64(
        &self,
        structure: &Structure,
        context: &mut SerializationContext,
        val: i64,
    ) -> Result<usize> {
        fail!(
            "{name} cannot accept I64({val}), expected {expected:?}",
            name = Self::NAME,
            expected = Self::EXPECTED
        );
    }

    fn accept_f32(
        &self,
        structure: &Structure,
        context: &mut SerializationContext,
        val: f32,
    ) -> Result<usize> {
        fail!(
            "{name} cannot accept F32({val}), expected {expected:?}",
            name = Self::NAME,
            expected = Self::EXPECTED
        );
    }

    fn accept_f64(
        &self,
        structure: &Structure,
        context: &mut SerializationContext,
        val: f64,
    ) -> Result<usize> {
        fail!(
            "{name} cannot accept F64({val}), expected {expected:?}",
            name = Self::NAME,
            expected = Self::EXPECTED
        );
    }

    fn accept_str(
        &self,
        structure: &Structure,
        context: &mut SerializationContext,
        val: &str,
    ) -> Result<usize> {
        fail!(
            "{name} cannot accept Str({val:?}), expected {expected:?}",
            name = Self::NAME,
            expected = Self::EXPECTED
        );
    }
}

macro_rules! dispatch_instruction {
    ($this:expr, $method:ident) => {
        {
            $this.program_counter = dispatch_bytecode!(
                &$this.structure.program[$this.program_counter],
                instr => instr.$method(&$this.structure, &mut $this.context)?
            );
            Ok(())
        }
    };
    ($this:expr, $method:ident, $($val:expr),*) => {
        {
            $this.program_counter = dispatch_bytecode!(
                &$this.structure.program[$this.program_counter],
                instr => instr.$method(&$this.structure, &mut $this.context, $($val),*)?
            );
            Ok(())
        }
    };
}

#[allow(clippy::match_single_binding)]
impl EventSink for Interpreter {
    fn accept(&mut self, event: Event<'_>) -> Result<()> {
        use Event::*;
        match event {
            StartSequence => self.accept_start_sequence(),
            StartTuple => self.accept_start_tuple(),
            StartMap => self.accept_start_map(),
            StartStruct => self.accept_start_struct(),
            EndSequence => self.accept_end_sequence(),
            EndTuple => self.accept_end_tuple(),
            EndMap => self.accept_end_map(),
            EndStruct => self.accept_end_struct(),
            Item => self.accept_item(),
            Null => self.accept_null(),
            Some => self.accept_some(),
            Default => self.accept_default(),
            Bool(val) => self.accept_bool(val),
            I8(val) => self.accept_i8(val),
            I16(val) => self.accept_i16(val),
            I32(val) => self.accept_i32(val),
            I64(val) => self.accept_i64(val),
            U8(val) => self.accept_u8(val),
            U16(val) => self.accept_u16(val),
            U32(val) => self.accept_u32(val),
            U64(val) => self.accept_u64(val),
            F32(val) => self.accept_f32(val),
            F64(val) => self.accept_f64(val),
            Str(val) => self.accept_str(val),
            OwnedStr(val) => self.accept_str(&val),
            Variant(name, idx) => self.accept_variant(name, idx),
            OwnedVariant(name, idx) => self.accept_variant(&name, idx),
        }
    }

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
        if !matches!(
            self.structure.program[self.program_counter],
            Bytecode::ProgramEnd(_)
        ) {
            fail!(
                "finished interpreting before program end, current instruction: {instr:?}",
                instr = self.structure.program[self.program_counter],
            )
        }
        self.program_counter = 0;

        Ok(())
    }
}
