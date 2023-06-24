use crate::internal::{
    error::{fail, Result},
    event::Event,
    source::EventSource,
};

use super::common::{define_bytecode, ArrayMapping, Buffers};

use half::f16;

#[allow(unused)]
pub fn compile_deserialization<'a>(
    num_items: usize,
    arrays: &'a [ArrayMapping],
    mut buffers: Buffers<'a>,
) -> Result<Interpreter<'a>> {
    let mut num_positions = 0;

    let mut program = Vec::new();

    program.push(Bytecode::EmitStartSequence(EmitStartSequence { next: 1 }));

    let count = buffers.push_u0(num_items);
    let item_pos = program.len();
    num_positions += 1;

    program.push(Bytecode::EmitItem(EmitItem {
        next: item_pos + 1,
        if_end: usize::MAX,
        position: 0,
        count,
    }));

    program.push(Bytecode::EmitStartStruct(EmitStartStruct { next: 3 }));

    for array in arrays {
        match array {
            ArrayMapping::F16 {
                field,
                buffer,
                validity,
            } => {
                if validity.is_some() {
                    todo!()
                }

                let name_buffer = buffers.push_u8(field.name.as_bytes());

                let position = num_positions;
                num_positions += 1;

                let instr = program.len();
                program.push(Bytecode::EmitConstantString(EmitConstantString {
                    next: instr + 1,
                    buffer: name_buffer,
                }));
                program.push(Bytecode::EmitF16(EmitF16 {
                    next: instr + 2,
                    buffer: *buffer,
                    position,
                }));
            }
            ArrayMapping::I32 {
                field,
                buffer,
                validity,
            } => {
                let name_buffer = buffers.push_u8(field.name.as_bytes());

                let position = num_positions;
                num_positions += 1;

                let instr = program.len();
                program.push(Bytecode::EmitConstantString(EmitConstantString {
                    next: instr + 1,
                    buffer: name_buffer,
                }));

                let option_instr;
                if let &Some(validity) = validity {
                    option_instr = Some(program.len());

                    program.push(Bytecode::EmitOptionPrimitive(EmitOptionPrimitive {
                        next: option_instr.unwrap() + 1,
                        position,
                        validity,
                        if_none: usize::MAX,
                    }))
                } else {
                    option_instr = None
                };
                let instr = program.len();
                program.push(Bytecode::EmitI32(EmitI32 {
                    next: instr + 1,
                    buffer: *buffer,
                    position,
                }));

                if let Some(option_instr) = option_instr {
                    let if_none = program.len();
                    let Some(Bytecode::EmitOptionPrimitive(instr)) = program.get_mut(option_instr) else { unreachable!() };
                    instr.if_none = if_none;
                }
            }
            _ => todo!(),
        }
    }

    program.push(Bytecode::EmitEndStruct(EmitEndStruct { next: item_pos }));

    let end_of_program = program.len();
    program.push(Bytecode::EndOfProgram(EndOfProgram {
        next: end_of_program,
    }));

    if let Bytecode::EmitItem(item) = &mut program[item_pos] {
        item.if_end = end_of_program;
    }

    Ok(Interpreter {
        current_instr: 0,
        program,
        positions: vec![0; num_positions],
        buffers,
    })
}

#[rustfmt::skip]
define_bytecode!{
    EmitStartSequence {},
    EmitItem {
        /// the instruction to jump to if the list is at its end
        position: usize,
        if_end: usize,
        /// the buffer that contains the number of items in this sequence
        count: usize,
    },
    EmitStartStruct{},
    EmitEndStruct{},
    EmitConstantString{
        buffer: usize,
    },
    EndOfProgram{},
    /// Emit nullability information for a primitive type
    /// 
    /// This instruction increases the primitives positions in case of null.
    EmitOptionPrimitive{
        /// The index of the position counter
        position: usize,
        /// The index of the u1 buffer containing the validity 
        validity: usize,
        /// The instruction to jump to, if the validity is false 
        if_none: usize,
    },
    EmitI32{
        position: usize,
        buffer: usize,
    },
    EmitF16 {
        position: usize,
        buffer: usize,
    },
}

trait Instruction: std::fmt::Debug {
    #[allow(unused_variables)]
    fn emit<'a>(
        &self,
        positions: &mut [usize],
        buffers: &Buffers<'a>,
    ) -> Result<(usize, Option<Event<'a>>)> {
        fail!("not implemented for {self:?}")
    }
}

impl Instruction for Bytecode {
    fn emit<'a>(
        &self,
        positions: &mut [usize],
        buffers: &Buffers<'a>,
    ) -> Result<(usize, Option<Event<'a>>)> {
        dispatch_bytecode!(&self, instr => instr.emit(positions, buffers))
    }
}

impl Instruction for EmitStartSequence {
    fn emit<'a>(
        &self,
        _positions: &mut [usize],
        _buffers: &Buffers<'a>,
    ) -> Result<(usize, Option<Event<'a>>)> {
        Ok((self.next, Some(Event::StartSequence)))
    }
}

impl Instruction for EmitItem {
    fn emit<'a>(
        &self,
        positions: &mut [usize],
        buffers: &Buffers<'a>,
    ) -> Result<(usize, Option<Event<'a>>)> {
        if positions[self.position] >= buffers.u0[self.count] {
            Ok((self.if_end, Some(Event::EndSequence)))
        } else {
            positions[self.position] += 1;
            Ok((self.next, Some(Event::Item)))
        }
    }
}

impl Instruction for EmitStartStruct {
    fn emit<'a>(
        &self,
        _positions: &mut [usize],
        _buffers: &Buffers<'a>,
    ) -> Result<(usize, Option<Event<'a>>)> {
        Ok((self.next, Some(Event::StartStruct)))
    }
}

impl Instruction for EmitEndStruct {
    fn emit<'a>(
        &self,
        _positions: &mut [usize],
        _buffers: &Buffers<'a>,
    ) -> Result<(usize, Option<Event<'a>>)> {
        Ok((self.next, Some(Event::EndStruct)))
    }
}

impl Instruction for EmitConstantString {
    fn emit<'a>(
        &self,
        _positions: &mut [usize],
        buffers: &Buffers<'a>,
    ) -> Result<(usize, Option<Event<'a>>)> {
        let s = std::str::from_utf8(buffers.u8[self.buffer])?;
        Ok((self.next, Some(Event::Str(s))))
    }
}

impl Instruction for EmitOptionPrimitive {
    fn emit<'a>(
        &self,
        positions: &mut [usize],
        buffers: &Buffers<'a>,
    ) -> Result<(usize, Option<Event<'a>>)> {
        if buffers.u1[self.validity].is_set(positions[self.position]) {
            Ok((self.next, Some(Event::Some)))
        } else {
            positions[self.position] += 1;
            Ok((self.if_none, Some(Event::Null)))
        }
    }
}

impl Instruction for EndOfProgram {
    fn emit<'a>(
        &self,
        _positions: &mut [usize],
        _buffers: &Buffers<'a>,
    ) -> Result<(usize, Option<Event<'a>>)> {
        Ok((self.next, None))
    }
}

impl Instruction for EmitI32 {
    fn emit<'a>(
        &self,
        positions: &mut [usize],
        buffers: &Buffers<'a>,
    ) -> Result<(usize, Option<Event<'a>>)> {
        let val =
            i32::from_ne_bytes(buffers.u32[self.buffer][positions[self.position]].to_ne_bytes());
        positions[self.position] += 1;
        Ok((self.next, Some(Event::I32(val))))
    }
}

impl Instruction for EmitF16 {
    fn emit<'a>(
        &self,
        positions: &mut [usize],
        buffers: &Buffers<'a>,
    ) -> Result<(usize, Option<Event<'a>>)> {
        let val = f16::from_bits(buffers.u16[self.buffer][positions[self.position]]);
        positions[self.position] += 1;
        Ok((self.next, Some(Event::F32(val.to_f32()))))
    }
}

pub struct Interpreter<'a> {
    current_instr: usize,
    program: Vec<Bytecode>,
    positions: Vec<usize>,
    buffers: Buffers<'a>,
}

impl<'a> EventSource<'a> for Interpreter<'a> {
    fn next(&mut self) -> Result<Option<Event<'a>>> {
        let (next_instr, ev) =
            self.program[self.current_instr].emit(&mut self.positions, &self.buffers)?;
        self.current_instr = next_instr;
        Ok(ev)
    }
}
