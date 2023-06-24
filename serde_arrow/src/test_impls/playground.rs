use serde::{Deserialize, Serialize};

use crate::{
    arrow, arrow2,
    base::{deserialize_from_source, Event, EventSource},
    internal::{
        common::{define_bytecode, ArrayMapping, Buffers},
        error::{error, fail},
        schema::{GenericDataType, GenericField},
    },
    Result,
};

use half::f16;

#[rustfmt::skip]
define_bytecode!{
    EmitStartSequence {},
    EmitItem {
        // the instruction to jump to if the list is at its end
        position: usize,
        if_end: usize,
        // the buffer that contains the number of items in this sequence
        count: usize,
    },
    EmitStartStruct{},
    EmitEndStruct{},
    EmitConstantString{
        buffer: usize,
    },
    EndOfProgram{},
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

struct Interpreter<'a> {
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
        return Ok(ev);
    }
}

trait BufferExtract {
    fn extract_buffers<'a>(
        &'a self,
        field: &GenericField,
        buffers: &mut Buffers<'a>,
    ) -> Result<ArrayMapping>;
}

impl<T> BufferExtract for T
where
    T: AsRef<dyn crate::_impl::arrow2::array::Array>,
{
    fn extract_buffers<'a>(
        &'a self,
        field: &GenericField,
        buffers: &mut Buffers<'a>,
    ) -> Result<ArrayMapping> {
        use crate::_impl::arrow2::{array::PrimitiveArray, types::f16};

        match &field.data_type {
            GenericDataType::I32 => {
                if field.nullable {
                    fail!("nullable fields are not yet supported");
                }

                let data = self
                    .as_ref()
                    .as_any()
                    .downcast_ref::<PrimitiveArray<i32>>()
                    .ok_or_else(|| error!("Cannot interpret array as I32 array"))?
                    .values()
                    .as_slice();
                let data: &[u32] = bytemuck::try_cast_slice(data)?;

                let buffer = buffers.u32.len();
                buffers.u32.push(data);

                Ok(ArrayMapping::I32 {
                    field: field.clone(),
                    buffer,
                    validity: None,
                })
            }
            GenericDataType::F16 => {
                if field.nullable {
                    fail!("nullable fields are not yet supported");
                }
                let data = self
                    .as_ref()
                    .as_any()
                    .downcast_ref::<PrimitiveArray<f16>>()
                    .ok_or_else(|| error!("Cannot interpret array as F16 array"))?
                    .values()
                    .as_slice();
                let data: &[u16] = bytemuck::try_cast_slice(data)?;

                let buffer = buffers.u16.len();
                buffers.u16.push(data);

                Ok(ArrayMapping::F16 {
                    field: field.clone(),
                    buffer,
                    validity: None,
                })
            }
            dt => fail!("BufferExtract for {dt} is not implemented"),
        }
    }
}

#[test]
fn example_arrow2() {
    use crate::_impl::arrow2::datatypes::Field;

    #[derive(Debug, PartialEq, Deserialize, Serialize)]
    struct S {
        a: i32,
        b: f32,
    }

    let items = &[S { a: 0, b: 2.0 }, S { a: 1, b: 3.0 }, S { a: 2, b: 4.0 }];

    let fields = vec![
        GenericField::new("a", GenericDataType::I32, false),
        GenericField::new("b", GenericDataType::F16, false),
    ];

    let arrays;
    {
        let fields = fields
            .iter()
            .map(|f| Field::try_from(f))
            .collect::<Result<Vec<_>>>()
            .unwrap();

        arrays = arrow2::serialize_into_arrays(&fields, items).unwrap();
    }

    let mut buffers = Buffers::new();

    let mut mappings = Vec::new();
    for (field, array) in fields.iter().zip(arrays.iter()) {
        mappings.push(array.extract_buffers(field, &mut buffers).unwrap());
    }

    // TODO: where to get the count from?
    buffers.u0.push(3);

    let mut num_positions = 0;

    let mut program = Vec::new();

    program.push(Bytecode::EmitStartSequence(EmitStartSequence { next: 1 }));

    num_positions += 1;
    program.push(Bytecode::EmitItem(EmitItem {
        next: 2,
        if_end: 8,
        position: 0,
        count: 0,
    }));

    program.push(Bytecode::EmitStartStruct(EmitStartStruct { next: 3 }));

    for mapping in &mappings {
        match mapping {
            ArrayMapping::F16 {
                field,
                buffer,
                validity,
            } => {
                if validity.is_some() {
                    todo!()
                }

                let name_buffer = buffers.u8.len();
                buffers.u8.push(field.name.as_bytes());

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
                if validity.is_some() {
                    todo!()
                }

                let name_buffer = buffers.u8.len();
                buffers.u8.push(field.name.as_bytes());

                let position = num_positions;
                num_positions += 1;

                let instr = program.len();
                program.push(Bytecode::EmitConstantString(EmitConstantString {
                    next: instr + 1,
                    buffer: name_buffer,
                }));
                program.push(Bytecode::EmitI32(EmitI32 {
                    next: instr + 2,
                    buffer: *buffer,
                    position,
                }));
            }
            _ => todo!(),
        }
    }

    program.push(Bytecode::EmitEndStruct(EmitEndStruct { next: 1 }));
    program.push(Bytecode::EndOfProgram(EndOfProgram { next: 8 }));

    let interpreter = Interpreter {
        current_instr: 0,
        program,
        positions: vec![0; num_positions],
        buffers,
    };

    let rountripped: Vec<S> = deserialize_from_source(interpreter).unwrap();

    assert_eq!(rountripped, items);
}

#[test]
fn example_arrow() {
    use crate::_impl::arrow::{
        array::PrimitiveArray,
        datatypes::{Field, Float16Type, Int32Type},
    };

    #[derive(Debug, PartialEq, Deserialize, Serialize)]
    struct S {
        a: i32,
        b: f32,
    }

    let items = &[S { a: 0, b: 2.0 }, S { a: 1, b: 3.0 }, S { a: 2, b: 4.0 }];

    let fields = vec![
        GenericField::new("a", GenericDataType::I32, false),
        GenericField::new("b", GenericDataType::F16, false),
    ];
    let fields = fields
        .iter()
        .map(|f| Field::try_from(f))
        .collect::<Result<Vec<_>>>()
        .unwrap();

    let arrays = arrow::serialize_into_arrays(&fields, items).unwrap();

    let a = &arrays[0];

    let a_data = a
        .as_any()
        .downcast_ref::<PrimitiveArray<Int32Type>>()
        .unwrap()
        .values();
    let a_data: &[u32] = bytemuck::try_cast_slice(a_data).unwrap();

    assert_eq!(a_data, &[0, 1, 2]);

    let b = &arrays[1];

    let b_data = b
        .as_any()
        .downcast_ref::<PrimitiveArray<Float16Type>>()
        .unwrap()
        .values();
    let b_data: &[u16] = bytemuck::try_cast_slice(b_data).unwrap();

    assert_eq!(b_data.len(), 3);
}
