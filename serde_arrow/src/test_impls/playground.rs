use serde::{Deserialize, Serialize};

use crate::{
    arrow, arrow2,
    base::{deserialize_from_source, Event, EventSource},
    internal::{
        common::{define_bytecode, Buffers},
        error::fail,
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

#[test]
fn example_arrow2() {
    use crate::_impl::arrow2::{array::PrimitiveArray, datatypes::Field, types::f16};

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

    let arrays = arrow2::serialize_into_arrays(&fields, items).unwrap();

    let a = &arrays[0];
    let a_data = a
        .as_any()
        .downcast_ref::<PrimitiveArray<i32>>()
        .unwrap()
        .values()
        .as_slice();
    let a_data: &[u32] = bytemuck::try_cast_slice(a_data).unwrap();

    let b = &arrays[1];
    let b_data = b
        .as_any()
        .downcast_ref::<PrimitiveArray<f16>>()
        .unwrap()
        .values()
        .as_slice();
    let b_data: &[u16] = bytemuck::try_cast_slice(b_data).unwrap();

    let mut buffers = Buffers::new();
    buffers.u0.push(3);
    buffers.u8.push(fields[0].name.as_bytes());
    buffers.u8.push(fields[1].name.as_bytes());
    buffers.u16.push(b_data);
    buffers.u32.push(a_data);

    let positions = vec![0, 0, 0];

    let program = vec![
        Bytecode::EmitStartSequence(EmitStartSequence { next: 1 }),
        Bytecode::EmitItem(EmitItem {
            next: 2,
            if_end: 8,
            position: 0,
            count: 0,
        }),
        Bytecode::EmitStartStruct(EmitStartStruct { next: 3 }),
        Bytecode::EmitConstantString(EmitConstantString { next: 4, buffer: 0 }),
        Bytecode::EmitI32(EmitI32 {
            next: 5,
            buffer: 0,
            position: 1,
        }),
        Bytecode::EmitConstantString(EmitConstantString { next: 6, buffer: 1 }),
        Bytecode::EmitF16(EmitF16 {
            next: 7,
            buffer: 0,
            position: 2,
        }),
        Bytecode::EmitEndStruct(EmitEndStruct { next: 1 }),
        Bytecode::EndOfProgram(EndOfProgram { next: 8 }),
    ];

    let interpreter = Interpreter {
        current_instr: 0,
        program,
        positions,
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
