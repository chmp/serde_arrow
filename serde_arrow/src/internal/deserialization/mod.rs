use std::collections::HashMap;

use crate::{
    internal::{
        error::{error, fail, Result},
        event::Event,
        source::EventSource,
    },
    schema::Strategy,
};

use super::{
    common::{define_bytecode, ArrayMapping, Buffers, DictionaryIndex, DictionaryValue},
    config::CONFIGURATION,
};

use half::f16;

const UNSET_INSTR: usize = usize::MAX;
const NEXT_INSTR: usize = usize::MAX - 1;

#[derive(Debug, Clone)]
pub struct CompilationOptions {
    pub wrap_with_struct: bool,
}

impl std::default::Default for CompilationOptions {
    fn default() -> Self {
        Self {
            wrap_with_struct: true,
        }
    }
}

impl CompilationOptions {
    pub fn wrap_with_struct(mut self, val: bool) -> Self {
        self.wrap_with_struct = val;
        self
    }
}

pub fn compile_deserialization<'a>(
    num_items: usize,
    arrays: &'a [ArrayMapping],
    buffers: Buffers<'a>,
    options: CompilationOptions,
) -> Result<Interpreter<'a>> {
    let mut compiler = Compiler {
        num_items,
        arrays,
        buffers,
        options,
        num_positions: 0,
        program: Vec::new(),
    };
    compiler.compile()?;

    let current_config = CONFIGURATION.read().unwrap().clone();
    if current_config.debug_print_program {
        println!("Program: {program:?}", program = compiler.program);
    }

    Ok(compiler.into_program())
}

struct Compiler<'a> {
    num_items: usize,
    arrays: &'a [ArrayMapping],
    buffers: Buffers<'a>,
    options: CompilationOptions,
    num_positions: usize,
    program: Vec<Bytecode>,
}

impl<'a> Compiler<'a> {
    fn compile(&mut self) -> Result<()> {
        self.compile_fields()?;
        self.fix_redirects()?;
        Ok(())
    }

    fn compile_fields(&mut self) -> Result<()> {
        if !self.options.wrap_with_struct && self.arrays.is_empty() {
            fail!("cannot compile deserialization without any arrays if not wrapped with struct");
        }

        self.push_instr(EmitOuterStartSequence { next: 1 });

        let outer_sequence_count = self.buffers.push_u0(self.num_items);
        let outer_sequence_item_pos = self.program.len();

        let outer_sequence_position = self.new_position();
        self.push_instr(EmitOuterItem {
            next: NEXT_INSTR,
            if_end: UNSET_INSTR,
            position: outer_sequence_position,
            count: outer_sequence_count,
        });

        let outer_sequence_content_pos = self.program.len();
        if self.options.wrap_with_struct {
            self.push_instr(EmitStartOuterStruct { next: NEXT_INSTR });
        }

        let mut child_positions = Vec::new();
        for array in self.arrays {
            if self.options.wrap_with_struct {
                let field = array.get_field();
                let name_buffer = self.buffers.push_u8(field.name.as_bytes());
                self.push_instr(EmitConstantString {
                    next: NEXT_INSTR,
                    buffer: name_buffer,
                });
            }

            self.compile_field(array, &mut child_positions)?;
        }
        // The top-level struct cannot be null
        drop(child_positions);

        if self.options.wrap_with_struct {
            self.push_instr(EmitEndOuterStruct { next: NEXT_INSTR });
        }

        self.push_instr(EmitOuterEndSequence {
            next: NEXT_INSTR,
            position: outer_sequence_position,
            if_item: outer_sequence_content_pos,
            count: outer_sequence_count,
        });

        let end_of_program = self.program.len();
        self.push_instr(EndOfProgram {
            next: end_of_program,
        });

        if let Bytecode::EmitOuterItem(item) = &mut self.program[outer_sequence_item_pos] {
            item.if_end = end_of_program;
        } else {
            fail!("invalid state in compilation")
        }

        Ok(())
    }

    /// Compile a field inside a struct (outermost or any nested struct)
    ///
    fn compile_field(
        &mut self,
        array: &'a ArrayMapping,
        child_positions: &mut Vec<usize>,
    ) -> Result<()> {
        let position = self.new_position();

        let option_instr;
        if let Some(validity) = array.get_validity() {
            option_instr = Some(self.program.len());

            self.push_instr(EmitOptionPrimitive {
                next: NEXT_INSTR,
                position,
                validity,
                positions_to_increment: Vec::new(),
                if_none: usize::MAX,
            });
        } else {
            option_instr = None
        };

        let mut inner_child_positions = vec![position];
        self.compile_field_inner(array, position, &mut inner_child_positions)?;

        child_positions.extend(inner_child_positions.iter().copied());

        if let Some(option_instr) = option_instr {
            let if_none = self.program.len();
            let Some(Bytecode::EmitOptionPrimitive(instr)) = self.program.get_mut(option_instr)
            else {
                unreachable!()
            };
            instr.if_none = if_none;
            instr.positions_to_increment = inner_child_positions;
        }
        Ok(())
    }

    fn compile_field_inner(
        &mut self,
        array: &'a ArrayMapping,
        position: usize,
        child_positions: &mut Vec<usize>,
    ) -> Result<()> {
        use ArrayMapping as M;

        let _ = match array {
            M::Null { .. } => self.push_instr(EmitNull { next: NEXT_INSTR }),
            &M::Bool { buffer, .. } => self.push_instr(EmitBool {
                next: NEXT_INSTR,
                buffer,
                position,
            }),
            &M::U8 { buffer, .. } => self.push_instr(EmitU8 {
                next: NEXT_INSTR,
                buffer,
                position,
            }),
            &M::U16 { buffer, .. } => self.push_instr(EmitU16 {
                next: NEXT_INSTR,
                buffer,
                position,
            }),
            &M::U32 { buffer, .. } => self.push_instr(EmitU32 {
                next: NEXT_INSTR,
                buffer,
                position,
            }),
            &M::U64 { buffer, .. } => self.push_instr(EmitU64 {
                next: NEXT_INSTR,
                buffer,
                position,
            }),
            &M::I8 { buffer, .. } => self.push_instr(EmitI8 {
                next: NEXT_INSTR,
                buffer,
                position,
            }),
            &M::I16 { buffer, .. } => self.push_instr(EmitI16 {
                next: NEXT_INSTR,
                buffer,
                position,
            }),
            &M::I32 { buffer, .. } => self.push_instr(EmitI32 {
                next: NEXT_INSTR,
                buffer,
                position,
            }),
            &M::I64 { buffer, .. } => self.push_instr(EmitI64 {
                next: NEXT_INSTR,
                buffer,
                position,
            }),
            &M::F16 { buffer, .. } => self.push_instr(EmitF16 {
                next: NEXT_INSTR,
                buffer,
                position,
            }),
            &M::F32 { buffer, .. } => self.push_instr(EmitF32 {
                next: NEXT_INSTR,
                buffer,
                position,
            }),
            &M::F64 { buffer, .. } => self.push_instr(EmitF64 {
                next: NEXT_INSTR,
                buffer,
                position,
            }),
            &M::Utf8 {
                buffer, offsets, ..
            } => self.push_instr(EmitStr32 {
                next: NEXT_INSTR,
                buffer,
                offsets,
                position,
            }),
            &M::LargeUtf8 {
                buffer, offsets, ..
            } => self.push_instr(EmitStr64 {
                next: NEXT_INSTR,
                buffer,
                offsets,
                position,
            }),
            &M::Dictionary {
                dictionary,
                indices,
                ..
            } => self.push_instr(EmitDictionaryStr {
                next: NEXT_INSTR,
                position,
                value: dictionary,
                index: indices,
            }),
            M::Date64 { field, buffer, .. } => match field.strategy.as_ref() {
                Some(Strategy::NaiveStrAsDate64) => self.push_instr(EmitDate64NaiveStr {
                    next: NEXT_INSTR,
                    buffer: *buffer,
                    position,
                }),
                Some(Strategy::UtcStrAsDate64) => self.push_instr(EmitDate64UtcStr {
                    next: NEXT_INSTR,
                    buffer: *buffer,
                    position,
                }),
                None => self.push_instr(EmitI64 {
                    next: NEXT_INSTR,
                    buffer: *buffer,
                    position,
                }),
                Some(strategy) => {
                    fail!("compilation of date64 with strategy {strategy} is not yet supported")
                }
            },
            M::List { item, offsets, .. } => self
                .compile_list(item, position, *offsets, false)
                .map(|_| 0)?,
            M::LargeList { item, offsets, .. } => self
                .compile_list(item, position, *offsets, true)
                .map(|_| 0)?,
            M::Struct { field, fields, .. } => match field.strategy.as_ref() {
                None => self
                    .compile_struct(fields, position, child_positions)
                    .map(|_| 0)?,
                Some(Strategy::TupleAsStruct) => self
                    .compile_tuple_struct(fields, position, child_positions)
                    .map(|_| 0)?,
                Some(Strategy::MapAsStruct) => self
                    .compile_map_struct(fields, position, child_positions)
                    .map(|_| 0)?,
                Some(strategy) => {
                    fail!("compilation of structs with strategy {strategy} is not yet supported")
                }
            },
            M::Map {
                offsets, entries, ..
            } => {
                let M::Struct {
                    fields: entries_fields,
                    ..
                } = entries.as_ref()
                else {
                    fail!("cannot extract entries arrays mapping")
                };
                let Some(key_field) = entries_fields.first() else {
                    fail!("cannot extract key field")
                };
                let Some(values_field) = entries_fields.get(1) else {
                    fail!("cannot extract values field")
                };
                // TODO: check that keys and values are truly non-nullable
                self.compile_map(key_field, values_field, position, *offsets)
                    .map(|_| 0)?
            }
            M::Union { fields, types, .. } => {
                self.compile_union(fields, position, *types).map(|_| 0)?
            }
        };
        Ok(())
    }
}

/// List support
impl<'a> Compiler<'a> {
    fn compile_list(
        &mut self,
        item: &'a ArrayMapping,
        position: usize,
        offsets: usize,
        is_large: bool,
    ) -> Result<()> {
        let inner_position = self.new_position();
        let emit_start_instr = self.push_instr(EmitStartSequence {
            next: NEXT_INSTR,
            if_end: UNSET_INSTR,
            position,
            inner_position,
            offsets,
            is_large,
        });

        let if_item_instr = self.program.len() + 1;
        let emit_item_instr = self.push_instr(EmitItemSequence {
            next: NEXT_INSTR,
            if_end: UNSET_INSTR,
            position,
            inner_position,
            offsets,
            is_large,
        });

        let mut child_positions = Vec::new();
        self.compile_field(item, &mut child_positions)?;
        drop(child_positions);

        let if_end_instr = self.program.len() + 1;
        self.push_instr(EmitEndSequence {
            next: NEXT_INSTR,
            if_item: if_item_instr,
            position,
            inner_position,
            offsets,
            is_large,
        });

        if let Some(Bytecode::EmitItemSequence(instr)) = self.program.get_mut(emit_item_instr) {
            instr.if_end = if_end_instr;
        } else {
            fail!("invalid state during compilation");
        }
        if let Some(Bytecode::EmitStartSequence(instr)) = self.program.get_mut(emit_start_instr) {
            instr.if_end = if_end_instr;
        } else {
            fail!("invalid state during compilation");
        }

        Ok(())
    }
}

/// Struct support
impl<'a> Compiler<'a> {
    fn compile_struct(
        &mut self,
        arrays: &'a [ArrayMapping],
        position: usize,
        child_positions: &mut Vec<usize>,
    ) -> Result<()> {
        self.push_instr(EmitStartStruct { next: NEXT_INSTR });

        for array in arrays {
            let field = array.get_field();
            let name_buffer = self.buffers.push_u8(field.name.as_bytes());
            self.push_instr(EmitConstantString {
                next: NEXT_INSTR,
                buffer: name_buffer,
            });

            self.compile_field(array, child_positions)?;
        }

        self.push_instr(EmitEndStruct {
            next: NEXT_INSTR,
            position,
        });
        Ok(())
    }

    fn compile_tuple_struct(
        &mut self,
        arrays: &'a [ArrayMapping],
        position: usize,
        child_positions: &mut Vec<usize>,
    ) -> Result<()> {
        self.push_instr(EmitStartTuple { next: NEXT_INSTR });

        for array in arrays {
            self.push_instr(EmitItemTuple { next: NEXT_INSTR });
            self.compile_field(array, child_positions)?;
        }

        self.push_instr(EmitEndTuple {
            next: NEXT_INSTR,
            position,
        });
        Ok(())
    }

    fn compile_map_struct(
        &mut self,
        arrays: &'a [ArrayMapping],
        position: usize,
        child_positions: &mut Vec<usize>,
    ) -> Result<()> {
        self.push_instr(EmitStartMapStruct { next: NEXT_INSTR });

        for array in arrays {
            let field = array.get_field();
            let name_buffer = self.buffers.push_u8(field.name.as_bytes());
            self.push_instr(EmitConstantString {
                next: NEXT_INSTR,
                buffer: name_buffer,
            });
            self.compile_field(array, child_positions)?;
        }

        self.push_instr(EmitEndMapStruct {
            next: NEXT_INSTR,
            position,
        });
        Ok(())
    }
}

/// Map support
impl<'a> Compiler<'a> {
    fn compile_map(
        &mut self,
        key_field: &'a ArrayMapping,
        value_field: &'a ArrayMapping,
        position: usize,
        offsets: usize,
    ) -> Result<()> {
        let inner_position = self.new_position();

        let emit_start_instr = self.push_instr(EmitStartMap {
            next: NEXT_INSTR,
            if_end: UNSET_INSTR,
            position,
            inner_position,
            offsets,
        });

        let if_item_instr = self.program.len() + 1;
        let emit_item_instr = self.push_instr(EmitItemMap {
            next: NEXT_INSTR,
            if_end: UNSET_INSTR,
            position,
            inner_position,
            offsets,
        });

        let mut dummy_positions = Vec::new();

        let keys_position = self.new_position();
        self.compile_field_inner(key_field, keys_position, &mut dummy_positions)?;

        let values_position = self.new_position();
        self.compile_field_inner(value_field, values_position, &mut dummy_positions)?;

        // null maps entries with non-empty segments are not supported
        drop(dummy_positions);

        let if_end_instr = self.program.len() + 1;
        self.push_instr(EmitEndMap {
            next: NEXT_INSTR,
            if_item: if_item_instr,
            position,
            inner_position,
            offsets,
        });

        if let Some(Bytecode::EmitItemMap(instr)) = self.program.get_mut(emit_item_instr) {
            instr.if_end = if_end_instr;
        } else {
            fail!("invalid state during compilation");
        }
        if let Some(Bytecode::EmitStartMap(instr)) = self.program.get_mut(emit_start_instr) {
            instr.if_end = if_end_instr;
        } else {
            fail!("invalid state during compilation");
        }

        Ok(())
    }
}

/// Union support
impl<'a> Compiler<'a> {
    fn compile_union(
        &mut self,
        fields: &'a [ArrayMapping],
        position: usize,
        types: usize,
    ) -> Result<()> {
        let mut field_names = Vec::new();
        for field in fields {
            let idx = self.buffers.push_u8(field.get_field().name.as_bytes());
            field_names.push(idx);
        }
        let dispatch_instr = self.push_instr(UnionDispatch {
            next: UNSET_INSTR,
            field_instr: Vec::new(),
            position,
            types,
            field_names,
        });

        let mut field_instr = Vec::new();
        let mut redirect_instrs = Vec::new();

        for field in fields {
            field_instr.push(self.program.len());

            let mut dummy_positions = Vec::new();
            self.compile_field(field, &mut dummy_positions)?;
            // unions in nullable structs are currently not supported
            drop(dummy_positions);

            let redirect_instr = self.push_instr(Redirect { next: UNSET_INSTR });
            redirect_instrs.push(redirect_instr);
        }

        let Some(Bytecode::UnionDispatch(instr)) = self.program.get_mut(dispatch_instr) else {
            fail!("internal error: did not find union dispatch")
        };
        instr.field_instr = field_instr;

        let instr_after_union = self.program.len();
        for redirect_instr in redirect_instrs {
            let Some(Bytecode::Redirect(instr)) = self.program.get_mut(redirect_instr) else {
                fail!("internal error: did not find redirect");
            };
            instr.next = instr_after_union;
        }

        Ok(())
    }

    fn fix_redirects(&mut self) -> Result<()> {
        let mut redirect_map = HashMap::new();
        for (idx, instr) in self.program.iter().enumerate() {
            if let Bytecode::Redirect(instr) = instr {
                redirect_map.insert(idx, instr.next);
            }
        }

        let mut multi_hop_map = HashMap::new();
        for (&start, &end) in &redirect_map {
            let mut budget = self.program.len();

            let mut end = end;
            while let Some(&next) = redirect_map.get(&end) {
                if budget == 0 {
                    fail!("budget exceed while following redirect: cycle?")
                }
                end = next;
                budget -= 1;
            }

            multi_hop_map.insert(start, end);
        }

        for instr in &mut self.program {
            instr.update_targets(&multi_hop_map)?;
        }

        Ok(())
    }
}

/// Utility functions
impl<'a> Compiler<'a> {
    fn push_instr<I: Into<Bytecode>>(&mut self, instr: I) -> usize {
        let instr_idx = self.program.len();

        let mut instr = instr.into();
        dispatch_bytecode!(&mut instr, instr => {
            if instr.next == NEXT_INSTR {
                instr.next = instr_idx + 1;
            }
        });

        self.program.push(instr);
        instr_idx
    }

    fn new_position(&mut self) -> usize {
        self.num_positions += 1;
        self.num_positions - 1
    }

    fn into_program(self) -> Interpreter<'a> {
        Interpreter {
            current_instr: 0,
            program: self.program,
            positions: vec![0; self.num_positions],
            buffers: self.buffers,
        }
    }
}

#[rustfmt::skip]
define_bytecode!{
    /// A dummy instruction used mark jump positions which should be rewritten
    /// to the next value of this instruction 
    Redirect {},
    EmitOuterStartSequence {},
    /// Handle the end-of-sequence / item case
    EmitOuterEndSequence {
        position: usize,
        /// the instruction to jump to if the list is not yet at its end
        if_item: usize,
        /// the buffer that contains the number of items in this sequence
        count: usize,
    },
    EmitOuterItem {
        position: usize,
        /// the instruction to jump to if the list is at its end
        if_end: usize,
        /// the buffer that contains the number of items in this sequence
        count: usize,
    },
    EmitStartSequence {
        /// the position inside the offsets array
        position: usize,
        /// the instruction to jump to if the list is at its end
        if_end: usize,
        /// the position inside the overall items
        inner_position: usize,
        /// the buffer that contains the offsets
        offsets: usize,
        /// whether to use i64 offsets (`true`) or i32 offsets (`false )`
        is_large: bool,
    },
    /// Handle the end-of-sequence / item case
    EmitEndSequence {
        /// the position inside the offsets array
        position: usize,
        /// the position inside the overall items
        inner_position: usize,
        /// the instruction to jump to if the list is not yet at its end
        if_item: usize,
        /// the buffer that contains the offsets
        offsets: usize,
        /// whether to use i64 offsets (`true`) or i32 offsets (`false )`
        is_large: bool,
    },
    EmitItemSequence {
        /// the position inside the offsets array
        position: usize,
        /// the position inside the overall items
        inner_position: usize,
        /// the instruction to jump to if the list is at its end
        if_end: usize,
        /// the buffer that contains the number of offsets in this sequence
        offsets: usize,
        /// whether to use i64 offsets (`true`) or i32 offsets (`false )`
        is_large: bool,
    },
    EmitStartMap {
        /// the position inside the offsets array
        position: usize,
        /// the instruction to jump to if the list is at its end
        if_end: usize,
        /// the position inside the overall items
        inner_position: usize,
        /// the buffer that contains the offsets
        offsets: usize,
    },
    /// Handle the end-of-sequence / item case
    EmitEndMap {
        /// the position inside the offsets array
        position: usize,
        /// the position inside the overall items
        inner_position: usize,
        /// the instruction to jump to if the list is not yet at its end
        if_item: usize,
        /// the buffer that contains the offsets
        offsets: usize,
    },
    EmitItemMap {
        /// the position inside the offsets array
        position: usize,
        /// the position inside the overall items
        inner_position: usize,
        /// the instruction to jump to if the list is at its end
        if_end: usize,
        /// the buffer that contains the number of offsets in this sequence
        offsets: usize,
    },
    EmitStartOuterStruct {},
    EmitEndOuterStruct {},
    EmitStartStruct {},
    EmitEndStruct {
        position: usize,
    },
    EmitStartTuple {},
    EmitEndTuple {
        position: usize,
    },
    EmitStartMapStruct {},
    EmitEndMapStruct {
        position: usize,
    },
    EmitItemTuple {},
    EmitConstantString{
        buffer: usize,
    },
    EndOfProgram {},
    /// Emit nullability information for a primitive type
    /// 
    /// This instruction increases the primitives positions in case of null.
    EmitOptionPrimitive {
        /// The index of the position counter for the validity
        position: usize,
        /// The index of the u1 buffer containing the validity 
        validity: usize,
        /// The instruction to jump to, if the validity is false 
        if_none: usize,
        /// The indices of the position counters to increment if none
        positions_to_increment: Vec<usize>,
    },
    EmitNull {},
    EmitBool {
        position: usize,
        buffer: usize,
    },
    EmitU8 {
        position: usize,
        buffer: usize,
    },
    EmitU16 {
        position: usize,
        buffer: usize,
    },
    EmitU32 {
        position: usize,
        buffer: usize,
    },
    EmitU64 {
        position: usize,
        buffer: usize,
    },
    EmitI8 {
        position: usize,
        buffer: usize,
    },
    EmitI16 {
        position: usize,
        buffer: usize,
    },
    EmitI32 {
        position: usize,
        buffer: usize,
    },
    EmitI64 {
        position: usize,
        buffer: usize,
    },
    EmitF16 {
        position: usize,
        buffer: usize,
    },
    EmitF32 {
        position: usize,
        buffer: usize,
    },
    EmitF64 {
        position: usize,
        buffer: usize,
    },
    EmitStr32 {
        position: usize,
        buffer: usize,
        offsets: usize,
    },
    EmitStr64 {
        position: usize,
        buffer: usize,
        offsets: usize,
    },
    EmitDate64NaiveStr {
        position: usize,
        buffer: usize,
    },
    EmitDate64UtcStr {
        position: usize,
        buffer: usize,
    },
    EmitDictionaryStr {
        position: usize,
        value: DictionaryValue,
        index: DictionaryIndex,
    },
    UnionDispatch {
        position: usize,
        types: usize,
        field_instr: Vec<usize>,
        field_names: Vec<usize>,
    },
}

trait Instruction: std::fmt::Debug {
    #[allow(unused_variables)]
    fn emit<'a>(
        &self,
        positions: &mut [usize],
        buffers: &Buffers<'a>,
    ) -> Result<(usize, Option<Event<'a>>)> {
        fail!("not implemented for {self:?} (positions: {positions:?})")
    }

    fn update_targets(&mut self, redirects: &HashMap<usize, usize>) -> Result<()>;
}

impl Instruction for Bytecode {
    fn emit<'a>(
        &self,
        positions: &mut [usize],
        buffers: &Buffers<'a>,
    ) -> Result<(usize, Option<Event<'a>>)> {
        dispatch_bytecode!(self, instr => instr.emit(positions, buffers))
    }

    fn update_targets(&mut self, redirects: &HashMap<usize, usize>) -> Result<()> {
        dispatch_bytecode!(self, instr => instr.update_targets(redirects))
    }
}

impl Instruction for Redirect {
    fn emit<'a>(
        &self,
        _positions: &mut [usize],
        _buffers: &Buffers<'a>,
    ) -> Result<(usize, Option<Event<'a>>)> {
        fail!(concat!(
            "internal error: redirect reached in execution. ",
            "This should not happen. Please open an issue.",
        ))
    }

    fn update_targets(&mut self, _redirects: &HashMap<usize, usize>) -> Result<()> {
        Ok(())
    }
}

impl Instruction for EmitOuterStartSequence {
    fn emit<'a>(
        &self,
        _positions: &mut [usize],
        _buffers: &Buffers<'a>,
    ) -> Result<(usize, Option<Event<'a>>)> {
        Ok((self.next, Some(Event::StartSequence)))
    }

    fn update_targets(&mut self, redirects: &HashMap<usize, usize>) -> Result<()> {
        self.next = get_target_update(redirects, self.next);
        Ok(())
    }
}

impl Instruction for EmitOuterItem {
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

    fn update_targets(&mut self, redirects: &HashMap<usize, usize>) -> Result<()> {
        self.next = get_target_update(redirects, self.next);
        self.if_end = get_target_update(redirects, self.if_end);
        Ok(())
    }
}

impl Instruction for EmitOuterEndSequence {
    fn emit<'a>(
        &self,
        positions: &mut [usize],
        buffers: &Buffers<'a>,
    ) -> Result<(usize, Option<Event<'a>>)> {
        if positions[self.position] >= buffers.u0[self.count] {
            Ok((self.next, Some(Event::EndSequence)))
        } else {
            positions[self.position] += 1;
            Ok((self.if_item, Some(Event::Item)))
        }
    }

    fn update_targets(&mut self, redirects: &HashMap<usize, usize>) -> Result<()> {
        self.next = get_target_update(redirects, self.next);
        self.if_item = get_target_update(redirects, self.if_item);
        Ok(())
    }
}

impl Instruction for EmitStartOuterStruct {
    fn emit<'a>(
        &self,
        _positions: &mut [usize],
        _buffers: &Buffers<'a>,
    ) -> Result<(usize, Option<Event<'a>>)> {
        Ok((self.next, Some(Event::StartStruct)))
    }

    fn update_targets(&mut self, redirects: &HashMap<usize, usize>) -> Result<()> {
        self.next = get_target_update(redirects, self.next);
        Ok(())
    }
}

impl Instruction for EmitEndOuterStruct {
    fn emit<'a>(
        &self,
        _positions: &mut [usize],
        _buffers: &Buffers<'a>,
    ) -> Result<(usize, Option<Event<'a>>)> {
        Ok((self.next, Some(Event::EndStruct)))
    }

    fn update_targets(&mut self, redirects: &HashMap<usize, usize>) -> Result<()> {
        self.next = get_target_update(redirects, self.next);
        Ok(())
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

    fn update_targets(&mut self, redirects: &HashMap<usize, usize>) -> Result<()> {
        self.next = get_target_update(redirects, self.next);
        Ok(())
    }
}

impl Instruction for EmitEndStruct {
    fn emit<'a>(
        &self,
        positions: &mut [usize],
        _buffers: &Buffers<'a>,
    ) -> Result<(usize, Option<Event<'a>>)> {
        positions[self.position] += 1;
        Ok((self.next, Some(Event::EndStruct)))
    }

    fn update_targets(&mut self, redirects: &HashMap<usize, usize>) -> Result<()> {
        self.next = get_target_update(redirects, self.next);
        Ok(())
    }
}

impl Instruction for EmitStartMapStruct {
    fn emit<'a>(
        &self,
        _positions: &mut [usize],
        _buffers: &Buffers<'a>,
    ) -> Result<(usize, Option<Event<'a>>)> {
        Ok((self.next, Some(Event::StartMap)))
    }

    fn update_targets(&mut self, redirects: &HashMap<usize, usize>) -> Result<()> {
        self.next = get_target_update(redirects, self.next);
        Ok(())
    }
}

impl Instruction for EmitEndMapStruct {
    fn emit<'a>(
        &self,
        positions: &mut [usize],
        _buffers: &Buffers<'a>,
    ) -> Result<(usize, Option<Event<'a>>)> {
        positions[self.position] += 1;
        Ok((self.next, Some(Event::EndMap)))
    }

    fn update_targets(&mut self, redirects: &HashMap<usize, usize>) -> Result<()> {
        self.next = get_target_update(redirects, self.next);
        Ok(())
    }
}

impl Instruction for EmitStartTuple {
    fn emit<'a>(
        &self,
        _positions: &mut [usize],
        _buffers: &Buffers<'a>,
    ) -> Result<(usize, Option<Event<'a>>)> {
        Ok((self.next, Some(Event::StartTuple)))
    }

    fn update_targets(&mut self, redirects: &HashMap<usize, usize>) -> Result<()> {
        self.next = get_target_update(redirects, self.next);
        Ok(())
    }
}

impl Instruction for EmitItemTuple {
    fn emit<'a>(
        &self,
        _positions: &mut [usize],
        _buffers: &Buffers<'a>,
    ) -> Result<(usize, Option<Event<'a>>)> {
        Ok((self.next, Some(Event::Item)))
    }

    fn update_targets(&mut self, redirects: &HashMap<usize, usize>) -> Result<()> {
        self.next = get_target_update(redirects, self.next);
        Ok(())
    }
}

impl Instruction for EmitEndTuple {
    fn emit<'a>(
        &self,
        positions: &mut [usize],
        _buffers: &Buffers<'a>,
    ) -> Result<(usize, Option<Event<'a>>)> {
        positions[self.position] += 1;
        Ok((self.next, Some(Event::EndTuple)))
    }

    fn update_targets(&mut self, redirects: &HashMap<usize, usize>) -> Result<()> {
        self.next = get_target_update(redirects, self.next);
        Ok(())
    }
}

impl Instruction for EmitStartSequence {
    fn emit<'a>(
        &self,
        positions: &mut [usize],
        buffers: &Buffers<'a>,
    ) -> Result<(usize, Option<Event<'a>>)> {
        let outer_pos = positions[self.position];

        let start: usize = if self.is_large {
            buffers
                .get_i64(self.offsets)
                .get(outer_pos)
                .copied()
                .ok_or_else(|| error!("attempting to to get non existing list"))?
                .try_into()?
        } else {
            buffers
                .get_i32(self.offsets)
                .get(outer_pos)
                .copied()
                .ok_or_else(|| error!("attempting to to get non existing list"))?
                .try_into()?
        };

        positions[self.inner_position] = start;

        Ok((self.next, Some(Event::StartSequence)))
    }

    fn update_targets(&mut self, redirects: &HashMap<usize, usize>) -> Result<()> {
        self.next = get_target_update(redirects, self.next);
        Ok(())
    }
}

impl Instruction for EmitItemSequence {
    fn emit<'a>(
        &self,
        positions: &mut [usize],
        buffers: &Buffers<'a>,
    ) -> Result<(usize, Option<Event<'a>>)> {
        let outer_pos = positions[self.position];

        let end: usize = if self.is_large {
            buffers
                .get_i64(self.offsets)
                .get(outer_pos + 1)
                .copied()
                .ok_or_else(|| error!("Cannot get offset"))?
                .try_into()?
        } else {
            buffers
                .get_i32(self.offsets)
                .get(outer_pos + 1)
                .copied()
                .ok_or_else(|| error!("Cannot get offset"))?
                .try_into()?
        };

        let inner_pos = positions[self.inner_position];
        if inner_pos >= end {
            positions[self.position] += 1;
            Ok((self.if_end, Some(Event::EndSequence)))
        } else {
            positions[self.inner_position] += 1;
            Ok((self.next, Some(Event::Item)))
        }
    }

    fn update_targets(&mut self, redirects: &HashMap<usize, usize>) -> Result<()> {
        self.next = get_target_update(redirects, self.next);
        self.if_end = get_target_update(redirects, self.if_end);
        Ok(())
    }
}

impl Instruction for EmitEndSequence {
    fn emit<'a>(
        &self,
        positions: &mut [usize],
        buffers: &Buffers<'a>,
    ) -> Result<(usize, Option<Event<'a>>)> {
        let outer_pos = positions[self.position];

        let end: usize = if self.is_large {
            buffers
                .get_i64(self.offsets)
                .get(outer_pos + 1)
                .copied()
                .ok_or_else(|| error!("Cannot get offset"))?
                .try_into()?
        } else {
            buffers
                .get_i32(self.offsets)
                .get(outer_pos + 1)
                .copied()
                .ok_or_else(|| error!("Cannot get offset"))?
                .try_into()?
        };

        let inner_pos = positions[self.inner_position];
        if inner_pos >= end {
            positions[self.position] += 1;
            Ok((self.next, Some(Event::EndSequence)))
        } else {
            positions[self.inner_position] += 1;
            Ok((self.if_item, Some(Event::Item)))
        }
    }

    fn update_targets(&mut self, redirects: &HashMap<usize, usize>) -> Result<()> {
        self.next = get_target_update(redirects, self.next);
        self.if_item = get_target_update(redirects, self.if_item);
        Ok(())
    }
}

impl Instruction for EmitStartMap {
    fn emit<'a>(
        &self,
        positions: &mut [usize],
        buffers: &Buffers<'a>,
    ) -> Result<(usize, Option<Event<'a>>)> {
        let outer_pos = positions[self.position];

        positions[self.inner_position] = buffers
            .get_i32(self.offsets)
            .get(outer_pos)
            .copied()
            .ok_or_else(|| error!("attempting to to get non existing list"))?
            .try_into()?;

        Ok((self.next, Some(Event::StartMap)))
    }

    fn update_targets(&mut self, redirects: &HashMap<usize, usize>) -> Result<()> {
        self.next = get_target_update(redirects, self.next);
        Ok(())
    }
}

impl Instruction for EmitItemMap {
    fn emit<'a>(
        &self,
        positions: &mut [usize],
        buffers: &Buffers<'a>,
    ) -> Result<(usize, Option<Event<'a>>)> {
        let outer_pos = positions[self.position];

        let end: usize = buffers
            .get_i32(self.offsets)
            .get(outer_pos + 1)
            .copied()
            .ok_or_else(|| error!("Cannot get offset"))?
            .try_into()?;

        let inner_pos = positions[self.inner_position];
        if inner_pos >= end {
            positions[self.position] += 1;
            Ok((self.if_end, Some(Event::EndMap)))
        } else {
            positions[self.inner_position] += 1;
            Ok((self.next, Some(Event::Item)))
        }
    }

    fn update_targets(&mut self, redirects: &HashMap<usize, usize>) -> Result<()> {
        self.next = get_target_update(redirects, self.next);
        self.if_end = get_target_update(redirects, self.if_end);
        Ok(())
    }
}

impl Instruction for EmitEndMap {
    fn emit<'a>(
        &self,
        positions: &mut [usize],
        buffers: &Buffers<'a>,
    ) -> Result<(usize, Option<Event<'a>>)> {
        let outer_pos = positions[self.position];

        let end: usize = buffers
            .get_i32(self.offsets)
            .get(outer_pos + 1)
            .copied()
            .ok_or_else(|| error!("Cannot get offset"))?
            .try_into()?;

        let inner_pos = positions[self.inner_position];
        if inner_pos >= end {
            positions[self.position] += 1;
            Ok((self.next, Some(Event::EndMap)))
        } else {
            positions[self.inner_position] += 1;
            Ok((self.if_item, Some(Event::Item)))
        }
    }

    fn update_targets(&mut self, redirects: &HashMap<usize, usize>) -> Result<()> {
        self.next = get_target_update(redirects, self.next);
        self.if_item = get_target_update(redirects, self.if_item);
        Ok(())
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

    fn update_targets(&mut self, redirects: &HashMap<usize, usize>) -> Result<()> {
        self.next = get_target_update(redirects, self.next);
        Ok(())
    }
}

impl Instruction for EmitOptionPrimitive {
    fn emit<'a>(
        &self,
        positions: &mut [usize],
        buffers: &Buffers<'a>,
    ) -> Result<(usize, Option<Event<'a>>)> {
        let pos = positions[self.position];
        if buffers.u1[self.validity].is_set(pos) {
            Ok((self.next, Some(Event::Some)))
        } else {
            for idx in &self.positions_to_increment {
                positions[*idx] += 1;
            }
            Ok((self.if_none, Some(Event::Null)))
        }
    }

    fn update_targets(&mut self, redirects: &HashMap<usize, usize>) -> Result<()> {
        self.next = get_target_update(redirects, self.next);
        self.if_none = get_target_update(redirects, self.if_none);
        Ok(())
    }
}

impl Instruction for UnionDispatch {
    fn emit<'a>(
        &self,
        positions: &mut [usize],
        buffers: &Buffers<'a>,
    ) -> Result<(usize, Option<Event<'a>>)> {
        let pos = positions[self.position];
        positions[self.position] += 1;

        let ty: usize = buffers.get_i8(self.types)[pos].try_into()?;
        let name = buffers.get_u8(self.field_names[ty]);
        let name = std::str::from_utf8(name)?;

        let next = self.field_instr[ty];

        Ok((next, Some(Event::Variant(name, ty))))
    }

    fn update_targets(&mut self, redirects: &HashMap<usize, usize>) -> Result<()> {
        self.next = get_target_update(redirects, self.next);
        for field_instr in &mut self.field_instr {
            *field_instr = get_target_update(redirects, *field_instr);
        }
        Ok(())
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

    fn update_targets(&mut self, redirects: &HashMap<usize, usize>) -> Result<()> {
        self.next = get_target_update(redirects, self.next);
        Ok(())
    }
}

impl Instruction for EmitNull {
    fn emit<'a>(
        &self,
        _positions: &mut [usize],
        _buffers: &Buffers<'a>,
    ) -> Result<(usize, Option<Event<'a>>)> {
        Ok((self.next, Some(Event::Null)))
    }

    fn update_targets(&mut self, redirects: &HashMap<usize, usize>) -> Result<()> {
        self.next = get_target_update(redirects, self.next);
        Ok(())
    }
}

impl Instruction for EmitBool {
    fn emit<'a>(
        &self,
        positions: &mut [usize],
        buffers: &Buffers<'a>,
    ) -> Result<(usize, Option<Event<'a>>)> {
        let val = buffers.u1[self.buffer].is_set(positions[self.position]);
        positions[self.position] += 1;
        Ok((self.next, Some(Event::Bool(val))))
    }

    fn update_targets(&mut self, redirects: &HashMap<usize, usize>) -> Result<()> {
        self.next = get_target_update(redirects, self.next);
        Ok(())
    }
}

impl Instruction for EmitU8 {
    fn emit<'a>(
        &self,
        positions: &mut [usize],
        buffers: &Buffers<'a>,
    ) -> Result<(usize, Option<Event<'a>>)> {
        let val = buffers.u8[self.buffer][positions[self.position]];
        positions[self.position] += 1;
        Ok((self.next, Some(Event::U8(val))))
    }

    fn update_targets(&mut self, redirects: &HashMap<usize, usize>) -> Result<()> {
        self.next = get_target_update(redirects, self.next);
        Ok(())
    }
}

impl Instruction for EmitU16 {
    fn emit<'a>(
        &self,
        positions: &mut [usize],
        buffers: &Buffers<'a>,
    ) -> Result<(usize, Option<Event<'a>>)> {
        let val = buffers.u16[self.buffer][positions[self.position]];
        positions[self.position] += 1;
        Ok((self.next, Some(Event::U16(val))))
    }

    fn update_targets(&mut self, redirects: &HashMap<usize, usize>) -> Result<()> {
        self.next = get_target_update(redirects, self.next);
        Ok(())
    }
}

impl Instruction for EmitU32 {
    fn emit<'a>(
        &self,
        positions: &mut [usize],
        buffers: &Buffers<'a>,
    ) -> Result<(usize, Option<Event<'a>>)> {
        let val = buffers.u32[self.buffer][positions[self.position]];
        positions[self.position] += 1;
        Ok((self.next, Some(Event::U32(val))))
    }

    fn update_targets(&mut self, redirects: &HashMap<usize, usize>) -> Result<()> {
        self.next = get_target_update(redirects, self.next);
        Ok(())
    }
}

impl Instruction for EmitU64 {
    fn emit<'a>(
        &self,
        positions: &mut [usize],
        buffers: &Buffers<'a>,
    ) -> Result<(usize, Option<Event<'a>>)> {
        let val = buffers.u64[self.buffer][positions[self.position]];
        positions[self.position] += 1;
        Ok((self.next, Some(Event::U64(val))))
    }

    fn update_targets(&mut self, redirects: &HashMap<usize, usize>) -> Result<()> {
        self.next = get_target_update(redirects, self.next);
        Ok(())
    }
}

impl Instruction for EmitI8 {
    fn emit<'a>(
        &self,
        positions: &mut [usize],
        buffers: &Buffers<'a>,
    ) -> Result<(usize, Option<Event<'a>>)> {
        let val =
            i8::from_ne_bytes(buffers.u8[self.buffer][positions[self.position]].to_ne_bytes());
        positions[self.position] += 1;
        Ok((self.next, Some(Event::I8(val))))
    }

    fn update_targets(&mut self, redirects: &HashMap<usize, usize>) -> Result<()> {
        self.next = get_target_update(redirects, self.next);
        Ok(())
    }
}

impl Instruction for EmitI16 {
    fn emit<'a>(
        &self,
        positions: &mut [usize],
        buffers: &Buffers<'a>,
    ) -> Result<(usize, Option<Event<'a>>)> {
        let val =
            i16::from_ne_bytes(buffers.u16[self.buffer][positions[self.position]].to_ne_bytes());
        positions[self.position] += 1;
        Ok((self.next, Some(Event::I16(val))))
    }

    fn update_targets(&mut self, redirects: &HashMap<usize, usize>) -> Result<()> {
        self.next = get_target_update(redirects, self.next);
        Ok(())
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

    fn update_targets(&mut self, redirects: &HashMap<usize, usize>) -> Result<()> {
        self.next = get_target_update(redirects, self.next);
        Ok(())
    }
}

impl Instruction for EmitI64 {
    fn emit<'a>(
        &self,
        positions: &mut [usize],
        buffers: &Buffers<'a>,
    ) -> Result<(usize, Option<Event<'a>>)> {
        let val =
            i64::from_ne_bytes(buffers.u64[self.buffer][positions[self.position]].to_ne_bytes());
        positions[self.position] += 1;
        Ok((self.next, Some(Event::I64(val))))
    }

    fn update_targets(&mut self, redirects: &HashMap<usize, usize>) -> Result<()> {
        self.next = get_target_update(redirects, self.next);
        Ok(())
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

    fn update_targets(&mut self, redirects: &HashMap<usize, usize>) -> Result<()> {
        self.next = get_target_update(redirects, self.next);
        Ok(())
    }
}

impl Instruction for EmitF32 {
    fn emit<'a>(
        &self,
        positions: &mut [usize],
        buffers: &Buffers<'a>,
    ) -> Result<(usize, Option<Event<'a>>)> {
        let val =
            f32::from_ne_bytes(buffers.u32[self.buffer][positions[self.position]].to_ne_bytes());
        positions[self.position] += 1;
        Ok((self.next, Some(Event::F32(val))))
    }

    fn update_targets(&mut self, redirects: &HashMap<usize, usize>) -> Result<()> {
        self.next = get_target_update(redirects, self.next);
        Ok(())
    }
}

impl Instruction for EmitF64 {
    fn emit<'a>(
        &self,
        positions: &mut [usize],
        buffers: &Buffers<'a>,
    ) -> Result<(usize, Option<Event<'a>>)> {
        let val =
            f64::from_ne_bytes(buffers.u64[self.buffer][positions[self.position]].to_ne_bytes());
        positions[self.position] += 1;
        Ok((self.next, Some(Event::F64(val))))
    }

    fn update_targets(&mut self, redirects: &HashMap<usize, usize>) -> Result<()> {
        self.next = get_target_update(redirects, self.next);
        Ok(())
    }
}

impl Instruction for EmitStr32 {
    fn emit<'a>(
        &self,
        positions: &mut [usize],
        buffers: &Buffers<'a>,
    ) -> Result<(usize, Option<Event<'a>>)> {
        let pos = positions[self.position];
        positions[self.position] += 1;

        let start = usize::try_from(buffers.get_i32(self.offsets)[pos])?;
        let end = usize::try_from(buffers.get_i32(self.offsets)[pos + 1])?;
        let s = std::str::from_utf8(&buffers.u8[self.buffer][start..end])?;
        Ok((self.next, Some(Event::Str(s))))
    }

    fn update_targets(&mut self, redirects: &HashMap<usize, usize>) -> Result<()> {
        self.next = get_target_update(redirects, self.next);
        Ok(())
    }
}

impl Instruction for EmitStr64 {
    fn emit<'a>(
        &self,
        positions: &mut [usize],
        buffers: &Buffers<'a>,
    ) -> Result<(usize, Option<Event<'a>>)> {
        let pos = positions[self.position];
        positions[self.position] += 1;

        let start = usize::try_from(buffers.get_i64(self.offsets)[pos])?;
        let end = usize::try_from(buffers.get_i64(self.offsets)[pos + 1])?;
        let s = std::str::from_utf8(&buffers.u8[self.buffer][start..end])?;
        Ok((self.next, Some(Event::Str(s))))
    }

    fn update_targets(&mut self, redirects: &HashMap<usize, usize>) -> Result<()> {
        self.next = get_target_update(redirects, self.next);
        Ok(())
    }
}

impl Instruction for EmitDate64NaiveStr {
    fn emit<'a>(
        &self,
        positions: &mut [usize],
        buffers: &Buffers<'a>,
    ) -> Result<(usize, Option<Event<'a>>)> {
        use chrono::NaiveDateTime;

        let val =
            i64::from_ne_bytes(buffers.u64[self.buffer][positions[self.position]].to_ne_bytes());
        positions[self.position] += 1;

        // TODO: update with chrono 0.5
        #[allow(deprecated)]
        let val = NaiveDateTime::from_timestamp(val / 1000, (val % 1000) as u32 * 100_000);

        // NOTE: chrono documents that Debug, not Display, can be parsed
        Ok((self.next, Some(format!("{:?}", val).into())))
    }

    fn update_targets(&mut self, redirects: &HashMap<usize, usize>) -> Result<()> {
        self.next = get_target_update(redirects, self.next);
        Ok(())
    }
}

impl Instruction for EmitDate64UtcStr {
    fn emit<'a>(
        &self,
        positions: &mut [usize],
        buffers: &Buffers<'a>,
    ) -> Result<(usize, Option<Event<'a>>)> {
        use chrono::{TimeZone, Utc};

        let val =
            i64::from_ne_bytes(buffers.u64[self.buffer][positions[self.position]].to_ne_bytes());
        positions[self.position] += 1;

        // TODO: update with chrono 0.5
        #[allow(deprecated)]
        let val = Utc.timestamp(val / 1000, (val % 1000) as u32 * 100_000);

        // NOTE: chrono documents that Debug, not Display, can be parsed
        Ok((self.next, Some(format!("{:?}", val).into())))
    }

    fn update_targets(&mut self, redirects: &HashMap<usize, usize>) -> Result<()> {
        self.next = get_target_update(redirects, self.next);
        Ok(())
    }
}

impl Instruction for EmitDictionaryStr {
    fn emit<'a>(
        &self,
        positions: &mut [usize],
        buffers: &Buffers<'a>,
    ) -> Result<(usize, Option<Event<'a>>)> {
        use {DictionaryIndex as I, DictionaryValue as V};

        let pos = positions[self.position];
        positions[self.position] += 1;

        let index: usize = match self.index {
            I::U8(buffer) => buffers.get_u8(buffer)[pos].into(),
            I::U16(buffer) => buffers.get_u16(buffer)[pos].into(),
            I::U32(buffer) => buffers.get_u32(buffer)[pos].try_into()?,
            I::U64(buffer) => buffers.get_u64(buffer)[pos].try_into()?,
            I::I8(buffer) => buffers.get_i8(buffer)[pos].try_into()?,
            I::I16(buffer) => buffers.get_i16(buffer)[pos].try_into()?,
            I::I32(buffer) => buffers.get_i32(buffer)[pos].try_into()?,
            I::I64(buffer) => buffers.get_i64(buffer)[pos].try_into()?,
        };

        match self.value {
            V::Utf8 { buffer, offsets } => {
                let start = usize::try_from(buffers.get_i32(offsets)[index])?;
                let end = usize::try_from(buffers.get_i32(offsets)[index + 1])?;
                let s = std::str::from_utf8(&buffers.u8[buffer][start..end])?;
                Ok((self.next, Some(Event::Str(s))))
            }
            V::LargeUtf8 { buffer, offsets } => {
                let start = usize::try_from(buffers.get_i64(offsets)[index])?;
                let end = usize::try_from(buffers.get_i64(offsets)[index + 1])?;
                let s = std::str::from_utf8(&buffers.u8[buffer][start..end])?;
                Ok((self.next, Some(Event::Str(s))))
            }
        }
    }

    fn update_targets(&mut self, redirects: &HashMap<usize, usize>) -> Result<()> {
        self.next = get_target_update(redirects, self.next);
        Ok(())
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

fn get_target_update(redirects: &HashMap<usize, usize>, instr: usize) -> usize {
    redirects.get(&instr).copied().unwrap_or(instr)
}
