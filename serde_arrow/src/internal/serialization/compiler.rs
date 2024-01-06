use crate::internal::{
    common::{ArrayMapping, DictionaryIndex, DictionaryValue},
    config::CONFIGURATION,
    error::Result,
    error::{error, fail},
    schema::{GenericDataType, GenericField, GenericTimeUnit, Strategy},
};

use super::{
    bit_set::BitSet,
    bytecode::{
        Bytecode, LargeListEnd, LargeListItem, LargeListStart, ListEnd, ListItem, ListStart,
        MapEnd, MapItem, MapStart, OptionMarker, OuterSequenceEnd, OuterSequenceItem,
        OuterSequenceStart, Panic, ProgramEnd, PushBool, PushDate64FromNaiveStr,
        PushDate64FromUtcStr, PushDictionary, PushF16, PushF32, PushF64, PushI16, PushI32, PushI64,
        PushI8, PushLargeUtf8, PushNull, PushU16, PushU32, PushU64, PushU8, PushUtf8, StructEnd,
        StructField, StructItem, StructStart, StructUnknownField, TupleStructEnd, TupleStructItem,
        TupleStructStart, UnionEnd, Variant,
    },
    structure::{
        FieldDefinition, ListDefinition, MapDefinition, NullDefinition, StructDefinition,
        UnionDefinition,
    },
};

const UNSET_INSTR: usize = usize::MAX;

pub fn compile_serialization(
    fields: &[GenericField],
    options: CompilationOptions,
) -> Result<Program> {
    let mut program = Program::new(options);
    program.compile(fields)?;

    {
        let current_config = CONFIGURATION.read().unwrap().clone();
        if current_config.debug_print_program {
            println!("Program: {program:?}");
        }
    }

    Ok(program)
}

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
    pub fn wrap_with_struct(mut self, value: bool) -> Self {
        self.wrap_with_struct = value;
        self
    }
}

trait Counter {
    fn next_value(&mut self) -> Self;
}

impl Counter for usize {
    fn next_value(&mut self) -> Self {
        let res = *self;
        *self += 1;
        res
    }
}

#[derive(Debug)]
pub struct Program {
    pub(crate) options: CompilationOptions,
    pub(crate) structure: Structure,
    pub(crate) buffers: BufferCounts,
}

#[derive(Debug, Default, Clone)]
pub struct Structure {
    // NOTE: the value UNSET_INSTR is used to mark an unknown jump target
    pub program: Vec<Bytecode>,
    pub lists: Vec<ListDefinition>,
    pub large_lists: Vec<ListDefinition>,
    pub maps: Vec<MapDefinition>,
    pub structs: Vec<StructDefinition>,
    pub unions: Vec<UnionDefinition>,
    pub nulls: Vec<NullDefinition>,
    pub array_mapping: Vec<ArrayMapping>,
}

/// See [MutableBuffers][super::interpreter::MutableBuffers] for details
#[derive(Debug, Default, Clone)]
pub struct BufferCounts {
    /// number of 0-bit buffers (counts)
    pub(crate) num_u0: usize,
    /// number of 1-bit buffers (bools)
    pub(crate) num_u1: usize,
    /// number of 8-bit buffers (u8, i8)
    pub(crate) num_u8: usize,
    /// number of 16-bit buffers (u16, i16, f16)
    pub(crate) num_u16: usize,
    /// number of 32-bit buffers (u32, i32, f32)
    pub(crate) num_u32: usize,
    /// number of 64-bit buffers (u64, i64, f64)
    pub(crate) num_u64: usize,
    /// number of offsets encoded with 32 bits
    pub(crate) num_u32_offsets: usize,
    /// number of offsets encoded with 64 bits
    pub(crate) num_u64_offsets: usize,
    /// number string -> index maps for dictionaries
    pub(crate) num_dictionaries: usize,
    /// number of bit-sets to record seen / unseen fields
    pub(crate) num_seen: usize,
}

impl Program {
    pub fn new(options: CompilationOptions) -> Self {
        Self {
            options,
            structure: Structure::default(),
            buffers: BufferCounts::default(),
        }
    }
}

impl Program {
    fn compile(&mut self, fields: &[GenericField]) -> Result<()> {
        self.compile_outer_structure(fields)?;
        self.update_jumps()?;
        self.validate()?;

        Ok(())
    }
}

impl Program {
    fn push_instr<I: Into<Bytecode>>(&mut self, instr: I) {
        self.structure.program.push(instr.into());
    }
}

impl Program {
    fn compile_outer_structure(&mut self, fields: &[GenericField]) -> Result<()> {
        if !self.options.wrap_with_struct && fields.len() != 1 {
            fail!("only single fields are supported without struct wrapping");
        }

        self.structure.large_lists.push(ListDefinition::default());
        self.push_instr(OuterSequenceStart { next: UNSET_INSTR });
        self.push_instr(OuterSequenceItem {
            next: UNSET_INSTR,
            list_idx: 0,
        });
        self.structure.large_lists[0].item = self.structure.program.len();

        if self.options.wrap_with_struct {
            self.structure.array_mapping = self.compile_struct_impl(fields, None)?;
        } else {
            let (f, _) = self.compile_field(&fields[0])?;
            self.structure.array_mapping = vec![f];
        }

        self.push_instr(OuterSequenceEnd {
            next: UNSET_INSTR,
            list_idx: 0,
        });
        self.structure.large_lists[0].r#return = self.structure.program.len();

        let next_instr = self.structure.program.len();
        self.push_instr(ProgramEnd { next: next_instr });

        Ok(())
    }

    fn compile_struct(
        &mut self,
        field: &GenericField,
        validity: Option<usize>,
    ) -> Result<ArrayMapping> {
        if field.nullable {
            if validity.is_none() {
                fail!("inconsistent arguments");
            }
            if field.children.is_empty() {
                fail!("Nullable structs without fields are not supported");
            }
        }

        let field_mapping = self.compile_struct_impl(&field.children, field.strategy.as_ref())?;

        Ok(ArrayMapping::Struct {
            field: field.clone(),
            fields: field_mapping,
            validity,
        })
    }

    fn compile_struct_impl(
        &mut self,
        children: &[GenericField],
        strategy: Option<&Strategy>,
    ) -> Result<Vec<ArrayMapping>> {
        let (is_tuple, is_map) = match strategy {
            None => (false, false),
            Some(Strategy::MapAsStruct) => (false, true),
            Some(Strategy::TupleAsStruct) => (true, false),
            Some(strategy) => fail!("Cannot compile struct with strategy {strategy}"),
        };

        let struct_idx = self.structure.structs.len();
        let seen: usize;

        if !is_tuple {
            seen = self.buffers.num_seen.next_value();
            self.structure.structs.push(StructDefinition::default());

            let start_pos = self.structure.program.len();
            self.push_instr(StructStart {
                next: start_pos + 2,
                seen,
            });

            let depth = self.buffers.num_u0.next_value();
            let unknown_field_pos = self.structure.program.len();
            self.push_instr(StructUnknownField {
                next: UNSET_INSTR,
                depth,
                self_pos: unknown_field_pos,
                struct_idx,
            });

            self.structure.structs[struct_idx].unknown_field = unknown_field_pos;
        } else {
            seen = usize::MAX;
            self.push_instr(TupleStructStart { next: UNSET_INSTR });
        }

        let mut field_mapping = vec![];

        for (field_idx, field) in children.iter().enumerate() {
            if !is_tuple {
                if field_idx >= BitSet::MAX {
                    fail!("Structs can contain at most {} fields", BitSet::MAX);
                }
                if is_map {
                    self.push_instr(StructItem {
                        next: UNSET_INSTR,
                        seen,
                        struct_idx,
                    });
                }
                let self_pos = self.structure.program.len();
                self.push_instr(StructField {
                    self_pos,
                    next: UNSET_INSTR,
                    struct_idx,
                    field_name: field.name.to_string(),
                    field_idx,
                    seen,
                });
                self.structure.structs[struct_idx].fields.insert(
                    field.name.to_string(),
                    FieldDefinition {
                        index: field_idx,
                        jump: self.structure.program.len(),
                        null_definition: None,
                    },
                );
            } else {
                self.push_instr(TupleStructItem { next: UNSET_INSTR });
            }
            let (f, null_definition) = self.compile_field(field)?;

            if !is_tuple {
                let field_def = self.structure.structs[struct_idx]
                    .fields
                    .get_mut(&field.name)
                    .ok_or_else(|| error!("compile error: could not read struct field"))?;
                field_def.null_definition = null_definition;
            }

            field_mapping.push(f);
        }

        if !is_tuple {
            let self_pos = self.structure.program.len();
            self.push_instr(StructEnd {
                next: UNSET_INSTR,
                self_pos,
                struct_idx,
                seen,
            });
            self.structure.structs[struct_idx].r#return = self.structure.program.len();
        } else {
            self.push_instr(TupleStructEnd { next: UNSET_INSTR });
        }

        Ok(field_mapping)
    }

    fn compile_list(
        &mut self,
        field: &GenericField,
        validity: Option<usize>,
    ) -> Result<ArrayMapping> {
        if field.nullable != validity.is_some() {
            fail!("inconsistent arguments");
        }

        let Some(item) = field.children.first() else {
            fail!("invalid list: no child");
        };

        let list_idx = self.structure.lists.len();
        let offsets = self.buffers.num_u32_offsets.next_value();

        self.structure.lists.push(ListDefinition::default());
        self.structure.lists[list_idx].offset = offsets;

        self.push_instr(ListStart { next: UNSET_INSTR });
        self.push_instr(ListItem {
            next: UNSET_INSTR,
            list_idx,
            offsets,
        });
        self.structure.lists[list_idx].item = self.structure.program.len();

        let (field_mapping, _) = self.compile_field(item)?;

        self.push_instr(ListEnd {
            next: UNSET_INSTR,
            list_idx,
            offsets,
        });
        self.structure.lists[list_idx].r#return = self.structure.program.len();

        Ok(ArrayMapping::List {
            field: field.clone(),
            item: Box::new(field_mapping),
            offsets,
            validity,
        })
    }

    fn compile_large_list(
        &mut self,
        field: &GenericField,
        validity: Option<usize>,
    ) -> Result<ArrayMapping> {
        if field.nullable != validity.is_some() {
            fail!("inconsistent arguments");
        }

        let Some(item) = field.children.first() else {
            fail!("invalid list: no child");
        };

        let list_idx = self.structure.large_lists.len();
        let offsets = self.buffers.num_u64_offsets.next_value();

        self.structure.large_lists.push(ListDefinition::default());
        self.structure.large_lists[list_idx].offset = offsets;

        self.push_instr(LargeListStart { next: UNSET_INSTR });
        self.push_instr(LargeListItem {
            next: UNSET_INSTR,
            list_idx,
            offsets,
        });
        self.structure.large_lists[list_idx].item = self.structure.program.len();

        let (field_mapping, _) = self.compile_field(item)?;

        self.push_instr(LargeListEnd {
            next: UNSET_INSTR,
            list_idx,
            offsets,
        });
        self.structure.large_lists[list_idx].r#return = self.structure.program.len();

        Ok(ArrayMapping::LargeList {
            field: field.clone(),
            item: Box::new(field_mapping),
            offsets,
            validity,
        })
    }

    fn compile_union(
        &mut self,
        field: &GenericField,
        validity: Option<usize>,
    ) -> Result<ArrayMapping> {
        if validity.is_some() {
            fail!("cannot compile nullable unions");
        }
        if field.children.is_empty() {
            fail!("cannot compile a union withouth children");
        }

        let union_idx = self.structure.unions.len();
        self.structure.unions.push(UnionDefinition::default());

        let type_idx = self.buffers.num_u8.next_value();

        let mut fields = Vec::new();
        let mut child_last_instr = Vec::new();

        self.push_instr(Variant {
            next: UNSET_INSTR,
            union_idx,
            type_idx,
        });

        for (child_idx, child) in field.children.iter().enumerate() {
            self.structure.unions[union_idx]
                .fields
                .push(self.structure.program.len());

            if matches!(child.strategy, Some(Strategy::UnknownVariant)) {
                let message = format!(
                    concat!(
                        "Serialization failed: an unknown variant with index {child_idx} for field was ",
                        "encountered. To fix this error, sure all variants are seen during ",
                        "schema tracing or add the relevant variants manually to the traced fields.",
                    ),
                    child_idx = child_idx,
                );
                fields.push(self.compile_panic(message)?);
            } else {
                let (array_mapping, _) = self.compile_field(child)?;
                fields.push(array_mapping);
            }
            child_last_instr.push(self.structure.program.len() - 1);
        }

        // each union fields jumps to after the "union"
        for pos in child_last_instr {
            let next_instr = self.structure.program.len();
            self.structure.program[pos].set_next(next_instr);
        }

        self.push_instr(UnionEnd { next: UNSET_INSTR });

        Ok(ArrayMapping::Union {
            field: field.clone(),
            fields,
            types: type_idx,
            validity: None,
        })
    }

    fn compile_panic(&mut self, message: String) -> Result<ArrayMapping> {
        self.push_instr(Panic {
            next: UNSET_INSTR,
            message,
        });

        let res = ArrayMapping::Null {
            field: GenericField::new("", GenericDataType::Null, true),
            buffer: self.buffers.num_u0,
            validity: None,
        };

        self.buffers.num_u0 += 1;
        Ok(res)
    }

    /// compile a single field and return the array mapping and optional null
    /// definition index
    ///
    fn compile_field(&mut self, field: &GenericField) -> Result<(ArrayMapping, Option<usize>)> {
        let mut option_marker_pos = None;
        let validity = if self.requires_null_check(field) {
            let validity = self.buffers.num_u1.next_value();

            let null_definition = self.structure.nulls.len();
            self.structure.nulls.push(NullDefinition::default());

            let self_pos = self.structure.program.len();
            option_marker_pos = Some(self_pos);
            self.push_instr(OptionMarker {
                self_pos,
                next: UNSET_INSTR,
                if_none: 0,
                validity,
                null_definition,
            });

            Some(validity)
        } else {
            None
        };

        let array_mapping = self.compile_field_inner(field, validity)?;

        if let Some(option_marker_pos) = option_marker_pos {
            let current_program_len = self.structure.program.len();
            let Bytecode::OptionMarker(instr) = &mut self.structure.program[option_marker_pos]
            else {
                fail!("Internal error during compilation");
            };
            instr.if_none = current_program_len;
            self.structure.nulls[instr.null_definition]
                .update_from_array_mapping(&array_mapping)?;
            self.structure.nulls[instr.null_definition].sort_indices();

            Ok((array_mapping, Some(instr.null_definition)))
        } else {
            Ok((array_mapping, None))
        }
    }

    fn requires_null_check(&self, field: &GenericField) -> bool {
        // NOTE: Null fields are handled via the PushNull primitive and do
        // not require additional null checks
        field.nullable && !matches!(field.data_type, GenericDataType::Null)
    }
}

macro_rules! compile_primtive {
    ($this:expr, $field:expr, $validity:expr, $num:ident, $instr:ident, $mapping:ident) => {{
        $this.push_instr($instr {
            next: UNSET_INSTR,
            idx: $this.buffers.$num,
        });
        let res = ArrayMapping::$mapping {
            field: $field.clone(),
            buffer: $this.buffers.$num,
            validity: $validity,
        };

        $this.buffers.$num += 1;
        Ok(res)
    }};
}

impl Program {
    fn compile_field_inner(
        &mut self,
        field: &GenericField,
        validity: Option<usize>,
    ) -> Result<ArrayMapping> {
        use GenericDataType as D;

        match &field.data_type {
            D::Null => compile_primtive!(self, field, validity, num_u0, PushNull, Null),
            D::Bool => compile_primtive!(self, field, validity, num_u1, PushBool, Bool),
            D::U8 => compile_primtive!(self, field, validity, num_u8, PushU8, U8),
            D::U16 => compile_primtive!(self, field, validity, num_u16, PushU16, U16),
            D::U32 => compile_primtive!(self, field, validity, num_u32, PushU32, U32),
            D::U64 => compile_primtive!(self, field, validity, num_u64, PushU64, U64),
            D::I8 => compile_primtive!(self, field, validity, num_u8, PushI8, I8),
            D::I16 => compile_primtive!(self, field, validity, num_u16, PushI16, I16),
            D::I32 => compile_primtive!(self, field, validity, num_u32, PushI32, I32),
            D::I64 => compile_primtive!(self, field, validity, num_u64, PushI64, I64),
            D::F16 => compile_primtive!(self, field, validity, num_u16, PushF16, F16),
            D::F32 => compile_primtive!(self, field, validity, num_u32, PushF32, F32),
            D::F64 => compile_primtive!(self, field, validity, num_u64, PushF64, F64),
            D::Utf8 => {
                let buffer = self.buffers.num_u8.next_value();
                let offsets = self.buffers.num_u32_offsets.next_value();

                self.push_instr(PushUtf8 {
                    next: UNSET_INSTR,
                    buffer,
                    offsets,
                });
                Ok(ArrayMapping::Utf8 {
                    field: field.clone(),
                    buffer,
                    offsets,
                    validity,
                })
            }
            D::LargeUtf8 => {
                let buffer = self.buffers.num_u8.next_value();
                let offsets = self.buffers.num_u64_offsets.next_value();

                self.push_instr(PushLargeUtf8 {
                    next: UNSET_INSTR,
                    buffer,
                    offsets,
                });
                Ok(ArrayMapping::LargeUtf8 {
                    field: field.clone(),
                    buffer,
                    offsets,
                    validity,
                })
            }
            D::Date64 => match field.strategy.as_ref() {
                Some(Strategy::NaiveStrAsDate64) => compile_primtive!(
                    self,
                    field,
                    validity,
                    num_u64,
                    PushDate64FromNaiveStr,
                    Date64
                ),
                Some(Strategy::UtcStrAsDate64) => {
                    compile_primtive!(self, field, validity, num_u64, PushDate64FromUtcStr, Date64)
                }
                None => compile_primtive!(self, field, validity, num_u64, PushI64, Date64),
                Some(strategy) => fail!("Cannot compile Date64 with strategy {strategy}"),
            },
            D::Dictionary => self.compile_dictionary(field, validity),
            D::Struct => self.compile_struct(field, validity),
            D::List => self.compile_list(field, validity),
            D::LargeList => self.compile_large_list(field, validity),
            D::Union => self.compile_union(field, validity),
            D::Map => self.compile_map(field, validity),
            D::Timestamp(unit, tz) => {
                if !matches!(unit, GenericTimeUnit::Millisecond) {
                    fail!("Only timestamps with millisecond unit are supported");
                }

                match tz.as_deref() {
                    None => compile_primtive!(
                        self,
                        field,
                        validity,
                        num_u64,
                        PushDate64FromNaiveStr,
                        Date64
                    ),
                    Some("UTC") => compile_primtive!(
                        self,
                        field,
                        validity,
                        num_u64,
                        PushDate64FromUtcStr,
                        Date64
                    ),
                    Some(tz) => fail!("Timezone {tz} is not supported"),
                }
            }
        }
    }
}

impl Program {
    fn compile_dictionary(
        &mut self,
        field: &GenericField,
        validity: Option<usize>,
    ) -> Result<ArrayMapping> {
        if field.children.len() != 2 {
            fail!("Dictionary must have 2 children");
        }

        use {ArrayMapping as M, DictionaryIndex as I, DictionaryValue as V, GenericDataType as D};

        let indices = match &field.children[0].data_type {
            D::U8 => I::U8(self.buffers.num_u8.next_value()),
            D::U16 => I::U16(self.buffers.num_u16.next_value()),
            D::U32 => I::U32(self.buffers.num_u32.next_value()),
            D::U64 => I::U64(self.buffers.num_u64.next_value()),
            D::I8 => I::I8(self.buffers.num_u8.next_value()),
            D::I16 => I::I16(self.buffers.num_u16.next_value()),
            D::I32 => I::I32(self.buffers.num_u32.next_value()),
            D::I64 => I::I64(self.buffers.num_u64.next_value()),
            dt => fail!("cannot compile dictionary with indices of type {dt}"),
        };

        let values = match &field.children[1].data_type {
            D::Utf8 => V::Utf8 {
                buffer: self.buffers.num_u8.next_value(),
                offsets: self.buffers.num_u32_offsets.next_value(),
            },
            D::LargeUtf8 => V::LargeUtf8 {
                buffer: self.buffers.num_u8.next_value(),
                offsets: self.buffers.num_u64_offsets.next_value(),
            },
            dt => fail!("cannot compile dictionary with values of type {dt}"),
        };
        let dictionary = self.buffers.num_dictionaries.next_value();

        self.push_instr(PushDictionary {
            next: UNSET_INSTR,
            dictionary,
            values,
            indices,
        });

        Ok(M::Dictionary {
            field: field.clone(),
            dictionary: values,
            indices,
            validity,
        })
    }
}

impl Program {
    fn compile_map(
        &mut self,
        field: &GenericField,
        validity: Option<usize>,
    ) -> Result<ArrayMapping> {
        if field.nullable != validity.is_some() {
            fail!("inconsistent arguments");
        }
        field.validate_map()?;

        let Some(entries) = field.children.first() else {
            fail!("invalid list: no child");
        };
        let Some(keys) = entries.children.first() else {
            fail!("entries without key field");
        };
        let Some(values) = entries.children.get(1) else {
            fail!("entries without values field");
        };

        let map_idx = self.structure.maps.len();
        let offsets = self.buffers.num_u32_offsets.next_value();

        self.structure.maps.push(MapDefinition::default());

        self.push_instr(MapStart { next: UNSET_INSTR });
        self.push_instr(MapItem {
            next: UNSET_INSTR,
            map_idx,
            offsets,
        });
        self.structure.maps[map_idx].key = self.structure.program.len();

        let (keys_mapping, _) = self.compile_field(keys)?;
        let (values_mapping, _) = self.compile_field(values)?;

        self.push_instr(MapEnd {
            next: UNSET_INSTR,
            map_idx,
            offsets,
        });
        self.structure.maps[map_idx].r#return = self.structure.program.len();

        let entries_mapping = ArrayMapping::Struct {
            field: entries.clone(),
            fields: vec![keys_mapping, values_mapping],
            validity: None,
        };

        Ok(ArrayMapping::Map {
            field: field.clone(),
            offsets,
            entries: Box::new(entries_mapping),
            validity,
        })
    }
}

impl Program {
    fn update_jumps(&mut self) -> Result<()> {
        for (pos, instr) in self.structure.program.iter_mut().enumerate() {
            if instr.get_next() == UNSET_INSTR {
                instr.set_next(pos + 1);
            }
        }

        fn follow(mut pos: usize, program: &[Bytecode]) -> usize {
            // NOTE: limit the number of jumps followed
            for _ in 0..program.len() {
                if !matches!(program[pos], Bytecode::UnionEnd(_)) {
                    return pos;
                }
                pos = program[pos].get_next();
            }
            panic!("More jumps than instructions: cycle?")
        }

        for pos in 0..self.structure.program.len() {
            let next = follow(
                self.structure.program[pos].get_next(),
                &self.structure.program,
            );
            self.structure.program[pos].set_next(next);
        }

        for s in &mut self.structure.structs {
            s.r#return = follow(s.r#return, &self.structure.program);
        }

        for l in &mut self.structure.large_lists {
            l.r#return = follow(l.r#return, &self.structure.program);
        }

        // TODO: handle unions, ...

        Ok(())
    }
}

impl Program {
    fn validate(&self) -> Result<()> {
        self.validate_lists("list", &self.structure.lists)?;
        self.validate_lists("large list", &self.structure.large_lists)?;
        self.validate_maps()?;
        self.validate_structs()?;
        self.validate_nulls()?;
        self.validate_array_mappings()?;
        self.validate_next_instruction()?;
        Ok(())
    }

    fn validate_lists(&self, label: &str, definitions: &[ListDefinition]) -> Result<()> {
        for (list_idx, list) in definitions.iter().enumerate() {
            let item_instr = self.instruction_before(list.item);
            if !matches!(
                item_instr,
                Some(Bytecode::ListItem(_))
                    | Some(Bytecode::LargeListItem(_))
                    | Some(&Bytecode::OuterSequenceItem(_))
            ) {
                fail!("invalid {label} definition ({list_idx}): item points to {item_instr:?}");
            }

            let before_return_instr = self.instruction_before(list.r#return);
            if !matches!(
                before_return_instr,
                Some(Bytecode::ListEnd(_))
                    | Some(Bytecode::LargeListEnd(_))
                    | Some(Bytecode::OuterSequenceEnd(_))
            ) {
                fail!("invalid {label} definition ({list_idx}): instr before return is {before_return_instr:?}");
            }
        }
        Ok(())
    }

    fn validate_structs(&self) -> Result<()> {
        for (struct_idx, r#struct) in self.structure.structs.iter().enumerate() {
            for (name, field_def) in &r#struct.fields {
                let field_instr = self.instruction_before(field_def.jump);
                let is_valid = if let Some(Bytecode::StructField(instr)) = field_instr {
                    instr.struct_idx == struct_idx && instr.field_name == *name
                } else {
                    false
                };
                if !is_valid {
                    fail!("invalid struct definition ({struct_idx}): instr for field {name} is {field_instr:?}");
                }
            }

            let before_return_instr = self.instruction_before(r#struct.r#return);
            if !matches!(
                before_return_instr,
                Some(&Bytecode::StructEnd(_)) | Some(&Bytecode::UnionEnd(_))
            ) {
                fail!("invalid struct definition ({struct_idx}): instr before return is {before_return_instr:?}");
            }

            if !self.structure.program[r#struct.r#return].is_allowed_jump_target() {
                fail!("invalid struct definition ({struct_idx}): return jumps to invalid target");
            }

            for (name, field_def) in &r#struct.fields {
                if !self.structure.program[field_def.jump].is_allowed_jump_target() {
                    fail!("invalid struct definition ({struct_idx}): field jump {name} to invalid target");
                }
            }
        }
        Ok(())
    }

    fn validate_maps(&self) -> Result<()> {
        // TODO: implement
        Ok(())
    }

    fn validate_nulls(&self) -> Result<()> {
        for (idx, null) in self.structure.nulls.iter().enumerate() {
            if null.u0.iter().any(|&idx| idx >= self.buffers.num_u0) {
                fail!("invalid null definition {idx}: null out of bounds {null:?}");
            }
            if null.u1.iter().any(|&idx| idx >= self.buffers.num_u1) {
                fail!("invalid null definition {idx}: bool out of bounds {null:?}");
            }
            if null.u8.iter().any(|&idx| idx >= self.buffers.num_u8) {
                fail!("invalid null definition {idx}: u8 out of bounds {null:?}");
            }
            if null.u16.iter().any(|&idx| idx >= self.buffers.num_u16) {
                fail!("invalid null definition {idx}: u16 out of bounds {null:?}");
            }
            if null.u32.iter().any(|&idx| idx >= self.buffers.num_u32) {
                fail!("invalid null definition {idx}: u32 out of bounds {null:?}");
            }
            if null.u64.iter().any(|&idx| idx >= self.buffers.num_u64) {
                fail!("invalid null definition {idx}: u64 out of bounds {null:?}");
            }
        }
        Ok(())
    }

    fn validate_array_mappings(&self) -> Result<()> {
        for (idx, array_mapping) in self.structure.array_mapping.iter().enumerate() {
            self.validate_array_mapping(format!("{idx}"), array_mapping)?;
        }
        Ok(())
    }

    fn validate_next_instruction(&self) -> Result<()> {
        for (pos, instr) in self.structure.program.iter().enumerate() {
            if instr.get_next() >= self.structure.program.len() {
                fail!(
                    "invalid next instruction for {pos}: {target}",
                    target = instr.get_next()
                );
            }
        }

        for (pos, instr) in self.structure.program.iter().enumerate() {
            if matches!(
                self.structure.program[instr.get_next()],
                Bytecode::UnionEnd(_)
            ) {
                fail!("invalid next instruction for {pos}: points to union end");
            }
        }

        let last = self.structure.program.len() - 1;
        if self.structure.program[last].get_next() != last {
            fail!("invalid next instruciton for program end");
        }

        Ok(())
    }

    fn instruction_before(&self, idx: usize) -> Option<&Bytecode> {
        if idx != 0 {
            self.structure.program.get(idx - 1)
        } else {
            None
        }
    }
}

macro_rules! validate_array_mapping_primitive {
    ($this:expr, $path:expr, $array_mapping:expr, $variant:ident, $counter:ident) => {
        {
            let ArrayMapping::$variant { field, buffer, validity } = $array_mapping else { unreachable!() };
            if *buffer >= $this.buffers.$counter {
                fail!(
                    "invalid array mapping {path}: buffer index ({buffer}) out of bounds ({counter}) ({array_mapping:?})",
                    path=$path,
                    buffer=*buffer,
                    counter=$this.buffers.$counter,
                    array_mapping=$array_mapping,
                );
            }
            if validity.is_some() != field.nullable {
                fail!(
                    "invalid array mapping {path}: inconsistent nullability ({array_mapping:?})",
                    path=$path,
                    array_mapping=$array_mapping,
                );
            }
            if let &Some(validity) = validity {
                if validity >= $this.buffers.num_u1 {
                    fail!(
                        "invalid array mapping {path}: validity out of bounds ({array_mapping:?})",
                        path=$path,
                        array_mapping=$array_mapping,
                    );
                }
            }
        }
    };
}

impl Program {
    fn validate_array_mapping(&self, path: String, mapping: &ArrayMapping) -> Result<()> {
        use ArrayMapping::*;
        match mapping {
            // TODO: add the remaining array mappings
            Bool { .. } => validate_array_mapping_primitive!(self, path, mapping, Bool, num_u1),
            U8 { .. } => validate_array_mapping_primitive!(self, path, mapping, U8, num_u8),
            U16 { .. } => validate_array_mapping_primitive!(self, path, mapping, U16, num_u16),
            U32 { .. } => validate_array_mapping_primitive!(self, path, mapping, U32, num_u32),
            U64 { .. } => validate_array_mapping_primitive!(self, path, mapping, U64, num_u64),
            I8 { .. } => validate_array_mapping_primitive!(self, path, mapping, I8, num_u8),
            I16 { .. } => validate_array_mapping_primitive!(self, path, mapping, I16, num_u16),
            I32 { .. } => validate_array_mapping_primitive!(self, path, mapping, I32, num_u32),
            I64 { .. } => validate_array_mapping_primitive!(self, path, mapping, I64, num_u64),
            F32 { .. } => validate_array_mapping_primitive!(self, path, mapping, F32, num_u32),
            F64 { .. } => validate_array_mapping_primitive!(self, path, mapping, F64, num_u64),
            _ => {}
        }
        Ok(())
    }
}
