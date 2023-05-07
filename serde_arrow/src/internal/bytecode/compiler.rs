use std::collections::{BTreeMap, HashMap};

use crate::{
    internal::{
        error::{error, fail},
        schema::{GenericDataType, GenericField},
    },
    schema::Strategy,
    Result,
};

pub fn compile_serialization(
    fields: &[GenericField],
    options: CompilationOptions,
) -> Result<Program> {
    let mut program = Program::new(options);
    program.compile(fields)?;
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

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum DictionaryIndices {
    U8(usize),
    U16(usize),
    U32(usize),
    U64(usize),
    I8(usize),
    I16(usize),
    I32(usize),
    I64(usize),
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum DictionaryValue {
    Utf8(usize),
    LargeUtf8(usize),
}

#[derive(Debug, PartialEq, Clone)]
pub enum Bytecode {
    ProgramEnd,
    OuterSequenceStart,
    OuterSequenceItem(usize),
    OuterSequenceEnd(usize),
    OuterRecordStart,
    OuterRecordField(usize, String),
    OuterRecordEnd,
    LargeListStart,
    /// `LargeListItem(list_idx, offsets)`
    LargeListItem(usize, usize),
    /// `LargeListItem(list_idx, offsets)`
    LargeListEnd(usize, usize),
    StructStart,
    /// `StructField(struct_idx, field_name)`
    StructField(usize, String),
    StructEnd,
    TupleStructStart,
    TupleStructItem,
    TupleStructEnd,
    PushNull(usize),
    PushU8(usize),
    PushU16(usize),
    PushU32(usize),
    PushU64(usize),
    PushI8(usize),
    PushI16(usize),
    PushI32(usize),
    PushI64(usize),
    PushF32(usize),
    PushF64(usize),
    PushBool(usize),
    PushUTF8(usize),
    PushLargeUTF8(usize),
    PushDate64FromNaiveStr(usize),
    PushDate64FromUtcStr(usize),
    /// `PushDictionaryU8LargeUTF8(dictionary, indices)`
    PushDictionary(DictionaryValue, DictionaryIndices),
    /// `Option(if_none, validity)`
    Option(usize, usize),
    ///  `Variant(union_idx, type)`
    Variant(usize, usize),
    /// A pseudo instruction, used to fix jumps to union positions
    UnionEnd,
}

#[derive(Default, Debug, PartialEq)]
pub struct StructDefinition {
    /// The jump target for the individual fields
    pub fields: BTreeMap<String, usize>,
    /// The jump target if a struct is closed
    pub r#return: usize,
}

#[derive(Default, Debug, PartialEq)]
pub struct ListDefinition {
    /// The jump target if another item is encountered
    pub item: usize,
    /// The jump target if a list is closed
    pub r#return: usize,
    pub offset: usize,
}

#[derive(Default, Debug, PartialEq)]
pub struct UnionDefinition {
    pub fields: Vec<usize>,
}

#[derive(Default, Debug, PartialEq)]
pub struct NullDefinition {
    pub null: Vec<usize>,
    pub bool: Vec<usize>,
    pub u8: Vec<usize>,
    pub u16: Vec<usize>,
    pub u32: Vec<usize>,
    pub u64: Vec<usize>,
    pub i8: Vec<usize>,
    pub i16: Vec<usize>,
    pub i32: Vec<usize>,
    pub i64: Vec<usize>,
    pub f32: Vec<usize>,
    pub f64: Vec<usize>,
    pub utf8: Vec<usize>,
    pub large_utf8: Vec<usize>,
    pub large_offsets: Vec<usize>,
    pub validity: Vec<usize>,
}

impl NullDefinition {
    pub fn update_from_array_mapping(&mut self, m: &ArrayMapping) -> Result<()> {
        match m {
            &ArrayMapping::Null {
                buffer, validity, ..
            } => {
                self.null.push(buffer);
                self.validity.extend(validity);
            }
            &ArrayMapping::Bool {
                buffer, validity, ..
            } => {
                self.bool.push(buffer);
                self.validity.extend(validity);
            }
            &ArrayMapping::U8 {
                buffer, validity, ..
            } => {
                self.u8.push(buffer);
                self.validity.extend(validity);
            }
            &ArrayMapping::U16 {
                buffer, validity, ..
            } => {
                self.u16.push(buffer);
                self.validity.extend(validity);
            }
            &ArrayMapping::U32 {
                buffer, validity, ..
            } => {
                self.u32.push(buffer);
                self.validity.extend(validity);
            }
            &ArrayMapping::U64 {
                buffer, validity, ..
            } => {
                self.u64.push(buffer);
                self.validity.extend(validity);
            }
            &ArrayMapping::I8 {
                buffer, validity, ..
            } => {
                self.i8.push(buffer);
                self.validity.extend(validity);
            }
            &ArrayMapping::I16 {
                buffer, validity, ..
            } => {
                self.i16.push(buffer);
                self.validity.extend(validity);
            }
            &ArrayMapping::I32 {
                buffer, validity, ..
            } => {
                self.i32.push(buffer);
                self.validity.extend(validity);
            }
            &ArrayMapping::I64 {
                buffer, validity, ..
            } => {
                self.i64.push(buffer);
                self.validity.extend(validity);
            }
            &ArrayMapping::F32 {
                buffer, validity, ..
            } => {
                self.f32.push(buffer);
                self.validity.extend(validity);
            }
            &ArrayMapping::F64 {
                buffer, validity, ..
            } => {
                self.f64.push(buffer);
                self.validity.extend(validity);
            }
            &ArrayMapping::Utf8 {
                buffer, validity, ..
            } => {
                self.utf8.push(buffer);
                self.validity.extend(validity);
            }
            &ArrayMapping::LargeUtf8 {
                buffer, validity, ..
            } => {
                self.large_utf8.push(buffer);
                self.validity.extend(validity);
            }
            &ArrayMapping::Date64 {
                buffer, validity, ..
            } => {
                self.i64.push(buffer);
                self.validity.extend(validity);
            }
            ArrayMapping::Struct {
                fields, validity, ..
            } => {
                for field in fields {
                    self.update_from_array_mapping(field)?;
                }
                self.validity.extend(validity.iter().copied());
            }
            ArrayMapping::LargeList {
                offsets, validity, ..
            } => {
                // NOTE: the item is not included
                self.large_offsets.push(*offsets);
                self.validity.extend(validity.iter().copied());
            }
            &ArrayMapping::Dictionary {
                indices, validity, ..
            } => {
                match indices {
                    DictionaryIndices::U8(idx) => self.u8.push(idx),
                    DictionaryIndices::U16(idx) => self.u16.push(idx),
                    DictionaryIndices::U32(idx) => self.u32.push(idx),
                    DictionaryIndices::U64(idx) => self.u64.push(idx),
                    DictionaryIndices::I8(idx) => self.i8.push(idx),
                    DictionaryIndices::I16(idx) => self.i16.push(idx),
                    DictionaryIndices::I32(idx) => self.i32.push(idx),
                    DictionaryIndices::I64(idx) => self.i64.push(idx),
                }
                self.validity.extend(validity);
            }
            m => todo!("cannot update null definition from {m:?}"),
        }
        Ok(())
    }

    pub fn sort_indices(&mut self) {
        self.bool.sort();
        self.u8.sort();
        self.u16.sort();
        self.u32.sort();
        self.u64.sort();
        self.i8.sort();
        self.i16.sort();
        self.i32.sort();
        self.i64.sort();
        self.f32.sort();
        self.f64.sort();
        self.large_utf8.sort();
        self.large_offsets.sort();
        self.validity.sort();
    }
}

/// Map the array to the corresponding builders
#[derive(Debug)]
pub enum ArrayMapping {
    Null {
        field: GenericField,
        buffer: usize,
        validity: Option<usize>,
    },
    Bool {
        field: GenericField,
        buffer: usize,
        validity: Option<usize>,
    },
    U8 {
        field: GenericField,
        buffer: usize,
        validity: Option<usize>,
    },
    U16 {
        field: GenericField,
        buffer: usize,
        validity: Option<usize>,
    },
    U32 {
        field: GenericField,
        buffer: usize,
        validity: Option<usize>,
    },
    U64 {
        field: GenericField,
        buffer: usize,
        validity: Option<usize>,
    },
    I8 {
        field: GenericField,
        buffer: usize,
        validity: Option<usize>,
    },
    I16 {
        field: GenericField,
        buffer: usize,
        validity: Option<usize>,
    },
    I32 {
        field: GenericField,
        buffer: usize,
        validity: Option<usize>,
    },
    I64 {
        field: GenericField,
        buffer: usize,
        validity: Option<usize>,
    },
    F32 {
        field: GenericField,
        buffer: usize,
        validity: Option<usize>,
    },
    F64 {
        field: GenericField,
        buffer: usize,
        validity: Option<usize>,
    },
    Utf8 {
        field: GenericField,
        buffer: usize,
        validity: Option<usize>,
    },
    LargeUtf8 {
        field: GenericField,
        buffer: usize,
        validity: Option<usize>,
    },
    Date64 {
        field: GenericField,
        buffer: usize,
        validity: Option<usize>,
    },
    List {
        field: GenericField,
        item: Box<ArrayMapping>,
        offsets: usize,
        validity: Option<usize>,
    },
    Dictionary {
        field: GenericField,
        dictionary: DictionaryValue,
        indices: DictionaryIndices,
        validity: Option<usize>,
    },
    LargeList {
        field: GenericField,
        item: Box<ArrayMapping>,
        offsets: usize,
        validity: Option<usize>,
    },
    Struct {
        field: GenericField,
        fields: Vec<ArrayMapping>,
        validity: Option<usize>,
    },
    Union {
        field: GenericField,
        fields: Vec<ArrayMapping>,
        types: usize,
    },
}

#[derive(Debug)]
pub struct Program {
    pub(crate) options: CompilationOptions,
    pub(crate) program: Vec<Bytecode>,
    pub(crate) large_lists: Vec<ListDefinition>,
    pub(crate) structs: Vec<StructDefinition>,
    pub(crate) unions: Vec<UnionDefinition>,
    pub(crate) nulls: Vec<NullDefinition>,
    pub(crate) array_mapping: Vec<ArrayMapping>,
    pub(crate) next_instruction: HashMap<usize, usize>,
    pub(crate) num_null: usize,
    pub(crate) num_bool: usize,
    pub(crate) num_u8: usize,
    pub(crate) num_u16: usize,
    pub(crate) num_u32: usize,
    pub(crate) num_u64: usize,
    pub(crate) num_i8: usize,
    pub(crate) num_i16: usize,
    pub(crate) num_i32: usize,
    pub(crate) num_i64: usize,
    pub(crate) num_f32: usize,
    pub(crate) num_f64: usize,
    pub(crate) num_utf8: usize,
    pub(crate) num_large_utf8: usize,
    pub(crate) num_validity: usize,
    pub(crate) num_offsets: usize,
    pub(crate) num_large_offsets: usize,
    pub(crate) num_dictionaries: usize,
    pub(crate) num_large_dictionaries: usize,
}

impl Program {
    pub fn new(options: CompilationOptions) -> Self {
        Self {
            options,
            program: Vec::new(),
            large_lists: Vec::new(),
            structs: Vec::new(),
            unions: Vec::new(),
            nulls: Vec::new(),
            array_mapping: Vec::new(),
            next_instruction: HashMap::new(),
            num_null: 0,
            num_bool: 0,
            num_u8: 0,
            num_u16: 0,
            num_u32: 0,
            num_u64: 0,
            num_i8: 0,
            num_i16: 0,
            num_i32: 0,
            num_i64: 0,
            num_f32: 0,
            num_f64: 0,
            num_utf8: 0,
            num_large_utf8: 0,
            num_validity: 0,
            num_offsets: 0,
            num_large_offsets: 0,
            num_dictionaries: 0,
            num_large_dictionaries: 0,
        }
    }
}

impl Program {
    fn compile(&mut self, fields: &[GenericField]) -> Result<()> {
        self.compile_outer_structure(fields)?;
        self.fix_union_jumps()?;
        self.validate()?;

        Ok(())
    }
}

impl Program {
    fn compile_outer_structure(&mut self, fields: &[GenericField]) -> Result<()> {
        if !self.options.wrap_with_struct && fields.len() != 1 {
            fail!("only single fields are supported without struct wrapping");
        }

        self.large_lists.push(ListDefinition::default());
        self.program.push(Bytecode::OuterSequenceStart);
        self.program.push(Bytecode::OuterSequenceItem(0));
        self.large_lists[0].item = self.program.len();

        if self.options.wrap_with_struct {
            self.structs.push(StructDefinition::default());
            self.program.push(Bytecode::OuterRecordStart);
        }

        for field in fields {
            if self.options.wrap_with_struct {
                self.program
                    .push(Bytecode::OuterRecordField(0, field.name.to_string()));
                self.structs[0]
                    .fields
                    .insert(field.name.to_string(), self.program.len());
            }
            let f = self.compile_field(field)?;
            self.array_mapping.push(f);
        }

        if self.options.wrap_with_struct {
            self.program.push(Bytecode::OuterRecordEnd);
            self.structs[0].r#return = self.program.len();
        }

        self.program.push(Bytecode::OuterSequenceEnd(0));
        self.large_lists[0].r#return = self.program.len();

        self.program.push(Bytecode::ProgramEnd);
        self.next_instruction
            .insert(self.program.len() - 1, self.program.len() - 1);

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

        let is_tuple = match field.strategy.as_ref() {
            None | Some(Strategy::MapAsStruct) => false,
            Some(Strategy::TupleAsStruct) => true,
            Some(strategy) => fail!("Cannot compile struct with strategy {strategy}"),
        };

        let idx = self.structs.len();

        if !is_tuple {
            self.structs.push(StructDefinition::default());
            self.program.push(Bytecode::StructStart);
        } else {
            self.program.push(Bytecode::TupleStructStart);
        }

        let mut field_mapping = vec![];

        for field in &field.children {
            if !is_tuple {
                self.program
                    .push(Bytecode::StructField(idx, field.name.to_string()));
                self.structs[idx]
                    .fields
                    .insert(field.name.to_string(), self.program.len());
            } else {
                self.program.push(Bytecode::TupleStructItem);
            }
            let f = self.compile_field(field)?;
            field_mapping.push(f);
        }

        if !is_tuple {
            self.program.push(Bytecode::StructEnd);
            self.structs[idx].r#return = self.program.len();
        } else {
            self.program.push(Bytecode::TupleStructEnd);
        }

        Ok(ArrayMapping::Struct {
            field: field.clone(),
            fields: field_mapping,
            validity,
        })
    }

    fn compile_list(
        &mut self,
        _field: &GenericField,
        _validity: Option<usize>,
    ) -> Result<ArrayMapping> {
        fail!("Cannot compile lists: Not implemented")
    }

    fn compile_large_list(
        &mut self,
        field: &GenericField,
        validity: Option<usize>,
    ) -> Result<ArrayMapping> {
        if field.nullable != validity.is_some() {
            fail!("inconsistent arguments");
        }

        let item = field
            .children
            .get(0)
            .ok_or_else(|| error!("invalid list: no child"))?;

        let idx = self.large_lists.len();
        let offsets = self.num_large_offsets.next_value();

        self.large_lists.push(ListDefinition::default());
        self.large_lists[idx].offset = offsets;

        self.program.push(Bytecode::LargeListStart);
        self.program.push(Bytecode::LargeListItem(idx, offsets));
        self.large_lists[idx].item = self.program.len();

        let field_mapping = self.compile_field(item)?;

        self.program.push(Bytecode::LargeListEnd(idx, offsets));
        self.large_lists[idx].r#return = self.program.len();

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

        let def = self.unions.len();
        self.unions.push(UnionDefinition::default());

        let types = self.num_i8.next_value();

        let mut fields = Vec::new();
        let mut child_last_instr = Vec::new();

        self.program.push(Bytecode::Variant(def, types));

        for child in &field.children {
            self.unions[def].fields.push(self.program.len());
            fields.push(self.compile_field(child)?);
            child_last_instr.push(self.program.len() - 1);
        }

        // each union fields jumps to after the "union"
        for pos in child_last_instr {
            self.next_instruction.insert(pos, self.program.len());
        }

        self.program.push(Bytecode::UnionEnd);

        Ok(ArrayMapping::Union {
            field: field.clone(),
            fields,
            types,
        })
    }

    fn compile_field(&mut self, field: &GenericField) -> Result<ArrayMapping> {
        let mut nullable_idx = None;
        let validity = if field.nullable {
            let validity = self.num_validity.next_value();
            self.nulls.push(NullDefinition::default());

            nullable_idx = Some(self.program.len());
            self.program.push(Bytecode::Option(0, validity));

            Some(validity)
        } else {
            None
        };

        let array_mapping = self.compile_field_inner(field, validity)?;

        if let Some(nullable_idx) = nullable_idx {
            let current_program_len = self.program.len();
            let Bytecode::Option(if_none, validity) = &mut self.program[nullable_idx] else {
                fail!("Internal error during compilation");
            };
            *if_none = current_program_len;
            self.nulls[*validity].update_from_array_mapping(&array_mapping)?;
            self.nulls[*validity].sort_indices();
        }

        Ok(array_mapping)
    }
}

macro_rules! compile_primtive {
    ($this:expr, $field:expr, $validity:expr, $num:ident, $instr:ident, $mapping:ident) => {{
        $this.program.push(Bytecode::$instr($this.$num));
        let res = ArrayMapping::$mapping {
            field: $field.clone(),
            buffer: $this.$num,
            validity: $validity,
        };

        $this.$num += 1;
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

        match field.data_type {
            D::Null => compile_primtive!(self, field, validity, num_null, PushNull, Null),
            D::Bool => compile_primtive!(self, field, validity, num_bool, PushBool, Bool),
            D::U8 => compile_primtive!(self, field, validity, num_u8, PushU8, U8),
            D::U16 => compile_primtive!(self, field, validity, num_u16, PushU16, U16),
            D::U32 => compile_primtive!(self, field, validity, num_u32, PushU32, U32),
            D::U64 => compile_primtive!(self, field, validity, num_u64, PushU64, U64),
            D::I8 => compile_primtive!(self, field, validity, num_i8, PushI8, I8),
            D::I16 => compile_primtive!(self, field, validity, num_i16, PushI16, I16),
            D::I32 => compile_primtive!(self, field, validity, num_i32, PushI32, I32),
            D::I64 => compile_primtive!(self, field, validity, num_i64, PushI64, I64),
            D::F32 => compile_primtive!(self, field, validity, num_f32, PushF32, F32),
            D::F64 => compile_primtive!(self, field, validity, num_f64, PushF64, F64),
            D::Utf8 => compile_primtive!(self, field, validity, num_utf8, PushUTF8, Utf8),
            D::LargeUtf8 => compile_primtive!(
                self,
                field,
                validity,
                num_large_utf8,
                PushLargeUTF8,
                LargeUtf8
            ),
            D::Date64 => match field.strategy.as_ref() {
                Some(Strategy::NaiveStrAsDate64) => compile_primtive!(
                    self,
                    field,
                    validity,
                    num_i64,
                    PushDate64FromNaiveStr,
                    Date64
                ),
                Some(Strategy::UtcStrAsDate64) => {
                    compile_primtive!(self, field, validity, num_i64, PushDate64FromUtcStr, Date64)
                }
                None => compile_primtive!(self, field, validity, num_i64, PushI64, Date64),
                Some(strategy) => fail!("Cannot compile Date64 with strategy {strategy}"),
            },
            D::Dictionary => self.compile_dictionary(field, validity),
            D::Struct => self.compile_struct(field, validity),
            D::List => self.compile_list(field, validity),
            D::LargeList => self.compile_large_list(field, validity),
            D::Union => self.compile_union(field, validity),
            dt => fail!("cannot compile {dt}: not implemented"),
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

        use {
            ArrayMapping as M, Bytecode as B, DictionaryIndices as I, DictionaryValue as V,
            GenericDataType as D,
        };

        let indices = match &field.children[0].data_type {
            D::U8 => I::U8(self.num_u8.next_value()),
            D::U16 => I::U16(self.num_u16.next_value()),
            D::U32 => I::U32(self.num_u32.next_value()),
            D::U64 => I::U64(self.num_u64.next_value()),
            D::I8 => I::I8(self.num_i8.next_value()),
            D::I16 => I::I16(self.num_i16.next_value()),
            D::I32 => I::I32(self.num_i32.next_value()),
            D::I64 => I::I64(self.num_i64.next_value()),
            dt => fail!("cannot compile dictionary with indices of type {dt}"),
        };

        let dictionary = match &field.children[1].data_type {
            D::Utf8 => V::Utf8(self.num_dictionaries.next_value()),
            D::LargeUtf8 => V::LargeUtf8(self.num_large_dictionaries.next_value()),
            dt => fail!("cannot compile dictionary with values of type {dt}"),
        };
        self.program.push(B::PushDictionary(dictionary, indices));

        Ok(M::Dictionary {
            field: field.clone(),
            dictionary,
            indices,
            validity,
        })
    }
}

impl Program {
    fn fix_union_jumps(&mut self) -> Result<()> {
        fn follow(mut current: usize, program: &Program) -> usize {
            for _ in 0..program.program.len() {
                current = program
                    .next_instruction
                    .get(&current)
                    .copied()
                    .unwrap_or(current + 1);
                if program.program[current] != Bytecode::UnionEnd {
                    return current;
                }
            }
            panic!("More jumps than instructions: cycle?")
        }

        let mut fixed = HashMap::new();
        for (&pos, &next) in &self.next_instruction {
            fixed.insert(pos, follow(next, self));
        }
        self.next_instruction = fixed;

        Ok(())
    }
}

impl Program {
    fn validate(&self) -> Result<()> {
        self.validate_lists()?;
        self.validate_structs()?;
        self.validate_nulls()?;
        self.validate_array_mappings()?;
        self.validate_next_instruction()?;
        Ok(())
    }

    fn validate_lists(&self) -> Result<()> {
        for (idx, list) in self.large_lists.iter().enumerate() {
            let offset = list.offset;
            let item_instr = self.instruction_before(list.item);
            if item_instr != Some(&Bytecode::LargeListItem(idx, offset))
                && item_instr != Some(&Bytecode::OuterSequenceItem(idx))
            {
                fail!("invalid list definition ({idx}): item points to {item_instr:?}");
            }

            let before_return_instr = self.instruction_before(list.r#return);
            if before_return_instr != Some(&Bytecode::LargeListEnd(idx, offset))
                && before_return_instr != Some(&Bytecode::OuterSequenceEnd(idx))
            {
                fail!("invalid list definition ({idx}): instr before return is {before_return_instr:?}");
            }
        }
        Ok(())
    }

    fn validate_structs(&self) -> Result<()> {
        for (idx, r#struct) in self.structs.iter().enumerate() {
            for (name, address) in &r#struct.fields {
                let field_instr = self.instruction_before(*address);
                let is_valid =
                    if let Some(Bytecode::StructField(actual_idx, actual_name)) = field_instr {
                        *actual_idx == idx && actual_name == name
                    } else if let Some(Bytecode::OuterRecordField(actual_idx, actual_name)) =
                        field_instr
                    {
                        *actual_idx == idx && actual_name == name
                    } else {
                        false
                    };
                if !is_valid {
                    fail!("invalid struct definition ({idx}): instr for field {name} is {field_instr:?}");
                }
            }

            let before_return_instr = self.instruction_before(r#struct.r#return);
            if before_return_instr != Some(&Bytecode::StructEnd)
                && before_return_instr != Some(&Bytecode::OuterRecordEnd)
            {
                fail!("invalid struct definition ({idx}): instr before return is {before_return_instr:?}");
            }
        }
        Ok(())
    }

    fn validate_nulls(&self) -> Result<()> {
        for (idx, null) in self.nulls.iter().enumerate() {
            if null.null.iter().any(|&idx| idx >= self.num_null) {
                fail!("invalid null definition {idx}: null out of bounds {null:?}");
            }
            if null.bool.iter().any(|&idx| idx >= self.num_bool) {
                fail!("invalid null definition {idx}: bool out of bounds {null:?}");
            }
            if null.u8.iter().any(|&idx| idx >= self.num_u8) {
                fail!("invalid null definition {idx}: u8 out of bounds {null:?}");
            }
            if null.u16.iter().any(|&idx| idx >= self.num_u16) {
                fail!("invalid null definition {idx}: u16 out of bounds {null:?}");
            }
            if null.u32.iter().any(|&idx| idx >= self.num_u32) {
                fail!("invalid null definition {idx}: u32 out of bounds {null:?}");
            }
            if null.u64.iter().any(|&idx| idx >= self.num_u64) {
                fail!("invalid null definition {idx}: u64 out of bounds {null:?}");
            }
            if null.i8.iter().any(|&idx| idx >= self.num_i8) {
                fail!("invalid null definition {idx}: i8 out of bounds {null:?}");
            }
            if null.i16.iter().any(|&idx| idx >= self.num_i16) {
                fail!("invalid null definition {idx}: i16 out of bounds {null:?}");
            }
            if null.i32.iter().any(|&idx| idx >= self.num_i32) {
                fail!("invalid null definition {idx}: i32 out of bounds {null:?}");
            }
            if null.i64.iter().any(|&idx| idx >= self.num_i64) {
                fail!("invalid null definition {idx}: i64 out of bounds {null:?}");
            }
            if null.f32.iter().any(|&idx| idx >= self.num_f32) {
                fail!("invalid null definition {idx}: f32 out of bounds {null:?}");
            }
            if null.f64.iter().any(|&idx| idx >= self.num_f64) {
                fail!("invalid null definition {idx}: f64 out of bounds {null:?}");
            }
            if null.utf8.iter().any(|&idx| idx >= self.num_utf8) {
                fail!("invalid null definition {idx}: u8 out of bounds {null:?}");
            }
            if null
                .large_utf8
                .iter()
                .any(|&idx| idx >= self.num_large_utf8)
            {
                fail!("invalid null definition {idx}: large_u8 out of bounds {null:?}");
            }
            if null.validity.iter().any(|&idx| idx >= self.num_validity) {
                fail!("invalid null definition {idx}: validity out of bounds {null:?}");
            }
        }
        Ok(())
    }

    fn validate_array_mappings(&self) -> Result<()> {
        for (idx, array_mapping) in self.array_mapping.iter().enumerate() {
            self.validate_array_mapping(format!("{idx}"), array_mapping)?;
        }
        Ok(())
    }

    fn validate_next_instruction(&self) -> Result<()> {
        for (&pos, &target) in &self.next_instruction {
            if target >= self.program.len() {
                fail!("invalid next instruction for {pos}: {target}");
            }
        }

        for pos in 0..self.program.len() - 1 {
            let next = self.next_instruction.get(&pos).copied().unwrap_or(pos + 1);
            if self.program[next] == Bytecode::UnionEnd {
                fail!("invalid next instruction for {pos}: points to union end");
            }
        }

        let last = self.program.len() - 1;
        if self.next_instruction.get(&last) != Some(&last) {
            fail!("invalid next instruciton for program end");
        }
        Ok(())
    }

    fn instruction_before(&self, idx: usize) -> Option<&Bytecode> {
        if idx != 0 {
            self.program.get(idx - 1)
        } else {
            None
        }
    }
}

macro_rules! validate_array_mapping_primitive {
    ($this:expr, $path:expr, $array_mapping:expr, $variant:ident, $counter:ident) => {
        {
            let ArrayMapping::$variant { field, buffer, validity } = $array_mapping else { unreachable!() };
            if *buffer >= $this.$counter {
                fail!(
                    "invalid array mapping {path}: buffer index ({buffer}) out of bounds ({counter}) ({array_mapping:?})",
                    path=$path,
                    buffer=*buffer,
                    counter=$this.$counter,
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
                if validity >= $this.num_validity {
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
            Bool { .. } => validate_array_mapping_primitive!(self, path, mapping, Bool, num_bool),
            U8 { .. } => validate_array_mapping_primitive!(self, path, mapping, U8, num_u8),
            U16 { .. } => validate_array_mapping_primitive!(self, path, mapping, U16, num_u16),
            U32 { .. } => validate_array_mapping_primitive!(self, path, mapping, U32, num_u32),
            U64 { .. } => validate_array_mapping_primitive!(self, path, mapping, U64, num_u64),
            I8 { .. } => validate_array_mapping_primitive!(self, path, mapping, I8, num_i8),
            I16 { .. } => validate_array_mapping_primitive!(self, path, mapping, I16, num_i16),
            I32 { .. } => validate_array_mapping_primitive!(self, path, mapping, I32, num_i32),
            I64 { .. } => validate_array_mapping_primitive!(self, path, mapping, I64, num_i64),
            F32 { .. } => validate_array_mapping_primitive!(self, path, mapping, F32, num_f32),
            F64 { .. } => validate_array_mapping_primitive!(self, path, mapping, F64, num_f64),
            Utf8 { .. } => validate_array_mapping_primitive!(self, path, mapping, Utf8, num_utf8),
            LargeUtf8 { .. } => {
                validate_array_mapping_primitive!(self, path, mapping, LargeUtf8, num_large_utf8)
            }
            _ => {}
        }
        Ok(())
    }
}
