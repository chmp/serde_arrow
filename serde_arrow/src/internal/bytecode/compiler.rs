use std::collections::BTreeMap;

use crate::{
    internal::{
        error::{error, fail},
        schema::{GenericDataType, GenericField},
    },
    schema::Strategy,
    Result,
};

const UNSET_INSTR: usize = usize::MAX;

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
    ProgramEnd(ProgramEnd),
    OuterSequenceStart(OuterSequenceStart),
    OuterSequenceItem(OuterSequenceItem),
    OuterSequenceEnd(OuterSequenceEnd),
    OuterRecordStart(OuterRecordStart),
    OuterRecordField(OuterRecordField),
    OuterRecordEnd(OuterRecordEnd),
    LargeListStart(LargeListStart),
    LargeListItem(LargeListItem),
    LargeListEnd(LargeListEnd),
    StructStart(StructStart),
    StructField(StructField),
    StructEnd(StructEnd),
    MapStart(MapStart),
    MapEnd(MapEnd),
    MapItem(MapItem),
    StructItem(StructItem),
    TupleStructStart(TupleStructStart),
    TupleStructItem(TupleStructItem),
    TupleStructEnd(TupleStructEnd),
    Option(OptionMarker),
    Variant(Variant),
    /// A pseudo instruction, used to fix jumps to union positions
    UnionEnd(UnionEnd),
    PushNull(PushNull),
    PushU8(PushU8),
    PushU16(PushU16),
    PushU32(PushU32),
    PushU64(PushU64),
    PushI8(PushI8),
    PushI16(PushI16),
    PushI32(PushI32),
    PushI64(PushI64),
    PushF32(PushF32),
    PushF64(PushF64),
    PushBool(PushBool),
    PushUtf8(PushUtf8),
    PushLargeUtf8(PushLargeUtf8),
    PushDate64FromNaiveStr(PushDate64FromNaiveStr),
    PushDate64FromUtcStr(PushDate64FromUtcStr),
    /// `PushDictionaryU8LargeUTF8(dictionary, indices)`
    PushDictionary(PushDictionary),
}

macro_rules! define_check_instructions {
    ($($name:ident,)*) => {
        $(
            #[derive(Debug, PartialEq, Clone)]
            pub struct $name {
                pub next: usize,
            }
        )*
    };
}

define_check_instructions!(
    ProgramEnd,
    OuterSequenceStart,
    OuterRecordStart,
    LargeListStart,
    StructStart,
    MapStart,
    TupleStructStart,
    TupleStructItem,
    TupleStructEnd,
    UnionEnd,
);

macro_rules! define_primitive_instructions {
    ($($name:ident,)*) => {
        $(
            #[derive(Debug, PartialEq, Clone)]
            pub struct $name {
                pub next: usize,
                pub idx: usize,
            }
        )*
    };
}

define_primitive_instructions!(
    PushNull,
    PushU8,
    PushU16,
    PushU32,
    PushU64,
    PushI8,
    PushI16,
    PushI32,
    PushI64,
    PushF32,
    PushF64,
    PushBool,
    PushUtf8,
    PushLargeUtf8,
    PushDate64FromNaiveStr,
    PushDate64FromUtcStr,
);

#[derive(Debug, PartialEq, Clone)]
pub struct OuterSequenceItem {
    pub next: usize,
    pub list_idx: usize,
}

#[derive(Debug, PartialEq, Clone)]
pub struct OuterSequenceEnd {
    pub next: usize,
    pub list_idx: usize,
}

#[derive(Debug, PartialEq, Clone)]
pub struct OuterRecordField {
    pub next: usize,
    pub struct_idx: usize,
    pub field_name: String,
}

#[derive(Debug, PartialEq, Clone)]
pub struct OuterRecordEnd {
    pub next: usize,
    pub struct_idx: usize,
}

#[derive(Debug, PartialEq, Clone)]
pub struct LargeListItem {
    pub next: usize,
    pub list_idx: usize,
    pub offsets: usize,
}

#[derive(Debug, PartialEq, Clone)]
pub struct LargeListEnd {
    pub next: usize,
    pub list_idx: usize,
    pub offsets: usize,
}

#[derive(Debug, PartialEq, Clone)]
pub struct StructItem {
    pub next: usize,
    pub struct_idx: usize,
}

#[derive(Debug, PartialEq, Clone)]
pub struct StructField {
    pub next: usize,
    pub struct_idx: usize,
    pub field_name: String,
}

#[derive(Debug, PartialEq, Clone)]
pub struct StructEnd {
    pub next: usize,
    pub struct_idx: usize,
}

#[derive(Debug, PartialEq, Clone)]
pub struct MapItem {
    pub next: usize,
    pub map_idx: usize,
    pub offsets: usize,
}

#[derive(Debug, PartialEq, Clone)]
pub struct MapEnd {
    pub next: usize,
    pub map_idx: usize,
    pub offsets: usize,
}

#[derive(Debug, PartialEq, Clone)]
pub struct OptionMarker {
    pub next: usize,
    pub if_none: usize,
    pub validity: usize,
}

#[derive(Debug, PartialEq, Clone)]
pub struct Variant {
    pub next: usize,
    pub union_idx: usize,
    pub type_idx: usize,
}

#[derive(Debug, PartialEq, Clone)]
pub struct PushDictionary {
    pub next: usize,
    pub values: DictionaryValue,
    pub indices: DictionaryIndices,
}

macro_rules! dispatch_bytecode {
    (
        $obj:expr,
        $instr:ident => $block:expr
    ) => {
        match $obj {
            Bytecode::LargeListEnd($instr) => $block,
            Bytecode::LargeListItem($instr) => $block,
            Bytecode::LargeListStart($instr) => $block,
            Bytecode::MapEnd($instr) => $block,
            Bytecode::MapItem($instr) => $block,
            Bytecode::MapStart($instr) => $block,
            Bytecode::Option($instr) => $block,
            Bytecode::OuterRecordEnd($instr) => $block,
            Bytecode::OuterRecordField($instr) => $block,
            Bytecode::OuterRecordStart($instr) => $block,
            Bytecode::OuterSequenceEnd($instr) => $block,
            Bytecode::OuterSequenceItem($instr) => $block,
            Bytecode::OuterSequenceStart($instr) => $block,
            Bytecode::ProgramEnd($instr) => $block,
            Bytecode::StructEnd($instr) => $block,
            Bytecode::StructField($instr) => $block,
            Bytecode::StructItem($instr) => $block,
            Bytecode::StructStart($instr) => $block,
            Bytecode::TupleStructEnd($instr) => $block,
            Bytecode::TupleStructItem($instr) => $block,
            Bytecode::TupleStructStart($instr) => $block,
            Bytecode::Variant($instr) => $block,
            Bytecode::UnionEnd($instr) => $block,
            Bytecode::PushNull($instr) => $block,
            Bytecode::PushU8($instr) => $block,
            Bytecode::PushU16($instr) => $block,
            Bytecode::PushU32($instr) => $block,
            Bytecode::PushU64($instr) => $block,
            Bytecode::PushI8($instr) => $block,
            Bytecode::PushI16($instr) => $block,
            Bytecode::PushI32($instr) => $block,
            Bytecode::PushI64($instr) => $block,
            Bytecode::PushF32($instr) => $block,
            Bytecode::PushF64($instr) => $block,
            Bytecode::PushBool($instr) => $block,
            Bytecode::PushUtf8($instr) => $block,
            Bytecode::PushLargeUtf8($instr) => $block,
            Bytecode::PushDate64FromNaiveStr($instr) => $block,
            Bytecode::PushDate64FromUtcStr($instr) => $block,
            Bytecode::PushDictionary($instr) => $block,
        }
    };
}

pub(crate) use dispatch_bytecode;

impl Bytecode {
    fn is_allowed_jump_target(&self) -> bool {
        !matches!(self, Bytecode::UnionEnd(_))
    }

    fn get_next(&self) -> usize {
        dispatch_bytecode!(self, instr => instr.next)
    }

    fn set_next(&mut self, val: usize) {
        dispatch_bytecode!(self, instr => { instr.next = val; });
    }
}

#[derive(Default, Debug, Clone, PartialEq)]
pub struct StructDefinition {
    /// The jump target for the individual fields
    pub fields: BTreeMap<String, usize>,
    /// The jump target for an item
    pub item: usize,
    /// The jump target if a struct is closed
    pub r#return: usize,
}

#[derive(Default, Debug, Clone, PartialEq)]
pub struct ListDefinition {
    /// The jump target if another item is encountered
    pub item: usize,
    /// The jump target if a list is closed
    pub r#return: usize,
    pub offset: usize,
}

#[derive(Default, Debug, Clone, PartialEq)]
pub struct MapDefinition {
    /// The jump target if another item is encountered
    pub key: usize,
    /// The jump target if a map is closed
    pub r#return: usize,
}

#[derive(Default, Debug, Clone, PartialEq)]
pub struct UnionDefinition {
    pub fields: Vec<usize>,
}

#[derive(Default, Debug, Clone, PartialEq)]
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
            &ArrayMapping::Map {
                offsets, validity, ..
            } => {
                // NOTE: the entries is not included
                self.large_offsets.push(offsets);
                self.validity.extend(validity);
            }
            &ArrayMapping::LargeList {
                offsets, validity, ..
            } => {
                // NOTE: the item is not included
                self.large_offsets.push(offsets);
                self.validity.extend(validity);
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
#[derive(Debug, Clone)]
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
    Map {
        field: GenericField,
        offsets: usize,
        validity: Option<usize>,
        entries: Box<ArrayMapping>,
    },
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
    pub large_lists: Vec<ListDefinition>,
    pub maps: Vec<MapDefinition>,
    pub structs: Vec<StructDefinition>,
    pub unions: Vec<UnionDefinition>,
    pub nulls: Vec<NullDefinition>,
    pub array_mapping: Vec<ArrayMapping>,
}

#[derive(Debug, Default, Clone)]
pub struct BufferCounts {
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
    fn push_instr(&mut self, instr: Bytecode) {
        self.structure.program.push(instr);
    }
}

impl Program {
    fn compile_outer_structure(&mut self, fields: &[GenericField]) -> Result<()> {
        if !self.options.wrap_with_struct && fields.len() != 1 {
            fail!("only single fields are supported without struct wrapping");
        }

        self.structure.large_lists.push(ListDefinition::default());
        self.push_instr(Bytecode::OuterSequenceStart(OuterSequenceStart {
            next: UNSET_INSTR,
        }));
        self.push_instr(Bytecode::OuterSequenceItem(OuterSequenceItem {
            next: UNSET_INSTR,
            list_idx: 0,
        }));
        self.structure.large_lists[0].item = self.structure.program.len();

        if self.options.wrap_with_struct {
            self.structure.structs.push(StructDefinition::default());
            self.push_instr(Bytecode::OuterRecordStart(OuterRecordStart {
                next: UNSET_INSTR,
            }));
        }

        for field in fields {
            if self.options.wrap_with_struct {
                self.push_instr(Bytecode::OuterRecordField(OuterRecordField {
                    next: UNSET_INSTR,
                    struct_idx: 0,
                    field_name: field.name.to_string(),
                }));
                self.structure.structs[0]
                    .fields
                    .insert(field.name.to_string(), self.structure.program.len());
            }
            let f = self.compile_field(field)?;
            self.structure.array_mapping.push(f);
        }

        if self.options.wrap_with_struct {
            self.push_instr(Bytecode::OuterRecordEnd(OuterRecordEnd {
                next: UNSET_INSTR,
                struct_idx: 0,
            }));
            self.structure.structs[0].r#return = self.structure.program.len();
        }

        self.push_instr(Bytecode::OuterSequenceEnd(OuterSequenceEnd {
            next: UNSET_INSTR,
            list_idx: 0,
        }));
        self.structure.large_lists[0].r#return = self.structure.program.len();

        let next_instr = self.structure.program.len();
        self.push_instr(Bytecode::ProgramEnd(ProgramEnd { next: next_instr }));

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

        let (is_tuple, is_map) = match field.strategy.as_ref() {
            None => (false, false),
            Some(Strategy::MapAsStruct) => (false, true),
            Some(Strategy::TupleAsStruct) => (true, false),
            Some(strategy) => fail!("Cannot compile struct with strategy {strategy}"),
        };

        let struct_idx = self.structure.structs.len();

        if !is_tuple {
            self.structure.structs.push(StructDefinition::default());
            self.push_instr(Bytecode::StructStart(StructStart { next: UNSET_INSTR }));
            self.structure.structs[struct_idx].item = UNSET_INSTR;
        } else {
            self.push_instr(Bytecode::TupleStructStart(TupleStructStart {
                next: UNSET_INSTR,
            }));
        }

        let mut field_mapping = vec![];

        for field in &field.children {
            if !is_tuple {
                if is_map {
                    self.push_instr(Bytecode::StructItem(StructItem {
                        next: UNSET_INSTR,
                        struct_idx,
                    }));
                    if self.structure.structs[struct_idx].item == UNSET_INSTR {
                        self.structure.structs[struct_idx].item = self.structure.program.len();
                    }
                }
                self.push_instr(Bytecode::StructField(StructField {
                    next: UNSET_INSTR,
                    struct_idx,
                    field_name: field.name.to_string(),
                }));
                self.structure.structs[struct_idx]
                    .fields
                    .insert(field.name.to_string(), self.structure.program.len());
            } else {
                self.push_instr(Bytecode::TupleStructItem(TupleStructItem {
                    next: UNSET_INSTR,
                }));
            }
            let f = self.compile_field(field)?;
            field_mapping.push(f);
        }

        if !is_tuple {
            self.push_instr(Bytecode::StructEnd(StructEnd {
                next: UNSET_INSTR,
                struct_idx,
            }));
            self.structure.structs[struct_idx].r#return = self.structure.program.len();
        } else {
            self.push_instr(Bytecode::TupleStructEnd(TupleStructEnd {
                next: UNSET_INSTR,
            }));
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

        let list_idx = self.structure.large_lists.len();
        let offsets = self.buffers.num_large_offsets.next_value();

        self.structure.large_lists.push(ListDefinition::default());
        self.structure.large_lists[list_idx].offset = offsets;

        self.push_instr(Bytecode::LargeListStart(LargeListStart {
            next: UNSET_INSTR,
        }));
        self.push_instr(Bytecode::LargeListItem(LargeListItem {
            next: UNSET_INSTR,
            list_idx,
            offsets,
        }));
        self.structure.large_lists[list_idx].item = self.structure.program.len();

        let field_mapping = self.compile_field(item)?;

        self.push_instr(Bytecode::LargeListEnd(LargeListEnd {
            next: UNSET_INSTR,
            list_idx,
            offsets,
        }));
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

        let type_idx = self.buffers.num_i8.next_value();

        let mut fields = Vec::new();
        let mut child_last_instr = Vec::new();

        self.push_instr(Bytecode::Variant(Variant {
            next: UNSET_INSTR,
            union_idx,
            type_idx,
        }));

        for child in &field.children {
            self.structure.unions[union_idx]
                .fields
                .push(self.structure.program.len());
            fields.push(self.compile_field(child)?);
            child_last_instr.push(self.structure.program.len() - 1);
        }

        // each union fields jumps to after the "union"
        for pos in child_last_instr {
            let next_instr = self.structure.program.len();
            self.structure.program[pos].set_next(next_instr);
        }

        self.push_instr(Bytecode::UnionEnd(UnionEnd { next: UNSET_INSTR }));

        Ok(ArrayMapping::Union {
            field: field.clone(),
            fields,
            types: type_idx,
        })
    }

    fn compile_field(&mut self, field: &GenericField) -> Result<ArrayMapping> {
        let mut nullable_idx = None;
        let validity = if field.nullable {
            let validity = self.buffers.num_validity.next_value();
            self.structure.nulls.push(NullDefinition::default());

            nullable_idx = Some(self.structure.program.len());
            self.push_instr(Bytecode::Option(OptionMarker {
                next: UNSET_INSTR,
                if_none: 0,
                validity,
            }));

            Some(validity)
        } else {
            None
        };

        let array_mapping = self.compile_field_inner(field, validity)?;

        if let Some(nullable_idx) = nullable_idx {
            let current_program_len = self.structure.program.len();
            let Bytecode::Option(instr) = &mut self.structure.program[nullable_idx] else {
                fail!("Internal error during compilation");
            };
            instr.if_none = current_program_len;
            self.structure.nulls[instr.validity].update_from_array_mapping(&array_mapping)?;
            self.structure.nulls[instr.validity].sort_indices();
        }

        Ok(array_mapping)
    }
}

macro_rules! compile_primtive {
    ($this:expr, $field:expr, $validity:expr, $num:ident, $instr:ident, $mapping:ident) => {{
        $this.push_instr(Bytecode::$instr($instr {
            next: UNSET_INSTR,
            idx: $this.buffers.$num,
        }));
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
            D::Utf8 => compile_primtive!(self, field, validity, num_utf8, PushUtf8, Utf8),
            D::LargeUtf8 => compile_primtive!(
                self,
                field,
                validity,
                num_large_utf8,
                PushLargeUtf8,
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
            D::Map => self.compile_map(field, validity),
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
            D::U8 => I::U8(self.buffers.num_u8.next_value()),
            D::U16 => I::U16(self.buffers.num_u16.next_value()),
            D::U32 => I::U32(self.buffers.num_u32.next_value()),
            D::U64 => I::U64(self.buffers.num_u64.next_value()),
            D::I8 => I::I8(self.buffers.num_i8.next_value()),
            D::I16 => I::I16(self.buffers.num_i16.next_value()),
            D::I32 => I::I32(self.buffers.num_i32.next_value()),
            D::I64 => I::I64(self.buffers.num_i64.next_value()),
            dt => fail!("cannot compile dictionary with indices of type {dt}"),
        };

        let values = match &field.children[1].data_type {
            D::Utf8 => V::Utf8(self.buffers.num_dictionaries.next_value()),
            D::LargeUtf8 => V::LargeUtf8(self.buffers.num_large_dictionaries.next_value()),
            dt => fail!("cannot compile dictionary with values of type {dt}"),
        };
        self.push_instr(B::PushDictionary(PushDictionary {
            next: UNSET_INSTR,
            values,
            indices,
        }));

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
        if !field.is_valid_map() {
            fail!("cannot compile invalid map field: {field:?}");
        }
        let Some(entries) = field.children.get(0) else {
            fail!("invalid list: no child");
        };
        let Some(keys) = entries.children.get(0) else {
            fail!("entries without key field");
        };
        let Some(values) = entries.children.get(1) else {
            fail!("entries without values field");
        };

        let map_idx = self.structure.maps.len();
        let offsets = self.buffers.num_offsets.next_value();

        self.structure.maps.push(MapDefinition::default());

        self.push_instr(Bytecode::MapStart(MapStart { next: UNSET_INSTR }));
        self.push_instr(Bytecode::MapItem(MapItem {
            next: UNSET_INSTR,
            map_idx,
            offsets,
        }));
        self.structure.maps[map_idx].key = self.structure.program.len();

        let keys_mapping = self.compile_field(keys)?;
        let values_mapping = self.compile_field(values)?;

        self.push_instr(Bytecode::MapEnd(MapEnd {
            next: UNSET_INSTR,
            map_idx,
            offsets,
        }));
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
        self.validate_lists()?;
        self.validate_maps()?;
        self.validate_structs()?;
        self.validate_nulls()?;
        self.validate_array_mappings()?;
        self.validate_next_instruction()?;
        Ok(())
    }

    fn validate_lists(&self) -> Result<()> {
        for (list_idx, list) in self.structure.large_lists.iter().enumerate() {
            let item_instr = self.instruction_before(list.item);
            if !matches!(
                item_instr,
                Some(Bytecode::LargeListItem(_)) | Some(&Bytecode::OuterSequenceItem(_))
            ) {
                fail!("invalid list definition ({list_idx}): item points to {item_instr:?}");
            }

            let before_return_instr = self.instruction_before(list.r#return);
            if !matches!(
                before_return_instr,
                Some(Bytecode::OuterSequenceEnd(_)) | Some(Bytecode::LargeListEnd(_))
            ) {
                fail!("invalid list definition ({list_idx}): instr before return is {before_return_instr:?}");
            }
        }
        Ok(())
    }

    fn validate_structs(&self) -> Result<()> {
        for (struct_idx, r#struct) in self.structure.structs.iter().enumerate() {
            for (name, address) in &r#struct.fields {
                let field_instr = self.instruction_before(*address);
                let is_valid = if let Some(Bytecode::StructField(instr)) = field_instr {
                    instr.struct_idx == struct_idx && instr.field_name == *name
                } else if let Some(Bytecode::OuterRecordField(instr)) = field_instr {
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
                Some(&Bytecode::StructEnd(_))
                    | Some(&Bytecode::OuterRecordEnd(_))
                    | Some(&Bytecode::UnionEnd(_))
            ) {
                fail!("invalid struct definition ({struct_idx}): instr before return is {before_return_instr:?}");
            }

            if !self.structure.program[r#struct.r#return].is_allowed_jump_target() {
                fail!("invalid struct definition ({struct_idx}): return jumps to invalid target");
            }

            for (name, address) in &r#struct.fields {
                if !self.structure.program[*address].is_allowed_jump_target() {
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
            if null.null.iter().any(|&idx| idx >= self.buffers.num_null) {
                fail!("invalid null definition {idx}: null out of bounds {null:?}");
            }
            if null.bool.iter().any(|&idx| idx >= self.buffers.num_bool) {
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
            if null.i8.iter().any(|&idx| idx >= self.buffers.num_i8) {
                fail!("invalid null definition {idx}: i8 out of bounds {null:?}");
            }
            if null.i16.iter().any(|&idx| idx >= self.buffers.num_i16) {
                fail!("invalid null definition {idx}: i16 out of bounds {null:?}");
            }
            if null.i32.iter().any(|&idx| idx >= self.buffers.num_i32) {
                fail!("invalid null definition {idx}: i32 out of bounds {null:?}");
            }
            if null.i64.iter().any(|&idx| idx >= self.buffers.num_i64) {
                fail!("invalid null definition {idx}: i64 out of bounds {null:?}");
            }
            if null.f32.iter().any(|&idx| idx >= self.buffers.num_f32) {
                fail!("invalid null definition {idx}: f32 out of bounds {null:?}");
            }
            if null.f64.iter().any(|&idx| idx >= self.buffers.num_f64) {
                fail!("invalid null definition {idx}: f64 out of bounds {null:?}");
            }
            if null.utf8.iter().any(|&idx| idx >= self.buffers.num_utf8) {
                fail!("invalid null definition {idx}: u8 out of bounds {null:?}");
            }
            if null
                .large_utf8
                .iter()
                .any(|&idx| idx >= self.buffers.num_large_utf8)
            {
                fail!("invalid null definition {idx}: large_u8 out of bounds {null:?}");
            }
            if null
                .validity
                .iter()
                .any(|&idx| idx >= self.buffers.num_validity)
            {
                fail!("invalid null definition {idx}: validity out of bounds {null:?}");
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
                if validity >= $this.buffers.num_validity {
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
