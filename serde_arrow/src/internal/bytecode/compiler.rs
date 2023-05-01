use std::collections::BTreeMap;

use crate::{
    internal::{
        error::{error, fail},
        schema::{GenericDataType, GenericField},
    },
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

#[derive(Debug, PartialEq)]
pub enum Bytecode {
    ProgramEnd,
    OuterSequenceStart(usize),
    OuterSequenceItem(usize),
    OuterSequenceEnd(usize),
    OuterRecordStart(usize),
    OuterRecordField(usize, String),
    OuterRecordEnd(usize),
    LargeListStart(usize),
    LargeListItem(usize),
    LargeListEnd(usize),
    StructStart(usize),
    StructField(usize, String),
    StructEnd(usize),
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
    /// `Option(if_none, validity)`
    Option(usize, usize),
}

#[derive(Default, Debug, PartialEq)]
pub struct StructDefinition {
    pub fields: BTreeMap<String, usize>,
}

#[derive(Default, Debug, PartialEq)]
pub struct ListDefinition {
    pub item: usize,
    pub r#return: usize,
    pub offset: usize,
}

#[derive(Default, Debug, PartialEq)]
pub struct NullDefinition {
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
    pub validity: Vec<usize>,
}

impl NullDefinition {
    pub fn update_from_array_mapping(&mut self, m: &ArrayMapping) -> Result<()> {
        match m {
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
            m => todo!("cannot update null definition from {m:?}"),
        }
        Ok(())
    }
}

/// Map the array to the corresponding builders
#[derive(Debug)]
pub enum ArrayMapping {
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
    List {
        field: GenericField,
        item: Box<ArrayMapping>,
        offsets: usize,
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
}

#[derive(Debug)]
pub struct Program {
    pub(crate) options: CompilationOptions,
    pub(crate) program: Vec<Bytecode>,
    pub(crate) large_lists: Vec<ListDefinition>,
    pub(crate) structs: Vec<StructDefinition>,
    pub(crate) nulls: Vec<NullDefinition>,
    pub(crate) array_mapping: Vec<ArrayMapping>,
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
}

impl Program {
    pub fn new(options: CompilationOptions) -> Self {
        Self {
            options,
            program: Vec::new(),
            large_lists: Vec::new(),
            structs: Vec::new(),
            nulls: Vec::new(),
            array_mapping: Vec::new(),
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
        }
    }
}

impl Program {
    fn compile(&mut self, fields: &[GenericField]) -> Result<()> {
        self.compile_outer_structure(fields)?;
        self.validate()?;

        Ok(())
    }

    fn compile_outer_structure(&mut self, fields: &[GenericField]) -> Result<()> {
        if !self.options.wrap_with_struct && fields.len() != 1 {
            fail!("only single fields are supported without struct wrapping");
        }

        self.large_lists.push(ListDefinition::default());
        self.program.push(Bytecode::OuterSequenceStart(0));
        self.program.push(Bytecode::OuterSequenceItem(0));
        self.large_lists[0].item = self.program.len();

        if self.options.wrap_with_struct {
            self.structs.push(StructDefinition::default());
            self.program.push(Bytecode::OuterRecordStart(0));
        }

        for field in fields {
            if self.options.wrap_with_struct {
                self.structs[0]
                    .fields
                    .insert(field.name.to_string(), self.program.len());
                self.program
                    .push(Bytecode::OuterRecordField(0, field.name.to_string()));
            }
            let f = self.compile_field(field)?;
            self.array_mapping.push(f);
        }

        if self.options.wrap_with_struct {
            self.program.push(Bytecode::OuterRecordEnd(0));
        }

        self.program.push(Bytecode::OuterSequenceEnd(0));
        self.large_lists[0].r#return = self.program.len();

        self.program.push(Bytecode::ProgramEnd);

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
            // TODO: if supported, check that at least a single field is present
            fail!("Nullable structs are not supported");
        }

        let idx = self.structs.len();

        self.structs.push(StructDefinition::default());
        self.program.push(Bytecode::StructStart(idx));

        let mut field_mapping = vec![];

        for field in &field.children {
            self.structs[idx]
                .fields
                .insert(field.name.to_string(), self.program.len());
            self.program
                .push(Bytecode::StructField(idx, field.name.to_string()));
            let f = self.compile_field(field)?;
            field_mapping.push(f);
        }
        self.program.push(Bytecode::StructEnd(idx));

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

        self.program.push(Bytecode::LargeListStart(idx));
        self.program.push(Bytecode::LargeListItem(idx));
        self.large_lists[idx].item = self.program.len();

        let field_mapping = self.compile_field(item)?;

        self.program.push(Bytecode::LargeListEnd(idx));
        self.large_lists[idx].r#return = self.program.len();

        Ok(ArrayMapping::LargeList {
            field: field.clone(),
            item: Box::new(field_mapping),
            offsets,
            validity,
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
            D::Struct => self.compile_struct(field, validity),
            D::List => self.compile_list(field, validity),
            D::LargeList => self.compile_large_list(field, validity),
            dt => fail!("cannot compile {dt}: not implemented"),
        }
    }
}

impl Program {
    fn validate(&self) -> Result<()> {
        self.validate_lists()?;
        self.validate_structs()?;
        self.validate_nulls()?;
        self.validate_array_mappings()?;
        Ok(())
    }

    fn validate_lists(&self) -> Result<()> {
        for (idx, list) in self.large_lists.iter().enumerate() {
            let item_instr = self.instruction_before(list.item);
            if item_instr != Some(&Bytecode::LargeListItem(idx))
                && item_instr != Some(&Bytecode::OuterSequenceItem(idx))
            {
                fail!("invalid list definition ({idx}): item points to {item_instr:?}");
            }

            let before_return_instr = self.instruction_before(list.r#return);
            if before_return_instr != Some(&Bytecode::LargeListEnd(idx))
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
                let field_instr = self.program.get(*address);
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

            /*let before_return_instr = self.instruction_before(r#struct.r#return);
            if before_return_instr != Some(&Bytecode::StructEnd(idx)) {
                fail!("invalid struct definition ({idx}): instr before return is {before_return_instr:?}");
            }*/
        }
        Ok(())
    }

    fn validate_nulls(&self) -> Result<()> {
        for (idx, null) in self.nulls.iter().enumerate() {
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
    fn validate_array_mapping(&self, path: String, array_mapping: &ArrayMapping) -> Result<()> {
        use ArrayMapping::*;
        match array_mapping {
            Bool { .. } => {
                validate_array_mapping_primitive!(self, path, array_mapping, Bool, num_bool)
            }
            U8 { .. } => validate_array_mapping_primitive!(self, path, array_mapping, U8, num_u8),
            U16 { .. } => {
                validate_array_mapping_primitive!(self, path, array_mapping, U16, num_u16)
            }
            U32 { .. } => {
                validate_array_mapping_primitive!(self, path, array_mapping, U32, num_u32)
            }
            U64 { .. } => {
                validate_array_mapping_primitive!(self, path, array_mapping, U64, num_u64)
            }
            I8 { .. } => validate_array_mapping_primitive!(self, path, array_mapping, I8, num_i8),
            I16 { .. } => {
                validate_array_mapping_primitive!(self, path, array_mapping, I16, num_i16)
            }
            I32 { .. } => {
                validate_array_mapping_primitive!(self, path, array_mapping, I32, num_i32)
            }
            I64 { .. } => {
                validate_array_mapping_primitive!(self, path, array_mapping, I64, num_i64)
            }
            F32 { .. } => {
                validate_array_mapping_primitive!(self, path, array_mapping, F32, num_f32)
            }
            F64 { .. } => {
                validate_array_mapping_primitive!(self, path, array_mapping, F64, num_f64)
            }
            Utf8 { .. } => {
                validate_array_mapping_primitive!(self, path, array_mapping, Utf8, num_utf8)
            }
            LargeUtf8 { .. } => validate_array_mapping_primitive!(
                self,
                path,
                array_mapping,
                LargeUtf8,
                num_large_utf8
            ),
            _ => {}
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::{compile_serialization, Bytecode};
    use crate::{
        _impl::bytecode::compiler::CompilationOptions,
        internal::schema::{GenericDataType, GenericField},
    };

    #[test]
    fn empty() {
        let program = compile_serialization(&[], CompilationOptions::default()).unwrap();

        assert_eq!(
            program.program,
            vec![
                Bytecode::OuterSequenceStart(0),
                Bytecode::OuterSequenceItem(0),
                Bytecode::OuterRecordStart(0),
                Bytecode::OuterRecordEnd(0),
                Bytecode::OuterSequenceEnd(0),
                Bytecode::ProgramEnd,
            ],
        );
    }

    #[test]
    fn single_field_field() {
        let program = compile_serialization(
            &[GenericField::new("foo", GenericDataType::Bool, false)],
            CompilationOptions::default(),
        )
        .unwrap();

        assert_eq!(
            program.program,
            vec![
                Bytecode::OuterSequenceStart(0),
                Bytecode::OuterSequenceItem(0),
                Bytecode::OuterRecordStart(0),
                Bytecode::OuterRecordField(0, "foo".into()),
                Bytecode::PushBool(0),
                Bytecode::OuterRecordEnd(0),
                Bytecode::OuterSequenceEnd(0),
                Bytecode::ProgramEnd,
            ],
        );
    }

    #[test]
    fn nested_structs() {
        let program = compile_serialization(
            &[
                GenericField::new("foo", GenericDataType::U8, true),
                GenericField::new("bar", GenericDataType::Struct, false)
                    .with_child(GenericField::new("x", GenericDataType::F32, false))
                    .with_child(GenericField::new("y", GenericDataType::F32, false)),
            ],
            CompilationOptions::default(),
        )
        .unwrap();

        assert_eq!(
            program.program,
            vec![
                Bytecode::OuterSequenceStart(0),
                Bytecode::OuterSequenceItem(0),
                Bytecode::OuterRecordStart(0),
                Bytecode::OuterRecordField(0, "foo".into()),
                Bytecode::Option(6, 0),
                Bytecode::PushU8(0),
                Bytecode::OuterRecordField(0, "bar".into()),
                Bytecode::StructStart(1),
                Bytecode::StructField(1, "x".into()),
                Bytecode::PushF32(0),
                Bytecode::StructField(1, "y".into()),
                Bytecode::PushF32(1),
                Bytecode::StructEnd(1),
                Bytecode::OuterRecordEnd(0),
                Bytecode::OuterSequenceEnd(0),
                Bytecode::ProgramEnd,
            ],
        );
    }

    #[test]
    fn benchmark_example() {
        let program = compile_serialization(
            &[
                GenericField::new("foo", GenericDataType::LargeUtf8, false),
                GenericField::new("bar", GenericDataType::LargeList, false).with_child(
                    GenericField::new("element", GenericDataType::Struct, false)
                        .with_child(GenericField::new("x", GenericDataType::F32, false))
                        .with_child(GenericField::new("y", GenericDataType::F32, false)),
                ),
                GenericField::new("baz", GenericDataType::Struct, false)
                    .with_child(GenericField::new("a", GenericDataType::Bool, false))
                    .with_child(GenericField::new("b", GenericDataType::F64, false))
                    .with_child(GenericField::new("c", GenericDataType::F32, true)),
            ],
            CompilationOptions::default(),
        )
        .unwrap();

        assert_eq!(
            program.program,
            vec![
                Bytecode::OuterSequenceStart(0),
                Bytecode::OuterSequenceItem(0),
                Bytecode::OuterRecordStart(0),
                Bytecode::OuterRecordField(0, "foo".into()),
                Bytecode::PushLargeUTF8(0),
                Bytecode::OuterRecordField(0, "bar".into()),
                Bytecode::LargeListStart(1),
                Bytecode::LargeListItem(1),
                Bytecode::StructStart(1),
                Bytecode::StructField(1, "x".into()),
                Bytecode::PushF32(0),
                Bytecode::StructField(1, "y".into()),
                Bytecode::PushF32(1),
                Bytecode::StructEnd(1),
                Bytecode::LargeListEnd(1),
                Bytecode::OuterRecordField(0, "baz".into()),
                Bytecode::StructStart(2),
                Bytecode::StructField(2, "a".into()),
                Bytecode::PushBool(0),
                Bytecode::StructField(2, "b".into()),
                Bytecode::PushF64(0),
                Bytecode::StructField(2, "c".into()),
                Bytecode::Option(24, 0),
                Bytecode::PushF32(2),
                Bytecode::StructEnd(2),
                Bytecode::OuterRecordEnd(0),
                Bytecode::OuterSequenceEnd(0),
                Bytecode::ProgramEnd,
            ],
        );
    }
}
