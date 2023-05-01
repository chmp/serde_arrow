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
    /// `Option(if_none)`
    Option(usize),
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

/// Map the array to the corresponding builders
#[derive(Debug)]
pub enum ArrayMapping {
    Bool {
        field: GenericField,
        buffer: usize,
        validity: Option<usize>,
    },
    U8(GenericField, usize),
    U16(GenericField, usize),
    U32(GenericField, usize),
    U64(GenericField, usize),
    I8(GenericField, usize),
    I16(GenericField, usize),
    I32(GenericField, usize),
    I64(GenericField, usize),
    F32(GenericField, usize),
    F64(GenericField, usize),
    Utf8(GenericField, usize),
    LargeUtf8(GenericField, usize),
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
            program: Default::default(),
            large_lists: Default::default(),
            structs: Default::default(),
            array_mapping: Default::default(),
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

    fn compile_struct(&mut self, field: &GenericField) -> Result<ArrayMapping> {
        if field.nullable {
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
            validity: None,
        })
    }

    fn compile_list(&mut self, _field: &GenericField) -> Result<ArrayMapping> {
        fail!("Cannot compile lists: Not implemented")
    }

    fn compile_large_list(&mut self, field: &GenericField) -> Result<ArrayMapping> {
        if field.nullable {
            fail!("Nullable lists are not supported");
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
            validity: None,
        })
    }

    fn compile_field(&mut self, field: &GenericField) -> Result<ArrayMapping> {
        let mut nullable_idx = None;
        if field.nullable {
            nullable_idx = Some(self.program.len());
            self.program.push(Bytecode::Option(0));
        }

        let array_mapping = self.compile_field_inner(field)?;

        if let Some(nullable_idx) = nullable_idx {
            let current_program_len = self.program.len();
            let Bytecode::Option(if_none) = &mut self.program[nullable_idx] else {
                fail!("Internal error during compilation");
            };
            *if_none = current_program_len;
        }

        Ok(array_mapping)
    }
}

macro_rules! compile_primtive {
    ($this:expr, $field:expr, $num:ident, $instr:ident, $mapping:ident) => {{
        if $field.nullable {
            fail!("Nullable primitive fields are not supported");
        }

        $this.program.push(Bytecode::$instr($this.$num));
        let res = ArrayMapping::$mapping {
            field: $field.clone(),
            buffer: $this.$num,
            validity: None,
        };

        $this.$num += 1;
        Ok(res)
    }};
}

impl Program {
    fn compile_field_inner(&mut self, field: &GenericField) -> Result<ArrayMapping> {
        fn prim(
            program: &mut Vec<Bytecode>,
            num: &mut usize,
            field: &GenericField,
            ctor: fn(usize) -> Bytecode,
            mapping: fn(GenericField, usize) -> ArrayMapping,
        ) -> Result<ArrayMapping> {
            program.push(ctor(*num));
            let res = mapping(field.clone(), *num);
            *num += 1;
            Ok(res)
        }

        use {ArrayMapping as M, Bytecode as B, GenericDataType as D};

        match field.data_type {
            D::Bool => compile_primtive!(self, field, num_bool, PushBool, Bool),
            D::U8 => prim(&mut self.program, &mut self.num_u8, field, B::PushU8, M::U8),
            D::U16 => prim(
                &mut self.program,
                &mut self.num_u16,
                field,
                B::PushU16,
                M::U16,
            ),
            D::U32 => prim(
                &mut self.program,
                &mut self.num_u32,
                field,
                B::PushU32,
                M::U32,
            ),
            D::U64 => prim(
                &mut self.program,
                &mut self.num_u64,
                field,
                B::PushU64,
                M::U64,
            ),
            D::I8 => prim(&mut self.program, &mut self.num_i8, field, B::PushI8, M::I8),
            D::I16 => prim(
                &mut self.program,
                &mut self.num_i16,
                field,
                B::PushI16,
                M::I16,
            ),
            D::I32 => prim(
                &mut self.program,
                &mut self.num_i32,
                field,
                B::PushI32,
                M::I32,
            ),
            D::I64 => prim(
                &mut self.program,
                &mut self.num_i64,
                field,
                B::PushI64,
                M::I64,
            ),
            D::F32 => prim(
                &mut self.program,
                &mut self.num_f32,
                field,
                B::PushF32,
                M::F32,
            ),
            D::F64 => prim(
                &mut self.program,
                &mut self.num_f64,
                field,
                B::PushF64,
                M::F64,
            ),
            D::Utf8 => prim(
                &mut self.program,
                &mut self.num_utf8,
                field,
                B::PushUTF8,
                M::Utf8,
            ),
            D::LargeUtf8 => prim(
                &mut self.program,
                &mut self.num_large_utf8,
                field,
                B::PushLargeUTF8,
                M::LargeUtf8,
            ),
            D::Struct => self.compile_struct(field),
            D::List => self.compile_list(field),
            D::LargeList => self.compile_large_list(field),
            dt => fail!("cannot compile {dt}: not implemented"),
        }
    }
}

impl Program {
    fn validate(&self) -> Result<()> {
        self.validate_lists()?;
        self.validate_structs()?;
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

    fn instruction_before(&self, idx: usize) -> Option<&Bytecode> {
        if idx != 0 {
            self.program.get(idx - 1)
        } else {
            None
        }
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
                Bytecode::Option(6),
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
                Bytecode::Option(24),
                Bytecode::PushF32(2),
                Bytecode::StructEnd(2),
                Bytecode::OuterRecordEnd(0),
                Bytecode::OuterSequenceEnd(0),
                Bytecode::ProgramEnd,
            ],
        );
    }
}
