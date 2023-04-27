use std::collections::BTreeMap;

use crate::{
    internal::{
        error::{error, fail},
        schema::{GenericDataType, GenericField},
    },
    Result,
};

pub fn compile_serialization(fields: &[GenericField]) -> Result<Program> {
    let mut program = Program::new();
    program.compile(fields)?;
    Ok(program)
}

#[derive(Debug, PartialEq)]
pub enum Bytecode {
    ProgramEnd,
    ListStart(usize),
    ListItem(usize),
    ListEnd(usize),
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
}

#[derive(Debug, Default)]
pub struct Program {
    pub(crate) program: Vec<Bytecode>,
    pub(crate) lists: Vec<ListDefinition>,
    pub(crate) structs: Vec<StructDefinition>,
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
}

impl Program {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Program {
    fn compile(&mut self, fields: &[GenericField]) -> Result<()> {
        self.lists.push(ListDefinition::default());
        self.structs.push(StructDefinition::default());

        self.program.push(Bytecode::ListStart(0));
        self.program.push(Bytecode::ListItem(0));
        self.lists[0].item = self.program.len();

        self.program.push(Bytecode::StructStart(0));

        for field in fields {
            self.structs[0]
                .fields
                .insert(field.name.to_string(), self.program.len());
            self.program
                .push(Bytecode::StructField(0, field.name.to_string()));
            self.compile_field(field)?;
        }

        self.program.push(Bytecode::StructEnd(0));
        self.program.push(Bytecode::ListEnd(0));
        self.lists[0].r#return = self.program.len();

        self.program.push(Bytecode::ProgramEnd);

        self.validate()?;

        Ok(())
    }

    fn compile_struct(&mut self, field: &GenericField) -> Result<()> {
        let idx = self.structs.len();
        self.structs.push(StructDefinition::default());

        self.program.push(Bytecode::StructStart(idx));

        for field in &field.children {
            self.structs[idx]
                .fields
                .insert(field.name.to_string(), self.program.len());
            self.program
                .push(Bytecode::StructField(idx, field.name.to_string()));
            self.compile_field(field)?;
        }
        self.program.push(Bytecode::StructEnd(idx));

        Ok(())
    }

    fn compile_list(&mut self, field: &GenericField) -> Result<()> {
        let item = field
            .children
            .get(0)
            .ok_or_else(|| error!("invalid list: no child"))?;

        let idx = self.lists.len();
        self.lists.push(ListDefinition::default());

        self.program.push(Bytecode::ListStart(idx));
        self.program.push(Bytecode::ListItem(idx));
        self.lists[idx].item = self.program.len();

        self.compile_field(item)?;

        self.program.push(Bytecode::ListEnd(idx));
        self.lists[idx].r#return = self.program.len();

        Ok(())
    }

    fn compile_field(&mut self, field: &GenericField) -> Result<()> {
        let mut nullable_idx = None;
        if field.nullable {
            nullable_idx = Some(self.program.len());
            self.program.push(Bytecode::Option(0));
        }

        self.compile_field_inner(field)?;

        if let Some(nullable_idx) = nullable_idx {
            let current_program_len = self.program.len();
            let Bytecode::Option(if_none) = &mut self.program[nullable_idx] else {
                fail!("Internal error during compilation");
            };
            *if_none = current_program_len;
        }

        Ok(())
    }

    fn compile_field_inner(&mut self, field: &GenericField) -> Result<()> {
        fn primitive(
            program: &mut Vec<Bytecode>,
            num: &mut usize,
            ctor: fn(usize) -> Bytecode,
        ) -> Result<()> {
            program.push(ctor(*num));
            *num += 1;
            Ok(())
        }

        use {Bytecode as B, GenericDataType as D};

        match field.data_type {
            D::Bool => primitive(&mut self.program, &mut self.num_bool, B::PushBool),
            D::U8 => primitive(&mut self.program, &mut self.num_u8, B::PushU8),
            D::U16 => primitive(&mut self.program, &mut self.num_u16, B::PushU16),
            D::U32 => primitive(&mut self.program, &mut self.num_u32, B::PushU32),
            D::U64 => primitive(&mut self.program, &mut self.num_u64, B::PushU64),
            D::I8 => primitive(&mut self.program, &mut self.num_i8, B::PushI8),
            D::I16 => primitive(&mut self.program, &mut self.num_i16, B::PushI16),
            D::I32 => primitive(&mut self.program, &mut self.num_i32, B::PushI32),
            D::I64 => primitive(&mut self.program, &mut self.num_i64, B::PushI64),
            D::F32 => primitive(&mut self.program, &mut self.num_f32, B::PushF32),
            D::F64 => primitive(&mut self.program, &mut self.num_f64, B::PushF64),
            D::Utf8 => primitive(&mut self.program, &mut self.num_utf8, B::PushUTF8),
            D::LargeUtf8 => primitive(
                &mut self.program,
                &mut self.num_large_utf8,
                B::PushLargeUTF8,
            ),
            D::Struct => self.compile_struct(field),
            D::List | D::LargeList => self.compile_list(field),
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
        for (idx, list) in self.lists.iter().enumerate() {
            let item_instr = self.instruction_before(list.item);
            if item_instr != Some(&Bytecode::ListItem(idx)) {
                fail!("invalid list definition ({idx}): item points to {item_instr:?}");
            }

            let before_return_instr = self.instruction_before(list.r#return);
            if before_return_instr != Some(&Bytecode::ListEnd(idx)) {
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
    use crate::internal::schema::{GenericDataType, GenericField};

    #[test]
    fn empty() {
        let program = compile_serialization(&[]).unwrap();

        assert_eq!(
            program.program,
            vec![
                Bytecode::ListStart(0),
                Bytecode::ListItem(0),
                Bytecode::StructStart(0),
                Bytecode::StructEnd(0),
                Bytecode::ListEnd(0),
                Bytecode::ProgramEnd,
            ],
        );
    }

    #[test]
    fn single_field_field() {
        let program =
            compile_serialization(&[GenericField::new("foo", GenericDataType::Bool, false)])
                .unwrap();

        assert_eq!(
            program.program,
            vec![
                Bytecode::ListStart(0),
                Bytecode::ListItem(0),
                Bytecode::StructStart(0),
                Bytecode::StructField(0, "foo".into()),
                Bytecode::PushBool(0),
                Bytecode::StructEnd(0),
                Bytecode::ListEnd(0),
                Bytecode::ProgramEnd,
            ],
        );
    }

    #[test]
    fn single_field_bool_nullable() {
        let program =
            compile_serialization(&[GenericField::new("foo", GenericDataType::Bool, true)])
                .unwrap();

        assert_eq!(
            program.program,
            vec![
                Bytecode::ListStart(0),
                Bytecode::ListItem(0),
                Bytecode::StructStart(0),
                Bytecode::StructField(0, "foo".into()),
                Bytecode::Option(6),
                Bytecode::PushBool(0),
                Bytecode::StructEnd(0),
                Bytecode::ListEnd(0),
                Bytecode::ProgramEnd,
            ],
        );
    }

    #[test]
    fn nested_structs() {
        let program = compile_serialization(&[
            GenericField::new("foo", GenericDataType::U8, true),
            GenericField::new("bar", GenericDataType::Struct, false)
                .with_child(GenericField::new("x", GenericDataType::F32, false))
                .with_child(GenericField::new("y", GenericDataType::F32, false)),
        ])
        .unwrap();

        assert_eq!(
            program.program,
            vec![
                Bytecode::ListStart(0),
                Bytecode::ListItem(0),
                Bytecode::StructStart(0),
                Bytecode::StructField(0, "foo".into()),
                Bytecode::Option(6),
                Bytecode::PushU8(0),
                Bytecode::StructField(0, "bar".into()),
                Bytecode::StructStart(1),
                Bytecode::StructField(1, "x".into()),
                Bytecode::PushF32(0),
                Bytecode::StructField(1, "y".into()),
                Bytecode::PushF32(1),
                Bytecode::StructEnd(1),
                Bytecode::StructEnd(0),
                Bytecode::ListEnd(0),
                Bytecode::ProgramEnd,
            ],
        );
    }

    #[test]
    fn benchmark_example() {
        let program = compile_serialization(&[
            GenericField::new("foo", GenericDataType::LargeUtf8, false),
            GenericField::new("bar", GenericDataType::List, false).with_child(
                GenericField::new("element", GenericDataType::Struct, false)
                    .with_child(GenericField::new("x", GenericDataType::F32, false))
                    .with_child(GenericField::new("y", GenericDataType::F32, false)),
            ),
            GenericField::new("baz", GenericDataType::Struct, false)
                .with_child(GenericField::new("a", GenericDataType::Bool, false))
                .with_child(GenericField::new("b", GenericDataType::F64, false))
                .with_child(GenericField::new("c", GenericDataType::F32, true)),
        ])
        .unwrap();

        assert_eq!(
            program.program,
            vec![
                Bytecode::ListStart(0),
                Bytecode::ListItem(0),
                Bytecode::StructStart(0),
                Bytecode::StructField(0, "foo".into()),
                Bytecode::PushLargeUTF8(0),
                Bytecode::StructField(0, "bar".into()),
                Bytecode::ListStart(1),
                Bytecode::ListItem(1),
                Bytecode::StructStart(1),
                Bytecode::StructField(1, "x".into()),
                Bytecode::PushF32(0),
                Bytecode::StructField(1, "y".into()),
                Bytecode::PushF32(1),
                Bytecode::StructEnd(1),
                Bytecode::ListEnd(1),
                Bytecode::StructField(0, "baz".into()),
                Bytecode::StructStart(2),
                Bytecode::StructField(2, "a".into()),
                Bytecode::PushBool(0),
                Bytecode::StructField(2, "b".into()),
                Bytecode::PushF64(0),
                Bytecode::StructField(2, "c".into()),
                Bytecode::Option(24),
                Bytecode::PushF32(2),
                Bytecode::StructEnd(2),
                Bytecode::StructEnd(0),
                Bytecode::ListEnd(0),
                Bytecode::ProgramEnd,
            ],
        );
    }
}
