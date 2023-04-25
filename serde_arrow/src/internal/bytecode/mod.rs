use std::collections::BTreeMap;

use crate::base::EventSink;

use super::{
    error::{fail, Result},
    sink::macros,
};

#[derive(Debug)]
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
    PushLargeUTF8(usize),
    /// `Option(jump_if_none)`
    Option(usize),
}

pub struct StructDefinition {
    pub start: usize,
    pub end: usize,
    pub fields: BTreeMap<String, usize>,
    pub r#return: usize,
}

pub struct ListDefinition {
    pub start: usize,
    pub end: usize,
    pub item: usize,
    pub r#return: usize,
}

pub type U8Builder = Vec<Option<u8>>;
pub type U16Builder = Vec<Option<u16>>;
pub type U32Builder = Vec<Option<u32>>;
pub type U64Builder = Vec<Option<u64>>;
pub type I8Builder = Vec<Option<i8>>;
pub type I16Builder = Vec<Option<i16>>;
pub type I32Builder = Vec<Option<i32>>;
pub type I64Builder = Vec<Option<i64>>;
pub type F32Builder = Vec<Option<f32>>;
pub type F64Builder = Vec<Option<f64>>;
pub type BoolBuilder = Vec<Option<bool>>;
pub type StringBuilder = Vec<Option<String>>;

pub struct Interpreter {
    pub program_counter: usize,
    pub program: Vec<Bytecode>,
    pub structs: Vec<StructDefinition>,
    pub lists: Vec<ListDefinition>,
    pub u8: Vec<U8Builder>,
    pub u16: Vec<U16Builder>,
    pub u32: Vec<U32Builder>,
    pub u64: Vec<U64Builder>,
    pub i8: Vec<I8Builder>,
    pub i16: Vec<I16Builder>,
    pub i32: Vec<I32Builder>,
    pub i64: Vec<I64Builder>,
    pub f32: Vec<F32Builder>,
    pub f64: Vec<F64Builder>,
    pub bool: Vec<BoolBuilder>,
    pub utf8: Vec<StringBuilder>,
}

macro_rules! accept_primitive {
    ($func:ident, $variant:ident, $builder:ident, $ty:ty) => {
        fn $func(&mut self, val: $ty) -> crate::Result<()> {
            match &self.program[self.program_counter] {
                Bytecode::$variant(array_idx) => {
                    self.$builder[*array_idx].push(Some(val));
                    self.program_counter += 1;
                    Ok(())
                }
                instr => fail!("Cannot accept {} in {instr:?}", stringify!($ty)),
            }
        }
    };
}

#[allow(clippy::match_single_binding)]
impl EventSink for Interpreter {
    macros::forward_generic_to_specialized!();

    accept_primitive!(accept_u8, PushU8, u8, u8);
    accept_primitive!(accept_u16, PushU16, u16, u16);
    accept_primitive!(accept_u32, PushU32, u32, u32);
    accept_primitive!(accept_u64, PushU64, u64, u64);
    accept_primitive!(accept_i8, PushI8, i8, i8);
    accept_primitive!(accept_i16, PushI16, i16, i16);
    accept_primitive!(accept_i32, PushI32, i32, i32);
    accept_primitive!(accept_i64, PushI64, i64, i64);
    accept_primitive!(accept_f32, PushF32, f32, f32);
    accept_primitive!(accept_f64, PushF64, f64, f64);
    accept_primitive!(accept_bool, PushBool, bool, bool);

    fn accept_start_sequence(&mut self) -> crate::Result<()> {
        match &self.program[self.program_counter] {
            &Bytecode::ListStart(_list_idx) => {
                self.program_counter += 1;
                Ok(())
            }
            instr => fail!("Cannot accept StartSequence in {instr:?}"),
        }
    }

    fn accept_end_sequence(&mut self) -> crate::Result<()> {
        match &self.program[self.program_counter] {
            &Bytecode::ListEnd(_list_idx) => {
                self.program_counter += 1;
                Ok(())
            }
            &Bytecode::ListItem(list_idx) => {
                self.program_counter = self.lists[list_idx].r#return;
                Ok(())
            }
            instr => fail!("Cannot accept EndSequence in {instr:?}"),
        }
    }

    fn accept_start_struct(&mut self) -> crate::Result<()> {
        match &self.program[self.program_counter] {
            &Bytecode::StructStart(_struct_idx) => {
                self.program_counter += 1;
                Ok(())
            }
            instr => fail!("Cannot accept StartStruct in {instr:?}"),
        }
    }

    fn accept_end_struct(&mut self) -> crate::Result<()> {
        match &self.program[self.program_counter] {
            &Bytecode::StructEnd(_struct_idx) => {
                self.program_counter += 1;
                Ok(())
            }
            instr => fail!("Cannot accept EndStruct in {instr:?}"),
        }
    }

    fn accept_item(&mut self) -> Result<()> {
        match &self.program[self.program_counter] {
            &Bytecode::ListItem(_list_idx) => {
                self.program_counter += 1;
                Ok(())
            }
            &Bytecode::ListEnd(list_idx) => {
                self.program_counter = self.lists[list_idx].item;
                Ok(())
            }
            instr => fail!("Cannot accept Item in {instr:?}"),
        }
    }

    fn accept_start_tuple(&mut self) -> Result<()> {
        match &self.program[self.program_counter] {
            Bytecode::ListStart(_list_idx) => {
                self.program_counter += 1;
                Ok(())
            }
            instr => fail!("Cannot accept StartTuple in {instr:?}"),
        }
    }

    fn accept_end_tuple(&mut self) -> Result<()> {
        match &self.program[self.program_counter] {
            Bytecode::ListEnd(_list_idx) => {
                self.program_counter += 1;
                Ok(())
            }
            &Bytecode::ListItem(list_idx) => {
                self.program_counter = self.lists[list_idx].r#return;
                Ok(())
            }
            instr => fail!("Cannot accept EndTuple in {instr:?}"),
        }
    }

    fn accept_start_map(&mut self) -> Result<()> {
        match &self.program[self.program_counter] {
            instr => fail!("Cannot accept StartMap in {instr:?}"),
        }
    }

    fn accept_end_map(&mut self) -> Result<()> {
        match &self.program[self.program_counter] {
            instr => fail!("Cannot accept EndMap in {instr:?}"),
        }
    }

    fn accept_some(&mut self) -> Result<()> {
        match &self.program[self.program_counter] {
            Bytecode::Option(_) => {
                self.program_counter += 1;
                Ok(())
            }
            instr => fail!("Cannot accept Some in {instr:?}"),
        }
    }

    fn accept_null(&mut self) -> Result<()> {
        match &self.program[self.program_counter] {
            &Bytecode::Option(skip_adress) => {
                // TODO: fix this. How to do this generically?
                self.utf8[0].push(None);
                self.program_counter = skip_adress;
                Ok(())
            }
            instr => fail!("Cannot accept Null in {instr:?}"),
        }
    }
    fn accept_default(&mut self) -> Result<()> {
        match &self.program[self.program_counter] {
            instr => fail!("Cannot accept Default in {instr:?}"),
        }
    }

    fn accept_str(&mut self, val: &str) -> Result<()> {
        match &self.program[self.program_counter] {
            Bytecode::StructField(_struct_idx, name) if name == val => {
                self.program_counter += 1;
                Ok(())
            }
            &Bytecode::PushLargeUTF8(array_idx) => {
                self.utf8[array_idx].push(Some(val.into()));
                self.program_counter += 1;
                Ok(())
            }
            instr => fail!("Cannot accept Str in {instr:?}"),
        }
    }

    fn accept_owned_str(&mut self, val: String) -> Result<()> {
        match &self.program[self.program_counter] {
            Bytecode::StructField(_struct_idx, name) if name == &val => {
                self.program_counter += 1;
                Ok(())
            }
            &Bytecode::PushLargeUTF8(array_idx) => {
                self.utf8[array_idx].push(Some(val));
                self.program_counter += 1;
                Ok(())
            }
            instr => fail!("Cannot accept OwnedStr in {instr:?}"),
        }
    }

    fn accept_variant(&mut self, _name: &str, _idx: usize) -> Result<()> {
        match &self.program[self.program_counter] {
            instr => fail!("Cannot accept Variant in {instr:?}"),
        }
    }

    fn accept_owned_variant(&mut self, _name: String, _idx: usize) -> Result<()> {
        match &self.program[self.program_counter] {
            instr => fail!("Cannot accept OwnedVariant in {instr:?}"),
        }
    }

    fn finish(&mut self) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use serde::{Deserialize, Serialize};

    use crate::base::{serialize_into_sink, Event, EventSink};

    use super::{Bytecode, Interpreter, ListDefinition, StructDefinition};

    #[test]
    fn example() {
        let items = &[
            Item {
                string: "foo".into(),
                points: vec![Point { x: 1.0, y: 2.0 }, Point { x: 3.0, y: 4.0 }],
                child: SubItem {
                    a: true,
                    b: 42.0,
                    c: Some(1.0),
                },
            },
            Item {
                string: "foo".into(),
                points: vec![Point { x: 1.0, y: 2.0 }, Point { x: 3.0, y: 4.0 }],
                child: SubItem {
                    a: true,
                    b: 42.0,
                    c: None,
                },
            },
        ];

        let (program, lists, structs) = build_programm();
        let mut interpreter = Interpreter {
            program,
            lists,
            structs,
            program_counter: 0,
            u8: vec![vec![]; 0],
            u16: vec![vec![]; 0],
            u32: vec![vec![]; 0],
            u64: vec![vec![]; 0],
            i8: vec![vec![]; 0],
            i16: vec![vec![]; 0],
            i32: vec![vec![]; 0],
            i64: vec![vec![]; 0],
            f32: vec![vec![]; 3],
            f64: vec![vec![]; 1],
            bool: vec![vec![]; 1],
            utf8: vec![vec![]; 1],
        };

        let mut events = Vec::<Event<'static>>::new();
        serialize_into_sink(&mut events, &items).unwrap();

        for ev in events {
            println!(
                "{pc} {ev} {instr:?}",
                pc = interpreter.program_counter,
                instr = interpreter.program.get(interpreter.program_counter),
            );
            interpreter.accept(ev).unwrap();
        }
    }

    macro_rules! btree_map {
        ($($key:expr => $val:expr,)*) => {
            {
                #[allow(unused_mut)]
                let mut res = std::collections::BTreeMap::new();
                $(res.insert($key.into(), $val.into());)*
                res
            }
        };
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct Item {
        string: String,
        points: Vec<Point>,
        child: SubItem,
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct Point {
        x: f32,
        y: f32,
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct SubItem {
        a: bool,
        b: f64,
        c: Option<f32>,
    }

    #[rustfmt::skip]
    fn build_programm() -> (Vec<Bytecode>, Vec<ListDefinition>, Vec<StructDefinition>) {
        let program = vec![
            /*  0 */ Bytecode::ListStart(0),
            /*  1 */ Bytecode::ListItem(0),
            /*  2 */ Bytecode::StructStart(9),
            /*  3 */ Bytecode::StructField(0, "string".into()),
            /*  4 */ Bytecode::PushLargeUTF8(0),
            /*  5 */ Bytecode::StructField(0, "points".into()),
            /*  6 */ Bytecode::ListStart(1),
            /*  7 */ Bytecode::ListItem(1),
            /*  8 */ Bytecode::StructStart(1),
            /*  9 */ Bytecode::StructField(1, "x".into()),
            /* 10 */ Bytecode::PushF32(0),
            /* 11 */ Bytecode::StructField(1, "y".into()),
            /* 12 */ Bytecode::PushF32(1),
            /* 13 */ Bytecode::StructEnd(1),
            /* 14 */ Bytecode::ListEnd(1),
            /* 15 */ Bytecode::StructField(0, "child".into()),
            /* 16 */ Bytecode::StructStart(2),
            /* 17 */ Bytecode::StructField(2, "a".into()),
            /* 18 */ Bytecode::PushBool(0),
            /* 19 */ Bytecode::StructField(2, "b".into()),
            /* 20 */ Bytecode::PushF64(0),
            /* 21 */ Bytecode::StructField(2, "c".into()),
            /* 22 */ Bytecode::Option(24),
            /* 23 */ Bytecode::PushF32(2),
            /* 24 */ Bytecode::StructEnd(2),
            /* 25 */ Bytecode::StructEnd(0),
            /* 26 */ Bytecode::ListEnd(0),
            /* 27 */ Bytecode::ProgramEnd,
        ];

        let lists = vec![
            ListDefinition {
                start: 0,
                end: 26,
                item: 2,
                r#return: 27,
            },
            ListDefinition {
                start: 6,
                end: 15,
                item: 8,
                r#return: 15,
            },
            
        ];
        let structs = vec![
            StructDefinition {
                start: 2,
                end: 25,
                r#return: 1,
                fields: btree_map!{
                    "string" => 3_usize,
                    "points" => 6_usize,
                    "child" => 16_usize,
                },
            },
        ];

        (program, lists, structs)
    }
}
