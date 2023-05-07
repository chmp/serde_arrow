use super::{
    buffers::{
        BitBuffer, NullBuffer, OffsetBuilder, PrimitiveBuffer, StringBuffer, StringDictonary,
    },
    compiler::{BufferCounts, Bytecode, DictionaryIndices, DictionaryValue, Program, Structure},
};

use crate::internal::{
    error::{fail, Result},
    event::Event,
    sink::macros,
    sink::EventSink,
};

pub struct Interpreter {
    pub program_counter: usize,
    pub structure: Structure,
    pub buffers: Buffers,
}

#[derive(Debug, Clone)]
pub struct Buffers {
    pub bool: Vec<BitBuffer>,
    pub null: Vec<NullBuffer>,
    pub u8: Vec<PrimitiveBuffer<u8>>,
    pub u16: Vec<PrimitiveBuffer<u16>>,
    pub u32: Vec<PrimitiveBuffer<u32>>,
    pub u64: Vec<PrimitiveBuffer<u64>>,
    pub i8: Vec<PrimitiveBuffer<i8>>,
    pub i16: Vec<PrimitiveBuffer<i16>>,
    pub i32: Vec<PrimitiveBuffer<i32>>,
    pub i64: Vec<PrimitiveBuffer<i64>>,
    pub f32: Vec<PrimitiveBuffer<f32>>,
    pub f64: Vec<PrimitiveBuffer<f64>>,
    pub utf8: Vec<StringBuffer<i32>>,
    pub large_utf8: Vec<StringBuffer<i64>>,
    pub validity: Vec<BitBuffer>,
    pub offset: Vec<OffsetBuilder<i32>>,
    pub large_offset: Vec<OffsetBuilder<i64>>,
    pub dictionaries: Vec<StringDictonary<i32>>,
    pub large_dictionaries: Vec<StringDictonary<i64>>,
}

impl Buffers {
    pub fn from_counts(counts: &BufferCounts) -> Self {
        Self {
            null: vec![Default::default(); counts.num_null],
            u8: vec![Default::default(); counts.num_u8],
            u16: vec![Default::default(); counts.num_u16],
            u32: vec![Default::default(); counts.num_u32],
            u64: vec![Default::default(); counts.num_u64],
            i8: vec![Default::default(); counts.num_i8],
            i16: vec![Default::default(); counts.num_i16],
            i32: vec![Default::default(); counts.num_i32],
            i64: vec![Default::default(); counts.num_i64],
            f32: vec![Default::default(); counts.num_f32],
            f64: vec![Default::default(); counts.num_f64],
            bool: vec![Default::default(); counts.num_bool],
            utf8: vec![Default::default(); counts.num_utf8],
            large_utf8: vec![Default::default(); counts.num_large_utf8],
            validity: vec![Default::default(); counts.num_validity],
            offset: vec![Default::default(); counts.num_offsets],
            large_offset: vec![Default::default(); counts.num_large_offsets],
            dictionaries: vec![Default::default(); counts.num_dictionaries],
            large_dictionaries: vec![Default::default(); counts.num_large_dictionaries],
        }
    }
}

impl Interpreter {
    pub fn new(program: Program) -> Self {
        Self {
            program_counter: 0,
            structure: program.structure,
            buffers: Buffers::from_counts(&program.buffers),
        }
    }
}

macro_rules! accept_primitive {
    ($func:ident, $ty:ty, $(($builder:ident, $variant:ident),)*) => {
        fn $func(&mut self, val: $ty) -> crate::Result<()> {
            match &self.structure.program[self.program_counter] {
                $(
                    &(next, Bytecode::$variant(array_idx)) => {
                        self.buffers.$builder[array_idx].push(val.try_into()?)?;
                        self.program_counter = next;
                    }
                )*
                instr => fail!("Cannot accept {} in {instr:?}", stringify!($ty)),
            }
            Ok(())
        }
    };
}

#[allow(clippy::match_single_binding)]
impl EventSink for Interpreter {
    macros::forward_generic_to_specialized!();

    accept_primitive!(
        accept_u8,
        u8,
        (u8, PushU8),
        (u16, PushU16),
        (u32, PushU32),
        (u64, PushU64),
        (i8, PushI8),
        (i16, PushI16),
        (i32, PushI32),
        (i64, PushI64),
    );
    accept_primitive!(
        accept_u16,
        u16,
        (u8, PushU8),
        (u16, PushU16),
        (u32, PushU32),
        (u64, PushU64),
        (i8, PushI8),
        (i16, PushI16),
        (i32, PushI32),
        (i64, PushI64),
    );
    accept_primitive!(
        accept_u32,
        u32,
        (u8, PushU8),
        (u16, PushU16),
        (u32, PushU32),
        (u64, PushU64),
        (i8, PushI8),
        (i16, PushI16),
        (i32, PushI32),
        (i64, PushI64),
    );
    accept_primitive!(
        accept_u64,
        u64,
        (u8, PushU8),
        (u16, PushU16),
        (u32, PushU32),
        (u64, PushU64),
        (i8, PushI8),
        (i16, PushI16),
        (i32, PushI32),
        (i64, PushI64),
    );
    accept_primitive!(
        accept_i8,
        i8,
        (u8, PushU8),
        (u16, PushU16),
        (u32, PushU32),
        (u64, PushU64),
        (i8, PushI8),
        (i16, PushI16),
        (i32, PushI32),
        (i64, PushI64),
    );
    accept_primitive!(
        accept_i16,
        i16,
        (u8, PushU8),
        (u16, PushU16),
        (u32, PushU32),
        (u64, PushU64),
        (i8, PushI8),
        (i16, PushI16),
        (i32, PushI32),
        (i64, PushI64),
    );
    accept_primitive!(
        accept_i32,
        i32,
        (u8, PushU8),
        (u16, PushU16),
        (u32, PushU32),
        (u64, PushU64),
        (i8, PushI8),
        (i16, PushI16),
        (i32, PushI32),
        (i64, PushI64),
    );
    accept_primitive!(
        accept_i64,
        i64,
        (u8, PushU8),
        (u16, PushU16),
        (u32, PushU32),
        (u64, PushU64),
        (i8, PushI8),
        (i16, PushI16),
        (i32, PushI32),
        (i64, PushI64),
    );
    accept_primitive!(accept_f32, f32, (f32, PushF32),);
    accept_primitive!(accept_f64, f64, (f64, PushF64),);
    accept_primitive!(accept_bool, bool, (bool, PushBool),);

    fn accept_start_sequence(&mut self) -> crate::Result<()> {
        // TOOD: add new offset
        use Bytecode as B;
        match &self.structure.program[self.program_counter] {
            &(next, B::LargeListStart | B::OuterSequenceStart) => {
                self.program_counter = next;
                Ok(())
            }
            instr => fail!("Cannot accept StartSequence in {instr:?}"),
        }
    }

    fn accept_end_sequence(&mut self) -> crate::Result<()> {
        use Bytecode as B;
        match &self.structure.program[self.program_counter] {
            &(next, B::LargeListEnd(_, offsets)) => {
                self.buffers.large_offset[offsets].push_current_items();
                self.program_counter = next;
            }
            &(next, B::OuterSequenceEnd(_)) => {
                self.program_counter = next;
            }
            &(_, B::LargeListItem(idx, offsets)) => {
                self.buffers.large_offset[offsets].push_current_items();
                self.program_counter = self.structure.large_lists[idx].r#return;
            }
            &(_, B::OuterSequenceItem(idx)) => {
                self.program_counter = self.structure.large_lists[idx].r#return;
            }
            instr => fail!("Cannot accept EndSequence in {instr:?}"),
        }
        Ok(())
    }

    fn accept_start_struct(&mut self) -> crate::Result<()> {
        use Bytecode as B;
        match &self.structure.program[self.program_counter] {
            &(next, B::StructStart | B::OuterRecordStart) => {
                self.program_counter = next;
            }
            instr => fail!("Cannot accept StartStruct in {instr:?}"),
        }
        Ok(())
    }

    fn accept_end_struct(&mut self) -> crate::Result<()> {
        use Bytecode as B;
        match &self.structure.program[self.program_counter] {
            &(next, B::StructEnd | B::OuterRecordEnd) => {
                self.program_counter = next;
            }
            &(_, B::StructField(struct_idx, _)) => {
                self.program_counter = self.structure.structs[struct_idx].r#return;
            }
            instr => fail!("Cannot accept EndStruct in {instr:?}"),
        }
        Ok(())
    }

    fn accept_item(&mut self) -> Result<()> {
        // TODO: increment the count
        use Bytecode as B;
        match &self.structure.program[self.program_counter] {
            &(next, B::LargeListItem(_, offsets)) => {
                self.buffers.large_offset[offsets].inc_current_items()?;
                self.program_counter = next;
            }
            &(_, B::LargeListEnd(idx, offsets)) => {
                self.buffers.large_offset[offsets].inc_current_items()?;
                self.program_counter = self.structure.large_lists[idx].item;
            }
            &(next, B::MapItem(_, offsets)) => {
                self.buffers.offset[offsets].inc_current_items()?;
                self.program_counter = next;
            }
            &(_, B::MapEnd(map_idx, offsets)) => {
                self.buffers.offset[offsets].inc_current_items()?;
                self.program_counter = self.structure.maps[map_idx].key;
            }
            &(_, B::OuterSequenceEnd(idx)) => {
                self.program_counter = self.structure.large_lists[idx].item;
            }
            &(next, B::OuterSequenceItem(_) | B::TupleStructItem | B::StructItem(_)) => {
                self.program_counter = next;
            }
            instr => fail!("Cannot accept Item in {instr:?}"),
        }
        Ok(())
    }

    fn accept_start_tuple(&mut self) -> Result<()> {
        // TOOD: add new offset
        use Bytecode as B;
        match &self.structure.program[self.program_counter] {
            &(next, B::LargeListStart | B::OuterSequenceStart | B::TupleStructStart) => {
                self.program_counter = next;
            }
            instr => fail!("Cannot accept StartTuple in {instr:?}"),
        }
        Ok(())
    }

    fn accept_end_tuple(&mut self) -> Result<()> {
        use Bytecode as B;
        match &self.structure.program[self.program_counter] {
            &(next, B::LargeListEnd(_, offsets)) => {
                self.buffers.large_offset[offsets].push_current_items();
                self.program_counter = next;
            }
            &(_, B::LargeListItem(idx, offsets)) => {
                self.buffers.large_offset[offsets].push_current_items();
                self.program_counter = self.structure.large_lists[idx].r#return;
            }
            &(_, B::OuterSequenceItem(idx)) => {
                self.program_counter = self.structure.large_lists[idx].r#return;
            }
            &(next, B::OuterSequenceEnd(_) | B::TupleStructEnd) => {
                self.program_counter = next;
            }
            instr => fail!("Cannot accept EndTuple in {instr:?}"),
        }
        Ok(())
    }

    fn accept_start_map(&mut self) -> Result<()> {
        use Bytecode as B;
        match &self.structure.program[self.program_counter] {
            &(next, B::StructStart | B::OuterRecordStart | B::MapStart) => {
                self.program_counter = next;
            }
            instr => fail!("Cannot accept StartMap in {instr:?}"),
        }
        Ok(())
    }

    fn accept_end_map(&mut self) -> Result<()> {
        use Bytecode as B;
        match &self.structure.program[self.program_counter] {
            &(next, B::StructEnd | B::OuterRecordEnd) => {
                self.program_counter = next;
            }
            &(_, B::MapItem(map_idx, offsets) | B::MapEnd(map_idx, offsets)) => {
                self.buffers.offset[offsets].push_current_items();
                self.program_counter = self.structure.maps[map_idx].r#return;
            }
            &(_, B::StructField(struct_idx, _)) => {
                self.program_counter = self.structure.structs[struct_idx].r#return;
            }
            instr => fail!("Cannot accept EndMap in {instr:?}"),
        }
        Ok(())
    }

    fn accept_some(&mut self) -> Result<()> {
        use Bytecode as B;
        match &self.structure.program[self.program_counter] {
            &(next, B::Option(_, validity)) => {
                self.buffers.validity[validity].push(true)?;
                self.program_counter = next;
            }
            // Todo: handle EndMap
            instr => fail!("Cannot accept Some in {instr:?}"),
        }
        Ok(())
    }

    fn accept_null(&mut self) -> Result<()> {
        use Bytecode as B;
        match &self.structure.program[self.program_counter] {
            &(_, B::Option(if_none, validity)) => {
                self.apply_null(validity)?;
                self.program_counter = if_none;
            }
            &(next, B::PushNull(idx)) => {
                self.buffers.null[idx].push(())?;
                self.program_counter = next;
            }
            // Todo: handle EndMap
            instr => fail!("Cannot accept Null in {instr:?}"),
        }
        Ok(())
    }
    fn accept_default(&mut self) -> Result<()> {
        match &self.structure.program[self.program_counter] {
            instr => fail!("Cannot accept Default in {instr:?}"),
        }
    }

    fn accept_str(&mut self, val: &str) -> Result<()> {
        use Bytecode as B;
        match &self.structure.program[self.program_counter] {
            (next, B::StructField(idx, name) | B::OuterRecordField(idx, name)) => {
                if name == val {
                    self.program_counter = *next;
                } else {
                    let Some(next) = self.structure.structs[*idx].fields.get(name) else {
                    fail!("Cannot find field {name} in struct {idx}");
                };
                    self.program_counter = *next;
                }
            }
            &(next, B::PushUTF8(idx)) => {
                self.buffers.utf8[idx].push(val)?;
                self.program_counter = next;
            }
            &(next, B::PushLargeUTF8(idx)) => {
                self.buffers.large_utf8[idx].push(val)?;
                self.program_counter = next;
            }
            &(next, B::PushDate64FromNaiveStr(idx)) => {
                use chrono::NaiveDateTime;

                self.buffers.i64[idx].push(val.parse::<NaiveDateTime>()?.timestamp_millis())?;
                self.program_counter = next;
            }
            &(next, B::PushDate64FromUtcStr(idx)) => {
                use chrono::{DateTime, Utc};

                self.buffers.i64[idx].push(val.parse::<DateTime<Utc>>()?.timestamp_millis())?;
                self.program_counter = next;
            }
            &(next, B::PushDictionary(dictionary, indices)) => {
                use {DictionaryIndices as I, DictionaryValue as V};
                let idx = match dictionary {
                    V::Utf8(dict) => self.buffers.dictionaries[dict].push(val)?,
                    V::LargeUtf8(dict) => self.buffers.large_dictionaries[dict].push(val)?,
                };
                match indices {
                    I::U8(indices) => self.buffers.u8[indices].push(idx.try_into()?)?,
                    I::U16(indices) => self.buffers.u16[indices].push(idx.try_into()?)?,
                    I::U32(indices) => self.buffers.u32[indices].push(idx.try_into()?)?,
                    I::U64(indices) => self.buffers.u64[indices].push(idx.try_into()?)?,
                    I::I8(indices) => self.buffers.i8[indices].push(idx.try_into()?)?,
                    I::I16(indices) => self.buffers.i16[indices].push(idx.try_into()?)?,
                    I::I32(indices) => self.buffers.i32[indices].push(idx.try_into()?)?,
                    I::I64(indices) => self.buffers.i64[indices].push(idx.try_into()?)?,
                }
                self.program_counter = next;
            }
            instr => fail!("Cannot accept {ev} in {instr:?}", ev = Event::Str(val)),
        }
        Ok(())
    }

    fn accept_variant(&mut self, _name: &str, idx: usize) -> Result<()> {
        use Bytecode as B;
        match &self.structure.program[self.program_counter] {
            &(_, B::Variant(union_idx, types)) => {
                // TODO: improve error message
                self.buffers.i8[types].push(idx.try_into()?)?;
                self.program_counter = self.structure.unions[union_idx].fields[idx];
            }
            // Todo: handle EndMap
            instr => fail!("Cannot accept Variant in {instr:?}"),
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        Ok(())
    }
}

macro_rules! apply_null {
    ($this:expr, $validity:expr, $name:ident) => {
        for &idx in &$this.structure.nulls[$validity].$name {
            $this.buffers.$name[idx].push(Default::default())?;
        }
    };
}

impl Interpreter {
    fn apply_null(&mut self, validity: usize) -> Result<()> {
        apply_null!(self, validity, null);
        apply_null!(self, validity, bool);
        apply_null!(self, validity, u8);
        apply_null!(self, validity, u16);
        apply_null!(self, validity, u32);
        apply_null!(self, validity, u64);
        apply_null!(self, validity, i8);
        apply_null!(self, validity, i16);
        apply_null!(self, validity, i32);
        apply_null!(self, validity, i64);
        apply_null!(self, validity, f32);
        apply_null!(self, validity, f64);
        apply_null!(self, validity, utf8);
        apply_null!(self, validity, large_utf8);
        apply_null!(self, validity, validity);

        for &idx in &self.structure.nulls[validity].large_offsets {
            self.buffers.large_offset[idx].push_current_items();
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use serde::Serialize;

    use crate::{
        _impl::bytecode::{compile_serialization, compiler::CompilationOptions, Interpreter},
        base::serialize_into_sink,
        internal::serialize_into_fields,
    };

    fn serialize_simple<T: Serialize + ?Sized>(items: &T) {
        let fields = serialize_into_fields(items, Default::default()).unwrap();
        let program = compile_serialization(&fields, CompilationOptions::default()).unwrap();
        let mut interpreter = Interpreter::new(program);

        serialize_into_sink(&mut interpreter, items).unwrap();
    }

    #[test]
    fn empty() {
        #[derive(Serialize)]
        struct Item {}

        serialize_simple(&[Item {}]);
    }

    #[test]
    fn primitives() {
        #[derive(Serialize)]
        struct Item {
            a: u64,
            b: f32,
        }

        serialize_simple(&[Item { a: 0, b: 21.0 }, Item { a: 10, b: 42.0 }]);
    }

    #[test]
    fn nested() {
        #[derive(Serialize)]
        struct Item {
            a: u64,
            b: f32,
            c: Child,
        }

        #[derive(Serialize)]
        struct Child {
            first: String,
            second: bool,
        }

        serialize_simple(&[
            Item {
                a: 0,
                b: 21.0,
                c: Child {
                    first: "foo".into(),
                    second: true,
                },
            },
            Item {
                a: 10,
                b: 42.0,
                c: Child {
                    first: "bar".into(),
                    second: false,
                },
            },
        ]);
    }
}
