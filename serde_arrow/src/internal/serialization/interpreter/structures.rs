use crate::internal::{
    error::{error, fail, Result},
    serialization::compiler::Structure,
};

use super::super::bytecode::{
    MapEnd, MapItem, MapStart, StructEnd, StructField, StructItem, StructStart, StructUnknownField,
};
use super::{misc::apply_null, Instruction, MutableBuffers};

fn struct_end(
    structure: &Structure,
    buffers: &mut MutableBuffers,
    struct_idx: usize,
    seen: usize,
) -> Result<()> {
    for (name, field_def) in &structure.structs[struct_idx].fields {
        if !buffers.seen[seen].contains(field_def.index) {
            let null_definition = field_def
                .null_definition
                .ok_or_else(|| error!("missing non-nullable field {name} in struct"))?;
            apply_null(structure, buffers, null_definition)?;
        }
    }
    buffers.seen[seen].clear();

    Ok(())
}

impl Instruction for StructStart {
    const NAME: &'static str = "StructStart";
    const EXPECTED: &'static [&'static str] = &["StartStruct", "StartMap"];

    fn accept_start_struct(
        &self,
        _structure: &Structure,
        buffers: &mut MutableBuffers,
    ) -> Result<usize> {
        buffers.seen[self.seen].clear();
        Ok(self.next)
    }

    fn accept_start_map(
        &self,
        structure: &Structure,
        buffers: &mut MutableBuffers,
    ) -> Result<usize> {
        self.accept_start_struct(structure, buffers)
    }
}

impl Instruction for StructUnknownField {
    const NAME: &'static str = "StructUnknownField";
    const EXPECTED: &'static [&'static str] = &["*"];

    fn accept_i8(&self, _: &Structure, buffers: &mut MutableBuffers, _: i8) -> Result<usize> {
        self.accept(buffers, DepthChange::None)
    }

    fn accept_i16(&self, _: &Structure, buffers: &mut MutableBuffers, _: i16) -> Result<usize> {
        self.accept(buffers, DepthChange::None)
    }

    fn accept_i32(&self, _: &Structure, buffers: &mut MutableBuffers, _: i32) -> Result<usize> {
        self.accept(buffers, DepthChange::None)
    }

    fn accept_i64(&self, _: &Structure, buffers: &mut MutableBuffers, _: i64) -> Result<usize> {
        self.accept(buffers, DepthChange::None)
    }

    fn accept_u8(&self, _: &Structure, buffers: &mut MutableBuffers, _: u8) -> Result<usize> {
        self.accept(buffers, DepthChange::None)
    }

    fn accept_u16(&self, _: &Structure, buffers: &mut MutableBuffers, _: u16) -> Result<usize> {
        self.accept(buffers, DepthChange::None)
    }

    fn accept_u32(&self, _: &Structure, buffers: &mut MutableBuffers, _: u32) -> Result<usize> {
        self.accept(buffers, DepthChange::None)
    }

    fn accept_u64(&self, _: &Structure, buffers: &mut MutableBuffers, _: u64) -> Result<usize> {
        self.accept(buffers, DepthChange::None)
    }

    fn accept_f32(&self, _: &Structure, buffers: &mut MutableBuffers, _: f32) -> Result<usize> {
        self.accept(buffers, DepthChange::None)
    }

    fn accept_f64(&self, _: &Structure, buffers: &mut MutableBuffers, _: f64) -> Result<usize> {
        self.accept(buffers, DepthChange::None)
    }

    fn accept_bool(&self, _: &Structure, buffers: &mut MutableBuffers, _: bool) -> Result<usize> {
        self.accept(buffers, DepthChange::None)
    }

    fn accept_str(&self, _: &Structure, buffers: &mut MutableBuffers, _: &str) -> Result<usize> {
        self.accept(buffers, DepthChange::None)
    }

    fn accept_null(&self, _: &Structure, buffers: &mut MutableBuffers) -> Result<usize> {
        self.accept(buffers, DepthChange::None)
    }

    fn accept_default(&self, _: &Structure, buffers: &mut MutableBuffers) -> Result<usize> {
        self.accept(buffers, DepthChange::None)
    }

    fn accept_some(&self, _: &Structure, _: &mut MutableBuffers) -> Result<usize> {
        Ok(self.self_pos)
    }

    fn accept_variant(
        &self,
        _: &Structure,
        _: &mut MutableBuffers,
        _: &str,
        _: usize,
    ) -> Result<usize> {
        Ok(self.self_pos)
    }

    fn accept_item(&self, _: &Structure, _: &mut MutableBuffers) -> Result<usize> {
        Ok(self.self_pos)
    }

    fn accept_start_sequence(&self, _: &Structure, buffers: &mut MutableBuffers) -> Result<usize> {
        self.accept(buffers, DepthChange::Inc)
    }

    fn accept_end_sequence(&self, _: &Structure, buffers: &mut MutableBuffers) -> Result<usize> {
        self.accept(buffers, DepthChange::Dec)
    }

    fn accept_start_tuple(&self, _: &Structure, buffers: &mut MutableBuffers) -> Result<usize> {
        self.accept(buffers, DepthChange::Inc)
    }

    fn accept_end_tuple(&self, _: &Structure, buffers: &mut MutableBuffers) -> Result<usize> {
        self.accept(buffers, DepthChange::Dec)
    }

    fn accept_start_map(&self, _: &Structure, buffers: &mut MutableBuffers) -> Result<usize> {
        self.accept(buffers, DepthChange::Inc)
    }

    fn accept_end_map(&self, _: &Structure, buffers: &mut MutableBuffers) -> Result<usize> {
        self.accept(buffers, DepthChange::Dec)
    }

    fn accept_start_struct(&self, _: &Structure, buffers: &mut MutableBuffers) -> Result<usize> {
        self.accept(buffers, DepthChange::Inc)
    }

    fn accept_end_struct(&self, _: &Structure, buffers: &mut MutableBuffers) -> Result<usize> {
        self.accept(buffers, DepthChange::Dec)
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum DepthChange {
    Inc,
    Dec,
    None,
}

impl StructUnknownField {
    fn accept(&self, buffers: &mut MutableBuffers, depth_change: DepthChange) -> Result<usize> {
        match (depth_change, buffers.u0[self.depth].len()) {
            (DepthChange::None, 0) => Ok(self.next),
            (DepthChange::None, _) => Ok(self.self_pos),
            (DepthChange::Dec, 0) => fail!("Invalid closing structure in unknown field"),
            (DepthChange::Dec, 1) => {
                buffers.u0[self.depth].pop(())?;
                Ok(self.next)
            }
            (DepthChange::Dec, _) => {
                buffers.u0[self.depth].pop(())?;
                Ok(self.self_pos)
            }
            (DepthChange::Inc, _) => {
                buffers.u0[self.depth].push(());
                Ok(self.self_pos)
            }
        }
    }
}

impl Instruction for StructField {
    const NAME: &'static str = "StructField";
    const EXPECTED: &'static [&'static str] = &["EndStruct", "EndMap", "Str"];

    fn accept_end_struct(
        &self,
        structure: &Structure,
        buffers: &mut MutableBuffers,
    ) -> Result<usize> {
        struct_end(structure, buffers, self.struct_idx, self.seen)?;
        Ok(structure.structs[self.struct_idx].r#return)
    }

    fn accept_end_map(&self, structure: &Structure, buffers: &mut MutableBuffers) -> Result<usize> {
        self.accept_end_struct(structure, buffers)
    }

    fn accept_str(
        &self,
        structure: &Structure,
        buffers: &mut MutableBuffers,
        val: &str,
    ) -> Result<usize> {
        if self.field_name == val {
            buffers.seen[self.seen].insert(self.field_idx);
            Ok(self.next)
        } else if let Some(field_def) = structure.structs[self.struct_idx].fields.get(val) {
            buffers.seen[self.seen].insert(field_def.index);
            Ok(field_def.jump)
        } else {
            Ok(structure.structs[self.struct_idx].unknown_field)
        }
    }

    // relevant for maps serialized as structs: stay at the current position and
    // wait for the following field name
    fn accept_item(&self, _structure: &Structure, _buffers: &mut MutableBuffers) -> Result<usize> {
        Ok(self.self_pos)
    }
}

impl Instruction for StructEnd {
    const NAME: &'static str = "StructEnd";
    const EXPECTED: &'static [&'static str] = &["EndStruct", "EndMap", "Str", "Item"];

    fn accept_end_struct(
        &self,
        structure: &Structure,
        buffers: &mut MutableBuffers,
    ) -> Result<usize> {
        struct_end(structure, buffers, self.struct_idx, self.seen)?;
        Ok(self.next)
    }

    fn accept_end_map(&self, structure: &Structure, buffers: &mut MutableBuffers) -> Result<usize> {
        self.accept_end_struct(structure, buffers)
    }

    fn accept_str(
        &self,
        structure: &Structure,
        buffers: &mut MutableBuffers,
        val: &str,
    ) -> Result<usize> {
        if let Some(field_def) = structure.structs[self.struct_idx].fields.get(val) {
            buffers.seen[self.seen].insert(field_def.index);
            Ok(field_def.jump)
        } else {
            Ok(structure.structs[self.struct_idx].unknown_field)
        }
    }

    // relevant for maps serialized as structs: stay at this position and wait
    // for the following field name
    fn accept_item(&self, _structure: &Structure, _buffers: &mut MutableBuffers) -> Result<usize> {
        Ok(self.self_pos)
    }
}

impl Instruction for StructItem {
    const NAME: &'static str = "StructItem";
    const EXPECTED: &'static [&'static str] = &["EndMap", "Item"];

    fn accept_item(&self, _structure: &Structure, _buffers: &mut MutableBuffers) -> Result<usize> {
        Ok(self.next)
    }

    fn accept_end_map(&self, structure: &Structure, buffers: &mut MutableBuffers) -> Result<usize> {
        struct_end(structure, buffers, self.struct_idx, self.seen)?;
        Ok(structure.structs[self.struct_idx].r#return)
    }
}

impl Instruction for MapStart {
    const NAME: &'static str = "MapStart";
    const EXPECTED: &'static [&'static str] = &["StartMap"];

    fn accept_start_map(
        &self,
        _structure: &Structure,
        _buffers: &mut MutableBuffers,
    ) -> Result<usize> {
        Ok(self.next)
    }
}

impl Instruction for MapEnd {
    const NAME: &'static str = "MapEnd";
    const EXPECTED: &'static [&'static str] = &["Item", "EndMap"];

    fn accept_item(&self, structure: &Structure, buffers: &mut MutableBuffers) -> Result<usize> {
        buffers.u32_offsets[self.offsets].inc_current_items()?;
        Ok(structure.maps[self.map_idx].key)
    }

    fn accept_end_map(&self, structure: &Structure, buffers: &mut MutableBuffers) -> Result<usize> {
        buffers.u32_offsets[self.offsets].push_current_items();
        Ok(structure.maps[self.map_idx].r#return)
    }
}

impl Instruction for MapItem {
    const NAME: &'static str = "MapItem";
    const EXPECTED: &'static [&'static str] = &["EndMap", "Item"];

    fn accept_item(&self, _structure: &Structure, buffers: &mut MutableBuffers) -> Result<usize> {
        buffers.u32_offsets[self.offsets].inc_current_items()?;
        Ok(self.next)
    }

    fn accept_end_map(&self, structure: &Structure, buffers: &mut MutableBuffers) -> Result<usize> {
        buffers.u32_offsets[self.offsets].push_current_items();
        Ok(structure.maps[self.map_idx].r#return)
    }
}
