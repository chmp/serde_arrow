use crate::internal::{error::Result, serialization::compiler::Structure};

use super::super::bytecode::{
    LargeListEnd, LargeListItem, LargeListStart, ListEnd, ListItem, ListStart, OuterSequenceEnd,
    OuterSequenceItem, OuterSequenceStart, TupleStructEnd, TupleStructItem, TupleStructStart,
};
use super::{Instruction, MutableBuffers};

impl Instruction for OuterSequenceStart {
    const NAME: &'static str = "OuterSequenceStart";
    const EXPECTED: &'static [&'static str] = &["StartSequence", "StartTuple"];

    fn accept_start_sequence(
        &self,
        _structure: &Structure,
        _buffers: &mut MutableBuffers,
    ) -> Result<usize> {
        Ok(self.next)
    }

    fn accept_start_tuple(
        &self,
        _structure: &Structure,
        _buffers: &mut MutableBuffers,
    ) -> Result<usize> {
        Ok(self.next)
    }
}

impl Instruction for OuterSequenceItem {
    const NAME: &'static str = "OuterSequenceItem";
    const EXPECTED: &'static [&'static str] = &["EndSequence", "EndTuple", "Item"];

    fn accept_end_sequence(
        &self,
        structure: &Structure,
        _buffers: &mut MutableBuffers,
    ) -> Result<usize> {
        Ok(structure.large_lists[self.list_idx].r#return)
    }

    fn accept_item(&self, _structure: &Structure, _buffers: &mut MutableBuffers) -> Result<usize> {
        Ok(self.next)
    }

    fn accept_end_tuple(
        &self,
        structure: &Structure,
        _buffers: &mut MutableBuffers,
    ) -> Result<usize> {
        Ok(structure.large_lists[self.list_idx].r#return)
    }
}

impl Instruction for OuterSequenceEnd {
    const NAME: &'static str = "OuterSequenceEnd";
    const EXPECTED: &'static [&'static str] = &["EndSequence", "EndTuple", "Item"];

    fn accept_end_sequence(
        &self,
        _structure: &Structure,
        _buffers: &mut MutableBuffers,
    ) -> Result<usize> {
        Ok(self.next)
    }

    fn accept_item(&self, structure: &Structure, _buffers: &mut MutableBuffers) -> Result<usize> {
        Ok(structure.large_lists[self.list_idx].item)
    }

    fn accept_end_tuple(
        &self,
        _structure: &Structure,
        _buffers: &mut MutableBuffers,
    ) -> Result<usize> {
        Ok(self.next)
    }
}

impl Instruction for LargeListStart {
    const NAME: &'static str = "LargeListStart";
    const EXPECTED: &'static [&'static str] = &["StartSequence", "StartTuple"];

    fn accept_start_sequence(
        &self,
        _structure: &Structure,
        _buffers: &mut MutableBuffers,
    ) -> Result<usize> {
        Ok(self.next)
    }

    fn accept_start_tuple(
        &self,
        _structure: &Structure,
        _buffers: &mut MutableBuffers,
    ) -> Result<usize> {
        Ok(self.next)
    }
}

impl Instruction for LargeListItem {
    const NAME: &'static str = "LargeListItem";
    const EXPECTED: &'static [&'static str] = &["EndSequence", "EndTuple", "Item"];

    fn accept_end_sequence(
        &self,
        structure: &Structure,
        buffers: &mut MutableBuffers,
    ) -> Result<usize> {
        buffers.u64_offsets[self.offsets].push_current_items();
        Ok(structure.large_lists[self.list_idx].r#return)
    }

    fn accept_end_tuple(
        &self,
        structure: &Structure,
        buffers: &mut MutableBuffers,
    ) -> Result<usize> {
        buffers.u64_offsets[self.offsets].push_current_items();
        Ok(structure.large_lists[self.list_idx].r#return)
    }

    fn accept_item(&self, _structure: &Structure, buffers: &mut MutableBuffers) -> Result<usize> {
        buffers.u64_offsets[self.offsets].inc_current_items()?;
        Ok(self.next)
    }
}

impl Instruction for LargeListEnd {
    const NAME: &'static str = "LargeListEnd";
    const EXPECTED: &'static [&'static str] = &["EndSequence", "EndTuple", "Item"];

    fn accept_end_sequence(
        &self,
        _structure: &Structure,
        buffers: &mut MutableBuffers,
    ) -> Result<usize> {
        buffers.u64_offsets[self.offsets].push_current_items();
        Ok(self.next)
    }

    fn accept_end_tuple(
        &self,
        _structure: &Structure,
        buffers: &mut MutableBuffers,
    ) -> Result<usize> {
        buffers.u64_offsets[self.offsets].push_current_items();
        Ok(self.next)
    }

    fn accept_item(&self, structure: &Structure, buffers: &mut MutableBuffers) -> Result<usize> {
        buffers.u64_offsets[self.offsets].inc_current_items()?;
        Ok(structure.large_lists[self.list_idx].item)
    }
}

impl Instruction for ListStart {
    const NAME: &'static str = "ListStart";
    const EXPECTED: &'static [&'static str] = &["StartSequence", "StartTuple"];

    fn accept_start_sequence(
        &self,
        _structure: &Structure,
        _buffers: &mut MutableBuffers,
    ) -> Result<usize> {
        Ok(self.next)
    }

    fn accept_start_tuple(
        &self,
        _structure: &Structure,
        _buffers: &mut MutableBuffers,
    ) -> Result<usize> {
        Ok(self.next)
    }
}

impl Instruction for ListItem {
    const NAME: &'static str = "ListItem";
    const EXPECTED: &'static [&'static str] = &["EndSequence", "EndTuple", "Item"];

    fn accept_end_sequence(
        &self,
        structure: &Structure,
        buffers: &mut MutableBuffers,
    ) -> Result<usize> {
        buffers.u32_offsets[self.offsets].push_current_items();
        Ok(structure.lists[self.list_idx].r#return)
    }

    fn accept_end_tuple(
        &self,
        structure: &Structure,
        buffers: &mut MutableBuffers,
    ) -> Result<usize> {
        buffers.u32_offsets[self.offsets].push_current_items();
        Ok(structure.lists[self.list_idx].r#return)
    }

    fn accept_item(&self, _structure: &Structure, buffers: &mut MutableBuffers) -> Result<usize> {
        buffers.u32_offsets[self.offsets].inc_current_items()?;
        Ok(self.next)
    }
}

impl Instruction for ListEnd {
    const NAME: &'static str = "ListEnd";
    const EXPECTED: &'static [&'static str] = &["EndSequence", "EndTuple", "Item"];

    fn accept_end_sequence(
        &self,
        _structure: &Structure,
        buffers: &mut MutableBuffers,
    ) -> Result<usize> {
        buffers.u32_offsets[self.offsets].push_current_items();
        Ok(self.next)
    }

    fn accept_end_tuple(
        &self,
        _structure: &Structure,
        buffers: &mut MutableBuffers,
    ) -> Result<usize> {
        buffers.u32_offsets[self.offsets].push_current_items();
        Ok(self.next)
    }

    fn accept_item(&self, structure: &Structure, buffers: &mut MutableBuffers) -> Result<usize> {
        buffers.u32_offsets[self.offsets].inc_current_items()?;
        Ok(structure.lists[self.list_idx].item)
    }
}

impl Instruction for TupleStructStart {
    const NAME: &'static str = "TupleStructStart";
    const EXPECTED: &'static [&'static str] = &["StartTuple"];

    fn accept_start_tuple(
        &self,
        _structure: &Structure,
        _buffers: &mut MutableBuffers,
    ) -> Result<usize> {
        Ok(self.next)
    }
}

impl Instruction for TupleStructItem {
    const NAME: &'static str = "TupleStructItem";
    const EXPECTED: &'static [&'static str] = &["Item"];
    fn accept_item(&self, _structure: &Structure, _buffers: &mut MutableBuffers) -> Result<usize> {
        Ok(self.next)
    }
}

impl Instruction for TupleStructEnd {
    const NAME: &'static str = "TupleStructEnd";
    const EXPECTED: &'static [&'static str] = &["EndTuple"];

    fn accept_end_tuple(
        &self,
        _structure: &Structure,
        _buffers: &mut MutableBuffers,
    ) -> Result<usize> {
        Ok(self.next)
    }
}
