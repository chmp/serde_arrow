use std::collections::BTreeMap;

use crate::internal::{
    common::{ArrayMapping, DictionaryIndex, MutableBuffers},
    error::Result,
};

#[derive(Default, Debug, Clone, PartialEq)]
pub struct StructDefinition {
    /// The fields of this struct
    pub fields: BTreeMap<String, FieldDefinition>,
    /// The jump target if a struct is closed
    pub r#return: usize,
    /// The instruction handling unknown fields
    pub unknown_field: usize,
}

/// Definition of a field inside a struct
#[derive(Default, Debug, Clone, PartialEq)]
pub struct FieldDefinition {
    /// The index of this field in the overall struct
    pub index: usize,
    /// The jump target for the individual fields
    pub jump: usize,
    /// The null definition for this field
    pub null_definition: Option<usize>,
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
    pub u0: Vec<usize>,
    pub u1: Vec<usize>,
    pub u8: Vec<usize>,
    pub u16: Vec<usize>,
    pub u32: Vec<usize>,
    pub u64: Vec<usize>,
    pub u128: Vec<usize>,
    pub u32_offsets: Vec<usize>,
    pub u64_offsets: Vec<usize>,
}

impl NullDefinition {
    pub fn update_from_array_mapping(&mut self, m: &ArrayMapping) -> Result<()> {
        match m {
            &ArrayMapping::Null {
                buffer, validity, ..
            } => {
                self.u0.push(buffer);
                self.u1.extend(validity);
            }
            &ArrayMapping::Bool {
                buffer, validity, ..
            } => {
                self.u1.push(buffer);
                self.u1.extend(validity);
            }
            &ArrayMapping::U8 {
                buffer, validity, ..
            } => {
                self.u8.push(buffer);
                self.u1.extend(validity);
            }
            &ArrayMapping::U16 {
                buffer, validity, ..
            } => {
                self.u16.push(buffer);
                self.u1.extend(validity);
            }
            &ArrayMapping::U32 {
                buffer, validity, ..
            } => {
                self.u32.push(buffer);
                self.u1.extend(validity);
            }
            &ArrayMapping::U64 {
                buffer, validity, ..
            } => {
                self.u64.push(buffer);
                self.u1.extend(validity);
            }
            &ArrayMapping::I8 {
                buffer, validity, ..
            } => {
                self.u8.push(buffer);
                self.u1.extend(validity);
            }
            &ArrayMapping::I16 {
                buffer, validity, ..
            } => {
                self.u16.push(buffer);
                self.u1.extend(validity);
            }
            &ArrayMapping::I32 {
                buffer, validity, ..
            } => {
                self.u32.push(buffer);
                self.u1.extend(validity);
            }
            &ArrayMapping::I64 {
                buffer, validity, ..
            } => {
                self.u64.push(buffer);
                self.u1.extend(validity);
            }
            &ArrayMapping::F16 {
                buffer, validity, ..
            } => {
                self.u16.push(buffer);
                self.u1.extend(validity);
            }
            &ArrayMapping::F32 {
                buffer, validity, ..
            } => {
                self.u32.push(buffer);
                self.u1.extend(validity);
            }
            &ArrayMapping::F64 {
                buffer, validity, ..
            } => {
                self.u64.push(buffer);
                self.u1.extend(validity);
            }
            &ArrayMapping::Utf8 {
                offsets, validity, ..
            } => {
                // NOTE: an empty string contains no data
                self.u32_offsets.push(offsets);
                self.u1.extend(validity);
            }
            &ArrayMapping::LargeUtf8 {
                offsets, validity, ..
            } => {
                // NOTE: an empty string contains no data
                self.u64_offsets.push(offsets);
                self.u1.extend(validity);
            }
            &ArrayMapping::Date64 {
                buffer, validity, ..
            } => {
                self.u64.push(buffer);
                self.u1.extend(validity);
            }
            ArrayMapping::Struct {
                fields, validity, ..
            } => {
                for field in fields {
                    self.update_from_array_mapping(field)?;
                }
                self.u1.extend(validity.iter().copied());
            }
            &ArrayMapping::Map {
                offsets, validity, ..
            } => {
                // NOTE: the entries is not included
                self.u64_offsets.push(offsets);
                self.u1.extend(validity);
            }
            &ArrayMapping::LargeList {
                offsets, validity, ..
            } => {
                // NOTE: the item is not included
                self.u64_offsets.push(offsets);
                self.u1.extend(validity);
            }
            &ArrayMapping::Dictionary {
                indices, validity, ..
            } => {
                match indices {
                    DictionaryIndex::U8(idx) => self.u8.push(idx),
                    DictionaryIndex::U16(idx) => self.u16.push(idx),
                    DictionaryIndex::U32(idx) => self.u32.push(idx),
                    DictionaryIndex::U64(idx) => self.u64.push(idx),
                    DictionaryIndex::I8(idx) => self.u8.push(idx),
                    DictionaryIndex::I16(idx) => self.u16.push(idx),
                    DictionaryIndex::I32(idx) => self.u32.push(idx),
                    DictionaryIndex::I64(idx) => self.u64.push(idx),
                }
                self.u1.extend(validity);
            }
            m => todo!("cannot update null definition from {m:?}"),
        }
        Ok(())
    }

    pub fn sort_indices(&mut self) {
        self.u1.sort();
        self.u8.sort();
        self.u16.sort();
        self.u32.sort();
        self.u64.sort();
        self.u32_offsets.sort();
        self.u64_offsets.sort();
    }

    pub fn apply(&self, buffers: &mut MutableBuffers) -> Result<()> {
        for &idx in &self.u0 {
            buffers.u0[idx].push(());
        }
        for &idx in &self.u1 {
            buffers.u1[idx].push(Default::default());
        }
        for &idx in &self.u8 {
            buffers.u8[idx].push(Default::default());
        }
        for &idx in &self.u16 {
            buffers.u16[idx].push(Default::default());
        }
        for &idx in &self.u32 {
            buffers.u32[idx].push(Default::default());
        }
        for &idx in &self.u64 {
            buffers.u64[idx].push(Default::default());
        }
        for &idx in &self.u128 {
            buffers.u128[idx].push(Default::default());
        }
        for &idx in &self.u32_offsets {
            buffers.u32_offsets[idx].push_current_items();
        }
        for &idx in &self.u64_offsets {
            buffers.u64_offsets[idx].push_current_items();
        }
        Ok(())
    }
}
