use std::collections::BTreeMap;

use crate::internal::{
    arrow::{Array, StructArray},
    error::{fail, set_default, try_, Context, ContextSupport, Result},
    utils::array_ext::{ArrayExt, CountArray, SeqArrayExt},
};

use super::{array_builder::ArrayBuilder, simple_serializer::SimpleSerializer};

#[derive(Debug, Clone)]
pub struct FlattenedUnionBuilder {
    pub path: String,
    pub fields: Vec<ArrayBuilder>,
    pub seq: CountArray,
}

impl FlattenedUnionBuilder {
    pub fn new(path: String, fields: Vec<ArrayBuilder>) -> Self {
        Self {
            path,
            fields,
            seq: CountArray::new(true),
        }
    }

    pub fn take_self(&mut self) -> Self {
        Self {
            path: self.path.clone(),
            fields: self.fields.clone(),
            seq: self.seq.take(),
        }
    }

    pub fn take(&mut self) -> ArrayBuilder {
        ArrayBuilder::FlattenedUnion(self.take_self())
    }

    pub fn is_nullable(&self) -> bool {
        self.seq.validity.is_some()
    }

    pub fn into_array(self) -> Result<Array> {
        let mut fields = Vec::new();

        for builder in self.fields.into_iter() {
            let ArrayBuilder::Struct(builder) = builder else {
                fail!("enum variant not built as a struct")
            };

            for (sub_builder, sub_meta) in builder.fields.into_iter() {
                fields.push((sub_builder.into_array()?, sub_meta));
            }
        }

        Ok(Array::Struct(StructArray {
            len: fields.len(),
            validity: self.seq.validity,
            fields,
        }))
    }
}

impl FlattenedUnionBuilder {
    pub fn serialize_variant(&mut self, variant_index: u32) -> Result<&mut ArrayBuilder> {
        // self.len += 1;

        let variant_index = variant_index as usize;

        // call push_none for any variant that was not selected
        for (idx, builder) in self.fields.iter_mut().enumerate() {
            if idx != variant_index {
                builder.serialize_none()?;
                self.seq.push_seq_none()?;
            }
        }

        let Some(variant_builder) = self.fields.get_mut(variant_index) else {
            fail!("Could not find variant {variant_index} in Union");
        };

        Ok(variant_builder)
    }
}

impl Context for FlattenedUnionBuilder {
    fn annotate(&self, annotations: &mut BTreeMap<String, String>) {
        set_default(annotations, "field", &self.path);
        set_default(annotations, "data_type", "Union(..)");
    }
}

impl SimpleSerializer for FlattenedUnionBuilder {
    // fn serialize_unit_variant(
    //     &mut self,
    //     _: &'static str,
    //     variant_index: u32,
    //     _: &'static str,
    // ) -> Result<()> {
    //     let mut ctx = BTreeMap::new();
    //     self.annotate(&mut ctx);

    //     try_(|| self.serialize_variant(variant_index)?.serialize_unit()).ctx(&ctx)
    // }

    // fn serialize_newtype_variant<V: serde::Serialize + ?Sized>(
    //     &mut self,
    //     _: &'static str,
    //     variant_index: u32,
    //     _: &'static str,
    //     value: &V,
    // ) -> Result<()> {
    //     let mut ctx = BTreeMap::new();
    //     self.annotate(&mut ctx);

    //     try_(|| {
    //         let variant_builder = self.serialize_variant(variant_index)?;
    //         value.serialize(Mut(variant_builder))
    //     })
    //     .ctx(&ctx)
    // }

    fn serialize_struct_variant_start<'this>(
        &'this mut self,
        _: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<&'this mut ArrayBuilder> {
        let mut ctx = BTreeMap::new();
        self.annotate(&mut ctx);
        self.seq.start_seq()?;
        self.seq.push_seq_elements(1)?;

        try_(|| {
            let variant_builder = self.serialize_variant(variant_index)?;
            variant_builder.serialize_struct_start(variant, len)?;
            Ok(variant_builder)
        })
        .ctx(&ctx)
    }

    // fn serialize_tuple_variant_start<'this>(
    //     &'this mut self,
    //     _: &'static str,
    //     variant_index: u32,
    //     variant: &'static str,
    //     len: usize,
    // ) -> Result<&'this mut ArrayBuilder> {
    //     let mut ctx = BTreeMap::new();
    //     self.annotate(&mut ctx);

    //     try_(|| {
    //         let variant_builder = self.serialize_variant(variant_index)?;
    //         variant_builder.serialize_tuple_struct_start(variant, len)?;
    //         Ok(variant_builder)
    //     })
    //     .ctx(&ctx)
    // }
}

// #[cfg(test)]
// mod tests {
//     fn test_serialize_union() {
//         #[derive(Serialize, Deserialize)]
//         enum Number {
//             Real { value: f32 },
//             Complex { i: f32, j: f32 },
//         }

//         let numbers = vec![];
//     }
// }
