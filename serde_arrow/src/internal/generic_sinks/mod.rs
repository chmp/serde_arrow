mod chrono;
mod dictionary;
mod list;
mod map;
mod null;
mod r#struct;
mod tuple;
mod r#union;

use super::{
    error::Result,
    error::{error, fail},
    schema::GenericField,
    schema::{GenericDataType, Strategy},
    sink::{ArrayBuilder, DynamicArrayBuilder},
};

pub use self::chrono::{NaiveDateTimeStrBuilder, UtcDateTimeStrBuilder};
pub use dictionary::DictionaryUtf8ArrayBuilder;
pub use list::ListArrayBuilder;
pub use map::MapArrayBuilder;
pub use null::NullArrayBuilder;
pub use r#struct::StructArrayBuilder;
pub use r#union::UnionArrayBuilder;
pub use tuple::TupleStructBuilder;

pub trait PrimitiveBuilders {
    type ArrayRef: 'static;

    fn null() -> DynamicArrayBuilder<Self::ArrayRef>;
    fn bool() -> DynamicArrayBuilder<Self::ArrayRef>;
    fn i8() -> DynamicArrayBuilder<Self::ArrayRef>;
    fn i16() -> DynamicArrayBuilder<Self::ArrayRef>;
    fn i32() -> DynamicArrayBuilder<Self::ArrayRef>;
    fn i64() -> DynamicArrayBuilder<Self::ArrayRef>;
    fn u8() -> DynamicArrayBuilder<Self::ArrayRef>;
    fn u16() -> DynamicArrayBuilder<Self::ArrayRef>;
    fn u32() -> DynamicArrayBuilder<Self::ArrayRef>;
    fn u64() -> DynamicArrayBuilder<Self::ArrayRef>;
    fn f16() -> DynamicArrayBuilder<Self::ArrayRef>;
    fn f32() -> DynamicArrayBuilder<Self::ArrayRef>;
    fn f64() -> DynamicArrayBuilder<Self::ArrayRef>;
    fn utf8() -> DynamicArrayBuilder<Self::ArrayRef>;
    fn large_utf8() -> DynamicArrayBuilder<Self::ArrayRef>;
    fn date64() -> DynamicArrayBuilder<Self::ArrayRef>;
}

pub fn build_array_builder<Arrow>(
    field: &GenericField,
) -> Result<DynamicArrayBuilder<Arrow::ArrayRef>>
where
    Arrow: PrimitiveBuilders,
    NaiveDateTimeStrBuilder<DynamicArrayBuilder<Arrow::ArrayRef>>: ArrayBuilder<Arrow::ArrayRef>,
    UtcDateTimeStrBuilder<DynamicArrayBuilder<Arrow::ArrayRef>>: ArrayBuilder<Arrow::ArrayRef>,
    TupleStructBuilder<DynamicArrayBuilder<Arrow::ArrayRef>>: ArrayBuilder<Arrow::ArrayRef>,
    StructArrayBuilder<DynamicArrayBuilder<Arrow::ArrayRef>>: ArrayBuilder<Arrow::ArrayRef>,
    UnionArrayBuilder<DynamicArrayBuilder<Arrow::ArrayRef>>: ArrayBuilder<Arrow::ArrayRef>,
    DictionaryUtf8ArrayBuilder<DynamicArrayBuilder<Arrow::ArrayRef>>: ArrayBuilder<Arrow::ArrayRef>,
    MapArrayBuilder<DynamicArrayBuilder<Arrow::ArrayRef>>: ArrayBuilder<Arrow::ArrayRef>,
    ListArrayBuilder<DynamicArrayBuilder<Arrow::ArrayRef>, i32>: ArrayBuilder<Arrow::ArrayRef>,
    ListArrayBuilder<DynamicArrayBuilder<Arrow::ArrayRef>, i64>: ArrayBuilder<Arrow::ArrayRef>,
{
    use GenericDataType::*;
    match field.data_type {
        Null => Ok(Arrow::null()),
        Bool => Ok(Arrow::bool()),
        I8 => Ok(Arrow::i8()),
        I16 => Ok(Arrow::i16()),
        I32 => Ok(Arrow::i32()),
        I64 => Ok(Arrow::i64()),
        U8 => Ok(Arrow::u8()),
        U16 => Ok(Arrow::u16()),
        U32 => Ok(Arrow::u32()),
        U64 => Ok(Arrow::u64()),
        F16 => Ok(Arrow::f16()),
        F32 => Ok(Arrow::f32()),
        F64 => Ok(Arrow::f64()),
        Utf8 => Ok(Arrow::utf8()),
        LargeUtf8 => Ok(Arrow::large_utf8()),
        Date64 => match field.strategy.as_ref() {
            Some(Strategy::NaiveStrAsDate64) => Ok(DynamicArrayBuilder::new(
                NaiveDateTimeStrBuilder(Arrow::date64()),
            )),
            Some(Strategy::UtcStrAsDate64) => Ok(DynamicArrayBuilder::new(UtcDateTimeStrBuilder(
                Arrow::date64(),
            ))),
            None => Ok(Arrow::date64()),
            Some(strategy) => fail!("Invalid strategy {strategy} for type Date64"),
        },
        Struct => match field.strategy.as_ref() {
            Some(Strategy::TupleAsStruct) => {
                let builders = field
                    .children
                    .iter()
                    .map(build_array_builder::<Arrow>)
                    .collect::<Result<Vec<_>>>()?;
                let nullable = field.children.iter().map(|f| f.nullable).collect();

                let builder = TupleStructBuilder::new(nullable, builders);
                Ok(DynamicArrayBuilder::new(builder))
            }
            None | Some(Strategy::MapAsStruct) => {
                let names = field.children.iter().map(|f| f.name.to_owned()).collect();
                let builders = field
                    .children
                    .iter()
                    .map(build_array_builder::<Arrow>)
                    .collect::<Result<Vec<_>>>()?;
                let nullable = field.children.iter().map(|f| f.nullable).collect();

                let builder = StructArrayBuilder::new(names, nullable, builders);
                Ok(DynamicArrayBuilder::new(builder))
            }
            Some(strategy) => fail!("Invalid strategy {strategy} for type Struct"),
        },
        Union => {
            let builders = field
                .children
                .iter()
                .map(build_array_builder::<Arrow>)
                .collect::<Result<Vec<_>>>()?;
            let nullable = field.children.iter().map(|f| f.nullable).collect();

            let builder = UnionArrayBuilder::new(builders, nullable, field.nullable);
            Ok(DynamicArrayBuilder::new(builder))
        }
        Dictionary => {
            let key = field
                .children
                .get(0)
                .ok_or_else(|| error!("Dictionary must have key/value children"))?;
            let key = build_array_builder::<Arrow>(key)?;

            let value = field
                .children
                .get(1)
                .ok_or_else(|| error!("Dictionary must have key/value children"))?;
            let value = build_array_builder::<Arrow>(value)?;

            Ok(DynamicArrayBuilder::new(DictionaryUtf8ArrayBuilder::new(
                key, value,
            )))
        }
        Map => {
            let key = field
                .children
                .get(0)
                .ok_or_else(|| error!("Dictionary must have key/value children"))?;
            let key = build_array_builder::<Arrow>(key)?;

            let value = field
                .children
                .get(1)
                .ok_or_else(|| error!("Dictionary must have key/value children"))?;
            let value = build_array_builder::<Arrow>(value)?;

            let builder = MapArrayBuilder::new(key, value, field.nullable);
            Ok(DynamicArrayBuilder::new(builder))
        }
        ty @ (List | LargeList) => {
            let child = field
                .children
                .first()
                .ok_or_else(|| error!("List must have a single child"))?;
            let values = build_array_builder::<Arrow>(child)?;
            let name = field.name.to_string();
            let nullable = field.nullable;

            if let List = ty {
                Ok(DynamicArrayBuilder::new(ListArrayBuilder::<_, i32>::new(
                    values, name, nullable,
                )))
            } else {
                Ok(DynamicArrayBuilder::new(ListArrayBuilder::<_, i64>::new(
                    values, name, nullable,
                )))
            }
        }
    }
}
