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
    type Output: 'static;

    fn null() -> DynamicArrayBuilder<Self::Output>;
    fn bool() -> DynamicArrayBuilder<Self::Output>;
    fn i8() -> DynamicArrayBuilder<Self::Output>;
    fn i16() -> DynamicArrayBuilder<Self::Output>;
    fn i32() -> DynamicArrayBuilder<Self::Output>;
    fn i64() -> DynamicArrayBuilder<Self::Output>;
    fn u8() -> DynamicArrayBuilder<Self::Output>;
    fn u16() -> DynamicArrayBuilder<Self::Output>;
    fn u32() -> DynamicArrayBuilder<Self::Output>;
    fn u64() -> DynamicArrayBuilder<Self::Output>;
    fn f16() -> DynamicArrayBuilder<Self::Output>;
    fn f32() -> DynamicArrayBuilder<Self::Output>;
    fn f64() -> DynamicArrayBuilder<Self::Output>;
    fn utf8() -> DynamicArrayBuilder<Self::Output>;
    fn large_utf8() -> DynamicArrayBuilder<Self::Output>;
    fn date64() -> DynamicArrayBuilder<Self::Output>;
}

pub fn build_struct_array_builder<Arrow>(
    fields: &[GenericField],
) -> Result<StructArrayBuilder<DynamicArrayBuilder<Arrow::Output>>>
where
    Arrow: PrimitiveBuilders,
    NaiveDateTimeStrBuilder<DynamicArrayBuilder<Arrow::Output>>: ArrayBuilder<Arrow::Output>,
    UtcDateTimeStrBuilder<DynamicArrayBuilder<Arrow::Output>>: ArrayBuilder<Arrow::Output>,
    TupleStructBuilder<DynamicArrayBuilder<Arrow::Output>>: ArrayBuilder<Arrow::Output>,
    StructArrayBuilder<DynamicArrayBuilder<Arrow::Output>>: ArrayBuilder<Arrow::Output>,
    UnionArrayBuilder<DynamicArrayBuilder<Arrow::Output>>: ArrayBuilder<Arrow::Output>,
    DictionaryUtf8ArrayBuilder<DynamicArrayBuilder<Arrow::Output>>: ArrayBuilder<Arrow::Output>,
    MapArrayBuilder<DynamicArrayBuilder<Arrow::Output>>: ArrayBuilder<Arrow::Output>,
    ListArrayBuilder<DynamicArrayBuilder<Arrow::Output>, i32>: ArrayBuilder<Arrow::Output>,
    ListArrayBuilder<DynamicArrayBuilder<Arrow::Output>, i64>: ArrayBuilder<Arrow::Output>,
{
    let mut builders = Vec::new();
    for field in fields {
        builders.push(build_array_builder::<Arrow>(field)?);
    }

    let mut field = GenericField::new("dummy", GenericDataType::Struct, true);
    field.children = fields.to_vec();

    Ok(StructArrayBuilder::new(field, builders))
}

pub fn build_array_builder<Arrow>(
    field: &GenericField,
) -> Result<DynamicArrayBuilder<Arrow::Output>>
where
    Arrow: PrimitiveBuilders,
    NaiveDateTimeStrBuilder<DynamicArrayBuilder<Arrow::Output>>: ArrayBuilder<Arrow::Output>,
    UtcDateTimeStrBuilder<DynamicArrayBuilder<Arrow::Output>>: ArrayBuilder<Arrow::Output>,
    TupleStructBuilder<DynamicArrayBuilder<Arrow::Output>>: ArrayBuilder<Arrow::Output>,
    StructArrayBuilder<DynamicArrayBuilder<Arrow::Output>>: ArrayBuilder<Arrow::Output>,
    UnionArrayBuilder<DynamicArrayBuilder<Arrow::Output>>: ArrayBuilder<Arrow::Output>,
    DictionaryUtf8ArrayBuilder<DynamicArrayBuilder<Arrow::Output>>: ArrayBuilder<Arrow::Output>,
    MapArrayBuilder<DynamicArrayBuilder<Arrow::Output>>: ArrayBuilder<Arrow::Output>,
    ListArrayBuilder<DynamicArrayBuilder<Arrow::Output>, i32>: ArrayBuilder<Arrow::Output>,
    ListArrayBuilder<DynamicArrayBuilder<Arrow::Output>, i64>: ArrayBuilder<Arrow::Output>,
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

                let builder = TupleStructBuilder::new(field.clone(), builders);
                Ok(DynamicArrayBuilder::new(builder))
            }
            None | Some(Strategy::MapAsStruct) => {
                let builders = field
                    .children
                    .iter()
                    .map(build_array_builder::<Arrow>)
                    .collect::<Result<Vec<_>>>()?;

                let builder = StructArrayBuilder::new(field.clone(), builders);
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

            let builder = UnionArrayBuilder::new(field.clone(), builders);
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
            let entries = field
                .children
                .get(0)
                .ok_or_else(|| error!("Dictionary must have an entries child"))?;

            if !matches!(entries.data_type, GenericDataType::Struct) {
                fail!("The entries child of a map must be of type struct");
            }
            let key = entries
                .children
                .get(0)
                .ok_or_else(|| error!("Dictionary entries must have key, value children"))?;
            let value = entries
                .children
                .get(1)
                .ok_or_else(|| error!("Dictionary entries must have key, value children"))?;

            let builder = MapArrayBuilder::new(
                field.clone(),
                build_array_builder::<Arrow>(key)?,
                build_array_builder::<Arrow>(value)?,
            );
            Ok(DynamicArrayBuilder::new(builder))
        }
        ty @ (List | LargeList) => {
            let child = field
                .children
                .first()
                .ok_or_else(|| error!("List must have a single child"))?;
            let values = build_array_builder::<Arrow>(child)?;

            if let List = ty {
                Ok(DynamicArrayBuilder::new(ListArrayBuilder::<_, i32>::new(
                    field.clone(),
                    values,
                )))
            } else {
                Ok(DynamicArrayBuilder::new(ListArrayBuilder::<_, i64>::new(
                    field.clone(),
                    values,
                )))
            }
        }
    }
}
