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

    fn null(path: String) -> DynamicArrayBuilder<Self::Output>;
    fn bool(path: String) -> DynamicArrayBuilder<Self::Output>;
    fn i8(path: String) -> DynamicArrayBuilder<Self::Output>;
    fn i16(path: String) -> DynamicArrayBuilder<Self::Output>;
    fn i32(path: String) -> DynamicArrayBuilder<Self::Output>;
    fn i64(path: String) -> DynamicArrayBuilder<Self::Output>;
    fn u8(path: String) -> DynamicArrayBuilder<Self::Output>;
    fn u16(path: String) -> DynamicArrayBuilder<Self::Output>;
    fn u32(path: String) -> DynamicArrayBuilder<Self::Output>;
    fn u64(path: String) -> DynamicArrayBuilder<Self::Output>;
    fn f16(path: String) -> DynamicArrayBuilder<Self::Output>;
    fn f32(path: String) -> DynamicArrayBuilder<Self::Output>;
    fn f64(path: String) -> DynamicArrayBuilder<Self::Output>;
    fn utf8(path: String) -> DynamicArrayBuilder<Self::Output>;
    fn large_utf8(path: String) -> DynamicArrayBuilder<Self::Output>;
    fn date64(path: String) -> DynamicArrayBuilder<Self::Output>;
}

pub fn build_struct_array_builder<Arrow>(
    path: String,
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
        builders.push(build_array_builder::<Arrow>(format!("{path}.{}", field.name), field)?);
    }

    let mut field = GenericField::new("dummy", GenericDataType::Struct, true);
    field.children = fields.to_vec();

    Ok(StructArrayBuilder::new(field, builders))
}

pub fn build_array_builder<Arrow>(
    path: String,
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
        Null => Ok(Arrow::null(path)),
        Bool => Ok(Arrow::bool(path)),
        I8 => Ok(Arrow::i8(path)),
        I16 => Ok(Arrow::i16(path)),
        I32 => Ok(Arrow::i32(path)),
        I64 => Ok(Arrow::i64(path)),
        U8 => Ok(Arrow::u8(path)),
        U16 => Ok(Arrow::u16(path)),
        U32 => Ok(Arrow::u32(path)),
        U64 => Ok(Arrow::u64(path)),
        F16 => Ok(Arrow::f16(path)),
        F32 => Ok(Arrow::f32(path)),
        F64 => Ok(Arrow::f64(path)),
        Utf8 => Ok(Arrow::utf8(path)),
        LargeUtf8 => Ok(Arrow::large_utf8(path)),
        Date64 => match field.strategy.as_ref() {
            Some(Strategy::NaiveStrAsDate64) => Ok(DynamicArrayBuilder::new(
                NaiveDateTimeStrBuilder(Arrow::date64(path)),
            )),
            Some(Strategy::UtcStrAsDate64) => Ok(DynamicArrayBuilder::new(UtcDateTimeStrBuilder(
                Arrow::date64(path),
            ))),
            None => Ok(Arrow::date64(path)),
            Some(strategy) => fail!("Invalid strategy {strategy} for type Date64"),
        },
        Struct => match field.strategy.as_ref() {
            Some(Strategy::TupleAsStruct) => {
                let builders = field
                    .children
                    .iter()
                    .map(|f| build_array_builder::<Arrow>(format!("{path}.{}", f.name), f))
                    .collect::<Result<Vec<_>>>()?;

                let builder = TupleStructBuilder::new(path, field.clone(), builders);
                Ok(DynamicArrayBuilder::new(builder))
            }
            None | Some(Strategy::MapAsStruct) => {
                let builders = field
                    .children
                    .iter()
                    .map(|f| build_array_builder::<Arrow>(format!("{path}.{}", f.name), f))
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
                .map(|f| build_array_builder::<Arrow>(format!("{path}.{}", f.name), f))
                .collect::<Result<Vec<_>>>()?;

            let builder = UnionArrayBuilder::new(field.clone(), builders);
            Ok(DynamicArrayBuilder::new(builder))
        }
        Dictionary => {
            let key = field
                .children
                .get(0)
                .ok_or_else(|| error!("Dictionary must have key/value children"))?;
            let key = build_array_builder::<Arrow>(format!("{path}.key"), key)?;

            let value = field
                .children
                .get(1)
                .ok_or_else(|| error!("Dictionary must have key/value children"))?;
            let value = build_array_builder::<Arrow>(format!("{path}.value"), value)?;

            Ok(DynamicArrayBuilder::new(DictionaryUtf8ArrayBuilder::new(
                path, key, value,
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
                build_array_builder::<Arrow>(format!("{path}.key"), key)?,
                build_array_builder::<Arrow>(format!("{path}.value"), value)?,
            );
            Ok(DynamicArrayBuilder::new(builder))
        }
        ty @ (List | LargeList) => {
            let child = field
                .children
                .first()
                .ok_or_else(|| error!("List must have a single child"))?;
            let values = build_array_builder::<Arrow>(format!("{path}.item"), child)?;

            if let List = ty {
                Ok(DynamicArrayBuilder::new(ListArrayBuilder::<_, i32>::new(
                    path,
                    field.clone(),
                    values,
                )))
            } else {
                Ok(DynamicArrayBuilder::new(ListArrayBuilder::<_, i64>::new(
                    path,
                    field.clone(),
                    values,
                )))
            }
        }
    }
}
