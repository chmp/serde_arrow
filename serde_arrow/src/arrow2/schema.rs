use crate::{
    arrow2::display,
    impls::arrow2::datatypes::{DataType, Field, IntegerType, UnionMode},
    internal::{
        error::{error, fail, Error},
        schema::{GenericDataType, GenericField, Strategy, STRATEGY_KEY},
    },
    Result,
};

/// Make sure the field is configured correctly if a strategy is used
///
/// TODO: make this a generic method
pub fn check_strategy(field: &Field) -> Result<()> {
    let strategy_str = match field.metadata.get(STRATEGY_KEY) {
        Some(strategy_str) => strategy_str,
        None => return Ok(()),
    };

    match strategy_str.parse::<Strategy>()? {
        Strategy::InconsistentTypes => {
            if !matches!(field.data_type, DataType::Null) {
                fail!(
                    "Invalid strategy for field {name}: {strategy_str} expects the data type Null, found: {dt}",
                    name = display::Str(&field.name),
                    dt = display::DataType(&field.data_type),
                );
            }
        }
        Strategy::UtcStrAsDate64 | Strategy::NaiveStrAsDate64 => {
            if !matches!(field.data_type, DataType::Date64) {
                fail!(
                    "Invalid strategy for field {name}: {strategy_str} expects the data type Date64, found: {dt}",
                    name = display::Str(&field.name),
                    dt = display::DataType(&field.data_type),
                );
            }
        }
        Strategy::TupleAsStruct | Strategy::MapAsStruct => {
            if !matches!(field.data_type, DataType::Struct(_)) {
                fail!(
                    "Invalid strategy for field {name}: {strategy_str} expects the data type Struct, found: {dt}",
                    name = display::Str(&field.name),
                    dt = display::DataType(&field.data_type),
                );
            }
        }
    }

    Ok(())
}

/// Lookup a nested field among a set of top-level fields
///
/// The `path` argument should be a dotted path to the target field, e.g.,
/// `"parent.child.subchild"`. This helper is intended to simplify modifying
/// nested schemas.
///
/// Example:
///
/// ```rust
/// # use serde_arrow::impls::arrow2::datatypes::DataType;
/// # use chrono::NaiveDateTime;
/// # use serde::Serialize;
/// #
/// use serde_arrow::{
///     arrow2::{serialize_into_fields, experimental},
///     schema::{Strategy, TracingOptions},
/// };
///
/// ##[derive(Serialize, Default)]
/// struct Outer {
///     a: u32,
///     b: Nested,
/// }
///
/// ##[derive(Serialize, Default)]
/// struct Nested {
///     dt: NaiveDateTime,
/// }
///
/// let mut fields = serialize_into_fields(
///     &[Outer::default()],
///     TracingOptions::default(),
/// ).unwrap();
///
/// let dt_field = experimental::find_field_mut(&mut fields, "b.dt").unwrap();
/// dt_field.data_type = DataType::Date64;
/// dt_field.metadata = Strategy::NaiveStrAsDate64.into();
/// ```
pub fn find_field_mut<'f>(fields: &'f mut [Field], path: &'_ str) -> Result<&'f mut Field> {
    if path.is_empty() {
        fail!("Cannot get field with empty path");
    } else if let Some((head, tail)) = path.split_once('.') {
        let field = find_field_shallow(fields, head)?;
        let fields = get_child_fields(&mut field.data_type)?;
        find_field_mut(fields, tail)
    } else {
        find_field_shallow(fields, path)
    }
}

fn find_field_shallow<'f>(fields: &'f mut [Field], name: &'_ str) -> Result<&'f mut Field> {
    fields
        .iter_mut()
        .find(|f| f.name == name)
        .ok_or_else(|| error!("Cannot find field {name}"))
}

fn get_child_fields(dt: &mut DataType) -> Result<&mut [Field]> {
    match dt {
        DataType::Struct(fields) | DataType::Union(fields, _, _) => Ok(fields),
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            get_child_fields(&mut field.as_mut().data_type)
        }
        DataType::Extension(_, dt, _) | DataType::Dictionary(_, dt, _) => get_child_fields(dt),
        DataType::Map(field, _) => {
            let fields = match &mut field.data_type {
                DataType::Struct(fields) => fields,
                dt => fail!(
                    "Expected struct as the interior type of a map, found: {dt}",
                    dt = display::DataType(dt)
                ),
            };
            Ok(fields)
        }
        dt => fail!(
            "Data type {dt} does not support nested fields",
            dt = display::DataType(dt)
        ),
    }
}

impl TryFrom<&Field> for GenericField {
    type Error = Error;

    fn try_from(field: &Field) -> Result<Self> {
        let strategy: Option<Strategy> = match field.metadata.get(STRATEGY_KEY) {
            Some(strategy_str) => Some(strategy_str.parse::<Strategy>()?),
            None => None,
        };
        let name = field.name.to_owned();
        let nullable = field.is_nullable;

        let mut children = Vec::<GenericField>::new();
        let data_type = match &field.data_type {
            DataType::Boolean => GenericDataType::Bool,
            DataType::Null => GenericDataType::Null,
            DataType::Int8 => GenericDataType::I8,
            DataType::Int16 => GenericDataType::I16,
            DataType::Int32 => GenericDataType::I32,
            DataType::Int64 => GenericDataType::I64,
            DataType::UInt8 => GenericDataType::U8,
            DataType::UInt16 => GenericDataType::U16,
            DataType::UInt32 => GenericDataType::U32,
            DataType::UInt64 => GenericDataType::U64,
            DataType::Float16 => GenericDataType::F16,
            DataType::Float32 => GenericDataType::F32,
            DataType::Float64 => GenericDataType::F64,
            DataType::Utf8 => GenericDataType::Utf8,
            DataType::LargeUtf8 => GenericDataType::LargeUtf8,
            DataType::Date64 => GenericDataType::Date64,
            DataType::List(field) => {
                children.push(GenericField::try_from(field.as_ref())?);
                GenericDataType::List
            }
            DataType::LargeList(field) => {
                children.push(field.as_ref().try_into()?);
                GenericDataType::LargeList
            }
            DataType::Struct(fields) => {
                for field in fields {
                    children.push(field.try_into()?);
                }
                GenericDataType::Struct
            }
            DataType::Map(field, _) => {
                let kv_fields = match field.data_type() {
                    DataType::Struct(fields) => fields,
                    dt => fail!(
                        "Expected inner field of Map to be Struct, found: {dt}",
                        dt = display::DataType(dt),
                    ),
                };
                if kv_fields.len() != 2 {
                    fail!(
                        "Expected two fields (key and value) in map struct, found: {}",
                        kv_fields.len()
                    );
                }
                for field in kv_fields {
                    children.push(field.try_into()?);
                }
                GenericDataType::Map
            }
            DataType::Union(fields, field_indices, mode) => {
                if field_indices.is_some() {
                    fail!("Union types with explicit field indices are not supported");
                }
                if !mode.is_dense() {
                    fail!("Only dense unions are supported at the moment");
                }

                for field in fields {
                    children.push(field.try_into()?);
                }
                GenericDataType::Union
            }
            DataType::Dictionary(int_type, data_type, sorted) => {
                if *sorted {
                    fail!("Sorted dictionary are not supported");
                }
                let key_type = match int_type {
                    IntegerType::Int8 => DataType::Int8,
                    IntegerType::Int16 => DataType::Int16,
                    IntegerType::Int32 => DataType::Int32,
                    IntegerType::Int64 => DataType::Int64,
                    IntegerType::UInt8 => DataType::UInt8,
                    IntegerType::UInt16 => DataType::UInt16,
                    IntegerType::UInt32 => DataType::UInt32,
                    IntegerType::UInt64 => DataType::UInt64,
                };
                children.push((&Field::new("", key_type, false)).try_into()?);
                children.push((&Field::new("", data_type.as_ref().clone(), false)).try_into()?);
                GenericDataType::Dictionary
            }
            dt => fail!("Cannot convert data type {dt}", dt = display::DataType(dt)),
        };

        Ok(GenericField {
            data_type,
            name,
            strategy,
            children,
            nullable,
        })
    }
}

impl TryFrom<&GenericField> for Field {
    type Error = Error;

    fn try_from(value: &GenericField) -> Result<Self> {
        let data_type = match &value.data_type {
            GenericDataType::Null => DataType::Null,
            GenericDataType::Bool => DataType::Boolean,
            GenericDataType::I8 => DataType::Int8,
            GenericDataType::I16 => DataType::Int16,
            GenericDataType::I32 => DataType::Int32,
            GenericDataType::I64 => DataType::Int64,
            GenericDataType::U8 => DataType::UInt8,
            GenericDataType::U16 => DataType::UInt16,
            GenericDataType::U32 => DataType::UInt32,
            GenericDataType::U64 => DataType::UInt64,
            GenericDataType::F16 => DataType::Float16,
            GenericDataType::F32 => DataType::Float32,
            GenericDataType::F64 => DataType::Float64,
            GenericDataType::Date64 => DataType::Date64,
            GenericDataType::Utf8 => DataType::Utf8,
            GenericDataType::LargeUtf8 => DataType::LargeUtf8,
            GenericDataType::List => DataType::List(Box::new(
                value
                    .children
                    .get(0)
                    .ok_or_else(|| error!("List must a single child"))?
                    .try_into()?,
            )),
            GenericDataType::LargeList => DataType::LargeList(Box::new(
                value
                    .children
                    .get(0)
                    .ok_or_else(|| error!("List must a single child"))?
                    .try_into()?,
            )),
            GenericDataType::Struct => DataType::Struct(
                value
                    .children
                    .iter()
                    .map(Field::try_from)
                    .collect::<Result<Vec<_>>>()?,
            ),
            GenericDataType::Map => {
                let key_field: Field = value
                    .children
                    .get(0)
                    .ok_or_else(|| error!("Map must a two children"))?
                    .try_into()?;
                let val_field: Field = value
                    .children
                    .get(1)
                    .ok_or_else(|| error!("Map must a two children"))?
                    .try_into()?;
                let element_field = Field::new(
                    "entries",
                    DataType::Struct(vec![key_field, val_field]),
                    false,
                );

                DataType::Map(Box::new(element_field), false)
            }
            GenericDataType::Union => DataType::Union(
                value
                    .children
                    .iter()
                    .map(Field::try_from)
                    .collect::<Result<Vec<_>>>()?,
                None,
                UnionMode::Dense,
            ),
            GenericDataType::Dictionary => {
                let key_field = value
                    .children
                    .get(0)
                    .ok_or_else(|| error!("Dictionary must a two children"))?;
                let val_field: Field = value
                    .children
                    .get(1)
                    .ok_or_else(|| error!("Dictionary must a two children"))?
                    .try_into()?;

                let key_type = match &key_field.data_type {
                    GenericDataType::U8 => IntegerType::UInt8,
                    GenericDataType::U16 => IntegerType::UInt16,
                    GenericDataType::U32 => IntegerType::UInt32,
                    GenericDataType::U64 => IntegerType::UInt64,
                    GenericDataType::I8 => IntegerType::Int8,
                    GenericDataType::I16 => IntegerType::Int16,
                    GenericDataType::I32 => IntegerType::Int32,
                    GenericDataType::I64 => IntegerType::Int64,
                    _ => fail!("Invalid key type for dictionary"),
                };

                DataType::Dictionary(key_type, Box::new(val_field.data_type), false)
            }
        };

        let mut field = Field::new(&value.name, data_type, value.nullable);
        if let Some(strategy) = value.strategy.as_ref() {
            field.metadata = strategy.clone().into();
        }

        Ok(field)
    }
}
