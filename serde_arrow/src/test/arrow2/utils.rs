use arrow2::{array::Array, datatypes::Field};

use crate::{
    arrow2::sources::build_record_source,
    base::{Event, EventSource},
    Result,
};

/// Collect the events for the given array
///
/// This functionality is mostly intended as a debug functionality.
///
pub fn collect_events_from_array<A>(fields: &[Field], arrays: &[A]) -> Result<Vec<Event<'static>>>
where
    A: AsRef<dyn Array>,
{
    let mut source = build_record_source(fields, arrays)?;
    let mut res = Vec::new();
    while let Some(ev) = source.next()? {
        res.push(ev.to_static());
    }
    Ok(res)
}

pub mod field {
    use arrow2::datatypes::{DataType, Field};

    pub fn uint8(name: &str, nullable: bool) -> Field {
        Field::new(name, DataType::UInt8, nullable)
    }

    pub fn uint16(name: &str, nullable: bool) -> Field {
        Field::new(name, DataType::UInt16, nullable)
    }

    pub fn int8(name: &str, nullable: bool) -> Field {
        Field::new(name, DataType::Int8, nullable)
    }

    pub fn int32(name: &str, nullable: bool) -> Field {
        Field::new(name, DataType::Int32, nullable)
    }

    pub fn large_list(name: &str, nullable: bool, field: Field) -> Field {
        Field::new(name, DataType::LargeList(Box::new(field)), nullable)
    }

    pub fn r#struct<const N: usize>(name: &str, nullable: bool, fields: [Field; N]) -> Field {
        Field::new(name, DataType::Struct(fields.to_vec()), nullable)
    }
}

pub mod access {
    use arrow2::{
        array::{Array, ListArray, PrimitiveArray, StructArray},
        datatypes::DataType,
        types::{NativeType, Offset},
    };

    use crate::{
        base::error::{error, fail},
        generic::schema::{IntoPath, PathDisplay, PathFragment},
        Result,
    };

    #[derive(Debug, Clone, PartialEq)]
    pub enum Value {
        Primitive(usize),
        List(usize),
        Struct(usize, usize),
        Null,
        Int8(i8),
        Int16(i16),
        Int32(i32),
        Int64(i64),
        UInt8(u8),
        UInt16(u16),
        UInt32(u32),
        UInt64(u64),
        Float32(f32),
        Float64(f64),
    }

    macro_rules! implement_from_value {
        ($ty:ty, $var:ident) => {
            impl From<$ty> for Value {
                fn from(val: $ty) -> Self {
                    Value::$var(val)
                }
            }
        };
    }

    implement_from_value!(i8, Int8);
    implement_from_value!(i16, Int16);
    implement_from_value!(i32, Int32);
    implement_from_value!(i64, Int64);
    implement_from_value!(u8, UInt8);
    implement_from_value!(u16, UInt16);
    implement_from_value!(u32, UInt32);
    implement_from_value!(u64, UInt64);
    implement_from_value!(f32, Float32);
    implement_from_value!(f64, Float64);

    pub fn get_value<P: IntoPath>(array: &dyn Array, path: P) -> Result<Value> {
        let path = path.into_path()?;
        get_value_impl(array, &path)
    }

    fn get_value_impl(array: &dyn Array, path: &[PathFragment]) -> Result<Value> {
        use PathFragment::*;
        match (array.data_type(), path) {
            (DataType::List(_) | DataType::LargeList(_), []) => Ok(Value::List(array.len())),
            (DataType::Struct(fields), []) => Ok(Value::Struct(array.len(), fields.len())),
            (_, []) => Ok(Value::Primitive(array.len())),
            (DataType::Int8, [Index(idx)]) => get_primitive::<i8>(array, *idx),
            (DataType::Int16, [Index(idx)]) => get_primitive::<i16>(array, *idx),
            (DataType::Int32, [Index(idx)]) => get_primitive::<i32>(array, *idx),
            (DataType::Int64, [Index(idx)]) => get_primitive::<i64>(array, *idx),
            (DataType::UInt8, [Index(idx)]) => get_primitive::<u8>(array, *idx),
            (DataType::UInt16, [Index(idx)]) => get_primitive::<u16>(array, *idx),
            (DataType::UInt32, [Index(idx)]) => get_primitive::<u32>(array, *idx),
            (DataType::UInt64, [Index(idx)]) => get_primitive::<u64>(array, *idx),
            (DataType::Float32, [Index(idx)]) => get_primitive::<f32>(array, *idx),
            (DataType::Float64, [Index(idx)]) => get_primitive::<f64>(array, *idx),
            (DataType::Struct(_), [Field(name), tail @ ..]) => {
                get_value_impl(get_struct_field(array, name)?, tail)
            }
            (DataType::List(_), [Index(idx), tail @ ..]) => {
                let item = get_list_item::<i32>(array, *idx)?;
                let item =
                    item.ok_or_else(|| error!("Null entries in lists are not yet supported"))?;
                get_value_impl(item.as_ref(), tail)
            }
            (DataType::LargeList(_), [Index(idx), tail @ ..]) => {
                let item = get_list_item::<i64>(array, *idx)?;
                let item =
                    item.ok_or_else(|| error!("Null entries in lists are not yet supported"))?;
                get_value_impl(item.as_ref(), tail)
            }
            (dt, path) => fail!(
                "Unknown combination of data type ({dt:?}) and path {}",
                PathDisplay(path)
            ),
        }
    }

    pub fn get_primitive<T: NativeType + Into<Value>>(
        array: &dyn Array,
        idx: usize,
    ) -> Result<Value> {
        let array = array
            .as_any()
            .downcast_ref::<PrimitiveArray<T>>()
            .ok_or_else(|| error!("Invalid array type"))?;
        if idx >= array.len() {
            fail!("Cannot get {idx} of a {} item array", array.len());
        }

        if array.is_null(idx) {
            Ok(Value::Null)
        } else {
            Ok(array.value(idx).into())
        }
    }

    pub fn get_struct_field<'a>(array: &'a dyn Array, field: &str) -> Result<&'a dyn Array> {
        let array = array
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| error!("Non struct array"))?;
        let idx = array
            .fields()
            .iter()
            .position(|f| f.name == field)
            .ok_or_else(|| error!("Cannot find field {field}"))?;
        let field = array
            .values()
            .get(idx)
            .ok_or_else(|| error!("Inconsistent array"))?;
        Ok(field.as_ref())
    }

    pub fn get_list_item<'a, O: Offset>(
        array: &dyn Array,
        idx: usize,
    ) -> Result<Option<Box<dyn Array>>> {
        let array = array
            .as_any()
            .downcast_ref::<ListArray<O>>()
            .ok_or_else(|| error!("Non list array"))?;

        if idx >= array.len() {
            fail!("Cannot get {idx} of a {} item array", array.len());
        }

        if array.is_null(idx) {
            return Ok(None);
        }

        Ok(Some(array.value(idx)))
    }
}
