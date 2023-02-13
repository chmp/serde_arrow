use arrow2::{array::Array, datatypes::Field};

use crate::{
    arrow2::sources::build_record_source,
    internal::{event::Event, source::EventSource},
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
