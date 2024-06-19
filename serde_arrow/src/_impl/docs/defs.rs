//! Common definitions for doc tests
use serde::{Serialize, Deserialize};

#[derive(Clone, Deserialize, Serialize)]
pub struct Record {
    pub a: Option<f32>,
    pub b: u64,
}

pub const fn example_records() -> &'static [Record] {
    &[Record { a: Some(1.0), b: 2}]
}

#[cfg(has_arrow)]
pub fn example_record_batch() -> crate::_impl::arrow::array::RecordBatch {
    use crate::schema::{SchemaLike, TracingOptions};

    let items = example_records();
    
    let fields = Vec::<crate::_impl::arrow::datatypes::FieldRef>::from_type::<Record>(TracingOptions::default()).unwrap();
    crate::to_record_batch(&fields, &items).unwrap()
}

#[cfg(has_arrow)]
pub fn example_arrow_arrays() -> (Vec<crate::_impl::arrow::datatypes::FieldRef>, Vec<crate::_impl::arrow::array::ArrayRef>) {
    use crate::schema::{SchemaLike, TracingOptions};

    let items = example_records();

    let fields = Vec::<crate::_impl::arrow::datatypes::FieldRef>::from_type::<Record>(TracingOptions::default()).unwrap();
    let arrays = crate::to_arrow(&fields, &items).unwrap();

    (fields, arrays)
}
