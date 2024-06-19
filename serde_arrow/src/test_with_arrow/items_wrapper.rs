use std::sync::Arc;

use crate::{
    self as serde_arrow,
    _impl::arrow::{_raw::schema::Schema, array::RecordBatch, datatypes::FieldRef},
    internal::error::PanicOnError,
    schema::{SchemaLike, TracingOptions},
    utils::{Item, Items},
};

#[test]
fn example() -> PanicOnError<()> {
    let items: Vec<u64> = vec![1, 2, 3, 4, 5];

    let fields_from_type = Vec::<FieldRef>::from_type::<Item<u64>>(TracingOptions::default())?;
    let fields_from_samples =
        Vec::<FieldRef>::from_samples(&Items(&items), TracingOptions::default())?;

    assert_eq!(fields_from_type, fields_from_samples);
    let fields = fields_from_type;

    let arrays = serde_arrow::to_arrow(&fields, &Items(&items))?;

    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields.clone())), arrays.clone())?;
    println!("{:#?}", batch);

    let Items(round_tripped): Items<Vec<u64>> = serde_arrow::from_arrow(&fields, &arrays)?;
    assert_eq!(items, round_tripped);

    Ok(())
}
