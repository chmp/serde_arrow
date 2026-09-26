pub fn marrow_to_arrow_arrays(
    arrays: Vec<serde_arrow::marrow::array::Array>,
) -> Vec<serde_arrow::_impl::arrow::array::ArrayRef> {
    arrays
        .into_iter()
        .map(serde_arrow::_impl::arrow::array::ArrayRef::try_from)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

pub mod serde_arrow_arrow {
    use serde::Serialize;
    use serde_arrow::{
        _impl::arrow::{array::ArrayRef, datatypes::FieldRef},
        schema::SchemaLike,
        Result,
    };

    pub fn trace(items: &(impl ?Sized + Serialize)) -> Vec<FieldRef> {
        Vec::<FieldRef>::from_samples(items, Default::default()).unwrap()
    }

    pub fn serialize(
        fields: &[FieldRef],
        items: &(impl Serialize + ?Sized),
    ) -> Result<Vec<ArrayRef>> {
        serde_arrow::to_arrow(fields, items)
    }
}

pub mod serde_arrow_marrow {
    use serde::Serialize;
    use serde_arrow::{
        marrow::{array::Array, datatypes::Field},
        schema::SchemaLike,
        ArrayBuilder,
    };

    pub fn trace(items: &(impl ?Sized + Serialize)) -> Vec<Field> {
        Vec::<Field>::from_samples(items, Default::default()).unwrap()
    }

    /// Serialize to marrow without including the subsequent Arrow conversion.
    pub fn serialize(fields: &[Field], items: &(impl Serialize + ?Sized)) -> Vec<Array> {
        serde_arrow::to_marrow(fields, items).unwrap()
    }

    /// Measure the record-at-a-time API independently from bulk serialization.
    pub fn serialize_by_push(fields: &[Field], items: &[impl Serialize]) -> Vec<Array> {
        let mut builder = ArrayBuilder::from_marrow(fields).unwrap();
        builder.reserve(items.len());
        for item in items {
            builder.push(item).unwrap();
        }
        builder.into_marrow().unwrap()
    }
}

pub mod serde_arrow_marrow_to_arrow {
    use serde::Serialize;
    use serde_arrow::{
        _impl::arrow::array::ArrayRef, marrow::datatypes::Field, schema::SchemaLike,
    };

    pub fn trace(items: &(impl ?Sized + Serialize)) -> Vec<Field> {
        Vec::<Field>::from_samples(items, Default::default()).unwrap()
    }

    /// Measure marrow serialization plus the cost of converting its result to Arrow.
    pub fn serialize(fields: &[Field], items: &(impl Serialize + ?Sized)) -> Vec<ArrayRef> {
        super::marrow_to_arrow_arrays(serde_arrow::to_marrow(fields, items).unwrap())
    }
}

pub mod arrow {

    use std::sync::Arc;

    use arrow_json::ReaderBuilder;
    use arrow_schema::Schema;

    use serde::Serialize;

    use serde_arrow::{
        _impl::arrow::{array::ArrayRef, datatypes::FieldRef},
        schema::SchemaLike,
        Error, ErrorKind, Result,
    };

    pub fn trace(items: &(impl ?Sized + Serialize)) -> Vec<FieldRef> {
        Vec::<FieldRef>::from_samples(items, Default::default()).unwrap()
    }

    pub fn serialize(fields: &[FieldRef], items: &[impl Serialize]) -> Result<Vec<ArrayRef>> {
        let schema = Schema::new(fields.to_vec());
        let mut decoder = ReaderBuilder::new(Arc::new(schema))
            .build_decoder()
            .map_err(|err| Error::new_from(ErrorKind::Custom, err.to_string(), err))?;
        decoder
            .serialize(items)
            .map_err(|err| Error::new_from(ErrorKind::Custom, err.to_string(), err))?;
        Ok(decoder
            .flush()
            .map_err(|err| Error::new_from(ErrorKind::Custom, err.to_string(), err))?
            .ok_or_else(|| Error::new(ErrorKind::Custom, "no items".into()))?
            .columns()
            .to_vec())
    }
}
