pub mod serde_arrow_arrow {
    use serde::Serialize;
    use serde_arrow::{
        _impl::arrow::{array::ArrayRef, datatypes::FieldRef},
        Result,
        schema::SchemaLike,
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
    use serde_arrow::marrow::{array::Array, datatypes::Field};
    use serde_arrow::schema::SchemaLike;

    pub fn trace(items: &(impl ?Sized + Serialize)) -> Vec<Field> {
        Vec::<Field>::from_samples(items, Default::default()).unwrap()
    }

    pub fn serialize(fields: &[Field], items: &(impl Serialize + ?Sized)) -> Vec<Array> {
        serde_arrow::to_marrow(fields, items).unwrap()
    }
}

pub mod arrow {

    use std::sync::Arc;

    use arrow_json::ReaderBuilder;
    use arrow_schema::Schema;

    use serde::Serialize;

    use serde_arrow::{
        _impl::arrow::{array::ArrayRef, datatypes::FieldRef},
        Error, ErrorKind, Result,
        schema::SchemaLike,
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

pub mod marrow_direct {
    use serde_arrow::{Result, marrow::array::Array};

    pub fn trace<T>(_items: &T) {}

    pub fn serialize<Element>(_fields: &(), items: &[Element]) -> Result<Vec<Array>>
    where
        Element: DirectMarrowBuild,
    {
        Ok(Element::build_marrow_arrays(items))
    }

    pub trait DirectMarrowBuild {
        fn build_marrow_arrays(items: &[Self]) -> Vec<Array>
        where
            Self: Sized;
    }
}
