pub mod serde_arrow_arrow {
    use serde::Serialize;
    use serde_arrow::{
        Result,
        _impl::arrow::{array::ArrayRef, datatypes::FieldRef},
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
    use marrow::{array::Array, datatypes::Field};
    use serde::Serialize;
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

    // arrow-version:replace: use arrow_json_{version}::ReaderBuilder;
    use arrow_json_56::ReaderBuilder;
    // arrow-version:replace: use arrow_schema_{version}::Schema;
    use arrow_schema_56::Schema;

    use serde::Serialize;

    use serde_arrow::{
        Error, Result,
        _impl::arrow::{array::ArrayRef, datatypes::FieldRef},
        schema::SchemaLike,
    };

    pub fn trace(items: &(impl ?Sized + Serialize)) -> Vec<FieldRef> {
        Vec::<FieldRef>::from_samples(items, Default::default()).unwrap()
    }

    pub fn serialize(fields: &[FieldRef], items: &[impl Serialize]) -> Result<Vec<ArrayRef>> {
        let schema = Schema::new(fields.to_vec());
        let mut decoder = ReaderBuilder::new(Arc::new(schema))
            .build_decoder()
            .map_err(|err| Error::custom_from(err.to_string(), err))?;
        decoder
            .serialize(items)
            .map_err(|err| Error::custom_from(err.to_string(), err))?;
        Ok(decoder
            .flush()
            .map_err(|err| Error::custom_from(err.to_string(), err))?
            .ok_or_else(|| Error::custom("no items".into()))?
            .columns()
            .to_vec())
    }
}

pub mod arrow2_convert {
    use arrow2_convert::{
        field::ArrowField,
        serialize::{ArrowSerialize, TryIntoArrow},
    };
    use serde_arrow::{Error, Result, _impl::arrow2::array::Array};

    pub fn trace<T>(_items: &T) {}

    pub fn serialize<Element>(_fields: &(), items: &[Element]) -> Result<Box<dyn Array>>
    where
        Element: ArrowSerialize + ArrowField<Type = Element> + 'static,
    {
        let array: Box<dyn Array> = items
            .try_into_arrow()
            .map_err(|err| Error::custom(err.to_string()))?;

        Ok(array)
    }
}
