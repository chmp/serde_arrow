use std::{borrow::Cow, sync::Arc};

use serde::{Deserialize, Serialize};

use crate::{
    _impl::arrow,
    schema::{SchemaLike, SerdeArrowSchema, TracingOptions},
    Result,
};

#[derive(Default)]
pub struct Arrays {
    pub arrow: Option<Vec<Arc<dyn arrow::array::Array>>>,
}

#[derive(Default)]
pub struct Fields {
    pub arrow: Option<Vec<arrow::datatypes::FieldRef>>,
}

#[derive(Default)]
pub struct Test {
    schema: Option<SerdeArrowSchema>,
    pub arrays: Arrays,
    pub fields: Fields,
}

impl Test {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_schema<T: Serialize>(mut self, schema: T) -> Self {
        self.schema =
            Some(SerdeArrowSchema::from_value(&schema).expect("Failed conversion of schema"));

        self
    }
}

impl Test {
    pub fn get_arrow_fields(&self) -> Cow<'_, Vec<arrow::datatypes::FieldRef>> {
        match self.schema.as_ref() {
            Some(schema) => Cow::Owned(
                Vec::<arrow::datatypes::FieldRef>::try_from(schema)
                    .expect("Cannot covert schema to arrow fields"),
            ),
            None => Cow::Borrowed(
                self.fields
                    .arrow
                    .as_ref()
                    .expect("Without schema override the fields must have been traced"),
            ),
        }
    }
}

impl Test {
    pub fn trace_schema_from_samples<T: Serialize + ?Sized>(
        mut self,
        items: &T,
        options: TracingOptions,
    ) -> Self {
        let schema_from_samples = SerdeArrowSchema::from_samples(items, options)
            .expect("Failed to trace the schema from samples");

        if let Some(reference) = self.schema.as_ref() {
            assert_eq!(schema_from_samples, *reference);
        } else {
            self.schema = Some(schema_from_samples);
        }

        self
    }

    pub fn trace_schema_from_type<'de, T: Deserialize<'de>>(
        mut self,
        options: TracingOptions,
    ) -> Self {
        let schema_from_type = SerdeArrowSchema::from_type::<T>(options)
            .expect("Failed to trace the schema from type");

        if let Some(reference) = self.schema.as_ref() {
            assert_eq!(schema_from_type, *reference);
        } else {
            self.schema = Some(schema_from_type);
        }

        self
    }

    pub fn try_serialize_arrow<T: Serialize + ?Sized>(&mut self, items: &T) -> Result<()> {
        let fields = self.get_arrow_fields().to_vec();
        let arrays = crate::to_arrow(&fields, items)?;

        assert_eq!(fields.len(), arrays.len());
        for (field, array) in std::iter::zip(&fields, &arrays) {
            assert_eq!(
                field.data_type(),
                array.data_type(),
                "Datatype of field {:?} ({}) != datatype of array ({})",
                field.name(),
                field.data_type(),
                array.data_type(),
            );
            // NOTE: do not check nullability. Arrow `array.is_nullable()`
            // checks the number of actual nulls, not the nullability
        }

        self.arrays.arrow = Some(arrays);

        let mut builder = crate::ArrayBuilder::from_arrow(&fields)?;
        builder.extend(items)?;
        let arrays = builder.to_arrow()?;
        assert_eq!(self.arrays.arrow, Some(arrays));

        Ok(())
    }

    pub fn serialize<T: Serialize + ?Sized>(mut self, items: &T) -> Self {
        self.try_serialize_arrow(items)
            .expect("Failed arrow serialization");
        self
    }

    /// Test deserializing into an owned type
    pub fn deserialize<T>(self, items: &[T]) -> Self
    where
        T: for<'a> Deserialize<'a> + std::fmt::Debug + PartialEq,
    {
        self.deserialize_borrowed(items);
        self
    }

    /// Test deserializing by borrowing from the previously serialized arrays
    pub fn deserialize_borrowed<'a, T>(&'a self, items: &[T])
    where
        T: Deserialize<'a> + std::fmt::Debug + PartialEq,
    {
        let fields = self.get_arrow_fields();
        let roundtripped: Vec<T> = crate::from_arrow(
            &fields,
            self.arrays
                .arrow
                .as_ref()
                .expect("Deserialization requires known arrow arrays"),
        )
        .expect("Failed arrow deserialization");
        assert_eq!(roundtripped, items);
    }

    pub fn check_nulls(self, nulls: &[&[bool]]) -> Self {
        let Some(arrow_arrays) = self.arrays.arrow.as_ref() else {
            panic!("cannot check_nulls without arrays");
        };
        let arrow_nulls = arrow_arrays
            .iter()
            .map(|arr| {
                (0..arr.len())
                    .map(|idx| arr.is_null(idx))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(arrow_nulls, nulls);
        self
    }

    pub fn also<F: FnOnce(&mut Self)>(mut self, block: F) -> Self {
        block(&mut self);
        self
    }
}
