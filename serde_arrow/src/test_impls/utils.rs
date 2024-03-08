use std::{borrow::Cow, sync::Arc};

use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{
    _impl::{arrow, arrow2},
    schema::{SchemaLike, SerdeArrowSchema, TracingOptions},
    Result,
};

#[derive(Default)]
pub struct Arrays {
    pub arrow: Option<Vec<Arc<dyn arrow::array::Array>>>,
    pub arrow2: Option<Vec<Box<dyn arrow2::array::Array>>>,
}

#[derive(Default)]
pub struct Fields {
    pub arrow: Option<Vec<arrow::datatypes::Field>>,
    pub arrow2: Option<Vec<arrow2::datatypes::Field>>,
}

pub struct Impls {
    arrow: bool,
    arrow2: bool,
}

impl std::default::Default for Impls {
    fn default() -> Self {
        Self {
            arrow: true,
            arrow2: true,
        }
    }
}

pub trait ResultAsserts {
    fn assert_error(&self, message: &str);
}

impl<T> ResultAsserts for Result<T> {
    fn assert_error(&self, message: &str) {
        let Err(err) = self else {
            panic!("Expected error");
        };
        assert!(err.to_string().contains(message), "unexpected error: {err}");
    }
}

#[derive(Default)]
pub struct Test {
    schema: Option<SerdeArrowSchema>,
    impls: Impls,
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

    pub fn skip_arrow2(mut self) -> Self {
        self.impls.arrow2 = false;
        self
    }
}

impl Test {
    fn get_arrow_fields(&self) -> Cow<'_, Vec<arrow::datatypes::Field>> {
        match self.schema.as_ref() {
            Some(schema) => Cow::Owned(
                schema
                    .to_arrow_fields()
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

    fn get_arrow2_fields(&self) -> Cow<'_, Vec<arrow2::datatypes::Field>> {
        match self.schema.as_ref() {
            Some(schema) => Cow::Owned(
                schema
                    .to_arrow2_fields()
                    .expect("Cannot covert schema to arrow fields"),
            ),
            None => Cow::Borrowed(
                self.fields
                    .arrow2
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

        let mut builder = crate::ArrowBuilder::new(&fields)?;
        builder.extend(items)?;
        let arrays = builder.build_arrays()?;
        assert_eq!(self.arrays.arrow, Some(arrays));

        Ok(())
    }

    pub fn try_serialize_arrow2<T: Serialize + ?Sized>(&mut self, items: &T) -> Result<()> {
        let fields = self.get_arrow2_fields().to_vec();
        let arrays = crate::to_arrow2(&fields, items)?;

        assert_eq!(fields.len(), arrays.len());
        for (field, array) in std::iter::zip(&fields, &arrays) {
            assert_eq!(field.data_type(), array.data_type());
        }

        self.arrays.arrow2 = Some(arrays);

        let mut builder = crate::Arrow2Builder::new(&fields)?;
        builder.extend(items)?;
        let arrays = builder.build_arrays()?;
        assert_eq!(self.arrays.arrow2, Some(arrays));

        // TODO: test that the result arrays has the fields as the schema

        Ok(())
    }

    pub fn serialize<T: Serialize + ?Sized>(mut self, items: &T) -> Self {
        if self.impls.arrow {
            self.try_serialize_arrow(items)
                .expect("Failed arrow serialization");
        }
        if self.impls.arrow2 {
            self.try_serialize_arrow2(items)
                .expect("Failed arrow2 serialization");
        }
        self
    }

    pub fn deserialize<T: DeserializeOwned + std::fmt::Debug + PartialEq>(
        self,
        items: &[T],
    ) -> Self {
        if self.impls.arrow {
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

        if self.impls.arrow2 {
            let fields = self.get_arrow2_fields();
            let roundtripped: Vec<T> = crate::from_arrow2(
                &fields,
                self.arrays
                    .arrow2
                    .as_ref()
                    .expect("Deserialization requires known arrow2 arrays"),
            )
            .expect("Failed arrow2 deserialization");
            assert_eq!(roundtripped, items);
        }

        self
    }

    pub fn check_nulls(self, nulls: &[&[bool]]) -> Self {
        if self.impls.arrow {
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
        }

        if self.impls.arrow2 {
            let Some(arrow2_arrays) = self.arrays.arrow2.as_ref() else {
                panic!("cannot check_nulls without arrays");
            };
            let arrow2_nulls = arrow2_arrays
                .iter()
                .map(|arr| {
                    (0..arr.len())
                        .map(|idx| arr.is_null(idx))
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();
            assert_eq!(arrow2_nulls, nulls);
        }

        self
    }

    pub fn also<F: FnOnce(&mut Self)>(mut self, block: F) -> Self {
        block(&mut self);
        self
    }
}
