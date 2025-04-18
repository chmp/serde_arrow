use marrow::datatypes::{DataType, Field};
use std::{env, sync::Arc};

use serde::{Deserialize, Serialize};

use crate::{
    _impl::{arrow, arrow2},
    schema::{SchemaLike, SerdeArrowSchema, TracingOptions},
    Result,
};

#[derive(Default)]
pub struct Arrays {
    pub marrow: Option<Vec<marrow::array::Array>>,
    pub arrow: Option<Vec<Arc<dyn arrow::array::Array>>>,
    pub arrow2: Option<Vec<Box<dyn arrow2::array::Array>>>,
}

pub struct Impls {
    pub arrow: bool,
    pub arrow2: bool,
}

impl std::default::Default for Impls {
    fn default() -> Self {
        let skip_arrow_tests = env::var("SERDE_ARROW_SKIP_ARROW_TESTS").is_ok();
        let skip_arrow2_tests = env::var("SERDE_ARROW_SKIP_ARROW2_TESTS").is_ok();

        Self {
            arrow: !skip_arrow_tests,
            arrow2: !skip_arrow2_tests,
        }
    }
}

pub fn new_field(name: &str, data_type: DataType, nullable: bool) -> Field {
    Field {
        name: name.to_owned(),
        data_type,
        nullable,
        metadata: Default::default(),
    }
}

#[derive(Default)]
pub struct Test {
    schema: Option<SerdeArrowSchema>,
    pub impls: Impls,
    pub arrays: Arrays,
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
    pub fn get_arrow_fields(&self) -> Vec<arrow::datatypes::FieldRef> {
        self.schema
            .as_ref()
            .unwrap()
            .try_into()
            .expect("Cannot covert schema to arrow fields")
    }

    pub fn get_arrow2_fields(&self) -> Vec<arrow2::datatypes::Field> {
        self.schema
            .as_ref()
            .unwrap()
            .try_into()
            .expect("Cannot covert schema to arrow fields")
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

    pub fn try_serialize_marrow<T: Serialize + ?Sized>(&mut self, items: &T) -> Result<()> {
        let fields = self.schema.clone().unwrap().fields;
        let arrays = crate::to_marrow(&fields, items)?;

        assert_eq!(fields.len(), arrays.len());
        for (field, array) in std::iter::zip(&fields, &arrays) {
            assert_eq!(
                field.data_type,
                array.data_type(),
                "Datatype of field {:?} ({:?}) != datatype of array ({:?})",
                field.name,
                field.data_type,
                array.data_type(),
            );
            // NOTE: do not check nullability. Arrow `array.is_nullable()`
            // checks the number of actual nulls, not the nullability
        }
        self.arrays.marrow = Some(arrays);

        Ok(())
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
        assert_eq!(self.arrays.arrow.as_ref(), Some(&arrays));

        assert_eq!(fields.len(), arrays.len());
        for (field, array) in std::iter::zip(&fields, &arrays) {
            assert_eq!(field.data_type(), array.data_type());
        }

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

        let mut builder = crate::ArrayBuilder::from_arrow2(&fields)?;
        builder.extend(items)?;
        let arrays = builder.to_arrow2()?;
        assert_eq!(self.arrays.arrow2.as_ref(), Some(&arrays));

        assert_eq!(fields.len(), arrays.len());
        for (field, array) in std::iter::zip(&fields, &arrays) {
            assert_eq!(field.data_type(), array.data_type());
        }
        Ok(())
    }

    pub fn serialize<T: Serialize + ?Sized>(mut self, items: &T) -> Self {
        self.try_serialize_marrow(items)
            .expect("Failed marrow serialization");
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
            assert_eq!(roundtripped, items, "left: roundtripped, right: items");
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
            assert_eq!(roundtripped, items, "left: roundtripped, right: items");
        }
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
