use std::{borrow::Cow, str::FromStr, sync::Arc};

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::json;

use crate::{
    _impl::{arrow, arrow2},
    schema::{SchemaLike, SerdeArrowSchema},
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
    pub fn serialize<T: Serialize + ?Sized>(mut self, items: &T) -> Self {
        let fields = self.get_arrow_fields();
        self.arrays.arrow =
            Some(crate::to_arrow(&fields, items).expect("Failed arrow serialization"));

        let fields = self.get_arrow2_fields();
        self.arrays.arrow2 =
            Some(crate::to_arrow2(&fields, items).expect("Failed arrow2 serialization"));

        self
    }

    pub fn deserialize<T: DeserializeOwned + std::fmt::Debug + PartialEq>(
        self,
        items: &[T],
    ) -> Self {
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

        self
    }

    pub fn also<F: FnOnce(&mut Self)>(mut self, block: F) -> Self {
        block(&mut self);
        self
    }
}