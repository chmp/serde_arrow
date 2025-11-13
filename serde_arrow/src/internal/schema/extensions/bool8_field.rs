use std::collections::HashMap;

use marrow::datatypes::{DataType, Field};

use crate::internal::{
    error::{Error, Result},
    schema::PrettyField,
};

/// A helper to construct new `Bool8` fields (`arrow.bool8`)
///
/// This extension type can be used with `overwrites` in schema tracing:
///
/// ```rust
/// # use serde_json::json;
/// # use serde_arrow::{Result, schema::{SerdeArrowSchema, SchemaLike, TracingOptions, ext::Bool8Field}};
/// # use serde::Deserialize;
/// # fn main() -> Result<()> {
/// ##[derive(Deserialize)]
/// struct Record {
///     int_field: i32,
///     nested: Nested,
/// }
///
/// ##[derive(Deserialize)]
/// struct Nested {
///     bool_field: bool,
/// }
///
/// let tracing_options = TracingOptions::default()
///     .overwrite("nested.bool_field", Bool8Field::new("bool_field"))?;
///
/// let schema = SerdeArrowSchema::from_type::<Record>(tracing_options)?;
/// # std::mem::drop(schema);
/// # Ok(())
/// # }
/// ```
///
/// It can also be converted to a `arrow` `Field` for manual schema manipulation.
///
pub struct Bool8Field {
    name: String,
    nullable: bool,
}

impl Bool8Field {
    /// Construct a new non-nullable `Bool8Field`
    pub fn new(name: &str) -> Self {
        Self {
            name: name.into(),
            nullable: false,
        }
    }

    /// Set the nullability of the field
    pub fn nullable(mut self, value: bool) -> Self {
        self.nullable = value;
        self
    }
}

impl TryFrom<&Bool8Field> for Field {
    type Error = Error;

    fn try_from(value: &Bool8Field) -> Result<Self> {
        let mut metadata = HashMap::new();
        metadata.insert("ARROW:extension:name".into(), "arrow.bool8".into());
        metadata.insert("ARROW:extension:metadata".into(), String::new());

        Ok(Field {
            name: value.name.to_owned(),
            nullable: value.nullable,
            data_type: DataType::Int8,
            metadata,
        })
    }
}

impl serde::ser::Serialize for Bool8Field {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::Error;
        let field = Field::try_from(self).map_err(S::Error::custom)?;
        PrettyField(&field).serialize(serializer)
    }
}

#[test]
fn bool8_repr() -> crate::internal::error::PanicOnError<()> {
    use serde_json::json;

    let field = Bool8Field::new("hello");

    let field = Field::try_from(&field)?;
    let actual = serde_json::to_value(PrettyField(&field))?;

    let expected = json!({
        "name": "hello",
        "data_type": "I8",
        "metadata": {
            "ARROW:extension:name": "arrow.bool8",
            "ARROW:extension:metadata": "",
        },
    });

    assert_eq!(actual, expected);
    Ok(())
}
