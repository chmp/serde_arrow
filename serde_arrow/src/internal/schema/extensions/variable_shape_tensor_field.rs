use std::collections::HashMap;

use crate::internal::{
    arrow::{DataType, Field},
    error::{fail, Error, Result},
    schema::{transmute_field, PrettyField},
};

use super::utils::{check_dim_names, check_permutation, write_list, DebugRepr};

/// Helper to build variable shape tensor fields (`arrow.variable_shape_tensor`)
///
/// See the [arrow docs][variable-shape-tensor-field-docs] for details on the
/// different fields.
///
/// [variable-shape-tensor-field-docs]:
///     https://arrow.apache.org/docs/format/CanonicalExtensions.html#variable-shape-tensor
pub struct VariableShapeTensorField {
    name: String,
    element: Field,
    ndim: usize,
    nullable: bool,
    dim_names: Option<Vec<String>>,
    permutation: Option<Vec<usize>>,
    uniform_shape: Option<Vec<Option<usize>>>,
}

impl VariableShapeTensorField {
    /// Create a new non-nullable `VariableShapeTensorField`
    pub fn new(name: &str, element: impl serde::ser::Serialize, ndim: usize) -> Result<Self> {
        let element = transmute_field(element)?;
        if element.name != "element" {
            fail!("The element field of FixedShapeTensorField must be named \"element\"");
        }

        Ok(Self {
            name: name.to_owned(),
            element,
            ndim,
            nullable: false,
            dim_names: None,
            permutation: None,
            uniform_shape: None,
        })
    }

    /// Set the nullability of the field
    pub fn nullable(mut self, value: bool) -> Self {
        self.nullable = value;
        self
    }

    /// Set the permutation of the dimension
    pub fn permutation(mut self, value: Vec<usize>) -> Result<Self> {
        check_permutation(self.ndim, &value)?;
        self.permutation = Some(value);
        Ok(self)
    }

    /// Set the dimension names
    pub fn dim_names(mut self, value: Vec<String>) -> Result<Self> {
        check_dim_names(self.ndim, &value)?;
        self.dim_names = Some(value);
        Ok(self)
    }

    /// Set the uniform shape
    pub fn uniform_shape(mut self, value: Vec<Option<usize>>) -> Result<Self> {
        if value.len() != self.ndim {
            fail!("Invalid uniform_shape value");
        }
        self.uniform_shape = Some(value);
        Ok(self)
    }
}

impl VariableShapeTensorField {
    fn get_ext_metadata(&self) -> Result<String> {
        use std::fmt::Write;

        let mut first_field = true;

        let mut ext_metadata = String::new();
        write!(&mut ext_metadata, "{{")?;

        if let Some(permutation) = self.permutation.as_ref() {
            if first_field {
                first_field = false;
                write!(&mut ext_metadata, ",")?;
            }
            write!(&mut ext_metadata, "\"permutation\":")?;
            write_list(&mut ext_metadata, permutation.iter())?;
        }

        if let Some(dim_names) = self.dim_names.as_ref() {
            if first_field {
                first_field = false;
                write!(&mut ext_metadata, ",")?;
            }
            write!(&mut ext_metadata, "\"dim_names\":")?;
            write_list(&mut ext_metadata, dim_names.iter().map(DebugRepr))?;
        }

        if let Some(uniform_shape) = self.uniform_shape.as_ref() {
            if first_field {
                first_field = false;
                write!(&mut ext_metadata, ",")?;
            }
            write!(&mut ext_metadata, "\"uniform_shape\":")?;
            write_list(
                &mut ext_metadata,
                uniform_shape.iter().map(|val| match val {
                    Some(val) => format!("{val}"),
                    None => String::from("null"),
                }),
            )?;
        }

        // silence "value not read" warning
        let _ = first_field;

        write!(&mut ext_metadata, "}}")?;
        Ok(ext_metadata)
    }
}

impl TryFrom<&VariableShapeTensorField> for Field {
    type Error = Error;

    fn try_from(value: &VariableShapeTensorField) -> Result<Self> {
        let mut metadata = HashMap::new();
        metadata.insert(
            "ARROW:extension:name".into(),
            "arrow.variable_shape_tensor".into(),
        );
        metadata.insert("ARROW:extension:metadata".into(), value.get_ext_metadata()?);

        let fields = vec![
            Field {
                name: String::from("data"),
                data_type: DataType::List(Box::new(value.element.clone())),
                nullable: false,
                metadata: HashMap::new(),
            },
            Field {
                name: String::from("shape"),
                data_type: DataType::FixedSizeList(
                    Box::new(Field {
                        name: String::from("element"),
                        data_type: DataType::Int32,
                        nullable: false,
                        metadata: HashMap::new(),
                    }),
                    value.ndim.try_into()?,
                ),
                nullable: false,
                metadata: HashMap::new(),
            },
        ];

        Ok(Field {
            name: value.name.clone(),
            nullable: value.nullable,
            data_type: DataType::Struct(fields),
            metadata,
        })
    }
}

impl serde::ser::Serialize for VariableShapeTensorField {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::Error;
        let field = Field::try_from(self).map_err(S::Error::custom)?;
        PrettyField(&field).serialize(serializer)
    }
}

#[test]
fn test_serialization() -> crate::internal::error::PanicOnError<()> {
    use serde_json::json;

    let field = VariableShapeTensorField::new(
        "foo bar",
        json!({"name": "element", "data_type": "Bool"}),
        2,
    )?;
    let field = Field::try_from(&field)?;
    let actual = serde_json::to_value(PrettyField(&field))?;

    let expected = json!({
        "name": "foo bar",
        "data_type": "Struct",
        "children": [
            {
                "name": "data",
                "data_type": "List",
                "children": [{"name": "element", "data_type": "Bool"}],
            },
            {"name": "shape", "data_type": "FixedSizeList(2)", "children": [{"name": "element", "data_type": "I32"}]}
        ],
        "metadata": {
            "ARROW:extension:metadata": "{}",
            "ARROW:extension:name": "arrow.variable_shape_tensor",
        },
    });

    assert_eq!(actual, expected);
    Ok(())
}
