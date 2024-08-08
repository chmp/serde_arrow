use std::collections::HashMap;

use crate::internal::{
    arrow::{DataType, Field},
    error::{fail, Error, Result},
    schema::{transmute_field, PrettyField},
};

use super::utils::{check_dim_names, check_permutation, write_list, DebugRepr};

/// Easily construct a fixed shape tensor fields (`arrow.fixed_shape_tensor`)
///
/// See the [arrow docs][fixed-shape-tensor-docs] for details on the different
/// fields.
///
/// The Rust value must serialize to a fixed size list that contains the
/// flattened tensor elements in C order. To support different orders, set the
/// [`permutation`][FixedShapeTensorField::permutation].
///
/// This struct is designed to be used with
/// [`TracingOptions::overwrite`][crate::schema::TracingOptions::overwrite]:
///
/// ```rust
/// # use serde_json::json;
/// # use serde_arrow::{Result, schema::{TracingOptions, ext::FixedShapeTensorField}};
/// # fn main() -> Result<()> {
/// TracingOptions::default().overwrite(
///     "tensor",
///     FixedShapeTensorField::new(
///         "tensor",
///         json!({"name": "element", "data_type": "I32"}),
///         vec![2, 2],
///     )?,
/// )?
/// # ;
/// # Ok(())
/// # }
/// ```
///
/// [fixed-shape-tensor-docs]:
///     https://arrow.apache.org/docs/format/CanonicalExtensions.html#variable-shape-tensor
///
#[derive(Clone, Debug, PartialEq)]
pub struct FixedShapeTensorField {
    name: String,
    nullable: bool,
    element: Field,
    shape: Vec<usize>,
    dim_names: Option<Vec<String>>,
    permutation: Option<Vec<usize>>,
}

impl FixedShapeTensorField {
    /// Construct a new non-nullable `FixedShapeTensorField`
    ///
    /// Note the element parameter must serialize into a valid field definition
    /// with the the name `"element"`. The field type can be any valid Arrow
    /// type.
    pub fn new(name: &str, element: impl serde::ser::Serialize, shape: Vec<usize>) -> Result<Self> {
        let element = transmute_field(element)?;
        if element.name != "element" {
            fail!("The element field of FixedShapeTensorField must be named \"element\"");
        }

        Ok(Self {
            name: name.to_owned(),
            shape,
            element,
            nullable: false,
            dim_names: None,
            permutation: None,
        })
    }

    /// Set the nullability of the field
    pub fn nullable(mut self, value: bool) -> Self {
        self.nullable = value;
        self
    }

    /// Set the permutation of the dimension
    pub fn permutation(mut self, value: Vec<usize>) -> Result<Self> {
        check_permutation(self.shape.len(), &value)?;
        self.permutation = Some(value);
        Ok(self)
    }

    /// Set the dimension names
    pub fn dim_names(mut self, value: Vec<String>) -> Result<Self> {
        check_dim_names(self.shape.len(), &value)?;
        self.dim_names = Some(value);
        Ok(self)
    }
}

impl FixedShapeTensorField {
    fn get_ext_metadata(&self) -> Result<String> {
        use std::fmt::Write;

        let mut ext_metadata = String::new();
        write!(&mut ext_metadata, "{{")?;

        write!(&mut ext_metadata, "\"shape\":")?;
        write_list(&mut ext_metadata, self.shape.iter())?;

        if let Some(permutation) = self.permutation.as_ref() {
            write!(&mut ext_metadata, ",\"permutation\":")?;
            write_list(&mut ext_metadata, permutation.iter())?;
        }

        if let Some(dim_names) = self.dim_names.as_ref() {
            write!(&mut ext_metadata, ",\"dim_names\":")?;
            write_list(&mut ext_metadata, dim_names.iter().map(DebugRepr))?;
        }

        write!(&mut ext_metadata, "}}")?;
        Ok(ext_metadata)
    }
}

impl TryFrom<&FixedShapeTensorField> for Field {
    type Error = Error;

    fn try_from(value: &FixedShapeTensorField) -> Result<Self> {
        let mut n = 1;
        for s in &value.shape {
            n *= *s;
        }

        let mut metadata = HashMap::new();
        metadata.insert(
            "ARROW:extension:name".into(),
            "arrow.fixed_shape_tensor".into(),
        );
        metadata.insert("ARROW:extension:metadata".into(), value.get_ext_metadata()?);

        Ok(Field {
            name: value.name.to_owned(),
            nullable: value.nullable,
            data_type: DataType::FixedSizeList(Box::new(value.element.clone()), n.try_into()?),
            metadata,
        })
    }
}

impl serde::ser::Serialize for FixedShapeTensorField {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::Error;
        let field = Field::try_from(self).map_err(S::Error::custom)?;
        PrettyField(&field).serialize(serializer)
    }
}

#[test]
fn fixed_shape_tensor_field_repr() -> crate::internal::error::PanicOnError<()> {
    use serde_json::json;

    let field = FixedShapeTensorField::new(
        "hello",
        json!({"name": "element", "data_type": "F32"}),
        vec![2, 3],
    )?;
    let field = Field::try_from(&field)?;
    let actual = serde_json::to_value(&PrettyField(&field))?;
    let expected = json!({
        "name": "hello",
        "data_type": "FixedSizeList(6)",
        "children": [{
            "name": "element",
            "data_type": "F32",
        }],
        "metadata": {
            "ARROW:extension:metadata": "{\"shape\":[2,3]}",
            "ARROW:extension:name": "arrow.fixed_shape_tensor",
        },
    });

    assert_eq!(actual, expected);
    Ok(())
}
