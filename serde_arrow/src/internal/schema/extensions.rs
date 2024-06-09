use std::collections::HashMap;

use serde::Serialize;

use super::{GenericDataType, GenericField};
use crate::internal::{
    error::{fail, Error, Result},
    utils::value,
};

/// Easily construct a field for tensors with fixed shape
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
///         [2, 2],
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
    element: GenericField,
    shape: Vec<usize>,
    dim_names: Option<Vec<String>>,
    permutation: Option<Vec<usize>>,
}

impl FixedShapeTensorField {
    /// Construct a new instance
    ///
    /// Note the element parameter must serialize into a valid field definition
    /// with the the name `"element"`. The field type can be any valid Arrow
    /// type.
    pub fn new(name: &str, element: impl Serialize, shape: Vec<usize>) -> Result<Self> {
        let element: GenericField = value::transmute(&element)?;
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
        if value.len() != self.shape.len() {
            fail!("Number of permutation entries must be equal to the number of dimensions");
        }
        let seen = vec![false; value.len()];
        for &i in &value {
            if i >= seen.len() {
                fail!(
                    "Invalid permutation: index {i} is not in range 0..{len}",
                    len = seen.len()
                );
            }
            if seen[i] {
                fail!("Invalid permutation: index {i} found multiple times");
            }
            seen[i];
        }
        for (i, seen) in seen.into_iter().enumerate() {
            if !seen {
                fail!("Invalid permutation: index {i} is not present");
            }
        }

        self.permutation = Some(value);
        Ok(self)
    }

    /// Set the dimension names
    pub fn dim_names(mut self, value: Vec<String>) -> Result<Self> {
        if value.len() != self.shape.len() {
            fail!("Number of dim names must be equal to the number of dimensions");
        }
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

impl TryFrom<&FixedShapeTensorField> for GenericField {
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

        Ok(GenericField {
            name: value.name.clone(),
            nullable: value.nullable,
            data_type: GenericDataType::FixedSizeList(n.try_into()?),
            children: vec![value.element.clone()],
            strategy: None,
            metadata,
        })
    }
}

impl serde::ser::Serialize for FixedShapeTensorField {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::Error;
        GenericField::try_from(self)
            .map_err(S::Error::custom)?
            .serialize(serializer)
    }
}

fn write_list(s: &mut String, items: impl Iterator<Item = impl std::fmt::Display>) -> Result<()> {
    use std::fmt::Write;

    write!(s, "[")?;
    for (idx, val) in items.enumerate() {
        if idx != 0 {
            write!(s, ",{val}")?;
        } else {
            write!(s, "{val}")?;
        }
    }
    write!(s, "]")?;
    Ok(())
}

struct DebugRepr<T: std::fmt::Debug>(T);

impl<T: std::fmt::Debug> std::fmt::Display for DebugRepr<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}
