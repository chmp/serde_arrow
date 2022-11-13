use arrow2::datatypes::{DataType, Field};

use crate::{error, fail, Result};

use super::{IntoPath, PathFragment, Strategy};

pub trait SchemaEditor {
    /// Get the raw field under the given path
    ///
    fn get_field_mut<P: IntoPath>(&mut self, path: P) -> Result<&mut Field>;

    /// Set the nullability of the field at the given path
    ///
    fn set_nullable<P: IntoPath>(&mut self, path: P, is_nullable: bool) -> Result<()> {
        self.get_field_mut(path)?.is_nullable = is_nullable;
        Ok(())
    }

    /// Set the data type of the field at the given path
    ///
    fn set_data_type<P: IntoPath>(&mut self, path: P, data_type: DataType) -> Result<()> {
        // TODO: warn if a nested type is turned into a primitive?
        self.get_field_mut(path)?.data_type = data_type;
        Ok(())
    }

    /// Configure the strategy to (de)serialize the field at the given path
    ///
    fn set_serde_arrow_strategy<P: IntoPath>(&mut self, path: P, strategy: Strategy) -> Result<()> {
        let field = self.get_field_mut(path)?;
        strategy.configure_field(field)?;
        Ok(())
    }
}

impl SchemaEditor for Vec<Field> {
    fn get_field_mut<P: IntoPath>(&mut self, path: P) -> Result<&mut Field> {
        let path = path.into_path()?;

        let (head, tail) = match path.as_slice() {
            [head @ .., tail] => (head, tail),
            [] => fail!("Cannot get root as a field"),
        };

        let parent = lookup_parent_mut(self, head)?;
        match tail {
            PathFragment::Field(tail) => get_field_mut(parent, tail),
            PathFragment::Index => parent
                .get_mut(0)
                .ok_or_else(|| error!("Not enought elements")),
        }
    }
}

fn lookup_parent_mut<'field>(
    fields: &'field mut [Field],
    path: &[PathFragment],
) -> Result<&'field mut [Field]> {
    let mut current_fields = fields;
    let mut current_path = path;

    while let [head, tail @ ..] = current_path {
        current_fields = match head {
            PathFragment::Field(head) => {
                let field = get_field_mut(current_fields, head)?;
                match &mut field.data_type {
                    DataType::Struct(fields) => fields,
                    _ => fail!("Cannot get fields of non struct "),
                }
            }
            _ => fail!("Sequences are not yet supported"),
        };
        current_path = tail;
    }

    Ok(current_fields)
}

fn get_field_mut<'field>(fields: &'field mut [Field], name: &str) -> Result<&'field mut Field> {
    fields
        .iter_mut()
        .find(|field| field.name == name)
        .ok_or_else(|| error!("Could not find field {name}"))
}
