use crate::internal::{
    arrow::{DataType, Field},
    error::{fail, Result},
};

pub fn check_dim_names(ndim: usize, dim_names: &[String]) -> Result<()> {
    if dim_names.len() != ndim {
        fail!("Number of dim names must be equal to the number of dimensions");
    }
    Ok(())
}

pub fn check_permutation(ndim: usize, permutation: &[usize]) -> Result<()> {
    if permutation.len() != ndim {
        fail!("Number of permutation entries must be equal to the number of dimensions");
    }
    let seen = vec![false; permutation.len()];
    for &i in permutation {
        if i >= seen.len() {
            fail!(
                "Invalid permutation: index {i} is not in range 0..{len}",
                len = seen.len()
            );
        }
        if seen[i] {
            fail!("Invalid permutation: index {i} found multiple times");
        }
    }
    for (i, seen) in seen.into_iter().enumerate() {
        if !seen {
            fail!("Invalid permutation: index {i} is not present");
        }
    }
    Ok(())
}

pub fn write_list(
    s: &mut String,
    items: impl Iterator<Item = impl std::fmt::Display>,
) -> Result<()> {
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

pub struct DebugRepr<T: std::fmt::Debug>(pub T);

impl<T: std::fmt::Debug> std::fmt::Display for DebugRepr<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

pub(crate) fn fix_dictionaries(field: &mut Field) {
    if matches!(field.data_type, DataType::Dictionary(_, _, _)) {
        field.nullable = true;
    } else if let DataType::Struct(children) = &mut field.data_type {
        for child in children {
            fix_dictionaries(child);
        }
    }
}
