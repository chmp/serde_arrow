use crate::internal::error::{fail, Result};

pub fn check_dim_names(ndim: usize, dim_names: &[String]) -> Result<()> {
    if dim_names.len() != ndim {
        fail!("Number of dim names must be equal to the number of dimensions");
    }
    Ok(())
}

/// Check that the permutation array contains indeed a permutation of dimension `ndim`
pub fn check_permutation(ndim: usize, permutation: &[usize]) -> Result<()> {
    if permutation.len() != ndim {
        fail!("Number of permutation entries must be equal to the number of dimensions");
    }
    let mut seen = vec![false; permutation.len()];
    for &i in permutation {
        let Some(i_was_seen) = seen.get_mut(i) else {
            fail!(
                "Invalid permutation: index {i} is not in range 0..{len}",
                len = seen.len()
            );
        };
        if *i_was_seen {
            fail!("Invalid permutation: index {i} found multiple times");
        }
        *i_was_seen = true;
    }
    for (i, seen) in seen.into_iter().enumerate() {
        if !seen {
            fail!("Invalid permutation: index {i} is not present");
        }
    }
    Ok(())
}

#[test]
fn test_check_permutation() {
    check_permutation(3, &[0, 1, 2]).unwrap();
    check_permutation(3, &[2, 0, 1]).unwrap();
    check_permutation(3, &[4, 1, 2]).unwrap_err();
    check_permutation(3, &[0, 0, 2]).unwrap_err();
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
