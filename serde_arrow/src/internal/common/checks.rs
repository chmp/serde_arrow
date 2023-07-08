use crate::internal::{
    common::BitBuffer,
    error::{fail, Result},
};

/// Check that the list layout given in terms of validity and offsets is
/// supported by serde_arrow
///
/// While the [arrow format spec][] explicitly allows null values in lists that
/// correspond to non-empty segments, this is currently not supported in arrow
/// deserialization. The spec says "a null value may correspond to a
/// **non-empty** segment in the child array."
///
/// [arrow format spec]: https://arrow.apache.org/docs/format/Columnar.html#variable-size-list-layout
pub fn check_supported_list_layout<'a, O>(
    validity: Option<BitBuffer<'a>>,
    offsets: &'a [O],
) -> Result<()>
where
    O: std::ops::Sub<Output = O> + std::cmp::PartialEq + From<i32> + Copy,
{
    let Some(validity) = validity else { return Ok(()) };

    if offsets.len() != validity.len() + 1 {
        fail!(
            "validity length {val} and offsets length {off} do not match (expected {val}, {exp})",
            val = validity.len(),
            off = offsets.len(),
            exp = validity.len() + 1,
        );
    }
    for i in 0..validity.len() {
        if !validity.is_set(i) && (offsets[i + 1] - offsets[i]) != O::from(0) {
            fail!("lists with data in null values are currently not supported in deserialization");
        }
    }

    Ok(())
}
