use std::sync::Arc;

use marrow::{array::Array, datatypes::DataType, view::View};

/// Helper to view an as the given variant
macro_rules! view_as {
    ($variant:path, $array_ref:expr) => {
        match marrow::view::View::try_from(&*$array_ref) {
            Ok($variant(view)) => Ok(view),
            Ok(view) => Err(marrow::error::MarrowError::new(
                marrow::error::ErrorKind::Unsupported,
                format!(
                    "Unexpected view: expected {expected}, got {actual:?}",
                    expected = stringify!($variant),
                    actual = view,
                ),
            )),
            Err(err) => Err(err),
        }
    };
}

pub(crate) use view_as;

#[derive(Debug)]
pub struct PanicOnErrorError;

impl<E: std::error::Error> From<E> for PanicOnErrorError {
    fn from(err: E) -> Self {
        panic!("{err:?}")
    }
}

pub type PanicOnError<T, E = PanicOnErrorError> = std::result::Result<T, E>;

pub fn as_array_ref<A: arrow_array::Array + 'static>(
    values: impl TryInto<A, Error = impl std::fmt::Debug>,
) -> arrow_array::ArrayRef {
    Arc::new(values.try_into().unwrap()) as arrow_array::ArrayRef
}

pub fn assert_arrays_eq(
    array_via_arrow: arrow_array::ArrayRef,
    marrow_array: Array,
) -> PanicOnError<()> {
    let array_via_marrow = arrow_array::ArrayRef::try_from(marrow_array.clone())?;

    assert_eq!(
        DataType::try_from(array_via_arrow.data_type())?,
        marrow_array.data_type(),
        "marrow data type: arrow (left) != marrow (right)"
    );
    assert_eq!(
        *array_via_arrow.data_type(),
        arrow_schema::DataType::try_from(&marrow_array.data_type())?,
        "arrow data type: arrow (left) != marrow (right)"
    );
    assert_eq!(
        array_via_arrow.data_type(),
        array_via_marrow.data_type(),
        "arrow data type: arrow (left) != marrow (right)"
    );

    assert_eq!(
        &array_via_arrow, &array_via_marrow,
        "array: arrow (left) != marrow (right)"
    );

    let view_via_arrow = View::try_from(&*array_via_arrow)?;
    let view_via_marrow = marrow_array.as_view();

    assert_eq!(
        DataType::try_from(array_via_arrow.data_type())?,
        view_via_marrow.data_type(),
        "view data_type: arrow (left) != marrow (right)"
    );
    assert_eq!(
        view_via_arrow, view_via_marrow,
        "view: arrow (left) != marrow (right)"
    );

    Ok(())
}
