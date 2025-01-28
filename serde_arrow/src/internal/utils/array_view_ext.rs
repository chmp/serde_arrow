use marrow::view::View;

use crate::internal::error::{fail, Result};

pub trait ViewExt {
    fn len(&self) -> Result<usize>;
}

impl ViewExt for View<'_> {
    fn len(&self) -> Result<usize> {
        use View as V;
        match self {
            V::Null(view) => Ok(view.len),
            V::Boolean(view) => Ok(view.len),
            V::Int8(view) => Ok(view.values.len()),
            V::Int16(view) => Ok(view.values.len()),
            V::Int32(view) => Ok(view.values.len()),
            V::Int64(view) => Ok(view.values.len()),
            V::UInt8(view) => Ok(view.values.len()),
            V::UInt16(view) => Ok(view.values.len()),
            V::UInt32(view) => Ok(view.values.len()),
            V::UInt64(view) => Ok(view.values.len()),
            V::Float16(view) => Ok(view.values.len()),
            V::Float32(view) => Ok(view.values.len()),
            V::Float64(view) => Ok(view.values.len()),
            V::Date32(view) => Ok(view.values.len()),
            V::Date64(view) => Ok(view.values.len()),
            V::Time32(view) => Ok(view.values.len()),
            V::Time64(view) => Ok(view.values.len()),
            V::Timestamp(view) => Ok(view.values.len()),
            V::Duration(view) => Ok(view.values.len()),
            V::Decimal128(view) => Ok(view.values.len()),
            V::Utf8(view) => Ok(view.offsets.len().saturating_sub(1)),
            V::Utf8View(view) => Ok(view.data.len()),
            V::LargeUtf8(view) => Ok(view.offsets.len().saturating_sub(1)),
            V::Binary(view) => Ok(view.offsets.len().saturating_sub(1)),
            V::LargeBinary(view) => Ok(view.offsets.len().saturating_sub(1)),
            V::BinaryView(view) => Ok(view.data.len()),
            V::FixedSizeBinary(view) => match usize::try_from(view.n) {
                Ok(n) if n > 0 => Ok(view.data.len() / n),
                _ => Ok(0),
            },
            V::FixedSizeList(view) => Ok(view.len),
            V::List(view) => Ok(view.offsets.len().saturating_sub(1)),
            V::LargeList(view) => Ok(view.offsets.len().saturating_sub(1)),
            V::Union(view) => Ok(view.types.len()),
            V::Map(view) => Ok(view.offsets.len().saturating_sub(1)),
            V::Struct(view) => Ok(view.len),
            V::Dictionary(view) => view.keys.len(),
            _ => fail!("Unknown view type"),
        }
    }
}
