use crate::internal::arrow::ArrayView;

pub trait ArrayViewExt {
    fn len(&self) -> usize;
}

impl<'a> ArrayViewExt for ArrayView<'a> {
    fn len(&self) -> usize {
        use ArrayView as V;
        match self {
            V::Null(view) => view.len,
            V::Boolean(view) => view.len,
            V::Int8(view) => view.values.len(),
            V::Int16(view) => view.values.len(),
            V::Int32(view) => view.values.len(),
            V::Int64(view) => view.values.len(),
            V::UInt8(view) => view.values.len(),
            V::UInt16(view) => view.values.len(),
            V::UInt32(view) => view.values.len(),
            V::UInt64(view) => view.values.len(),
            V::Float16(view) => view.values.len(),
            V::Float32(view) => view.values.len(),
            V::Float64(view) => view.values.len(),
            V::Date32(view) => view.values.len(),
            V::Date64(view) => view.values.len(),
            V::Time32(view) => view.values.len(),
            V::Time64(view) => view.values.len(),
            V::Timestamp(view) => view.values.len(),
            V::Duration(view) => view.values.len(),
            V::Decimal128(view) => view.values.len(),
            V::Utf8(view) => view.offsets.len().saturating_sub(1),
            V::LargeUtf8(view) => view.offsets.len().saturating_sub(1),
            V::Binary(view) => view.offsets.len().saturating_sub(1),
            V::LargeBinary(view) => view.offsets.len().saturating_sub(1),
            V::FixedSizeBinary(view) => match usize::try_from(view.n) {
                Ok(n) if n > 0 => view.data.len() / n,
                _ => 0,
            },
            V::FixedSizeList(view) => view.len,
            V::List(view) => view.offsets.len().saturating_sub(1),
            V::LargeList(view) => view.offsets.len().saturating_sub(1),
            V::DenseUnion(view) => view.types.len(),
            V::Map(view) => view.offsets.len().saturating_sub(1),
            V::Struct(view) => view.len,
            V::Dictionary(view) => view.indices.len(),
        }
    }
}
