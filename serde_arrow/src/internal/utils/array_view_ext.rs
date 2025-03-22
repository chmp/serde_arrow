use marrow::view::{BytesView, BytesViewView, PrimitiveView, View};

use crate::internal::{
    deserialization::utils::bitset_is_set,
    error::{fail, Result},
};

use super::Offset;

pub trait ViewExt {
    fn is_nullable(&self) -> Result<bool>;
    fn len(&self) -> Result<usize>;
}

impl ViewExt for View<'_> {
    fn is_nullable(&self) -> Result<bool> {
        use View as V;
        match self {
            V::Null(_) => Ok(true),
            V::Union(_) => Ok(false),
            V::Boolean(view) => Ok(view.validity.is_some()),
            V::Int8(view) => Ok(view.validity.is_some()),
            V::Int16(view) => Ok(view.validity.is_some()),
            V::Int32(view) => Ok(view.validity.is_some()),
            V::Int64(view) => Ok(view.validity.is_some()),
            V::UInt8(view) => Ok(view.validity.is_some()),
            V::UInt16(view) => Ok(view.validity.is_some()),
            V::UInt32(view) => Ok(view.validity.is_some()),
            V::UInt64(view) => Ok(view.validity.is_some()),
            V::Float16(view) => Ok(view.validity.is_some()),
            V::Float32(view) => Ok(view.validity.is_some()),
            V::Float64(view) => Ok(view.validity.is_some()),
            V::Date32(view) => Ok(view.validity.is_some()),
            V::Date64(view) => Ok(view.validity.is_some()),
            V::Time32(view) => Ok(view.validity.is_some()),
            V::Time64(view) => Ok(view.validity.is_some()),
            V::Timestamp(view) => Ok(view.validity.is_some()),
            V::Duration(view) => Ok(view.validity.is_some()),
            V::Decimal128(view) => Ok(view.validity.is_some()),
            V::Utf8(view) => Ok(view.validity.is_some()),
            V::Utf8View(view) => Ok(view.validity.is_some()),
            V::LargeUtf8(view) => Ok(view.validity.is_some()),
            V::Binary(view) => Ok(view.validity.is_some()),
            V::LargeBinary(view) => Ok(view.validity.is_some()),
            V::BinaryView(view) => Ok(view.validity.is_some()),
            V::FixedSizeBinary(view) => Ok(view.validity.is_some()),
            V::FixedSizeList(view) => Ok(view.validity.is_some()),
            V::List(view) => Ok(view.validity.is_some()),
            V::LargeList(view) => Ok(view.validity.is_some()),
            V::Map(view) => Ok(view.validity.is_some()),
            V::Struct(view) => Ok(view.validity.is_some()),
            V::Dictionary(view) => view.keys.is_nullable(),
            _ => fail!("Unknown view type"),
        }
    }

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

pub trait ViewAccess<'a, Item: ?Sized + 'a> {
    fn get(&self, idx: usize) -> Result<Option<&'a Item>>;

    fn get_required(&self, idx: usize) -> Result<&'a Item>
    where
        Self: 'a,
    {
        if let Some(val) = self.get(idx)? {
            Ok(val)
        } else {
            fail!("Required item was not present");
        }
    }

    fn is_some(&self, idx: usize) -> Result<bool>
    where
        Self: 'a,
    {
        Ok(self.get(idx)?.is_some())
    }
}

impl<'a, T> ViewAccess<'a, T> for PrimitiveView<'a, T> {
    fn get(&self, idx: usize) -> Result<Option<&'a T>> {
        if let Some(value) = self.values.get(idx) {
            if let Some(validity) = self.validity.as_ref() {
                if !bitset_is_set(validity, idx)? {
                    return Ok(None);
                }
            }
            Ok(Some(value))
        } else {
            fail!("Access beyond array length");
        }
    }
}

impl<'a, O: Offset> ViewAccess<'a, [u8]> for BytesView<'a, O> {
    fn get(&self, idx: usize) -> Result<Option<&'a [u8]>> {
        if idx + 1 > self.offsets.len() {
            fail!(
                "Invalid access: tried to get element {idx} of array with {len} elements",
                len = self.offsets.len().saturating_sub(1)
            );
        }

        if let Some(validity) = &self.validity {
            if !bitset_is_set(validity, idx)? {
                return Ok(None);
            }
        }

        let start = self.offsets[idx].try_into_usize()?;
        let end = self.offsets[idx + 1].try_into_usize()?;
        Ok(Some(&self.data[start..end]))
    }
}

impl<'a> ViewAccess<'a, [u8]> for BytesViewView<'a> {
    fn get(&self, idx: usize) -> Result<Option<&'a [u8]>> {
        let Some(desc) = self.data.get(idx) else {
            fail!(
                "Invalid access: tried to get element {idx} of array with {len} elements",
                len = self.data.len()
            );
        };

        if let Some(validity) = &self.validity {
            if !bitset_is_set(validity, idx)? {
                return Ok(None);
            }
        }

        let len = (*desc as u32) as usize;
        let res = || -> Option<&'a [u8]> {
            if len <= 12 {
                let bytes: &[u8] = bytemuck::try_cast_slice(std::slice::from_ref(desc)).ok()?;
                bytes.get(4..4 + len)
            } else {
                let buf_idx = ((*desc >> 64) as u32) as usize;
                let offset = ((*desc >> 96) as u32) as usize;
                self.buffers.get(buf_idx)?.get(offset..offset + len)
            }
        }();

        if res.is_none() {
            fail!("invalid state in bytes deserialization");
        }
        Ok(res)
    }
}

impl<'a, V> ViewAccess<'a, str> for V
where
    V: ViewAccess<'a, [u8]>,
{
    fn get(&self, idx: usize) -> Result<Option<&'a str>> {
        match ViewAccess::<[u8]>::get(self, idx) {
            Ok(Some(data)) => Ok(Some(std::str::from_utf8(data)?)),
            Ok(None) => Ok(None),
            Err(err) => Err(err),
        }
    }
}
