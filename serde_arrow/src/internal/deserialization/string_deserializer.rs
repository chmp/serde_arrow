use marrow::view::{BytesView, BytesViewView};

use crate::internal::{
    error::{set_default, try_, Context, ContextSupport, Result},
    utils::array_view_ext::ViewAccess,
};

use super::{
    enums_as_string_impl::EnumAccess, random_access_deserializer::RandomAccessDeserializer,
    Utf8Validation,
};

pub trait StringDeserializerDataType {
    const DATA_TYPE_NAME: &'static str;
}

impl StringDeserializerDataType for BytesView<'_, i32> {
    const DATA_TYPE_NAME: &'static str = "Utf8";
}

impl StringDeserializerDataType for BytesView<'_, i64> {
    const DATA_TYPE_NAME: &'static str = "LargeUtf8";
}

impl StringDeserializerDataType for BytesViewView<'_> {
    const DATA_TYPE_NAME: &'static str = "Utf8View";
}

pub struct StringDeserializer<V> {
    pub path: String,
    pub view: V,
    utf8_validation: Utf8Validation,
}

impl<V> StringDeserializer<V> {
    pub fn new(path: String, view: V, utf8_validation: Utf8Validation) -> Self {
        Self {
            path,
            view,
            utf8_validation,
        }
    }
}

pub(super) fn get_utf8<'a, V>(view: &V, idx: usize, validation: Utf8Validation) -> Result<&'a str>
where
    V: ViewAccess<'a, str> + ViewAccess<'a, [u8]> + 'a,
{
    if validation.is_trusted() {
        let bytes = ViewAccess::<[u8]>::get_required(view, idx)?;
        // SAFETY: the trusted marker requires all string views to come from
        // valid Arrow arrays. Their unsafe Array contract guarantees UTF-8.
        Ok(unsafe { std::str::from_utf8_unchecked(bytes) })
    } else {
        ViewAccess::<str>::get_required(view, idx)
    }
}

impl<V: StringDeserializerDataType> Context for StringDeserializer<V> {
    fn annotate(&self, annotations: &mut std::collections::BTreeMap<String, String>) {
        set_default(annotations, "field", &self.path);
        set_default(annotations, "data_type", V::DATA_TYPE_NAME);
    }
}

impl<'a, VV> RandomAccessDeserializer<'a> for StringDeserializer<VV>
where
    VV: ViewAccess<'a, str> + ViewAccess<'a, [u8]> + StringDeserializerDataType + 'a,
{
    fn is_some(&self, idx: usize) -> Result<bool> {
        if self.utf8_validation.is_trusted() {
            ViewAccess::<[u8]>::is_some(&self.view, idx)
        } else {
            ViewAccess::<str>::is_some(&self.view, idx)
        }
    }

    fn deserialize_option<V: serde::de::Visitor<'a>>(
        &self,
        visitor: V,
        idx: usize,
    ) -> Result<V::Value> {
        if self.utf8_validation.is_trusted() {
            return try_(|| {
                if self.is_some(idx)? {
                    visitor.visit_some(self.at(idx))
                } else {
                    visitor.visit_none()
                }
            })
            .ctx(self);
        }

        try_(|| match ViewAccess::<str>::get(&self.view, idx)? {
            Some(value) => visitor.visit_some(
                ValidatedStringDeserializer {
                    original: self,
                    value,
                }
                .at(idx),
            ),
            None => visitor.visit_none(),
        })
        .ctx(self)
    }

    fn deserialize_any_some<V: serde::de::Visitor<'a>>(
        &self,
        visitor: V,
        idx: usize,
    ) -> Result<V::Value> {
        self.deserialize_str(visitor, idx)
    }

    fn deserialize_str<V: serde::de::Visitor<'a>>(
        &self,
        visitor: V,
        idx: usize,
    ) -> Result<V::Value> {
        try_(|| visitor.visit_borrowed_str(get_utf8(&self.view, idx, self.utf8_validation)?))
            .ctx(self)
    }

    fn deserialize_string<V: serde::de::Visitor<'a>>(
        &self,
        visitor: V,
        idx: usize,
    ) -> Result<V::Value> {
        try_(|| visitor.visit_string(get_utf8(&self.view, idx, self.utf8_validation)?.to_owned()))
            .ctx(self)
    }

    fn deserialize_bytes<V: serde::de::Visitor<'a>>(
        &self,
        visitor: V,
        idx: usize,
    ) -> Result<V::Value> {
        try_(|| visitor.visit_bytes(get_utf8(&self.view, idx, self.utf8_validation)?.as_bytes()))
            .ctx(self)
    }

    fn deserialize_byte_buf<V: serde::de::Visitor<'a>>(
        &self,
        visitor: V,
        idx: usize,
    ) -> Result<V::Value> {
        try_(|| {
            visitor.visit_byte_buf(
                get_utf8(&self.view, idx, self.utf8_validation)?
                    .to_owned()
                    .into_bytes(),
            )
        })
        .ctx(self)
    }

    fn deserialize_enum<V: serde::de::Visitor<'a>>(
        &self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
        idx: usize,
    ) -> Result<V::Value> {
        try_(|| {
            let variant = get_utf8(&self.view, idx, self.utf8_validation)?;
            visitor.visit_enum(EnumAccess(variant))
        })
        .ctx(self)
    }
}

struct ValidatedStringDeserializer<'this, 'a, VV> {
    original: &'this StringDeserializer<VV>,
    value: &'a str,
}

impl<VV: StringDeserializerDataType> Context for ValidatedStringDeserializer<'_, '_, VV> {
    fn annotate(&self, annotations: &mut std::collections::BTreeMap<String, String>) {
        self.original.annotate(annotations);
    }
}

impl<'a, VV> RandomAccessDeserializer<'a> for ValidatedStringDeserializer<'_, 'a, VV>
where
    VV: StringDeserializerDataType,
{
    fn is_some(&self, _idx: usize) -> Result<bool> {
        Ok(true)
    }

    fn deserialize_any_some<V: serde::de::Visitor<'a>>(
        &self,
        visitor: V,
        idx: usize,
    ) -> Result<V::Value> {
        self.deserialize_str(visitor, idx)
    }

    fn deserialize_str<V: serde::de::Visitor<'a>>(
        &self,
        visitor: V,
        _idx: usize,
    ) -> Result<V::Value> {
        try_(|| visitor.visit_borrowed_str(self.value)).ctx(self)
    }

    fn deserialize_string<V: serde::de::Visitor<'a>>(
        &self,
        visitor: V,
        _idx: usize,
    ) -> Result<V::Value> {
        try_(|| visitor.visit_string(self.value.to_owned())).ctx(self)
    }

    fn deserialize_bytes<V: serde::de::Visitor<'a>>(
        &self,
        visitor: V,
        _idx: usize,
    ) -> Result<V::Value> {
        try_(|| visitor.visit_bytes(self.value.as_bytes())).ctx(self)
    }

    fn deserialize_byte_buf<V: serde::de::Visitor<'a>>(
        &self,
        visitor: V,
        _idx: usize,
    ) -> Result<V::Value> {
        try_(|| visitor.visit_byte_buf(self.value.to_owned().into_bytes())).ctx(self)
    }

    fn deserialize_enum<V: serde::de::Visitor<'a>>(
        &self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
        _idx: usize,
    ) -> Result<V::Value> {
        try_(|| visitor.visit_enum(EnumAccess(self.value))).ctx(self)
    }
}

#[cfg(test)]
mod tests {
    use marrow::{
        datatypes::{DataType, Field},
        view::{BitsWithOffset, BytesView, View},
    };
    use serde::Deserialize;

    #[derive(Debug, Deserialize, PartialEq)]
    struct Row<'a> {
        #[serde(borrow)]
        value: Option<&'a str>,
    }

    #[test]
    fn nullable_marrow_string_borrows_valid_values_and_reports_invalid_utf8() {
        let fields = [Field {
            name: "value".into(),
            data_type: DataType::LargeUtf8,
            nullable: true,
            metadata: Default::default(),
        }];
        let offsets = [0_i64, 5, 5];
        let valid = [0b0000_0001];
        let views = [View::LargeUtf8(BytesView {
            offsets: &offsets,
            data: "café".as_bytes(),
            validity: Some(BitsWithOffset {
                offset: 0,
                data: &valid,
            }),
        })];
        let decoded: Vec<Row<'_>> = crate::from_marrow(&fields, &views).unwrap();
        assert_eq!(
            decoded,
            vec![
                Row {
                    value: Some("café")
                },
                Row { value: None }
            ]
        );

        let invalid_offsets = [0_i64, 2];
        let invalid = [b'a', 0xff];
        let views = [View::LargeUtf8(BytesView {
            offsets: &invalid_offsets,
            data: &invalid,
            validity: Some(BitsWithOffset {
                offset: 0,
                data: &valid,
            }),
        })];
        let error = crate::from_marrow::<Vec<Row<'_>>>(&fields, &views).unwrap_err();
        assert!(error.to_string().contains("invalid UTF-8 data"));
    }
}
