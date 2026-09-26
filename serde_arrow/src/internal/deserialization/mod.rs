pub mod array_deserializer;
pub mod binary_deserializer;
pub mod bool_deserializer;
pub mod date_deserializer;
pub mod decimal_deserializer;
pub mod dictionary_deserializer;
pub mod duration_deserializer;
pub mod enum_deserializer;
pub mod enums_as_string_impl;
pub mod fixed_size_binary_deserializer;
pub mod fixed_size_list_deserializer;
pub mod float_deserializer;
pub mod integer_deserializer;
pub mod integer_impls;
pub mod list_deserializer;
pub mod map_deserializer;
pub mod null_deserializer;
pub mod random_access_deserializer;
pub mod string_deserializer;
pub mod struct_deserializer;
pub mod time_deserializer;
pub mod timestamp_deserializer;
pub mod utils;

#[derive(Clone, Copy)]
pub(crate) struct Utf8Validation {
    trusted: bool,
}

impl Utf8Validation {
    pub(crate) const CHECKED: Self = Self { trusted: false };

    /// # Safety
    /// Every string view used with this marker must originate from a valid Arrow
    /// string array, including nested and dictionary values.
    pub(crate) unsafe fn trusted_arrow() -> Self {
        Self { trusted: true }
    }

    pub(crate) fn is_trusted(self) -> bool {
        self.trusted
    }
}
