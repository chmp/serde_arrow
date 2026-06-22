//! Support for arrow
#![cfg_attr(any(), rustfmt::skip)]

// arrow-version:insert: #[cfg(feature = "arrow-{version}")]{\n}mod arrow_{version} {{{\n}    use {{arrow_array_{version} as arrow_array, arrow_buffer_{version} as arrow_buffer, arrow_data_{version} as arrow_data, arrow_schema_{version} as arrow_schema}};{\n}    include!("impl_api_53.rs");{\n}}}
#[cfg(feature = "arrow-59")]
mod arrow_59 {
    use {arrow_array_59 as arrow_array, arrow_buffer_59 as arrow_buffer, arrow_data_59 as arrow_data, arrow_schema_59 as arrow_schema};
    include!("impl_api_53.rs");
}
#[cfg(feature = "arrow-58")]
mod arrow_58 {
    use {arrow_array_58 as arrow_array, arrow_buffer_58 as arrow_buffer, arrow_data_58 as arrow_data, arrow_schema_58 as arrow_schema};
    include!("impl_api_53.rs");
}
#[cfg(feature = "arrow-57")]
mod arrow_57 {
    use {arrow_array_57 as arrow_array, arrow_buffer_57 as arrow_buffer, arrow_data_57 as arrow_data, arrow_schema_57 as arrow_schema};
    include!("impl_api_53.rs");
}
#[cfg(feature = "arrow-56")]
mod arrow_56 {
    use {arrow_array_56 as arrow_array, arrow_buffer_56 as arrow_buffer, arrow_data_56 as arrow_data, arrow_schema_56 as arrow_schema};
    include!("impl_api_53.rs");
}
#[cfg(feature = "arrow-55")]
mod arrow_55 {
    use {arrow_array_55 as arrow_array, arrow_buffer_55 as arrow_buffer, arrow_data_55 as arrow_data, arrow_schema_55 as arrow_schema};
    include!("impl_api_53.rs");
}
#[cfg(feature = "arrow-54")]
mod arrow_54 {
    use {arrow_array_54 as arrow_array, arrow_buffer_54 as arrow_buffer, arrow_data_54 as arrow_data, arrow_schema_54 as arrow_schema};
    include!("impl_api_53.rs");
}
#[cfg(feature = "arrow-53")]
mod arrow_53 {
    use {arrow_array_53 as arrow_array, arrow_buffer_53 as arrow_buffer, arrow_data_53 as arrow_data, arrow_schema_53 as arrow_schema};
    include!("impl_api_53.rs");
}
