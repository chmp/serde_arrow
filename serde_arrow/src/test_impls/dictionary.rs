use super::macros::test_example;

test_example!(
    test_name = string_dict_u32,
    test_deserialization = [],
    tracing_options = TracingOptions::default().string_dictionary_encoding(true),
    field = GenericField::new("root", GenericDataType::Dictionary, false)
        .with_child(GenericField::new("key", GenericDataType::U32, false))
        .with_child(GenericField::new(
            "value",
            GenericDataType::LargeUtf8,
            false
        )),
    ty = String,
    values = [String::from("a"), String::from("b"), String::from("a")],
    nulls = [false, false, false],
);

test_example!(
    test_name = string_dict_nullable_u32,
    test_deserialization = [],
    tracing_options = TracingOptions::default().string_dictionary_encoding(true),
    field = GenericField::new("root", GenericDataType::Dictionary, true)
        .with_child(GenericField::new("key", GenericDataType::U32, false))
        .with_child(GenericField::new("value", GenericDataType::LargeUtf8, false)),
    ty = Option<String>,
    values = [Some(String::from("a")), None, Some(String::from("a"))],
    nulls = [false, true, false],
);

test_example!(
    test_name = string_dict_u8,
    test_deserialization = [],
    tracing_options = TracingOptions::default().string_dictionary_encoding(true),
    field = GenericField::new("root", GenericDataType::Dictionary, false)
        .with_child(GenericField::new("key", GenericDataType::U32, false))
        .with_child(GenericField::new(
            "value",
            GenericDataType::LargeUtf8,
            false
        )),
    overwrite_field = GenericField::new("root", GenericDataType::Dictionary, false)
        .with_child(GenericField::new("key", GenericDataType::U8, false))
        .with_child(GenericField::new(
            "value",
            GenericDataType::LargeUtf8,
            false
        )),
    ty = String,
    values = [String::from("a"), String::from("b"), String::from("a")],
    nulls = [false, false, false],
);

test_example!(
    test_name = string_dict_nullable_u8,
    test_deserialization = [],
    tracing_options = TracingOptions::default().string_dictionary_encoding(true),
    field = GenericField::new("root", GenericDataType::Dictionary, true)
        .with_child(GenericField::new("key", GenericDataType::U32, false))
        .with_child(GenericField::new("value", GenericDataType::LargeUtf8, false)),
    overwrite_field = GenericField::new("root", GenericDataType::Dictionary, true)
        .with_child(GenericField::new("key", GenericDataType::U8, false))
        .with_child(GenericField::new("value", GenericDataType::LargeUtf8, false)),
    ty = Option<String>,
    values = [Some(String::from("a")), None, Some(String::from("a"))],
    nulls = [false, true, false],
);

test_example!(
    test_name = string_dict_u16,
    test_deserialization = [],
    tracing_options = TracingOptions::default().string_dictionary_encoding(true),
    field = GenericField::new("root", GenericDataType::Dictionary, false)
        .with_child(GenericField::new("key", GenericDataType::U32, false))
        .with_child(GenericField::new(
            "value",
            GenericDataType::LargeUtf8,
            false
        )),
    overwrite_field = GenericField::new("root", GenericDataType::Dictionary, false)
        .with_child(GenericField::new("key", GenericDataType::U16, false))
        .with_child(GenericField::new(
            "value",
            GenericDataType::LargeUtf8,
            false
        )),
    ty = String,
    values = [String::from("a"), String::from("b"), String::from("a")],
    nulls = [false, false, false],
);

test_example!(
    test_name = string_dict_nullable_u16,
    test_deserialization = [],
    tracing_options = TracingOptions::default().string_dictionary_encoding(true),
    field = GenericField::new("root", GenericDataType::Dictionary, true)
        .with_child(GenericField::new("key", GenericDataType::U32, false))
        .with_child(GenericField::new("value", GenericDataType::LargeUtf8, false)),
    overwrite_field = GenericField::new("root", GenericDataType::Dictionary, true)
        .with_child(GenericField::new("key", GenericDataType::U16, false))
        .with_child(GenericField::new("value", GenericDataType::LargeUtf8, false)),
    ty = Option<String>,
    values = [Some(String::from("a")), None, Some(String::from("a"))],
    nulls = [false, true, false],
);

test_example!(
    test_name = string_dict_u64,
    test_deserialization = [],
    tracing_options = TracingOptions::default().string_dictionary_encoding(true),
    field = GenericField::new("root", GenericDataType::Dictionary, false)
        .with_child(GenericField::new("key", GenericDataType::U32, false))
        .with_child(GenericField::new(
            "value",
            GenericDataType::LargeUtf8,
            false
        )),
    overwrite_field = GenericField::new("root", GenericDataType::Dictionary, false)
        .with_child(GenericField::new("key", GenericDataType::U64, false))
        .with_child(GenericField::new(
            "value",
            GenericDataType::LargeUtf8,
            false
        )),
    ty = String,
    values = [String::from("a"), String::from("b"), String::from("a")],
    nulls = [false, false, false],
);

test_example!(
    test_name = string_dict_nullable_u64,
    test_deserialization = [],
    tracing_options = TracingOptions::default().string_dictionary_encoding(true),
    field = GenericField::new("root", GenericDataType::Dictionary, true)
        .with_child(GenericField::new("key", GenericDataType::U32, false))
        .with_child(GenericField::new("value", GenericDataType::LargeUtf8, false)),
    overwrite_field = GenericField::new("root", GenericDataType::Dictionary, true)
        .with_child(GenericField::new("key", GenericDataType::U64, false))
        .with_child(GenericField::new("value", GenericDataType::LargeUtf8, false)),
    ty = Option<String>,
    values = [Some(String::from("a")), None, Some(String::from("a"))],
    nulls = [false, true, false],
);

test_example!(
    test_name = string_dict_i32,
    test_deserialization = [],
    tracing_options = TracingOptions::default().string_dictionary_encoding(true),
    field = GenericField::new("root", GenericDataType::Dictionary, false)
        .with_child(GenericField::new("key", GenericDataType::U32, false))
        .with_child(GenericField::new(
            "value",
            GenericDataType::LargeUtf8,
            false
        )),
    overwrite_field = GenericField::new("root", GenericDataType::Dictionary, false)
        .with_child(GenericField::new("key", GenericDataType::I32, false))
        .with_child(GenericField::new(
            "value",
            GenericDataType::LargeUtf8,
            false
        )),
    ty = String,
    values = [String::from("a"), String::from("b"), String::from("a")],
    nulls = [false, false, false],
);

test_example!(
    test_name = string_dict_nullable_i32,
    test_deserialization = [],
    tracing_options = TracingOptions::default().string_dictionary_encoding(true),
    field = GenericField::new("root", GenericDataType::Dictionary, true)
        .with_child(GenericField::new("key", GenericDataType::U32, false))
        .with_child(GenericField::new("value", GenericDataType::LargeUtf8, false)),
        overwrite_field = GenericField::new("root", GenericDataType::Dictionary, true)
        .with_child(GenericField::new("key", GenericDataType::I32, false))
        .with_child(GenericField::new("value", GenericDataType::LargeUtf8, false)),
    ty = Option<String>,
    values = [Some(String::from("a")), None, Some(String::from("a"))],
    nulls = [false, true, false],
);

test_example!(
    test_name = string_dict_i8,
    test_deserialization = [],
    tracing_options = TracingOptions::default().string_dictionary_encoding(true),
    field = GenericField::new("root", GenericDataType::Dictionary, false)
        .with_child(GenericField::new("key", GenericDataType::U32, false))
        .with_child(GenericField::new(
            "value",
            GenericDataType::LargeUtf8,
            false
        )),
    overwrite_field = GenericField::new("root", GenericDataType::Dictionary, false)
        .with_child(GenericField::new("key", GenericDataType::I8, false))
        .with_child(GenericField::new(
            "value",
            GenericDataType::LargeUtf8,
            false
        )),
    ty = String,
    values = [String::from("a"), String::from("b"), String::from("a")],
    nulls = [false, false, false],
);

test_example!(
    test_name = string_dict_nullable_i8,
    test_deserialization = [],
    tracing_options = TracingOptions::default().string_dictionary_encoding(true),
    field = GenericField::new("root", GenericDataType::Dictionary, true)
        .with_child(GenericField::new("key", GenericDataType::U32, false))
        .with_child(GenericField::new("value", GenericDataType::LargeUtf8, false)),
    overwrite_field = GenericField::new("root", GenericDataType::Dictionary, true)
        .with_child(GenericField::new("key", GenericDataType::I8, false))
        .with_child(GenericField::new("value", GenericDataType::LargeUtf8, false)),
    ty = Option<String>,
    values = [Some(String::from("a")), None, Some(String::from("a"))],
    nulls = [false, true, false],
);

test_example!(
    test_name = string_dict_i16,
    test_deserialization = [],
    tracing_options = TracingOptions::default().string_dictionary_encoding(true),
    field = GenericField::new("root", GenericDataType::Dictionary, false)
        .with_child(GenericField::new("key", GenericDataType::U32, false))
        .with_child(GenericField::new(
            "value",
            GenericDataType::LargeUtf8,
            false
        )),
    overwrite_field = GenericField::new("root", GenericDataType::Dictionary, false)
        .with_child(GenericField::new("key", GenericDataType::I16, false))
        .with_child(GenericField::new(
            "value",
            GenericDataType::LargeUtf8,
            false
        )),
    ty = String,
    values = [String::from("a"), String::from("b"), String::from("a")],
    nulls = [false, false, false],
);

test_example!(
    test_name = string_dict_nullable_i16,
    test_deserialization = [],
    tracing_options = TracingOptions::default().string_dictionary_encoding(true),
    field = GenericField::new("root", GenericDataType::Dictionary, true)
        .with_child(GenericField::new("key", GenericDataType::U32, false))
        .with_child(GenericField::new("value", GenericDataType::LargeUtf8, false)),
    overwrite_field = GenericField::new("root", GenericDataType::Dictionary, true)
        .with_child(GenericField::new("key", GenericDataType::I16, false))
        .with_child(GenericField::new("value", GenericDataType::LargeUtf8, false)),
    ty = Option<String>,
    values = [Some(String::from("a")), None, Some(String::from("a"))],
    nulls = [false, true, false],
);

test_example!(
    test_name = string_dict_i64,
    test_deserialization = [],
    tracing_options = TracingOptions::default().string_dictionary_encoding(true),
    field = GenericField::new("root", GenericDataType::Dictionary, false)
        .with_child(GenericField::new("key", GenericDataType::U32, false))
        .with_child(GenericField::new(
            "value",
            GenericDataType::LargeUtf8,
            false
        )),
    overwrite_field = GenericField::new("root", GenericDataType::Dictionary, false)
        .with_child(GenericField::new("key", GenericDataType::I64, false))
        .with_child(GenericField::new(
            "value",
            GenericDataType::LargeUtf8,
            false
        )),
    ty = String,
    values = [String::from("a"), String::from("b"), String::from("a")],
    nulls = [false, false, false],
);

test_example!(
    test_name = string_dict_nullable_i64,
    test_deserialization = [],
    tracing_options = TracingOptions::default().string_dictionary_encoding(true),
    field = GenericField::new("root", GenericDataType::Dictionary, true)
        .with_child(GenericField::new("key", GenericDataType::U32, false))
        .with_child(GenericField::new("value", GenericDataType::LargeUtf8, false)),
    overwrite_field = GenericField::new("root", GenericDataType::Dictionary, true)
        .with_child(GenericField::new("key", GenericDataType::I64, false))
        .with_child(GenericField::new("value", GenericDataType::LargeUtf8, false)),
    ty = Option<String>,
    values = [Some(String::from("a")), None, Some(String::from("a"))],
    nulls = [false, true, false],
);

test_example!(
    test_name = string_dict_u32_utf8,
    test_deserialization = [],
    tracing_options = TracingOptions::default().string_dictionary_encoding(true),
    field = GenericField::new("root", GenericDataType::Dictionary, false)
        .with_child(GenericField::new("key", GenericDataType::U32, false))
        .with_child(GenericField::new(
            "value",
            GenericDataType::LargeUtf8,
            false
        )),
    overwrite_field = GenericField::new("root", GenericDataType::Dictionary, false)
        .with_child(GenericField::new("key", GenericDataType::U32, false))
        .with_child(GenericField::new("value", GenericDataType::Utf8, false)),
    ty = String,
    values = [String::from("a"), String::from("b"), String::from("a")],
    nulls = [false, false, false],
);

test_example!(
    test_name = string_dict_nullable_u32_utf8,
    test_deserialization = [],
    tracing_options = TracingOptions::default().string_dictionary_encoding(true),
    field = GenericField::new("root", GenericDataType::Dictionary, true)
        .with_child(GenericField::new("key", GenericDataType::U32, false))
        .with_child(GenericField::new("value", GenericDataType::LargeUtf8, false)),
    overwrite_field = GenericField::new("root", GenericDataType::Dictionary, true)
        .with_child(GenericField::new("key", GenericDataType::U32, false))
        .with_child(GenericField::new("value", GenericDataType::Utf8, false)),
    ty = Option<String>,
    values = [Some(String::from("a")), None, Some(String::from("a"))],
    nulls = [false, true, false],
);

test_example!(
    test_name = string_dict_u8_utf8,
    test_deserialization = [],
    tracing_options = TracingOptions::default().string_dictionary_encoding(true),
    field = GenericField::new("root", GenericDataType::Dictionary, false)
        .with_child(GenericField::new("key", GenericDataType::U32, false))
        .with_child(GenericField::new(
            "value",
            GenericDataType::LargeUtf8,
            false
        )),
    overwrite_field = GenericField::new("root", GenericDataType::Dictionary, false)
        .with_child(GenericField::new("key", GenericDataType::U8, false))
        .with_child(GenericField::new("value", GenericDataType::Utf8, false)),
    ty = String,
    values = [String::from("a"), String::from("b"), String::from("a")],
    nulls = [false, false, false],
);

test_example!(
    test_name = string_dict_nullable_u8_utf8,
    test_deserialization = [],
    tracing_options = TracingOptions::default().string_dictionary_encoding(true),
    field = GenericField::new("root", GenericDataType::Dictionary, true)
        .with_child(GenericField::new("key", GenericDataType::U32, false))
        .with_child(GenericField::new("value", GenericDataType::LargeUtf8, false)),
    overwrite_field = GenericField::new("root", GenericDataType::Dictionary, true)
        .with_child(GenericField::new("key", GenericDataType::U8, false))
        .with_child(GenericField::new("value", GenericDataType::Utf8, false)),
    ty = Option<String>,
    values = [Some(String::from("a")), None, Some(String::from("a"))],
    nulls = [false, true, false],
);

test_example!(
    test_name = string_dict_u16_utf8,
    test_deserialization = [],
    tracing_options = TracingOptions::default().string_dictionary_encoding(true),
    field = GenericField::new("root", GenericDataType::Dictionary, false)
        .with_child(GenericField::new("key", GenericDataType::U32, false))
        .with_child(GenericField::new(
            "value",
            GenericDataType::LargeUtf8,
            false
        )),
    overwrite_field = GenericField::new("root", GenericDataType::Dictionary, false)
        .with_child(GenericField::new("key", GenericDataType::U16, false))
        .with_child(GenericField::new("value", GenericDataType::Utf8, false)),
    ty = String,
    values = [String::from("a"), String::from("b"), String::from("a")],
    nulls = [false, false, false],
);

test_example!(
    test_name = string_dict_nullable_u16_utf8,
    test_deserialization = [],
    tracing_options = TracingOptions::default().string_dictionary_encoding(true),
    field = GenericField::new("root", GenericDataType::Dictionary, true)
        .with_child(GenericField::new("key", GenericDataType::U32, false))
        .with_child(GenericField::new("value", GenericDataType::LargeUtf8, false)),
    overwrite_field = GenericField::new("root", GenericDataType::Dictionary, true)
        .with_child(GenericField::new("key", GenericDataType::U16, false))
        .with_child(GenericField::new("value", GenericDataType::Utf8, false)),
    ty = Option<String>,
    values = [Some(String::from("a")), None, Some(String::from("a"))],
    nulls = [false, true, false],
);

test_example!(
    test_name = string_dict_u64_utf8,
    test_deserialization = [],
    tracing_options = TracingOptions::default().string_dictionary_encoding(true),
    field = GenericField::new("root", GenericDataType::Dictionary, false)
        .with_child(GenericField::new("key", GenericDataType::U32, false))
        .with_child(GenericField::new(
            "value",
            GenericDataType::LargeUtf8,
            false
        )),
    overwrite_field = GenericField::new("root", GenericDataType::Dictionary, false)
        .with_child(GenericField::new("key", GenericDataType::U64, false))
        .with_child(GenericField::new("value", GenericDataType::Utf8, false)),
    ty = String,
    values = [String::from("a"), String::from("b"), String::from("a")],
    nulls = [false, false, false],
);

test_example!(
    test_name = string_dict_nullable_u64_utf8,
    test_deserialization = [],
    tracing_options = TracingOptions::default().string_dictionary_encoding(true),
    field = GenericField::new("root", GenericDataType::Dictionary, true)
        .with_child(GenericField::new("key", GenericDataType::U32, false))
        .with_child(GenericField::new("value", GenericDataType::LargeUtf8, false)),
    overwrite_field = GenericField::new("root", GenericDataType::Dictionary, true)
        .with_child(GenericField::new("key", GenericDataType::U64, false))
        .with_child(GenericField::new("value", GenericDataType::Utf8, false)),
    ty = Option<String>,
    values = [Some(String::from("a")), None, Some(String::from("a"))],
    nulls = [false, true, false],
);

test_example!(
    test_name = string_dict_i32_utf8,
    test_deserialization = [],
    tracing_options = TracingOptions::default().string_dictionary_encoding(true),
    field = GenericField::new("root", GenericDataType::Dictionary, false)
        .with_child(GenericField::new("key", GenericDataType::U32, false))
        .with_child(GenericField::new(
            "value",
            GenericDataType::LargeUtf8,
            false
        )),
    overwrite_field = GenericField::new("root", GenericDataType::Dictionary, false)
        .with_child(GenericField::new("key", GenericDataType::I32, false))
        .with_child(GenericField::new("value", GenericDataType::Utf8, false)),
    ty = String,
    values = [String::from("a"), String::from("b"), String::from("a")],
    nulls = [false, false, false],
);

test_example!(
    test_name = string_dict_nullable_i32_utf8,
    test_deserialization = [],
    tracing_options = TracingOptions::default().string_dictionary_encoding(true),
    field = GenericField::new("root", GenericDataType::Dictionary, true)
        .with_child(GenericField::new("key", GenericDataType::U32, false))
        .with_child(GenericField::new("value", GenericDataType::LargeUtf8, false)),
        overwrite_field = GenericField::new("root", GenericDataType::Dictionary, true)
        .with_child(GenericField::new("key", GenericDataType::I32, false))
        .with_child(GenericField::new("value", GenericDataType::Utf8, false)),
    ty = Option<String>,
    values = [Some(String::from("a")), None, Some(String::from("a"))],
    nulls = [false, true, false],
);

test_example!(
    test_name = string_dict_i8_utf8,
    test_deserialization = [],
    tracing_options = TracingOptions::default().string_dictionary_encoding(true),
    field = GenericField::new("root", GenericDataType::Dictionary, false)
        .with_child(GenericField::new("key", GenericDataType::U32, false))
        .with_child(GenericField::new(
            "value",
            GenericDataType::LargeUtf8,
            false
        )),
    overwrite_field = GenericField::new("root", GenericDataType::Dictionary, false)
        .with_child(GenericField::new("key", GenericDataType::I8, false))
        .with_child(GenericField::new("value", GenericDataType::Utf8, false)),
    ty = String,
    values = [String::from("a"), String::from("b"), String::from("a")],
    nulls = [false, false, false],
);

test_example!(
    test_name = string_dict_nullable_i8_utf8,
    test_deserialization = [],
    tracing_options = TracingOptions::default().string_dictionary_encoding(true),
    field = GenericField::new("root", GenericDataType::Dictionary, true)
        .with_child(GenericField::new("key", GenericDataType::U32, false))
        .with_child(GenericField::new("value", GenericDataType::LargeUtf8, false)),
    overwrite_field = GenericField::new("root", GenericDataType::Dictionary, true)
        .with_child(GenericField::new("key", GenericDataType::I8, false))
        .with_child(GenericField::new("value", GenericDataType::Utf8, false)),
    ty = Option<String>,
    values = [Some(String::from("a")), None, Some(String::from("a"))],
    nulls = [false, true, false],
);

test_example!(
    test_name = string_dict_i16_utf8,
    test_deserialization = [],
    tracing_options = TracingOptions::default().string_dictionary_encoding(true),
    field = GenericField::new("root", GenericDataType::Dictionary, false)
        .with_child(GenericField::new("key", GenericDataType::U32, false))
        .with_child(GenericField::new(
            "value",
            GenericDataType::LargeUtf8,
            false
        )),
    overwrite_field = GenericField::new("root", GenericDataType::Dictionary, false)
        .with_child(GenericField::new("key", GenericDataType::I16, false))
        .with_child(GenericField::new("value", GenericDataType::Utf8, false)),
    ty = String,
    values = [String::from("a"), String::from("b"), String::from("a")],
    nulls = [false, false, false],
);

test_example!(
    test_name = string_dict_nullable_i16_utf8,
    test_deserialization = [],
    tracing_options = TracingOptions::default().string_dictionary_encoding(true),
    field = GenericField::new("root", GenericDataType::Dictionary, true)
        .with_child(GenericField::new("key", GenericDataType::U32, false))
        .with_child(GenericField::new("value", GenericDataType::LargeUtf8, false)),
    overwrite_field = GenericField::new("root", GenericDataType::Dictionary, true)
        .with_child(GenericField::new("key", GenericDataType::I16, false))
        .with_child(GenericField::new("value", GenericDataType::Utf8, false)),
    ty = Option<String>,
    values = [Some(String::from("a")), None, Some(String::from("a"))],
    nulls = [false, true, false],
);

test_example!(
    test_name = string_dict_i64_utf8,
    test_deserialization = [],
    tracing_options = TracingOptions::default().string_dictionary_encoding(true),
    field = GenericField::new("root", GenericDataType::Dictionary, false)
        .with_child(GenericField::new("key", GenericDataType::U32, false))
        .with_child(GenericField::new(
            "value",
            GenericDataType::LargeUtf8,
            false
        )),
    overwrite_field = GenericField::new("root", GenericDataType::Dictionary, false)
        .with_child(GenericField::new("key", GenericDataType::I64, false))
        .with_child(GenericField::new("value", GenericDataType::Utf8, false)),
    ty = String,
    values = [String::from("a"), String::from("b"), String::from("a")],
    nulls = [false, false, false],
);

test_example!(
    test_name = string_dict_nullable_i64_utf8,
    test_deserialization = [],
    tracing_options = TracingOptions::default().string_dictionary_encoding(true),
    field = GenericField::new("root", GenericDataType::Dictionary, true)
        .with_child(GenericField::new("key", GenericDataType::U32, false))
        .with_child(GenericField::new("value", GenericDataType::LargeUtf8, false)),
    overwrite_field = GenericField::new("root", GenericDataType::Dictionary, true)
        .with_child(GenericField::new("key", GenericDataType::I64, false))
        .with_child(GenericField::new("value", GenericDataType::Utf8, false)),
    ty = Option<String>,
    values = [Some(String::from("a")), None, Some(String::from("a"))],
    nulls = [false, true, false],
);
