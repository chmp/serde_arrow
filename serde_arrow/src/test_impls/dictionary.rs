use super::macros::test_example;

test_example!(
    test_name = string_dict,
    tracing_options = TracingOptions::default().string_dictionary_encoding(true),
    field = GenericField::new("root", GenericDataType::Dictionary, false)
        .with_child(GenericField::new("key", GenericDataType::U32, false))
        .with_child(GenericField::new("value", GenericDataType::LargeUtf8, false)),
    ty = &'static str,
    values = ["a", "b", "a"],
    nulls = [false, false, false],
);

test_example!(
    test_name = string_dict_nullable,
    tracing_options = TracingOptions::default().string_dictionary_encoding(true),
    field = GenericField::new("root", GenericDataType::Dictionary, true)
        .with_child(GenericField::new("key", GenericDataType::U32, false))
        .with_child(GenericField::new("value", GenericDataType::LargeUtf8, false)),
    ty = Option<&'static str>,
    values = [Some("a"), None, Some("a")],
    nulls = [false, true, false],
);
