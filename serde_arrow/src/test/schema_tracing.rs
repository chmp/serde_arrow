use serde_json::json;

use crate::internal::{
    error::PanicOnError,
    schema::{SchemaLike, SerdeArrowSchema, TracingOptions},
};

/// A mixture of nulls and strings is parsed as a nullable LargeUtf8
///
/// see https://github.com/chmp/serde_arrow/issues/216
mod json_utf8_null {
    use super::*;

    macro_rules! test {
        ($name:ident, $($data:tt)*) => {
            #[test]
            fn $name() -> PanicOnError<()> {
                let expected = SerdeArrowSchema::from_value(&json!([
                    {"name": "flavor", "data_type": "LargeUtf8", "nullable": true},
                ]))?;

                let data = json!($($data)*);
                let actual = SerdeArrowSchema::from_samples(&data, TracingOptions::default())?;
                assert_eq!(actual, expected);
                Ok(())
            }
        };
    }

    test!(str_null, [{"flavor": "delicious"}, {"flavor": null}]);
    test!(null_str, [{"flavor": null}, {"flavor": "delicious"}]);
}

/// A mixture of negative, positive ints and nulls is traced as a nullable i64
mod json_i64_null {
    use super::*;

    macro_rules! test {
        ($name:ident, $($data:tt)*) => {
            #[test]
            fn $name() -> PanicOnError<()> {
                let expected = SerdeArrowSchema::from_value(&json!([
                    {"name": "value", "data_type": "I64", "nullable": true},
                ]))?;

                let data = json!($($data)*);
                let actual = SerdeArrowSchema::from_samples(&data, TracingOptions::default().coerce_numbers(true))?;
                assert_eq!(actual, expected);
                Ok(())
            }
        };
    }

    test!(pos_null_neg, [{"value": 42}, {"value": null}, {"value": -13}]);
    test!(pos_neg_null, [{"value": 42}, {"value": -13}, {"value": null}]);
    test!(null_neg_pos, [{"value": null}, {"value": -13}, {"value": 42}]);
    test!(null_pos_neg, [{"value": null}, {"value": 42}, {"value": -13}]);
    test!(neg_null_pos, [{"value": -13}, {"value": null}, {"value": 42}]);
    test!(neg_pos_null, [{"value": -13}, {"value": 42}, {"value": null}]);
}

/// Positive ints and nulls are traced as a nullable u64
mod json_u64_null {
    use super::*;

    macro_rules! test {
        ($name:ident, $($data:tt)*) => {
            #[test]
            fn $name() -> PanicOnError<()> {
                let expected = SerdeArrowSchema::from_value(&json!([
                    {"name": "value", "data_type": "U64", "nullable": true},
                ]))?;

                let data = json!($($data)*);
                let actual = SerdeArrowSchema::from_samples(&data, TracingOptions::default())?;
                assert_eq!(actual, expected);
                Ok(())
            }
        };
    }

    test!(pos_null, [{"value": 42}, {"value": null}]);
    test!(null_pos, [{"value": null}, {"value": 42}]);
}

/// Bools and nulls are traced as a nullable Bool
mod json_bool_null {
    use super::*;

    macro_rules! test {
        ($name:ident, $($data:tt)*) => {
            #[test]
            fn $name() -> PanicOnError<()> {
                let expected = SerdeArrowSchema::from_value(&json!([
                    {"name": "value", "data_type": "Bool", "nullable": true},
                ]))?;

                let data = json!($($data)*);
                let actual = SerdeArrowSchema::from_samples(&data, TracingOptions::default())?;
                assert_eq!(actual, expected);
                Ok(())
            }
        };
    }

    test!(pos_null, [{"value": true}, {"value": null}]);
    test!(null_pos, [{"value": null}, {"value": false}]);
}

/// Structs and nulls are traced as nullable structs
mod json_struct_null {
    use super::*;

    macro_rules! test {
        ($name:ident, $($data:tt)*) => {
            #[test]
            fn $name() -> PanicOnError<()> {
                let expected = SerdeArrowSchema::from_value(&json!([
                    {
                        "name": "value",
                        "data_type": "Struct",
                        "nullable": true,
                        "strategy": "MapAsStruct",
                        "children": [
                            {"name": "child", "data_type": "U64"},
                        ]
                    },
                ]))?;

                let data = json!($($data)*);
                let actual = SerdeArrowSchema::from_samples(&data, TracingOptions::default())?;
                assert_eq!(actual, expected);
                Ok(())
            }
        };
    }

    test!(struct_null, [{"value": {"child": 13}}, {"value": null}]);
    test!(null_struct, [{"value": null}, {"value": {"child": 13}}]);
}

/// Lists and nulls are traced as nullable lists
mod json_list_null {
    use super::*;

    macro_rules! test {
        ($name:ident, $($data:tt)*) => {
            #[test]
            fn $name() -> PanicOnError<()> {
                let expected = SerdeArrowSchema::from_value(&json!([
                    {
                        "name": "value",
                        "data_type": "LargeList",
                        "nullable": true,
                        "children": [
                            {"name": "element", "data_type": "U64"},
                        ]
                    },
                ]))?;

                let data = json!($($data)*);
                let actual = SerdeArrowSchema::from_samples(&data, TracingOptions::default())?;
                assert_eq!(actual, expected);
                Ok(())
            }
        };
    }

    test!(list_null, [{"value": [13]}, {"value": null}]);
    test!(null_list, [{"value": null}, {"value": [13]}]);
}

/// Strings encoding dates and nulls are traced as nullable Date64 fields
mod json_timestamp_naive_null {
    use super::*;

    macro_rules! test {
        ($name:ident, $($data:tt)*) => {
            #[test]
            fn $name() -> PanicOnError<()> {
                let expected = SerdeArrowSchema::from_value(&json!([
                    {
                        "name": "date",
                        "data_type": "Timestamp(Millisecond, None)",
                        "nullable": true,
                    },
                ]))?;

                let data = json!($($data)*);
                let actual = SerdeArrowSchema::from_samples(&data, TracingOptions::default().guess_dates(true))?;
                assert_eq!(actual, expected);
                Ok(())
            }
        };
    }

    test!(str_null, [{"date": "2024-08-09T12:15:00"}, {"date": null}]);
    test!(null_str, [{"date": null}, {"date": "2024-08-09T12:15:00"}]);
}

/// Strings encoding dates and nulls are traced as nullable Date64 fields
mod json_timestamp_utc_null {
    use super::*;

    macro_rules! test {
        ($name:ident, $($data:tt)*) => {
            #[test]
            fn $name() -> PanicOnError<()> {
                let expected = SerdeArrowSchema::from_value(&json!([
                    {
                        "name": "date",
                        "data_type": "Timestamp(Millisecond, Some(\"UTC\"))",
                        "nullable": true,
                    },
                ]))?;

                let data = json!($($data)*);
                let actual = SerdeArrowSchema::from_samples(&data, TracingOptions::default().guess_dates(true))?;
                assert_eq!(actual, expected);
                Ok(())
            }
        };
    }

    test!(str_null, [{"date": "2024-08-09T12:15:00Z"}, {"date": null}]);
    test!(null_str, [{"date": null}, {"date": "2024-08-09T12:15:00Z"}]);
}

/// Mixing different date formats or dates and non-dates, results in Strings
mod json_date64_to_string_coercions {
    macro_rules! test {
        ($name:ident, $($data:tt)*) => {
            mod $name {
                use super::super::*;

                #[test]
                fn large_utf8() -> PanicOnError<()> {
                    let expected = SerdeArrowSchema::from_value(&json!([
                       {
                          "name": "date",
                          "data_type": "LargeUtf8",
                          "nullable": true,
                      },
                  ]))?;

                  let data = json!($($data)*);
                  let actual = SerdeArrowSchema::from_samples(&data, TracingOptions::default().guess_dates(true))?;
                  assert_eq!(actual, expected);
                  Ok(())
              }
              #[test]
                fn utf8() -> PanicOnError<()> {
                    let expected = SerdeArrowSchema::from_value(&json!([
                       {
                          "name": "date",
                          "data_type": "Utf8",
                          "nullable": true,
                      },
                  ]))?;

                  let data = json!($($data)*);
                  let actual = SerdeArrowSchema::from_samples(&data, TracingOptions::default().guess_dates(true).strings_as_large_utf8(false))?;
                  assert_eq!(actual, expected);
                  Ok(())
              }
            }
        };
    }

    test!(utc_naive_null, [{"date": "2024-08-09T12:15:00Z"}, {"date": "2024-08-09T12:15:00"}, {"date": null}]);
    test!(utc_null_naive, [{"date": "2024-08-09T12:15:00Z"}, {"date": null}, {"date": "2024-08-09T12:15:00"}]);
    test!(naive_utc_null, [{"date": "2024-08-09T12:15:00"}, {"date": "2024-08-09T12:15:00Z"}, {"date": null}]);
    test!(naive_null_utc, [{"date": "2024-08-09T12:15:00"}, {"date": null}, {"date": "2024-08-09T12:15:00Z"}]);
    test!(null_naive_utc, [{"date": null}, {"date": "2024-08-09T12:15:00"}, {"date": "2024-08-09T12:15:00Z"}]);
    test!(null_utc_naive, [{"date": null}, {"date": "2024-08-09T12:15:00Z"}, {"date": "2024-08-09T12:15:00"}]);

    test!(utc_str_null, [{"date": "2024-08-09T12:15:00Z"}, {"date": "foo"}, {"date": null}]);
    test!(utc_null_str, [{"date": "2024-08-09T12:15:00Z"}, {"date": null}, {"date": "foo"}]);
    test!(str_utc_null, [{"date": "bar"}, {"date": "2024-08-09T12:15:00Z"}, {"date": null}]);
    test!(str_null_utc, [{"date": "bar"}, {"date": null}, {"date": "2024-08-09T12:15:00Z"}]);
    test!(null_str_utc, [{"date": null}, {"date": "baz"}, {"date": "2024-08-09T12:15:00Z"}]);
    test!(null_utc_str, [{"date": null}, {"date": "2024-08-09T12:15:00Z"}, {"date": "baz"}]);

    test!(naive_str_null, [{"date": "2024-08-09T12:15:00"}, {"date": "foo"}, {"date": null}]);
    test!(naive_null_str, [{"date": "2024-08-09T12:15:00"}, {"date": null}, {"date": "foo"}]);
    test!(str_naive_null, [{"date": "bar"}, {"date": "2024-08-09T12:15:00"}, {"date": null}]);
    test!(str_null_naive, [{"date": "bar"}, {"date": null}, {"date": "2024-08-09T12:15:00"}]);
    test!(null_str_naive, [{"date": null}, {"date": "baz"}, {"date": "2024-08-09T12:15:00"}]);
    test!(null_naive_str, [{"date": null}, {"date": "2024-08-09T12:15:00"}, {"date": "baz"}]);
}

/// Test how different number and null types are coerced (using untagged enums)
mod untagged_enum_number_coercion {
    use serde::Serialize;

    use super::*;

    #[derive(Debug, Serialize)]
    #[serde(untagged)]
    enum Num {
        Null(()),
        I8(i8),
        I16(i16),
        I32(i32),
        I64(i64),
        U8(u8),
        U16(u16),
        U32(u32),
        U64(u64),
        F32(f32),
        F64(f64),
    }

    macro_rules! test_impl {
        ($name:ident, $data_type:expr, $nullable:expr, $($data:tt)*) => {
            #[test]
            fn $name() -> PanicOnError<()> {
                let expected = SerdeArrowSchema::from_value(&json!([
                    {
                        "name": "0",
                        "data_type": $data_type,
                        "nullable": $nullable,
                    },
                ]))?;

                let data = $($data)*;
                let actual = SerdeArrowSchema::from_samples(&data, TracingOptions::default().coerce_numbers(true))?;
                assert_eq!(actual, expected);
                Ok(())
            }
        };
    }

    macro_rules! test {
        ($name:ident, $data_type:expr, $nullable:expr, [$a:expr, $b:expr]) => {
            mod $name {
                use super::*;

                test_impl!(ab, $data_type, $nullable, [$a, $b]);
                test_impl!(ba, $data_type, $nullable, [$b, $a]);
            }
        };
        ($name:ident, $data_type:expr, $nullable:expr, [$a:expr, $b:expr, $c:expr]) => {
            mod $name {
                use super::*;

                test_impl!(abc, $data_type, $nullable, [$a, $b, $c]);
                test_impl!(acb, $data_type, $nullable, [$a, $c, $b]);
                test_impl!(bac, $data_type, $nullable, [$b, $a, $c]);
                test_impl!(bca, $data_type, $nullable, [$b, $c, $a]);
                test_impl!(cab, $data_type, $nullable, [$c, $a, $b]);
                test_impl!(cba, $data_type, $nullable, [$c, $b, $a]);
            }
        };
    }

    test!(i32_i32_undecorated, "I32", false, [(0_i32,), (0_i32,)]);
    test!(i8_i8, "I8", false, [(Num::I8(0),), (Num::I8(0),)]);
    test!(i16_i16, "I16", false, [(Num::I16(0),), (Num::I16(0),)]);
    test!(i32_i32, "I32", false, [(Num::I32(0),), (Num::I32(0),)]);
    test!(i64_i64, "I64", false, [(Num::I64(0),), (Num::I64(0),)]);
    test!(u8_u8, "U8", false, [(Num::U8(0),), (Num::U8(0),)]);
    test!(u16_u16, "U16", false, [(Num::U16(0),), (Num::U16(0),)]);
    test!(u32_u32, "U32", false, [(Num::U32(0),), (Num::U32(0),)]);
    test!(u64_u64, "U64", false, [(Num::U64(0),), (Num::U64(0),)]);
    test!(f32_f32, "F32", false, [(Num::F32(0.0),), (Num::F32(0.0),)]);
    test!(f64_f64, "F64", false, [(Num::F64(0.0),), (Num::F64(0.0),)]);

    // _, null -> nullable
    test!(i8_null, "I8", true, [(Num::I8(0),), (Num::Null(()),)]);
    test!(i16_null, "I16", true, [(Num::I16(0),), (Num::Null(()),)]);
    test!(i32_null, "I32", true, [(Num::I32(0),), (Num::Null(()),)]);
    test!(i64_null, "I64", true, [(Num::I64(0),), (Num::Null(()),)]);
    test!(u8_null, "U8", true, [(Num::U8(0),), (Num::Null(()),)]);
    test!(u16_null, "U16", true, [(Num::U16(0),), (Num::Null(()),)]);
    test!(u32_null, "U32", true, [(Num::U32(0),), (Num::Null(()),)]);
    test!(u64_null, "U64", true, [(Num::U64(0),), (Num::Null(()),)]);
    test!(f32_null, "F32", true, [(Num::F32(0.0),), (Num::Null(()),)]);
    test!(f64_null, "F64", true, [(Num::F64(0.0),), (Num::Null(()),)]);

    // unsigned, unsigned -> u64
    test!(u8_u16, "U64", false, [(Num::U8(0),), (Num::U16(0),)]);
    test!(u8_u32, "U64", false, [(Num::U8(0),), (Num::U32(0),)]);
    test!(u8_u64, "U64", false, [(Num::U8(0),), (Num::U64(0),)]);
    test!(u16_u32, "U64", false, [(Num::U16(0),), (Num::U32(0),)]);
    test!(u16_u64, "U64", false, [(Num::U16(0),), (Num::U64(0),)]);
    test!(u32_u64, "U64", false, [(Num::U32(0),), (Num::U64(0),)]);

    // signed,signed -> i64
    test!(i8_i16, "I64", false, [(Num::I8(0),), (Num::I16(0),)]);
    test!(i8_i32, "I64", false, [(Num::I8(0),), (Num::I32(0),)]);
    test!(i8_i64, "I64", false, [(Num::I8(0),), (Num::I64(0),)]);
    test!(i16_i32, "I64", false, [(Num::I16(0),), (Num::I32(0),)]);
    test!(i16_i64, "I64", false, [(Num::I16(0),), (Num::I64(0),)]);
    test!(i32_i64, "I64", false, [(Num::I32(0),), (Num::I64(0),)]);

    // float, float -> f64
    test!(f32_f64, "F64", false, [(Num::F32(0.0),), (Num::F64(0.0),)]);

    // float, number -> f64
    test!(f32_i8, "F64", false, [(Num::F32(0.0),), (Num::I8(0),)]);
    test!(f32_i16, "F64", false, [(Num::F32(0.0),), (Num::I16(0),)]);
    test!(f32_i32, "F64", false, [(Num::F32(0.0),), (Num::I32(0),)]);
    test!(f32_i64, "F64", false, [(Num::F32(0.0),), (Num::I64(0),)]);
    test!(f32_u8, "F64", false, [(Num::F32(0.0),), (Num::U8(0),)]);
    test!(f32_u16, "F64", false, [(Num::F32(0.0),), (Num::U16(0),)]);
    test!(f32_u32, "F64", false, [(Num::F32(0.0),), (Num::U32(0),)]);
    test!(f32_u64, "F64", false, [(Num::F32(0.0),), (Num::U64(0),)]);
    test!(f64_i8, "F64", false, [(Num::F64(0.0),), (Num::I8(0),)]);
    test!(f64_i16, "F64", false, [(Num::F64(0.0),), (Num::I16(0),)]);
    test!(f64_i32, "F64", false, [(Num::F64(0.0),), (Num::I32(0),)]);
    test!(f64_i64, "F64", false, [(Num::F64(0.0),), (Num::I64(0),)]);
    test!(f64_u8, "F64", false, [(Num::F64(0.0),), (Num::U8(0),)]);
    test!(f64_u16, "F64", false, [(Num::F64(0.0),), (Num::U16(0),)]);
    test!(f64_u32, "F64", false, [(Num::F64(0.0),), (Num::U32(0),)]);
    test!(f64_u64, "F64", false, [(Num::F64(0.0),), (Num::U64(0),)]);

    // some coercion + null examples
    test!(
        f64_u8_null,
        "F64",
        true,
        [(Num::F64(0.0),), (Num::U8(0),), (Num::Null(()),)]
    );
    test!(
        i32_i64_null,
        "I64",
        true,
        [(Num::I32(0),), (Num::I64(0),), (Num::Null(()),)]
    );
    test!(
        f32_f64_null,
        "F64",
        true,
        [(Num::F32(0.0),), (Num::F64(0.0),), (Num::Null(()),)]
    );
    test!(
        u8_u16_null,
        "U64",
        true,
        [(Num::U8(0),), (Num::U16(0),), (Num::Null(()),)]
    );
}
