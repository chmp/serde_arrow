use serde_json::json;

use crate::internal::{
    error::PanicOnError,
    schema::{SchemaLike, SerdeArrowSchema, TracingOptions},
};

// see https://github.com/chmp/serde_arrow/issues/216
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

// A mixture of negative and positive ints is traced as i64
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

// Positive ints are traced as u64
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

mod json_date64_naive_null {
    use super::*;

    macro_rules! test {
        ($name:ident, $($data:tt)*) => {
            #[test]
            fn $name() -> PanicOnError<()> {
                let expected = SerdeArrowSchema::from_value(&json!([
                    {
                        "name": "date",
                        "data_type": "Date64",
                        "strategy": "NaiveStrAsDate64",
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

mod json_date64_utc_null {
    use super::*;

    macro_rules! test {
        ($name:ident, $($data:tt)*) => {
            #[test]
            fn $name() -> PanicOnError<()> {
                let expected = SerdeArrowSchema::from_value(&json!([
                    {
                        "name": "date",
                        "data_type": "Date64",
                        "strategy": "UtcStrAsDate64",
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
    use super::*;

    macro_rules! test {
        ($name:ident, $($data:tt)*) => {
            #[test]
            fn $name() -> PanicOnError<()> {
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
