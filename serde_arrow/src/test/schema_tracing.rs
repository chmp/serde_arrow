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
