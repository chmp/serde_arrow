use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use serde::Deserialize;
use serde_json::Value;

use crate::{
    internal::{
        schema::{SchemaLike, SerdeArrowSchema},
        testing::assert_error_contains,
        utils::Item,
    },
    schema::TracingOptions,
};

#[test]
fn example() {
    // NOTE: Value cannot be traced with from_type, as it is not self-describing
    let res = SerdeArrowSchema::from_type::<Item<Vec<Value>>>(TracingOptions::default());
    assert_error_contains(&res, "path: \"$.item.element\"");
    assert_error_contains(&res, "tracer_type: \"Unknown\"");
}

#[test]
fn chrono_types_are_not_self_describing() {
    let res = SerdeArrowSchema::from_type::<Item<DateTime<Utc>>>(TracingOptions::default());
    assert_error_contains(&res, "path: \"$.item\"");

    let res = SerdeArrowSchema::from_type::<Item<NaiveDateTime>>(TracingOptions::default());
    assert_error_contains(&res, "path: \"$.item\"");

    let res = SerdeArrowSchema::from_type::<Item<NaiveTime>>(TracingOptions::default());
    assert_error_contains(&res, "path: \"$.item\"");

    let res = SerdeArrowSchema::from_type::<Item<NaiveDate>>(TracingOptions::default());
    assert_error_contains(&res, "path: \"$.item\"");
}

#[test]
fn unsupported_recursive_types() {
    #[allow(unused)]
    #[derive(Deserialize)]
    struct Tree {
        left: Option<Box<Tree>>,
        right: Option<Box<Tree>>,
    }

    let res = SerdeArrowSchema::from_type::<Tree>(TracingOptions::default());
    assert_error_contains(&res, "Too deeply nested type detected");
    // NOTE: do not check the complete path, it depends on the recursion limit
    assert_error_contains(&res, "path: \"$.left.left.left.left.left.left");
}
