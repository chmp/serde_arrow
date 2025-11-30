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
    let err =
        SerdeArrowSchema::from_type::<Item<Vec<Value>>>(TracingOptions::default()).unwrap_err();
    assert_error_contains(
        &err,
        "Non self describing types cannot be traced with `from_type`.",
    );
    assert_error_contains(&err, "path: \"$.item.element\"");
    assert_error_contains(&err, "tracer_type: \"Unknown\"");
}

#[test]
fn chrono_types_are_not_self_describing() {
    let err =
        SerdeArrowSchema::from_type::<Item<DateTime<Utc>>>(TracingOptions::default()).unwrap_err();
    assert_error_contains(&err, "path: \"$.item\"");
    assert_error_contains(&err, "non self describing type");

    let err =
        SerdeArrowSchema::from_type::<Item<NaiveDateTime>>(TracingOptions::default()).unwrap_err();
    assert_error_contains(&err, "path: \"$.item\"");
    assert_error_contains(&err, "non self describing type");

    let err =
        SerdeArrowSchema::from_type::<Item<NaiveTime>>(TracingOptions::default()).unwrap_err();
    assert_error_contains(&err, "path: \"$.item\"");
    assert_error_contains(&err, "non self describing type");

    let err =
        SerdeArrowSchema::from_type::<Item<NaiveDate>>(TracingOptions::default()).unwrap_err();
    assert_error_contains(&err, "path: \"$.item\"");
    assert_error_contains(&err, "non self describing type");
}

#[test]
fn net_ip_addr_is_not_self_describing() {
    let err = SerdeArrowSchema::from_type::<Item<std::net::IpAddr>>(TracingOptions::default())
        .unwrap_err();
    assert_error_contains(&err, "path: \"$.item\"");
    assert_error_contains(&err, "non self describing type");
}

#[test]
fn unsupported_recursive_types() {
    #[allow(unused)]
    #[derive(Deserialize)]
    struct Tree {
        left: Option<Box<Tree>>,
        right: Option<Box<Tree>>,
    }

    let err = SerdeArrowSchema::from_type::<Tree>(TracingOptions::default()).unwrap_err();
    assert_error_contains(&err, "Too deeply nested type detected");
    // NOTE: do not check the complete path, it depends on the recursion limit
    assert_error_contains(&err, "path: \"$.left.left.left.left.left.left");
}
