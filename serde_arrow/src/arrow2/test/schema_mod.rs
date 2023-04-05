//! Test helpers to modify schemas

use std::collections::HashMap;

use crate::_impl::arrow2::{
    datatypes::{Field, PhysicalType},
    types::PrimitiveType,
};
use serde::Serialize;

use crate::{
    arrow2::{schema::find_field_mut, serialize_into_field},
    schema::TracingOptions,
};

fn find_physical_type(fields: &mut [Field], path: &str) -> PhysicalType {
    find_field_mut(fields, path)
        .unwrap()
        .data_type
        .to_physical_type()
}

#[test]
fn example_nested_structs() {
    #[derive(Serialize)]
    struct Outer {
        inner: Inner,
    }

    #[derive(Serialize)]
    struct Inner {
        value: u32,
    }

    let field = serialize_into_field(
        &[Outer {
            inner: Inner { value: 42 },
        }],
        "outer",
        Default::default(),
    )
    .unwrap();

    let fields = &mut [field];

    assert_eq!(find_physical_type(fields, "outer"), PhysicalType::Struct);
    assert_eq!(
        find_physical_type(fields, "outer.inner"),
        PhysicalType::Struct,
    );
    assert_eq!(
        find_physical_type(fields, "outer.inner.value"),
        PhysicalType::Primitive(PrimitiveType::UInt32),
    );
}

#[test]
fn example_nested_structs_list() {
    #[derive(Serialize)]
    struct Outer {
        inner: Vec<Inner>,
    }

    #[derive(Serialize)]
    struct Inner {
        value: u32,
    }

    let field = serialize_into_field(
        &[Outer {
            inner: vec![Inner { value: 42 }],
        }],
        "outer",
        Default::default(),
    )
    .unwrap();
    let fields = &mut [field];

    assert_eq!(find_physical_type(fields, "outer"), PhysicalType::Struct);
    assert_eq!(
        find_physical_type(fields, "outer.inner"),
        PhysicalType::LargeList,
    );
    assert_eq!(
        find_physical_type(fields, "outer.inner.value"),
        PhysicalType::Primitive(PrimitiveType::UInt32),
    );
}

#[test]
fn example_nested_structs_map() {
    #[derive(Serialize)]
    struct Outer {
        inner: HashMap<String, Inner>,
    }

    #[derive(Serialize)]
    struct Inner {
        value: u32,
    }

    let field = serialize_into_field(
        &[Outer {
            inner: HashMap::from([(String::from("key"), Inner { value: 42 })]),
        }],
        "outer",
        TracingOptions::new().map_as_struct(false),
    )
    .unwrap();
    let fields = &mut [field];

    assert_eq!(find_physical_type(fields, "outer"), PhysicalType::Struct);
    assert_eq!(find_physical_type(fields, "outer.inner"), PhysicalType::Map);
    assert_eq!(
        find_physical_type(fields, "outer.inner.key"),
        PhysicalType::LargeUtf8,
    );
    assert_eq!(
        find_physical_type(fields, "outer.inner.value"),
        PhysicalType::Struct,
    );
    assert_eq!(
        find_physical_type(fields, "outer.inner.value.value"),
        PhysicalType::Primitive(PrimitiveType::UInt32),
    );
}
