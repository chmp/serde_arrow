use crate::{schema::TracingOptions, utils::Items};

use super::utils::Test;

#[test]
fn outer_vec() {
    let items: Vec<u32> = vec![0_u32, 1_u32, 2_u32];
    Test::new()
        .trace_schema_from_samples(&Items(&items), TracingOptions::default())
        .serialize(&Items(&items));
}

#[test]
fn outer_slice() {
    let items: &[u32] = &[0_u32, 1_u32, 2_u32];
    Test::new()
        .trace_schema_from_samples(&Items(items), TracingOptions::default())
        .serialize(&Items(items));
}

#[test]
fn outer_array() {
    let items: &[u32; 3] = &[0_u32, 1_u32, 2_u32];
    Test::new()
        .trace_schema_from_samples(&Items(items), TracingOptions::default())
        .serialize(&Items(items));
}

#[test]
fn outer_tuple() {
    // Note: the standard Items wrapper does not work with tuples, use a custom impl here
    #[derive(serde::Serialize)]
    struct Item {
        item: u32,
    }

    let items: &(Item, Item, Item) = &(
        Item { item: 0_u32 },
        Item { item: 1_u32 },
        Item { item: 2_u32 },
    );

    Test::new()
        .trace_schema_from_samples(items, TracingOptions::default())
        .serialize(items);
}
