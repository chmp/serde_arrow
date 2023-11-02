use super::macros::test_generic;

/*
        #[test]
        fn serialization() {
            $($($definitions)*)?

            let items = &$values;
            let field = serialize_into_field(&items, "item", TracingOptions::default()).unwrap();
            let array = serialize_into_array(&field, &items).unwrap();

            drop(array);
        }
*/

test_generic!(
    fn outer_vec() {
        let items: Vec<u32> = vec![0_u32, 1_u32, 2_u32];
        let fields = Vec::<Field>::from_samples(&Items(&items), TracingOptions::default()).unwrap();
        let arrays = to_arrow(&fields, &Items(&items)).unwrap();

        drop(arrays);
    }
);

test_generic!(
    fn outer_slice() {
        let items: &[u32] = &[0_u32, 1_u32, 2_u32];
        let fields = Vec::<Field>::from_samples(&Items(items), TracingOptions::default()).unwrap();
        let arrays = to_arrow(&fields, &Items(items)).unwrap();

        drop(arrays);
    }
);

test_generic!(
    fn outer_array() {
        let items: &[u32; 3] = &[0_u32, 1_u32, 2_u32];
        let fields = Vec::<Field>::from_samples(&Items(items), TracingOptions::default()).unwrap();
        let arrays = to_arrow(&fields, &Items(items)).unwrap();

        drop(arrays);
    }
);

test_generic!(
    fn outer_tupple() {
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
        let fields = Vec::<Field>::from_samples(items, TracingOptions::default()).unwrap();
        let arrays = to_arrow(&fields, &items).unwrap();

        drop(arrays);
    }
);
