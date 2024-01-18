use serde::Serialize;

use super::{
    array_builder::ArrayBuilder, i8_builder::I8Builder, list_builder::ListBuilder,
    struct_builder::StructBuilder, utils::Mut,
};

#[test]
fn i8_array() {
    let items: Vec<i8> = vec![4, 5, 6];

    let mut builder = ArrayBuilder::list(ArrayBuilder::i8(false), false);
    items.serialize(Mut(&mut builder)).unwrap();

    let ListBuilder {
        offsets, element, ..
    } = builder.unwrap_list();
    let I8Builder { buffer, .. } = element.unwrap_i8();

    assert_eq!(offsets.offsets.as_slice(), &[0, 3]);
    assert_eq!(buffer.as_slice(), &[4, 5, 6]);
}

#[test]
fn struct_array() {
    #[derive(Serialize)]
    struct S {
        a: i8,
        b: i8,
    }

    let items = vec![S { a: 3, b: 4 }, S { a: 5, b: 6 }];

    let mut builder = ArrayBuilder::list(
        ArrayBuilder::r#struct(
            vec![
                (String::from("a"), ArrayBuilder::i8(false)),
                (String::from("b"), ArrayBuilder::i8(false)),
            ],
            false,
        )
        .unwrap(),
        false,
    );
    items.serialize(Mut(&mut builder)).unwrap();

    let ListBuilder {
        offsets, element, ..
    } = builder.unwrap_list();
    let StructBuilder { named_fields, .. } = element.unwrap_struct();

    assert_eq!(&named_fields[0].0, "a");
    assert_eq!(&named_fields[1].0, "b");

    let I8Builder {
        buffer: buffer_a, ..
    } = named_fields[0].1.clone().unwrap_i8();
    let I8Builder {
        buffer: buffer_b, ..
    } = named_fields[1].1.clone().unwrap_i8();

    assert_eq!(offsets.offsets, vec![0, 2]);
    assert_eq!(buffer_a, vec![3, 5]);
    assert_eq!(buffer_b, vec![4, 6]);
}
