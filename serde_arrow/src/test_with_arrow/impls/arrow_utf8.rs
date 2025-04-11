use marrow::datatypes::DataType;

use crate::internal::{schema::TracingOptions, utils::Item};

use super::utils::{new_field, Test};

#[test]
fn str() {
    let field = new_field("item", DataType::LargeUtf8, false);
    type Ty = String;
    let values = [
        Item(String::from("a")),
        Item(String::from("b")),
        Item(String::from("c")),
        Item(String::from("d")),
    ];

    Test::new()
        .with_schema(vec![field])
        .trace_schema_from_samples(&values, TracingOptions::default())
        .trace_schema_from_type::<Item<Ty>>(TracingOptions::default())
        .serialize(&values)
        .deserialize(&values);
}

#[test]
fn str_utf8() {
    let field = new_field("item", DataType::Utf8, false);
    type Ty = String;
    let values = [
        Item(String::from("a")),
        Item(String::from("b")),
        Item(String::from("c")),
        Item(String::from("d")),
    ];

    Test::new()
        .with_schema(vec![field])
        .trace_schema_from_samples(
            &values,
            TracingOptions::default().strings_as_large_utf8(false),
        )
        .trace_schema_from_type::<Item<Ty>>(TracingOptions::default().strings_as_large_utf8(false))
        .serialize(&values)
        .deserialize(&values);
}

#[test]
fn nullable_str() {
    let field = new_field("item", DataType::LargeUtf8, true);
    type Ty = Option<String>;
    let values = [
        Item(Some(String::from("a"))),
        Item(None),
        Item(None),
        Item(Some(String::from("d"))),
    ];

    Test::new()
        .with_schema(vec![field])
        .trace_schema_from_samples(&values, TracingOptions::default())
        .trace_schema_from_type::<Item<Ty>>(TracingOptions::default())
        .serialize(&values)
        .deserialize(&values);
}

#[test]
fn str_u32() {
    let field = new_field("item", DataType::Utf8, false);
    let values = [
        Item(String::from("a")),
        Item(String::from("b")),
        Item(String::from("c")),
        Item(String::from("d")),
    ];

    Test::new()
        .with_schema(vec![field])
        .serialize(&values)
        .deserialize(&values);
}

#[test]
fn nullable_str_u32() {
    let field = new_field("item", DataType::Utf8, true);
    let values = [
        Item(Some(String::from("a"))),
        Item(None),
        Item(None),
        Item(Some(String::from("d"))),
    ];

    Test::new()
        .with_schema(vec![field])
        .serialize(&values)
        .deserialize(&values);
}

#[test]
fn borrowed_str() {
    let field = new_field("item", DataType::LargeUtf8, false);

    type Ty<'a> = &'a str;

    let values = [Item("a"), Item("b"), Item("c"), Item("d")];

    Test::new()
        .with_schema(vec![field])
        .trace_schema_from_samples(&values, TracingOptions::default())
        .trace_schema_from_type::<Item<Ty>>(TracingOptions::default())
        .serialize(&values)
        .deserialize_borrowed(&values);
}

#[test]
fn nullabe_borrowed_str() {
    let field = new_field("item", DataType::LargeUtf8, true);

    type Ty<'a> = Option<&'a str>;

    let values = [Item(Some("a")), Item(None), Item(None), Item(Some("d"))];

    Test::new()
        .with_schema(vec![field])
        .trace_schema_from_samples(&values, TracingOptions::default())
        .trace_schema_from_type::<Item<Ty>>(TracingOptions::default())
        .serialize(&values)
        .deserialize_borrowed(&values);
}

#[test]
fn borrowed_str_u32() {
    let field = new_field("item", DataType::Utf8, false);

    let values = [Item("a"), Item("b"), Item("c"), Item("d")];

    Test::new()
        .with_schema(vec![field])
        .serialize(&values)
        .deserialize_borrowed(&values);
}

#[test]
fn nullabe_borrowed_str_u32() {
    let field = new_field("item", DataType::Utf8, true);

    let values = [Item(Some("a")), Item(None), Item(None), Item(Some("d"))];

    Test::new()
        .with_schema(vec![field])
        .serialize(&values)
        .deserialize_borrowed(&values);
}

#[test]
fn i8_to_utf8() {
    let field = new_field("item", DataType::Utf8, false);
    let input = [Item(-1i8), Item(0), Item(1), Item(10)];
    let output = [Item("-1"), Item("0"), Item("1"), Item("10")];

    Test::new()
        .with_schema(vec![field])
        .serialize(&input)
        .deserialize_borrowed(&output);
}

#[test]
fn i16_to_utf8() {
    let field = new_field("item", DataType::Utf8, false);
    let input = [Item(-1i16), Item(0), Item(1), Item(10)];
    let output = [Item("-1"), Item("0"), Item("1"), Item("10")];

    Test::new()
        .with_schema(vec![field])
        .serialize(&input)
        .deserialize_borrowed(&output);
}
#[test]
fn i32_to_utf8() {
    let field = new_field("item", DataType::Utf8, false);
    let input = [Item(-1i32), Item(0), Item(1), Item(10)];
    let output = [Item("-1"), Item("0"), Item("1"), Item("10")];

    Test::new()
        .with_schema(vec![field])
        .serialize(&input)
        .deserialize_borrowed(&output);
}

#[test]
fn i64_to_utf8() {
    let field = new_field("item", DataType::Utf8, false);
    let input = [Item(-1i64), Item(0), Item(1), Item(10)];
    let output = [Item("-1"), Item("0"), Item("1"), Item("10")];

    Test::new()
        .with_schema(vec![field])
        .serialize(&input)
        .deserialize_borrowed(&output);
}

#[test]
fn u8_to_utf8() {
    let field = new_field("item", DataType::Utf8, false);
    let input = [Item(0u8), Item(1), Item(2), Item(10)];
    let output = [Item("0"), Item("1"), Item("2"), Item("10")];

    Test::new()
        .with_schema(vec![field])
        .serialize(&input)
        .deserialize_borrowed(&output);
}

#[test]
fn u16_to_utf8() {
    let field = new_field("item", DataType::Utf8, false);
    let input = [Item(0u16), Item(1), Item(2), Item(10)];
    let output = [Item("0"), Item("1"), Item("2"), Item("10")];

    Test::new()
        .with_schema(vec![field])
        .serialize(&input)
        .deserialize_borrowed(&output);
}

#[test]
fn u32_to_utf8() {
    let field = new_field("item", DataType::Utf8, false);
    let input = [Item(0u32), Item(1), Item(2), Item(10)];
    let output = [Item("0"), Item("1"), Item("2"), Item("10")];

    Test::new()
        .with_schema(vec![field])
        .serialize(&input)
        .deserialize_borrowed(&output);
}

#[test]
fn u64_to_utf8() {
    let field = new_field("item", DataType::Utf8, false);
    let input = [Item(0u64), Item(1), Item(2), Item(10)];
    let output = [Item("0"), Item("1"), Item("2"), Item("10")];

    Test::new()
        .with_schema(vec![field])
        .serialize(&input)
        .deserialize_borrowed(&output);
}

#[test]
fn f32_to_utf8() {
    let field = new_field("item", DataType::Utf8, false);
    let input = [Item(1.0f32), Item(0.5), Item(-0.5), Item(10.5)];
    let output = [Item("1"), Item("0.5"), Item("-0.5"), Item("10.5")];

    Test::new()
        .with_schema(vec![field])
        .serialize(&input)
        .deserialize_borrowed(&output);
}

#[test]
fn f64_to_utf8() {
    let field = new_field("item", DataType::Utf8, false);
    let input = [Item(1.0f64), Item(0.5), Item(-0.5), Item(10.5)];
    let output = [Item("1"), Item("0.5"), Item("-0.5"), Item("10.5")];

    Test::new()
        .with_schema(vec![field])
        .serialize(&input)
        .deserialize_borrowed(&output);
}

#[test]
fn bool_to_utf8() {
    let field = new_field("item", DataType::Utf8, false);
    let input = [Item(true), Item(false)];
    let output = [Item("true"), Item("false")];

    Test::new()
        .with_schema(vec![field])
        .serialize(&input)
        .deserialize_borrowed(&output);
}

#[test]
fn char_to_utf8() {
    let field = new_field("item", DataType::Utf8, false);
    let input = [Item('a'), Item('b'), Item('c')];
    let output = [Item("a"), Item("b"), Item("c")];

    Test::new()
        .with_schema(vec![field])
        .serialize(&input)
        .deserialize_borrowed(&output);
}
