use serde::{Deserialize, Serialize};

use crate::{
    _impl::arrow::{
        array::ArrayRef,
        datatypes::{Field, FieldRef},
    },
    schema::{SchemaLike, TracingOptions},
    ArrayBuilder, Deserializer,
};

#[derive(Debug, Deserialize, Serialize, PartialEq, Clone, Copy)]
struct Record {
    a: i32,
    b: Option<bool>,
}

#[derive(Debug, Deserialize, Serialize)]
struct StructWrapper(Record, Record);

fn serialize<I: Serialize + ?Sized>(fields: &[impl AsRef<Field>], items: &I) -> Vec<ArrayRef> {
    let mut builder = ArrayBuilder::from_arrow(&fields).unwrap();
    items
        .serialize(crate::Serializer::new(&mut builder))
        .unwrap();
    builder.to_arrow().unwrap()
}

fn deserialize<'de, I: Deserialize<'de>>(
    fields: &[impl AsRef<Field>],
    arrays: &'de [ArrayRef],
) -> I {
    I::deserialize(Deserializer::<'de>::from_arrow(fields, arrays).unwrap()).unwrap()
}

#[test]
fn serialize_tuples() {
    let fields = Vec::<FieldRef>::from_type::<Record>(TracingOptions::default()).unwrap();

    let item = Record {
        a: 0,
        b: Some(true),
    };

    let _ = serialize(&fields, &(item, item));
    let _ = serialize(&fields, &StructWrapper(item, item));
    let _ = serialize::<[Record]>(&fields, &[item, item]);
    let _ = serialize::<[Record; 2]>(&fields, &[item, item]);
}

#[test]
fn deserialize_tuples() {
    let fields = Vec::<FieldRef>::from_type::<Record>(TracingOptions::default()).unwrap();
    let arrays = serialize(
        &fields,
        &[
            Record {
                a: 0,
                b: Some(true),
            },
            Record { a: 1, b: None },
        ],
    );

    // try the different options to deserialize
    let _ = deserialize::<(Record, Record)>(&fields, &arrays);
    let _ = deserialize::<[Record; 2]>(&fields, &arrays);
    let _ = deserialize::<StructWrapper>(&fields, &arrays);
    let _ = deserialize::<Vec<Record>>(&fields, &arrays);
}
