use crate::{
    event::{Event, RecordBatchSource},
    ng::from_record_batch,
    Result,
};

use serde::{Deserialize, Serialize};

#[test]
fn event_source() -> Result<()> {
    let original = &[
        Example { int8: 0, int32: 21 },
        Example { int8: 1, int32: 42 },
    ];
    let schema = crate::trace_schema(&original)?;
    let record_batch = crate::to_record_batch(&original, &schema)?;

    let mut event_source = RecordBatchSource::new(&record_batch)?;

    assert_eq!(event_source.next(), Event::StartSequence);
    assert_eq!(event_source.next(), Event::StartMap);
    assert_eq!(event_source.next(), Event::Key("int8"));
    assert_eq!(event_source.next(), Event::I8(0));
    assert_eq!(event_source.next(), Event::Key("int32"));
    assert_eq!(event_source.next(), Event::I32(21));
    assert_eq!(event_source.next(), Event::EndMap);
    assert_eq!(event_source.next(), Event::StartMap);
    assert_eq!(event_source.next(), Event::Key("int8"));
    assert_eq!(event_source.next(), Event::I8(1));
    assert_eq!(event_source.next(), Event::Key("int32"));
    assert_eq!(event_source.next(), Event::I32(42));
    assert_eq!(event_source.next(), Event::EndMap);
    assert_eq!(event_source.next(), Event::EndSequence);

    Ok(())
}

#[test]
fn example() -> Result<()> {
    let original = &[
        Example { int8: 0, int32: 21 },
        Example { int8: 1, int32: 42 },
    ];
    let schema = crate::trace_schema(&original)?;
    let record_batch = crate::to_record_batch(&original, &schema)?;
    let round_tripped = from_record_batch::<Vec<Example>>(&record_batch)?;

    assert_eq!(round_tripped, original);

    Ok(())
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
struct Example {
    int8: i8,
    int32: i32,
}
