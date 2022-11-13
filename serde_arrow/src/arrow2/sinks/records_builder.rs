use std::collections::{HashMap, HashSet};

use arrow2::{array::Array, datatypes::Field};

use crate::{
    error,
    event::{Event, EventSink},
    fail, Result,
};

use super::{
    base::{ArrayBuilder, DynamicArrayBuilder},
    builders::build_dynamic_array_builder,
};

pub struct RecordsBuilder {
    fields: Vec<Field>,
    builders: Vec<DynamicArrayBuilder>,
    field_index: HashMap<String, usize>,
    next: State,
    seen: HashSet<usize>,
}

impl RecordsBuilder {
    pub fn new(fields: Vec<Field>) -> Result<Self> {
        let mut builders = Vec::new();
        let mut field_index = HashMap::new();

        for (idx, field) in fields.iter().enumerate() {
            builders.push(build_dynamic_array_builder(field.data_type())?);
            if field_index.contains_key(&field.name) {
                fail!("Duplicate field {}", field.name);
            }
            field_index.insert(field.name.to_owned(), idx);
        }

        Ok(Self {
            fields,
            builders,
            field_index,
            next: State::StartSequence,
            seen: HashSet::new(),
        })
    }

    pub fn into_records(self) -> Result<(Vec<Field>, Vec<Box<dyn Array>>)> {
        if !matches!(self.next, State::Done) {
            fail!("Invalid state");
        }
        let arrays: Result<Vec<Box<dyn Array>>> = self
            .builders
            .into_iter()
            .map(|builder| builder.into_array())
            .collect();
        let arrays = arrays?;
        Ok((self.fields, arrays))
    }
}

#[derive(Debug, Clone, Copy)]
enum State {
    StartSequence,
    StartMap,
    Key,
    Value(usize, usize),
    Done,
}

impl EventSink for RecordsBuilder {
    fn accept(&mut self, event: Event<'_>) -> Result<()> {
        use State::*;

        self.next = match (self.next, event.as_ref()) {
            (StartSequence, Event::StartSequence) => StartMap,
            (StartMap, Event::EndSequence) => Done,
            (StartMap, Event::StartMap) => {
                self.seen.clear();
                Key
            }
            (Key, Event::Key(k)) => {
                let &idx = self
                    .field_index
                    .get(k)
                    .ok_or_else(|| error!("Unknown field {k}"))?;
                if self.seen.contains(&idx) {
                    fail!("Duplicate field {k}");
                }
                self.seen.insert(idx);

                Value(idx, 0)
            }
            (Key, Event::EndMap) => StartMap,
            (Value(idx, depth), ev) => {
                let next = match ev {
                    Event::StartSequence | Event::StartMap => Value(idx, depth + 1),
                    Event::EndSequence | Event::EndMap if depth > 1 => Value(idx, depth - 1),
                    Event::EndSequence | Event::EndMap if depth == 0 => fail!("Invalid state"),
                    // the closing event for the nested type
                    Event::EndSequence | Event::EndMap => Key,
                    _ if depth == 0 => Key,
                    _ => Value(idx, depth),
                };

                self.builders[idx].accept(ev)?;
                next
            }
            (state, ev) => fail!("Invalid event {ev} in state {state:?}"),
        };
        Ok(())
    }
}
