use crate::{
    error,
    event::{Event, EventSource, PeekableEventSource},
    fail, Result,
};

pub struct RecordSource<'a, S: EventSource<'a> + 'a> {
    columns: Vec<&'a str>,
    values: Vec<PeekableEventSource<'a, S>>,
    next: RecordSourceState,
}

impl<'a, S: EventSource<'a> + 'a> RecordSource<'a, S> {
    pub fn new(columns: Vec<&'a str>, values: Vec<S>) -> Self {
        let values = values.into_iter().map(PeekableEventSource::new).collect();

        RecordSource {
            columns,
            values,
            next: RecordSourceState::StartSequence,
        }
    }
}

impl<'a, S: EventSource<'a> + 'a> EventSource<'a> for RecordSource<'a, S> {
    fn next(&mut self) -> Result<Option<Event<'a>>> {
        use RecordSourceState::*;

        match self.next {
            StartSequence => {
                self.next = StartMapOrEnd;
                Ok(Some(Event::StartSequence))
            }
            StartMapOrEnd => {
                if self.columns.is_empty() || self.values[0].peek()?.is_none() {
                    self.next = Done;
                    Ok(Some(Event::EndSequence))
                } else {
                    self.next = Key(0);
                    Ok(Some(Event::StartMap))
                }
            }
            Key(i) if i >= self.columns.len() => {
                self.next = StartMapOrEnd;
                Ok(Some(Event::EndMap))
            }
            Key(i) => {
                self.next = Value(i, 0);
                Ok(Some(Event::Key(self.columns[i])))
            }
            Value(i, depth) => {
                let ev = self.values[i]
                    .next()?
                    .ok_or_else(|| error!("Unbalanced values"))?;

                self.next = match (&ev, depth) {
                    (Event::StartMap | Event::StartSequence, _) => Value(i, depth + 1),
                    (Event::EndMap | Event::EndSequence, 0) => fail!("Invalid nested value"),
                    (Event::EndMap | Event::EndSequence, 1) => Key(i + 1),
                    (Event::EndMap | Event::EndSequence, _) => Value(i, depth - 1),
                    (_, 0) => Key(i + 1),
                    _ => Value(i, depth),
                };

                Ok(Some(ev))
            }
            Done => Ok(None),
        }
    }
}

#[derive(Clone, Copy)]
enum RecordSourceState {
    StartSequence,
    StartMapOrEnd,
    Key(usize),
    Value(usize, usize),
    Done,
}
