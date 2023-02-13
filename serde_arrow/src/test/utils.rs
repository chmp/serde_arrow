use crate::internal::{error::Result, event::Event, source::EventSource};

pub fn collect_events<'event, S: EventSource<'event> + 'event>(
    mut source: S,
) -> Result<Vec<Event<'static>>> {
    let mut res = Vec::new();
    while let Some(ev) = source.next()? {
        res.push(ev.to_static());
    }
    Ok(res)
}
