use crate::internal::{
    error::{fail, Result},
    event::Event,
    sink::{macros, EventSink},
};

#[derive(Debug, Default)]
pub struct NullArrayBuilder {
    pub path: String,
    pub length: usize,
    pub finished: bool,
}

impl NullArrayBuilder {
    pub fn new(path: String) -> Self {
        Self {
            path,
            length: 0,
            finished: true,
        }
    }
}

impl EventSink for NullArrayBuilder {
    macros::forward_generic_to_specialized!();
    macros::accept_start!((this, ev, _val, _next) {
        fail!("Cannot handle event {ev} in NullArrayBuilder [{}]", this.path);
    });
    macros::accept_end!((this, ev, _val, _next) {
        fail!("Cannot handle event {ev} in NullArrayBuilder [{}]", this.path);
    });
    macros::accept_marker!((this, ev, _val, _next) {
        if !matches!(ev, Event::Some) {
            fail!("Cannot handle event {ev} in NullArrayBuilder [{}]", this.path);
        }
        Ok(())
    });
    macros::accept_value!((this, ev, _val, _next) {
        match ev {
            Event::Null | Event::Default => {
                this.length += 1;
            },
            ev => fail!("Cannot handle event {ev} in NullArrayBuilder [{}]", this.path),
        }
        Ok(())
    });

    fn finish(&mut self) -> Result<()> {
        self.finished = true;
        Ok(())
    }
}
