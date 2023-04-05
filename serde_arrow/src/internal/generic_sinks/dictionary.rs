use std::collections::HashMap;

use crate::internal::{
    error::{error, fail, Result},
    event::Event,
    sink::{macros, EventSink},
};

#[derive(Debug, Default)]
pub struct DictionaryUtf8ArrayBuilder<B> {
    pub index: HashMap<String, u64>,
    pub keys: B,
    pub values: B,
}

impl<B> DictionaryUtf8ArrayBuilder<B> {
    pub fn new(keys: B, values: B) -> Self {
        Self {
            index: Default::default(),
            keys,
            values,
        }
    }
}

impl<B: EventSink> DictionaryUtf8ArrayBuilder<B> {
    fn get_key<S: Into<String> + AsRef<str>>(&mut self, s: S) -> Result<u64> {
        if self.index.contains_key(s.as_ref()) {
            Ok(self.index[s.as_ref()])
        } else {
            let idx =
                u64::try_from(self.index.len()).map_err(|_| error!("Cannot convert index"))?;
            self.values.accept_str(s.as_ref())?;
            self.index.insert(s.into(), idx);
            Ok(idx)
        }
    }
}

impl<B: EventSink> EventSink for DictionaryUtf8ArrayBuilder<B> {
    macros::accept_start!((_this, ev, _val, _next) {
        fail!("Cannot handle event {ev} in DictionaryUtf8ArrayBuilder")
    });
    macros::accept_end!((_this, ev, _val, _next) {
        fail!("Cannot handle event {ev} in DictionaryUtf8ArrayBuilder")
    });
    macros::accept_marker!((_this, ev, _val, _next) {
        match ev {
            Event::Some => Ok(()),
            _ => fail!("Cannot handle event {ev} in DictionaryUtf8ArrayBuilder"),
        }
    });
    macros::fail_on_non_string_primitive!("DictionaryUtf8ArrayBuilder");

    fn accept_str(&mut self, val: &str) -> Result<()> {
        let key = self.get_key(val)?;
        self.keys.accept_u64(key)
    }

    fn accept_owned_str(&mut self, val: String) -> Result<()> {
        let key = self.get_key(val)?;
        self.keys.accept_u64(key)
    }

    fn accept_default(&mut self) -> Result<()> {
        let key = self.get_key("")?;
        self.keys.accept_u64(key)
    }

    fn accept_null(&mut self) -> Result<()> {
        self.keys.accept_null()
    }

    fn accept(&mut self, event: Event<'_>) -> Result<()> {
        match event {
            Event::Some => self.accept_some(),
            Event::Default => self.accept_default(),
            Event::Null => self.accept_null(),
            Event::Str(val) => self.accept_str(val),
            Event::OwnedStr(val) => self.accept_owned_str(val),
            ev => fail!("Cannot handle event {ev} in BooleanArrayBuilder"),
        }
    }

    fn finish(&mut self) -> Result<()> {
        self.keys.finish()?;
        self.values.finish()?;
        Ok(())
    }
}
