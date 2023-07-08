//! Helpers to convert between arrow and arrow2 arrays
//!
use crate::experimental::Configuration;

pub struct ScopedConfiguration {
    prev_config: Configuration,
}

impl ScopedConfiguration {
    pub fn configure<F: FnOnce(&mut Configuration)>(effect: F) -> Self {
        let mut prev_config = Configuration::default();
        {
            let prev_config = &mut prev_config;
            crate::experimental::configure(move |c| {
                *prev_config = c.clone();
                effect(c);
            });
        }
        Self { prev_config }
    }
}

impl std::ops::Drop for ScopedConfiguration {
    fn drop(&mut self) {
        crate::experimental::configure(|c| {
            *c = self.prev_config.clone();
        })
    }
}
