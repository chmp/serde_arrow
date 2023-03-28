mod api_chrono;
mod error;
mod events;

#[cfg(all(test, any(feature = "arrow2-0-16", feature = "arrow2-0-17")))]
mod implementation_docs;

#[cfg(all(test, any(feature = "arrow2-0-16", feature = "arrow2-0-17")))]
mod arrow2;

mod utils;
