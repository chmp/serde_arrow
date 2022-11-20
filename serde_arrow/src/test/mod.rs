mod api_chrono;
mod events;
mod generic;

#[cfg(all(test, feature = "arrow2"))]
mod implementation_docs;

#[cfg(all(test, feature = "arrow2"))]
mod arrow2;

mod utils;
