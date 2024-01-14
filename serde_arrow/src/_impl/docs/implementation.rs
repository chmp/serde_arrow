//! How does it work?
//!
//!`serde_arrow` allows to convert between Rust objects that implement
//![Serialize][serde::Serialize] or [Deserialize][serde::Deserialize] and arrow
//!arrays. `serde_arrow` introduces an intermediate JSON-like representation in
//!the form of a stream of Event objects. Consider the following Rust vector
//!
//! ```rust
//! # use serde::{Serialize, Deserialize};
//! ##[derive(Debug, PartialEq, Serialize, Deserialize)]
//! struct Record {
//!     a: i32,
//!     b: u32,
//! }
//!
//! let items = vec![
//!     Record { a: 1, b: 2},
//!     Record { a: 3, b: 4},
//!     // ...
//! ];
//! ```
//!
//! The items vector can be converted into a stream of events as in:
//!
//! ```rust
//! # fn main() -> serde_arrow::_impl::PanicOnError<()> {
//! # use serde_arrow::_impl::{serialize_into_sink, Event};
//! # use serde::{Serialize, Deserialize};
//! #
//! # #[derive(Debug, PartialEq, Serialize, Deserialize)]
//! # struct Record { a: i32, b: u32 }
//! # let items = vec![ Record { a: 1, b: 2}, Record { a: 3, b: 4} ];
//! #
//! # let mut events: Vec<Event<'static>> = Vec::new();
//! # serialize_into_sink(&mut events, &items)?;
//! #
//! # assert_eq!( events,
//! vec![
//!     Event::StartSequence,
//!     Event::Item,
//!     Event::StartStruct,
//!     Event::Str("a").to_owned(),
//!     Event::I32(1),
//!     Event::Str("b").to_owned(),
//!     Event::U32(2),
//!     Event::EndStruct,
//!     Event::Item,
//!     Event::StartStruct,
//!     Event::Str("a").to_owned(),
//!     Event::I32(3),
//!     Event::Str("b").to_owned(),
//!     Event::U32(4),
//!     Event::EndStruct,
//!     Event::EndSequence
//! ]
//! # );
//! # Ok(())
//! # }
//! ```
//!
//! `serde_arrow` includes functionality that builds arrow arrays from Rust
//! objects or vice versa by interpreting the stream of events. In both cases,
//! `serde_arrow` requires additional information over the array types in in the
//! form of arrow fields
//!
//! ```rust
//! # #[cfg(has_arrow)]
//! # fn main() {
//! # use serde_arrow::_impl::arrow::datatypes::{Field, DataType};
//! #
//! let fields = vec![
//!     Field::new("a", DataType::Int32, false),
//!     Field::new("b", DataType::UInt32, false),
//! ];
//! # }
//! # #[cfg(not(has_arrow))] fn main() { }
//! ```
//!
//! With the fields the records can be converted into arrays by calling
//! `serialize_into_arrays`
//!
//! ```rust
//! # #[cfg(has_arrow)]
//! # fn main() -> serde_arrow::_impl::PanicOnError<()> {
//! # let fields = [];
//! # let items = [serde_arrow::utils::Item(())];
//! let arrays = serde_arrow::to_arrow(&fields, &items)?;
//! # Ok(())
//! # }
//! # #[cfg(not(has_arrow))] fn main() {}
//! ```
//!
//! Due to the two conversions in play (Rust <-> Intermediate Format <-> Arrow)
//! there are different options to convert Rust types to Arrow. For examples,
//! dates can be stored as string, integer or date columns depending on
//! configuration.
//!
//! First, there is the conversion from Rust to the intermediate format. Per
//! default [chrono] serializes date time objects to strings, but by using its
//! serde module it can be configured to serialize date times to integers.
//!
//! For example:
//!
//! ```rust
//! # use serde::Serialize;
//! use chrono::{DateTime, Utc};
//!
//! ##[derive(Serialize)]
//! struct DateAsString {
//!     date: DateTime<Utc>,
//! }
//! ```
//!
//! will generate a sequence of events similar to
//!
//! ```rust
//! # use serde::Serialize;
//! # use chrono::{DateTime, Utc};
//! # #[derive(Serialize)]
//! # struct DateAsString { date: DateTime<Utc> }
//! #
//! # fn main() -> serde_arrow::_impl::PanicOnError<()> {
//! # use serde_arrow::_impl::{Event, serialize_into_sink};
//! #
//! # let items = vec![ DateAsString { date: DateTime::<Utc>::from_timestamp(0, 0).unwrap() } ];
//! #
//! # let mut events: Vec<Event<'static>> = Vec::new();
//! # serialize_into_sink(&mut events, &items)?;
//! # assert_eq!(events,
//! vec![
//!     Event::StartSequence,
//!     Event::Item,
//!     Event::StartStruct,
//!     Event::Str("date").to_owned(),
//!     Event::Str("1970-01-01T00:00:00Z").to_owned(),
//!     Event::EndStruct,
//!     // ...
//!     Event::EndSequence,
//! ]
//! # );
//! # Ok(())
//! # }
//! ```
//!
//! Using the `chrono::serde::ts_milliseconds` helper, the struct defined as
//!
//! ```rust
//! # use serde::Serialize;
//! use chrono::{DateTime, Utc};
//!
//! ##[derive(Serialize)]
//! struct DateAsInteger {
//!     #[serde(with = "chrono::serde::ts_milliseconds")]
//!     date: DateTime<Utc>,
//! }
//! ```
//!
//! will generate an event sequence similar to
//!
//! ```rust
//! # use serde::Serialize;
//! # use chrono::{DateTime, Utc};
//! #
//! # #[derive(Serialize)]
//! # struct DateAsInteger {
//! #     #[serde(with = "chrono::serde::ts_milliseconds")]
//! #     date: DateTime<Utc>,
//! # }
//! #
//! # fn main() -> serde_arrow::_impl::PanicOnError<()> {
//! # use serde_arrow::_impl::{Event, serialize_into_sink};
//! #
//! # let items = vec![ DateAsInteger { date: DateTime::<Utc>::from_timestamp(0, 0).unwrap() } ];
//! #
//! # let mut events: Vec<Event<'static>> = Vec::new();
//! # serialize_into_sink(&mut events, &items)?;
//! # assert_eq!(events,
//! vec![
//!     Event::StartSequence,
//!     Event::Item,
//!     Event::StartStruct,
//!     Event::Str("date").to_owned(),
//!     Event::I64(0),
//!     Event::EndStruct,
//!     // ..
//!     Event::EndSequence,
//! ]
//! # );
//! # Ok(())
//! # }
//! ```
//!
//! Both variants can be used to create `Date64` arrow arrays. In the first
//! case, the field needs to be configured with additional metadata to tell
//! `serde_arrow` to interpret the strings as dates
//!
//! ```rust
//! # #[cfg(has_arrow)]
//! # fn main() {
//! # use serde_arrow::_impl::arrow::datatypes::{Field, DataType};
//! use serde_arrow::schema::Strategy;
//!
//! let field = Field::new("date", DataType::Date64, false)
//!     // only required if the datetime objects are serialized as strings
//!     .with_metadata(Strategy::UtcStrAsDate64.into());
//! # }
//! # #[cfg(not(has_arrow))] fn main() {}
//! ```
//!
//! In the second case, the events can be interpreted as `Date64` values as-is
//! and the field needs to be merely configured with the correct data type
//!
//! ```rust
//! # #[cfg(has_arrow)]
//! # fn main() {
//! # use serde_arrow::_impl::arrow::datatypes::{Field, DataType};
//! let field = Field::new("date", DataType::Date64, false);
//! # }
//! # #[cfg(not(has_arrow))] fn main() {}
//! ```
//!