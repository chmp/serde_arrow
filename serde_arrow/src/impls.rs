//! The arrow implementation to use
#[cfg(feature = "arrow2-0-17")]
pub use arrow2_0_17 as arrow2;

#[cfg(all(feature = "arrow2-0-16", not(feature = "arrow2-0-17")))]
pub use arrow2_0_16 as arrow2;
