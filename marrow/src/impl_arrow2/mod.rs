//! Support for arrow2
#![cfg_attr(any(), rustfmt::skip)]

#[cfg(feature = "arrow2-0-17")]
mod arrow2_0_17 {
    use arrow2_0_17 as arrow2;
    include!("impl.rs");
}

#[cfg(feature = "arrow2-0-16")]
mod arrow2_0_16 {
    use arrow2_0_16 as arrow2;
    include!("impl.rs");
}
