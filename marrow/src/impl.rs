//! Internal do not use
//!
//! This module exports helpers for use in doc tests. This setup is a workaround for the fact that
//! there is no way to expose functionality only in doc tests. See [this
//! issue](https://github.com/rust-lang/rust/issues/67295).
#[macro_export]
#[doc(hidden)]
macro_rules! _with_arrow {
    ($($tt:tt)*) => {
        // arrow-version: replace:         #[cfg(feature = "arrow-53")]
        #[cfg(feature = "arrow-53")]
        {
            use $crate::r#impl::arrow;
            $($tt)*
        }
        return Ok(());
    };
}

// arrow-version: replace: #[cfg(feature = "arrow-{version}")]
#[cfg(feature = "arrow-53")]
#[doc(hidden)]
#[allow(unused)]
pub mod arrow {
    // arrow-version: replace:     use arrow_array_{version} as _arrow_array;
    use arrow_array_53 as _arrow_array;

    // arrow-version: replace:     use arrow_schema_{version} as _arrow_schema;
    use arrow_schema_53 as _arrow_schema;

    pub mod array {
        pub use super::_arrow_array::array::{
            make_array, Array, ArrayRef, ArrowPrimitiveType, BinaryArray, BooleanArray,
            Date32Array, Date64Array, DictionaryArray, DurationMicrosecondArray,
            DurationMillisecondArray, DurationNanosecondArray, DurationSecondArray,
            FixedSizeBinaryArray, FixedSizeListArray, Float16Array, Float32Array, Float64Array,
            GenericBinaryArray, GenericListArray, GenericStringArray, Int16Array, Int32Array,
            Int64Array, Int8Array, LargeBinaryArray, LargeStringArray, MapArray, NullArray,
            OffsetSizeTrait, PrimitiveArray, StringArray, StructArray, Time32MillisecondArray,
            Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray,
            TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
            TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array, UnionArray,
        };
        pub use super::_arrow_array::builder::{
            FixedSizeListBuilder, Int32Builder, LargeListBuilder, ListBuilder, MapBuilder,
            StringBuilder,
        };
        pub use super::_arrow_array::RecordBatch;
    }
    pub mod datatypes {
        pub use super::_arrow_array::types::{
            ArrowDictionaryKeyType, ArrowPrimitiveType, Date32Type, Date64Type, Decimal128Type,
            DurationMicrosecondType, DurationMillisecondType, DurationNanosecondType,
            DurationSecondType, Float16Type, Float32Type, Float64Type, Int16Type, Int32Type,
            Int64Type, Int8Type, Time32MillisecondType, Time32SecondType, Time64MicrosecondType,
            Time64NanosecondType, TimestampMicrosecondType, TimestampMillisecondType,
            TimestampNanosecondType, TimestampSecondType, UInt16Type, UInt32Type, UInt64Type,
            UInt8Type,
        };
        pub use super::_arrow_schema::{DataType, Field, FieldRef, Schema, TimeUnit, UnionMode};
    }
    pub mod error {
        pub use super::_arrow_schema::ArrowError;
    }
}
