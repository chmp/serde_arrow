//! Helpers to work with bytecodes
mod array_mapping;
mod buffers;

pub use array_mapping::{ArrayMapping, DictionaryIndex, DictionaryValue};
pub use buffers::{BitBuffer, BufferExtract, Buffers, NullBuffer, Offset, OffsetBuilder};

macro_rules! define_bytecode {
    (
        $(
            $variant:ident {
                $(
                    $(#[doc = $doc:literal])?
                    $field:ident: $ty:ty,
                )*
            },
        )*
    ) => {
        #[derive(Debug, PartialEq, Clone)]
        pub enum Bytecode {
            $($variant($variant),)*
        }

        $(
            #[derive(Debug, PartialEq, Clone)]
            pub struct $variant {
                pub next: usize,
                $(
                    $(#[doc = $doc])?
                    pub $field: $ty,
                )*
            }
        )*

        $(
            impl From<$variant> for Bytecode {
                fn from(val: $variant) -> Bytecode {
                    Bytecode::$variant(val)
                }
            }
        )*

        macro_rules! dispatch_bytecode {
            ($obj:expr, $instr:ident => $block:expr) => {
                match $obj {
                    $(Bytecode::$variant($instr) => $block,)*
                }
            };
        }

        pub(crate) use dispatch_bytecode;
    }
}

pub(crate) use define_bytecode;
