//! Helpers to work with bytecodes
mod array_mapping;
mod buffers;
mod checks;

pub use array_mapping::{ArrayMapping, DictionaryIndex, DictionaryValue};
pub use buffers::{
    BitBuffer, BufferExtract, Buffers, MutableBitBuffer, MutableCountBuffer, MutableOffsetBuffer,
};
pub use checks::check_supported_list_layout;

macro_rules! define_bytecode {
    (
        $(
            $(#[doc = $variant_doc:literal])*
            $variant:ident {
                $(
                    $(#[doc = $field_doc:literal])*
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
            $(#[doc = $variant_doc])*
            #[derive(Debug, PartialEq, Clone)]
            pub struct $variant {
                pub next: usize,
                $(
                    $(#[doc = $field_doc])*
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
