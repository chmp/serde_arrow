use crate::internal::schema::GenericField;

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum DictionaryIndex {
    U8(usize),
    U16(usize),
    U32(usize),
    U64(usize),
    I8(usize),
    I16(usize),
    I32(usize),
    I64(usize),
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum DictionaryValue {
    Utf8 { buffer: usize, offsets: usize },
    LargeUtf8 { buffer: usize, offsets: usize },
}

macro_rules! define_array_mapping {
    (
        $(
            $variant:ident {
                $($field:ident:$ty:ty,)*
            },
        )*
    ) => {
        /// Map an array to its corresponding buffers
        #[derive(Debug, Clone)]
        pub enum ArrayMapping {
            $(
                $variant {
                    field: GenericField,
                    validity: Option<usize>,
                    $( $field:$ty, )*
                },
            )*
        }

        impl ArrayMapping {
            pub fn get_field(&self) -> &GenericField {
                match self {
                    $(  ArrayMapping::$variant { field, .. } => field, )*
                }
            }

            pub fn get_validity(&self) -> Option<usize> {
                match self {
                    $(  ArrayMapping::$variant { validity, .. } => *validity, )*
                }
            }
        }
    };
}

#[rustfmt::skip]
define_array_mapping!(
    Null {
        buffer: usize,
    },
    Bool {
        buffer: usize,
    },
    U8 {
        buffer: usize,
    },
    U16 {
        buffer: usize,
    },
    U32 {
        buffer: usize,
    },
    U64 {
        buffer: usize,
    },
    I8 {
        buffer: usize,
    },
    I16 {
        buffer: usize,
    },
    I32 {
        buffer: usize,
    },
    I64 {
        buffer: usize,
    },
    F16 {
        buffer: usize,
    },
    F32 {
        buffer: usize,
    },
    F64 {
        buffer: usize,
    },
    Utf8 {
        buffer: usize,
        offsets: usize,
    },
    LargeUtf8 {
        buffer: usize,
        offsets: usize,
    },
    Date64 {
        buffer: usize,
    },
    List {
        item: Box<ArrayMapping>,
        offsets: usize,
    },
    Dictionary {
        dictionary: DictionaryValue,
        indices: DictionaryIndex,
    },
    LargeList {
        item: Box<ArrayMapping>,
        offsets: usize,
    },
    Struct {
        fields: Vec<ArrayMapping>,
    },
    Union {
        fields: Vec<ArrayMapping>,
        types: usize,
    },
    Map {
        offsets: usize,
        entries: Box<ArrayMapping>,
    },
);
