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

/// Map an array to its corresponding buffers
#[derive(Debug, Clone)]
pub enum ArrayMapping {
    Null {
        field: GenericField,
        buffer: usize,
        validity: Option<usize>,
    },
    Bool {
        field: GenericField,
        buffer: usize,
        validity: Option<usize>,
    },
    U8 {
        field: GenericField,
        buffer: usize,
        validity: Option<usize>,
    },
    U16 {
        field: GenericField,
        buffer: usize,
        validity: Option<usize>,
    },
    U32 {
        field: GenericField,
        buffer: usize,
        validity: Option<usize>,
    },
    U64 {
        field: GenericField,
        buffer: usize,
        validity: Option<usize>,
    },
    I8 {
        field: GenericField,
        buffer: usize,
        validity: Option<usize>,
    },
    I16 {
        field: GenericField,
        buffer: usize,
        validity: Option<usize>,
    },
    I32 {
        field: GenericField,
        buffer: usize,
        validity: Option<usize>,
    },
    I64 {
        field: GenericField,
        buffer: usize,
        validity: Option<usize>,
    },
    F16 {
        field: GenericField,
        buffer: usize,
        validity: Option<usize>,
    },
    F32 {
        field: GenericField,
        buffer: usize,
        validity: Option<usize>,
    },
    F64 {
        field: GenericField,
        buffer: usize,
        validity: Option<usize>,
    },
    Utf8 {
        field: GenericField,
        buffer: usize,
        offsets: usize,
        validity: Option<usize>,
    },
    LargeUtf8 {
        field: GenericField,
        buffer: usize,
        offsets: usize,
        validity: Option<usize>,
    },
    Date64 {
        field: GenericField,
        buffer: usize,
        validity: Option<usize>,
    },
    #[allow(unused)]
    List {
        field: GenericField,
        item: Box<ArrayMapping>,
        offsets: usize,
        validity: Option<usize>,
    },
    Dictionary {
        field: GenericField,
        dictionary: DictionaryValue,
        indices: DictionaryIndex,
        validity: Option<usize>,
    },
    LargeList {
        field: GenericField,
        item: Box<ArrayMapping>,
        offsets: usize,
        validity: Option<usize>,
    },
    Struct {
        field: GenericField,
        fields: Vec<ArrayMapping>,
        validity: Option<usize>,
    },
    Union {
        field: GenericField,
        fields: Vec<ArrayMapping>,
        types: usize,
    },
    Map {
        field: GenericField,
        offsets: usize,
        validity: Option<usize>,
        entries: Box<ArrayMapping>,
    },
}
