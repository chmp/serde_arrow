use crate::internal::common::{define_bytecode, DictionaryIndex, DictionaryValue};

#[rustfmt::skip]
define_bytecode!(
    Panic {
        message: String,
    },
    ProgramEnd {},
    LargeListStart {},
    ListStart {},
    MapStart {},
    TupleStructStart {},
    TupleStructItem {},
    TupleStructEnd {},
    UnionEnd {},
    PushNull {
        idx: usize,
    },
    PushU8 {
        idx: usize,
    },
    PushU16 {
        idx: usize,
    },
    PushU32 {
        idx: usize,
    },
    PushU64 {
        idx: usize,
    },
    PushI8 {
        idx: usize,
    },
    PushI16 {
        idx: usize,
    },
    PushI32 {
        idx: usize,
    },
    PushI64 {
        idx: usize,
    },
    PushF16 {
        idx: usize,
    },
    PushF32 {
        idx: usize,
    },
    PushF64 {
        idx: usize,
    },
    PushBool {
        idx: usize,
    },
    PushDate64FromNaiveStr {
        idx: usize,
    },
    PushDate64FromUtcStr {
        idx: usize,
    },
    PushUtf8 {
        buffer: usize,
        offsets: usize,
    },
    PushLargeUtf8 {
        buffer: usize,
        offsets: usize,
    },
    OuterSequenceStart {},
    OuterSequenceItem {
        list_idx: usize,
    },
    OuterSequenceEnd {
        list_idx: usize,
    },
    LargeListItem {
        list_idx: usize,
        offsets: usize,
    },
    LargeListEnd {
        list_idx: usize,
        offsets: usize,
    },
    ListItem {
        list_idx: usize,
        offsets: usize,
    },
    ListEnd {
        list_idx: usize,
        offsets: usize,
    },
    StructItem {
        struct_idx: usize,
        seen: usize,
    },
    /// Process unknown fields by ignoring any events emitted. Nested data is
    /// processed by tracking nesting level.
    StructUnknownField {
        /// The index of the underlying struct
        struct_idx: usize,
        /// The program position of this instruction
        self_pos: usize,
        /// The index of the depth counter
        depth: usize,
    },
    StructStart {
        seen: usize,
    },
    StructField {
        self_pos: usize,
        struct_idx: usize,
        field_name: String,
        field_idx: usize,
        seen: usize,
    },
    StructEnd {
        self_pos: usize,
        struct_idx: usize,
        seen: usize,
    },
    MapItem {
        map_idx: usize,
        offsets: usize,
    },
    MapEnd {
        map_idx: usize,
        offsets: usize,
    },
    OptionMarker {
        self_pos: usize,
        if_none: usize,
        /// The index of the relevant bit buffer on the buffers
        validity: usize,
        /// The index of the relevant null definition of the structure
        null_definition: usize,
    },
    Variant {
        union_idx: usize,
        type_idx: usize,
    },
    PushDictionary {
        values: DictionaryValue,
        indices: DictionaryIndex,
        dictionary: usize,
    },
);

impl Bytecode {
    pub fn is_allowed_jump_target(&self) -> bool {
        !matches!(self, Bytecode::UnionEnd(_))
    }

    pub fn get_next(&self) -> usize {
        dispatch_bytecode!(self, instr => instr.next)
    }

    pub fn set_next(&mut self, val: usize) {
        dispatch_bytecode!(self, instr => { instr.next = val; });
    }
}
