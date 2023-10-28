use serde::{Deserialize, Serialize};

use crate::internal::{
    common::{BufferExtract, Buffers},
    deserialization,
    error::{Error, Result},
    schema::GenericField,
    serialization,
    sink::{serialize_into_sink, EventSerializer, EventSink},
    source::deserialize_from_source,
};

pub struct GenericBuilder(pub serialization::Interpreter);

impl GenericBuilder {
    pub fn new_for_array(field: GenericField) -> Result<Self> {
        let program = serialization::compile_serialization(
            std::slice::from_ref(&field),
            serialization::CompilationOptions::default().wrap_with_struct(false),
        )?;
        let interpreter = serialization::Interpreter::new(program);

        Ok(Self(interpreter))
    }

    pub fn new_for_arrays(fields: &[GenericField]) -> Result<Self> {
        let program = serialization::compile_serialization(
            fields,
            serialization::CompilationOptions::default(),
        )?;
        let interpreter = serialization::Interpreter::new(program);

        Ok(Self(interpreter))
    }

    pub fn push<T: Serialize + ?Sized>(&mut self, item: &T) -> Result<()> {
        self.0.accept_start_sequence()?;
        self.0.accept_item()?;
        item.serialize(EventSerializer(&mut self.0))?;
        self.0.accept_end_sequence()?;
        self.0.finish()
    }

    pub fn extend<T: Serialize + ?Sized>(&mut self, items: &T) -> Result<()> {
        serialize_into_sink(&mut self.0, items)
    }
}

pub fn deserialize_from_array<'de, T, F, A>(field: &'de F, array: &'de A) -> Result<T>
where
    T: Deserialize<'de>,
    F: 'static,
    GenericField: TryFrom<&'de F, Error = Error>,
    A: BufferExtract + ?Sized,
{
    let field = GenericField::try_from(field)?;
    let num_items = array.len();

    let mut buffers = Buffers::new();
    let mapping = array.extract_buffers(&field, &mut buffers)?;

    let interpreter = deserialization::compile_deserialization(
        num_items,
        std::slice::from_ref(&mapping),
        buffers,
        deserialization::CompilationOptions::default().wrap_with_struct(false),
    )?;
    deserialize_from_source(interpreter)
}
