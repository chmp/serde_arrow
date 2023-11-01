use serde::{ser::SerializeSeq, Deserialize, Serialize};

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

/// A wrapper around a sequence of items
///
/// When serialized or deserialized, it behaves as if each item was wrapped in a
/// struct with a single attribute `"item"`.
///
/// ```rust
/// # fn main() -> serde_arrow::_impl::PanicOnError<()> {
/// # use serde_arrow::utils::Items;
/// #
/// assert_eq!(
///     serde_json::to_string(&Items([13, 21]))?,
///     r#"[{"item":13},{"item":21}]"#,
/// );
///
/// let Items(items): Items<Vec<u32>> = serde_json::from_str(r#"[
///     {"item": 21},
///     {"item": 42}
/// ]"#)?;
/// assert_eq!(items, &[21, 42]);
/// # Ok(())
/// # }
/// ```
pub struct Items<T>(
    /// The wrapped object
    pub T,
);

/// A wrapper around a single item
///
/// When serialized or deserialized, it behaves as if the Item was wrapped in a
/// struct with a single attribute `"item"`.
///
/// ```rust
/// # fn main() -> serde_arrow::_impl::PanicOnError<()> {
/// # use serde_arrow::utils::Item;
/// #
/// assert_eq!(serde_json::to_string(&Item(42))?, r#"{"item":42}"#);
///
/// let Item(item): Item<u32> = serde_json::from_str(r#"{"item":21}"#)?;
/// assert_eq!(item, 21);
/// # Ok(())
/// # }
/// ```
pub struct Item<T>(
    /// The wrapped object
    pub T,
);

impl<T: Serialize> Serialize for Item<T> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        #[derive(Debug, Serialize)]
        struct Item<'a, T> {
            item: &'a T,
        }
        Item { item: &self.0 }.serialize(serializer)
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for Item<T> {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        #[derive(Debug, Deserialize)]
        struct Item<T> {
            item: T,
        }
        let item = Item::<T>::deserialize(deserializer)?;
        Ok(Item(item.item))
    }
}

// TODO: implement for all types?
impl<'de, T: Deserialize<'de>> Deserialize<'de> for Items<Vec<T>> {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let items = Vec::<Item<T>>::deserialize(deserializer)?
            .into_iter()
            .map(|item| item.0)
            .collect();
        Ok(Items(items))
    }
}

impl<T: Serialize> Serialize for Items<Vec<T>> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        Items(self.0.as_slice()).serialize(serializer)
    }
}

impl<'a, T: Serialize> Serialize for Items<&'a Vec<T>> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        Items(self.0.as_slice()).serialize(serializer)
    }
}

impl<const N: usize, T: Serialize> Serialize for Items<[T; N]> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        Items(self.0.as_slice()).serialize(serializer)
    }
}

impl<'a, const N: usize, T: Serialize> Serialize for Items<&'a [T; N]> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        Items(self.0.as_slice()).serialize(serializer)
    }
}

impl<'a, T: Serialize> Serialize for Items<&'a [T]> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
        for item in self.0 {
            seq.serialize_element(&Item(item))?;
        }
        seq.end()
    }
}
