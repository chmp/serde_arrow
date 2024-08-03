use crate::internal::{
    deserialization::array_deserializer::ArrayDeserializer,
    error::{fail, Result},
    schema::GenericField,
};

use crate::_impl::arrow2::array::Array;

pub fn build_struct_fields<'a>(
    fields: &[GenericField],
    arrays: &[&'a dyn Array],
) -> Result<(Vec<(String, ArrayDeserializer<'a>)>, usize)> {
    if fields.len() != arrays.len() {
        fail!(
            "different number of fields ({}) and arrays ({})",
            fields.len(),
            arrays.len()
        );
    }
    let len = arrays.first().map(|array| array.len()).unwrap_or_default();

    let mut deserializers = Vec::new();
    for (field, &array) in std::iter::zip(fields, arrays) {
        if array.len() != len {
            fail!("arrays of different lengths are not supported");
        }
        let deserializer = ArrayDeserializer::new(field.strategy.as_ref(), array.try_into()?)?;
        deserializers.push((field.name.clone(), deserializer));
    }

    Ok((deserializers, len))
}
