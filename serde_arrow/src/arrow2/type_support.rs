use arrow2::{
    datatypes::{DataType, Field, UnionMode},
    types::f16,
};

use crate::{
    base::{error::fail, Event},
    Error, Result,
};

impl<'a> TryFrom<Event<'a>> for f16 {
    type Error = Error;

    fn try_from(value: Event<'a>) -> Result<Self> {
        match value {
            Event::F32(value) => Ok(f16::from_f32(value)),
            ev => fail!("Cannot convert {ev} to f16"),
        }
    }
}

impl<'a> From<f16> for Event<'a> {
    fn from(value: f16) -> Self {
        Event::F32(value.to_f32())
    }
}

pub(crate) struct DataTypeDisplay<'a>(pub &'a DataType);

impl<'a> std::fmt::Display for DataTypeDisplay<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use DataType::*;

        match self.0 {
            Null => write!(f, "Null"),
            Int8 => write!(f, "Int8"),
            Int16 => write!(f, "Int16"),
            Int32 => write!(f, "Int32"),
            Int64 => write!(f, "Int64"),
            UInt8 => write!(f, "Int8"),
            UInt16 => write!(f, "UInt16"),
            UInt32 => write!(f, "UInt32"),
            UInt64 => write!(f, "UInt64"),
            Boolean => write!(f, "Boolean"),
            Float16 => write!(f, "Float16"),
            Float32 => write!(f, "Float32"),
            Float64 => write!(f, "Float64"),
            Date32 => write!(f, "Date32"),
            Date64 => write!(f, "Date64"),
            Binary => write!(f, "Binary"),
            FixedSizeBinary(n) => write!(f, "FixedSizedBinary({n})"),
            LargeBinary => write!(f, "LargeBinary"),
            Utf8 => write!(f, "Utf8"),
            LargeUtf8 => write!(f, "LargeUft8"),
            List(field) => write!(f, "List({})", FieldDisplay(field)),
            FixedSizeList(field, size) => {
                write!(f, "FixedSizeList({}, {})", FieldDisplay(field), size)
            }
            LargeList(field) => write!(f, "LargeList({})", FieldDisplay(field)),
            Struct(fields) => {
                write!(f, "Struct([")?;
                for (idx, field) in fields.iter().enumerate() {
                    if idx != 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", FieldDisplay(field))?;
                }
                write!(f, "])")?;
                Ok(())
            }
            Map(field, sorted) => write!(f, "Map({}, {})", FieldDisplay(field.as_ref()), sorted),
            Union(fields, indices, mode) => {
                write!(f, "Union(")?;
                for (idx, field) in fields.iter().enumerate() {
                    if idx != 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", FieldDisplay(field))?;
                }

                if let Some(indices) = indices {
                    write!(f, "Some([")?;
                    for (idx_idx, idx) in indices.iter().enumerate() {
                        if idx_idx != 0 {
                            write!(f, ", ")?;
                        }
                        write!(f, "{idx}")?;
                    }
                    write!(f, "]), ")?;
                } else {
                    write!(f, "None, ")?;
                }

                match mode {
                    UnionMode::Sparse => write!(f, "Sparse)")?,
                    UnionMode::Dense => write!(f, "Dense)")?,
                }

                Ok(())
            }
            // TODO: implement the remaining data types
            dt => write!(f, "{dt:?}"),
        }
    }
}

pub(crate) struct FieldDisplay<'a>(pub &'a Field);

impl<'a> std::fmt::Display for FieldDisplay<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.0.name, DataTypeDisplay(&self.0.data_type))?;

        if !self.0.metadata.is_empty() {
            write!(f, "[")?;
            for (idx, (key, val)) in self.0.metadata.iter().enumerate() {
                if idx != 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{key}: {val}")?;
            }
            write!(f, "]")?;
        }

        Ok(())
    }
}
