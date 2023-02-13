use std::iter;

use arrow2::datatypes::{DataType, Field, Metadata, UnionMode};

use crate::{
    arrow2::display,
    internal::{
        error::{error, fail},
        schema::{
            FieldBuilder, ListTracer, MapTracer, PrimitiveTracer, PrimitiveType, Strategy,
            StructMode, StructTracer, Tracer, TupleTracer, UnionTracer, UnknownTracer,
            STRATEGY_KEY,
        },
    },
    Result,
};

impl From<Strategy> for Metadata {
    fn from(value: Strategy) -> Self {
        let mut res = Metadata::new();
        res.insert(STRATEGY_KEY.to_string(), value.to_string());
        res
    }
}

/// Make sure the field is configured correctly if a strategy is used
///
pub fn check_strategy(field: &Field) -> Result<()> {
    let strategy_str = match field.metadata.get(STRATEGY_KEY) {
        Some(strategy_str) => strategy_str,
        None => return Ok(()),
    };

    match strategy_str.parse::<Strategy>()? {
        Strategy::InconsistentTypes => {
            if !matches!(field.data_type, DataType::Null) {
                fail!(
                    "Invalid strategy for field {name}: {strategy_str} expects the data type Null, found: {dt}",
                    name = display::Str(&field.name),
                    dt = display::DataType(&field.data_type),
                );
            }
        }
        Strategy::UtcStrAsDate64 | Strategy::NaiveStrAsDate64 => {
            if !matches!(field.data_type, DataType::Date64) {
                fail!(
                    "Invalid strategy for field {name}: {strategy_str} expects the data type Date64, found: {dt}",
                    name = display::Str(&field.name),
                    dt = display::DataType(&field.data_type),
                );
            }
        }
        Strategy::TupleAsStruct | Strategy::MapAsStruct => {
            if !matches!(field.data_type, DataType::Struct(_)) {
                fail!(
                    "Invalid strategy for field {name}: {strategy_str} expects the data type Struct, found: {dt}",
                    name = display::Str(&field.name),
                    dt = display::DataType(&field.data_type),
                );
            }
        }
    }

    Ok(())
}

pub fn get_optional_strategy(metadata: &Metadata) -> Result<Option<Strategy>> {
    if let Some(strategy) = metadata.get(STRATEGY_KEY) {
        Ok(Some(strategy.parse()?))
    } else {
        Ok(None)
    }
}

impl FieldBuilder<Field> for Tracer {
    fn to_field(&self, name: &str) -> Result<Field> {
        match self {
            Self::Unknown(tracer) => tracer.to_field(name),
            Self::Primitive(tracer) => tracer.to_field(name),
            Self::List(tracer) => tracer.to_field(name),
            Self::Struct(tracer) => tracer.to_field(name),
            Self::Tuple(tracer) => tracer.to_field(name),
            Self::Union(tracer) => tracer.to_field(name),
            Self::Map(tracer) => tracer.to_field(name),
        }
    }
}

impl FieldBuilder<Field> for UnknownTracer {
    fn to_field(&self, name: &str) -> Result<Field> {
        if !self.finished {
            fail!("Cannot build field {name} from unfinished tracer");
        }
        Ok(Field::new(name, DataType::Null, self.nullable))
    }
}

impl FieldBuilder<Field> for PrimitiveTracer {
    fn to_field(&self, name: &str) -> Result<Field> {
        type T = PrimitiveType;
        type D = DataType;

        if !self.finished {
            fail!("Cannot build field {name} from unfinished tracer");
        }

        match self.item_type {
            T::Unknown => Ok(Field::new(name, D::Null, self.nullable)),
            T::Bool => Ok(Field::new(name, D::Boolean, self.nullable)),
            T::Str => Ok(Field::new(name, D::LargeUtf8, self.nullable)),
            T::I8 => Ok(Field::new(name, D::Int8, self.nullable)),
            T::I16 => Ok(Field::new(name, D::Int16, self.nullable)),
            T::I32 => Ok(Field::new(name, D::Int32, self.nullable)),
            T::I64 => Ok(Field::new(name, D::Int64, self.nullable)),
            T::U8 => Ok(Field::new(name, D::UInt8, self.nullable)),
            T::U16 => Ok(Field::new(name, D::UInt16, self.nullable)),
            T::U32 => Ok(Field::new(name, D::UInt32, self.nullable)),
            T::U64 => Ok(Field::new(name, D::UInt64, self.nullable)),
            T::F32 => Ok(Field::new(name, D::Float32, self.nullable)),
            T::F64 => Ok(Field::new(name, D::Float64, self.nullable)),
        }
    }
}

impl FieldBuilder<Field> for ListTracer {
    fn to_field(&self, name: &str) -> Result<Field> {
        if !self.finished {
            fail!("Cannot build field {name} from unfinished tracer");
        }

        let item_type = self.item_tracer.to_field("element")?;
        let item_type = Box::new(item_type);
        Ok(Field::new(
            name,
            DataType::LargeList(item_type),
            self.nullable,
        ))
    }
}

impl FieldBuilder<Field> for StructTracer {
    fn to_field(&self, name: &str) -> Result<Field> {
        if !self.finished {
            fail!("Cannot build field {name} from unfinished tracer");
        }

        let mut fields = Vec::new();
        for (tracer, name) in iter::zip(&self.field_tracers, &self.field_names) {
            fields.push(tracer.to_field(name)?);
        }
        let mut field = Field::new(name, DataType::Struct(fields), self.nullable);
        if let StructMode::Map = self.mode {
            field
                .metadata
                .insert(STRATEGY_KEY.to_string(), Strategy::MapAsStruct.to_string());
        }
        Ok(field)
    }
}

impl FieldBuilder<Field> for TupleTracer {
    fn to_field(&self, name: &str) -> Result<Field> {
        if !self.finished {
            fail!("Cannot build field {name} from unfinished tracer");
        }

        let mut fields = Vec::new();
        for (idx, tracer) in self.field_tracers.iter().enumerate() {
            fields.push(tracer.to_field(&idx.to_string())?);
        }
        let mut field = Field::new(name, DataType::Struct(fields), self.nullable);
        field.metadata.insert(
            STRATEGY_KEY.to_string(),
            Strategy::TupleAsStruct.to_string(),
        );
        Ok(field)
    }
}

impl FieldBuilder<Field> for UnionTracer {
    fn to_field(&self, name: &str) -> Result<Field> {
        if !self.finished {
            fail!("Cannot build field {name} from unfinished tracer");
        }

        let mut fields = Vec::new();
        for (idx, (name, tracer)) in std::iter::zip(&self.variants, &self.tracers).enumerate() {
            let field = match name {
                Some(name) => tracer.to_field(name)?,
                None => tracer.to_field(&format!("unknown_variant_{idx}"))?,
            };
            fields.push(field);
        }

        let field = Field::new(
            name,
            DataType::Union(fields, None, UnionMode::Dense),
            self.nullable,
        );
        Ok(field)
    }
}

impl FieldBuilder<Field> for MapTracer {
    fn to_field(&self, name: &str) -> Result<Field> {
        if !self.finished {
            fail!("Cannot build field {name} from unfinished tracer");
        }

        let key = self.key.to_field("key")?;
        let value = self.value.to_field("value")?;

        let entries = Field::new("entries", DataType::Struct(vec![key, value]), false);
        let field = Field::new(name, DataType::Map(Box::new(entries), false), self.nullable);
        Ok(field)
    }
}

/// Lookup a nested field among a set of top-level fields
///
/// The `path` argument should be a dotted path to the target field, e.g.,
/// `"parent.child.subchild"`. This helper is intended to simplify modifying
/// nested schemas.
///
/// Example:
///
/// ```rust
/// # use arrow2::datatypes::DataType;
/// # use chrono::NaiveDateTime;
/// # use serde::Serialize;
/// #
/// use serde_arrow::{
///     arrow2::{serialize_into_fields, experimental::find_field_mut},
///     schema::{Strategy, TracingOptions},
/// };
///
/// ##[derive(Serialize, Default)]
/// struct Outer {
///     a: u32,
///     b: Nested,
/// }
///
/// ##[derive(Serialize, Default)]
/// struct Nested {
///     dt: NaiveDateTime,
/// }
///
/// let mut fields = serialize_into_fields(
///     &[Outer::default()],
///     TracingOptions::default(),
/// ).unwrap();
///
/// let dt_field = find_field_mut(&mut fields, "b.dt").unwrap();
/// dt_field.data_type = DataType::Date64;
/// dt_field.metadata = Strategy::NaiveStrAsDate64.into();
/// ```
pub fn find_field_mut<'f>(fields: &'f mut [Field], path: &'_ str) -> Result<&'f mut Field> {
    if path.is_empty() {
        fail!("Cannot get field with empty path");
    } else if let Some((head, tail)) = path.split_once('.') {
        let field = find_field_shallow(fields, head)?;
        let fields = get_child_fields(&mut field.data_type)?;
        find_field_mut(fields, tail)
    } else {
        find_field_shallow(fields, path)
    }
}

fn find_field_shallow<'f>(fields: &'f mut [Field], name: &'_ str) -> Result<&'f mut Field> {
    fields
        .iter_mut()
        .find(|f| f.name == name)
        .ok_or_else(|| error!("Cannot find field {name}"))
}

fn get_child_fields(dt: &mut DataType) -> Result<&mut [Field]> {
    match dt {
        DataType::Struct(fields) | DataType::Union(fields, _, _) => Ok(fields),
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            get_child_fields(&mut field.as_mut().data_type)
        }
        DataType::Extension(_, dt, _) | DataType::Dictionary(_, dt, _) => get_child_fields(dt),
        DataType::Map(field, _) => {
            let fields = match &mut field.data_type {
                DataType::Struct(fields) => fields,
                dt => fail!(
                    "Expected struct as the interior type of a map, found: {dt}",
                    dt = display::DataType(dt)
                ),
            };
            Ok(fields)
        }
        dt => fail!(
            "Data type {dt} does not support nested fields",
            dt = display::DataType(dt)
        ),
    }
}
