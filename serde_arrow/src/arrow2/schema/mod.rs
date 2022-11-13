pub(crate) mod editor;

use std::{collections::HashMap, str::FromStr};

use arrow2::datatypes::{DataType, Field};

use crate::{
    error,
    event::{Event, EventSink},
    fail, Error, Result,
};

pub const STRATEGY_KEY: &'static str = "SERDE_ARROW:strategy";

pub enum Strategy {
    DateTimeStr,
    NaiveDateTimeStr,
}

impl Strategy {
    pub fn configure_field(&self, field: &mut Field) -> Result<()> {
        match self {
            Self::DateTimeStr | Self::NaiveDateTimeStr => {
                if !matches!(
                    field.data_type,
                    DataType::Null | DataType::Utf8 | DataType::LargeUtf8
                ) {
                    fail!(
                        "Cannot configure DateTimeStr for field of type {:?}",
                        field.data_type
                    );
                }
                field.data_type = DataType::Date64;
                field
                    .metadata
                    .insert(String::from(STRATEGY_KEY), self.to_string());
            }
        }

        Ok(())
    }
}

impl std::fmt::Display for Strategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DateTimeStr => write!(f, "DateTimeStr"),
            Self::NaiveDateTimeStr => write!(f, "NaiveDateTimeStr"),
        }
    }
}

impl FromStr for Strategy {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "DateTimeStr" => Ok(Self::DateTimeStr),
            "NaiveDateTimeStr" => Ok(Self::NaiveDateTimeStr),
            _ => fail!("Unknown strategy {s}"),
        }
    }
}

/// A schema traced from a sequence of rust objects
///
/// This object supports
pub struct TracedSchema {
    next: State,
    path: Vec<PathFragment>,
    builder: SchemaBuilder,
}

impl TracedSchema {
    pub fn new() -> Self {
        Self {
            next: State::StartSequence,
            path: Vec::new(),
            builder: SchemaBuilder::new(),
        }
    }

    /// Get the traced fields
    ///
    /// This function checks the internal state of the tracer and the detected
    /// fields.
    ///
    pub fn into_fields(self) -> Result<Vec<Field>> {
        if !matches!(self.next, State::Done) {
            fail!("Incomplete trace");
        }

        Ok(self.builder.fields)
    }
}

#[derive(Debug, Clone, Copy)]
enum State {
    StartSequence,
    StartRecord,
    Content(usize),
    Done,
}

impl EventSink for TracedSchema {
    fn accept(&mut self, event: Event<'_>) -> Result<()> {
        use State::*;

        self.next = match (self.next, event.as_ref()) {
            (StartSequence, Event::StartSequence) => StartRecord,
            (StartRecord, Event::EndSequence) => Done,
            (StartRecord, Event::StartMap) => Content(0),
            (Content(depth), Event::Key(name)) => {
                self.path.push(PathFragment::Field(name.to_owned()));
                self.builder.ensure_field_exist(&self.path)?;
                Content(depth)
            }
            (Content(_), Event::StartSequence | Event::EndSequence) => {
                fail!("Nested sequences are not supported")
            }
            (Content(depth), Event::StartMap) => {
                self.builder.mark_struct(&self.path)?;
                Content(depth + 1)
            }
            (Content(0), Event::EndMap) => StartRecord,
            (Content(depth), Event::EndMap) => {
                self.path.pop();
                Content(depth - 1)
            }
            (Content(depth), Event::Null) => {
                self.builder.mark_null(&self.path)?;
                self.path.pop();
                Content(depth)
            }
            (Content(depth), Event::Some) => Content(depth),
            (Content(depth), ev) => {
                let data_type = get_event_data_type(ev)?;
                self.builder.mark_primitive(&self.path, data_type)?;
                self.path.pop();
                Content(depth)
            }
            (Done, _) => fail!("Finished schema cannot handle extra events"),
            (state, ev) => fail!("Unexpected event {ev} in state {state:?}"),
        };
        Ok(())
    }
}

#[derive(Debug)]
pub struct SchemaBuilder {
    fields: Vec<Field>,
    index: NestedFieldIndex,
}

impl SchemaBuilder {
    pub fn new() -> Self {
        Self {
            fields: Vec::new(),
            index: NestedFieldIndex::new(),
        }
    }

    pub fn ensure_field_exist(&mut self, path: &[PathFragment]) -> Result<()> {
        let (head, tail) = match path {
            [] => fail!("Cannot handle empty paths"),
            [head @ .., tail] => (head, tail),
        };

        let (index, fields) = self.index.lookup_parent_mut(&mut self.fields, head)?;
        if index.contains(tail) {
            return Ok(());
        }

        index.insert(tail)?;
        fields.push(Field::new(tail.to_string(), DataType::Null, false));

        Ok(())
    }

    pub fn mark_struct(&mut self, path: &[PathFragment]) -> Result<()> {
        let tail = match path {
            [] => return Ok(()),
            [.., tail] => tail,
        };

        let field = self.index.lookup_leaf_mut(&mut self.fields, path)?;

        match field.data_type() {
            DataType::Null => {
                *field = Field::new(tail.to_string(), DataType::Struct(Vec::new()), false);
            }
            DataType::Struct(_) => {}
            dt => fail!("Cannot mark data {dt:?} as a struct"),
        };

        Ok(())
    }

    pub fn mark_primitive(&mut self, path: &[PathFragment], data_type: DataType) -> Result<()> {
        let field = self.index.lookup_leaf_mut(&mut self.fields, path)?;

        if let DataType::Null = field.data_type {
            field.data_type = data_type;
        } else if field.data_type != data_type {
            fail!(
                "Cannot set field {} with data type {:?} to data type {:?}",
                PathDisplay(path),
                field.data_type,
                data_type
            );
        }

        Ok(())
    }

    pub fn mark_null(&mut self, path: &[PathFragment]) -> Result<()> {
        let field = self.index.lookup_leaf_mut(&mut self.fields, path)?;
        field.is_nullable = true;
        Ok(())
    }
}

#[derive(Debug, Hash, PartialEq, Eq, Clone)]
pub enum PathFragment {
    Field(String),
    Index,
}

impl std::fmt::Display for PathFragment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PathFragment::Field(field) => write!(f, "{field}"),
            PathFragment::Index => write!(f, "0"),
        }
    }
}

struct PathDisplay<'a>(&'a [PathFragment]);

impl<'a> std::fmt::Display for PathDisplay<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[")?;
        for (idx, step) in self.0.iter().enumerate() {
            if idx != 0 {
                write!(f, ".")?;
            }
            write!(f, "{step}")?;
        }
        write!(f, "]")?;
        Ok(())
    }
}

impl From<String> for PathFragment {
    fn from(field: String) -> Self {
        Self::Field(field)
    }
}

impl From<&String> for PathFragment {
    fn from(field: &String) -> Self {
        Self::Field(field.to_owned())
    }
}

impl From<&str> for PathFragment {
    fn from(field: &str) -> Self {
        Self::Field(field.to_owned())
    }
}

impl From<usize> for PathFragment {
    fn from(_: usize) -> Self {
        Self::Index
    }
}

pub trait IntoPath {
    fn into_path(self) -> Result<Vec<PathFragment>>;
}

macro_rules! implement_into_path_tuple {
    ($($idx:tt : $ty:ident),*) => {
        impl<$($ty : Into<PathFragment>),*> IntoPath for ($($ty,)*) {
            fn into_path(self) -> Result<Vec<PathFragment>> {
                Ok(vec![$(self.$idx.into()),*])
            }
        }
    };
}

implement_into_path_tuple!();
implement_into_path_tuple!(0: A);
implement_into_path_tuple!(0: A, 1: B);
implement_into_path_tuple!(0: A, 1: B, 2: C);
implement_into_path_tuple!(0: A, 1: B, 2: C, 3: D);
implement_into_path_tuple!(0: A, 1: B, 2: C, 3: D, 4: E);
implement_into_path_tuple!(0: A, 1: B, 2: C, 3: D, 4: E, 5: F);
implement_into_path_tuple!(0: A, 1: B, 2: C, 3: D, 4: E, 5: F, 6: G);

impl IntoPath for Vec<PathFragment> {
    fn into_path(self) -> Result<Vec<PathFragment>> {
        Ok(self)
    }
}

#[derive(Debug, Default)]
struct NestedFieldIndex {
    fields: HashMap<PathFragment, (usize, NestedFieldIndex)>,
}

impl NestedFieldIndex {
    fn new() -> Self {
        Self::default()
    }

    fn insert(&mut self, fragment: &PathFragment) -> Result<()> {
        if !self.fields.contains_key(fragment) {
            let new_idx = self.fields.len();
            self.fields
                .insert(fragment.clone(), (new_idx, NestedFieldIndex::new()));
        }
        Ok(())
    }

    fn contains(&self, fragment: &PathFragment) -> bool {
        self.fields.contains_key(fragment)
    }

    fn index(&self, fragment: &PathFragment) -> Result<usize> {
        let (idx, _) = self
            .fields
            .get(fragment)
            .ok_or_else(|| error!("Unknown field {fragment}"))?;
        Ok(*idx)
    }

    fn lookup_parent_mut<'field, 'info>(
        &mut self,
        fields: &'field mut Vec<Field>,
        path: &[PathFragment],
    ) -> Result<(&mut NestedFieldIndex, &'field mut Vec<Field>)> {
        let mut current_index = self;
        let mut current_fields = fields;
        let mut current_path = path;

        while !current_path.is_empty() {
            let head = &current_path[0];
            let (idx, next_index) = current_index
                .fields
                .get_mut(head)
                .ok_or_else(|| error!("Unknown field {head}"))?;
            let field = current_fields
                .get_mut(*idx)
                .ok_or_else(|| error!("Cannot lookup field"))?;

            current_index = next_index;
            current_fields = get_fields_of_type_mut(&mut field.data_type)?;
            current_path = &current_path[1..];
        }
        Ok((current_index, current_fields))
    }

    fn lookup_leaf_mut<'field, 'info>(
        &mut self,
        fields: &'field mut Vec<Field>,
        path: &[PathFragment],
    ) -> Result<&'field mut Field> {
        let (head, tail) = match path {
            [] => fail!("Cannot lookup the root as a leaf"),
            [head @ .., tail] => (head, tail),
        };

        let (index, fields) = self.lookup_parent_mut(fields, head)?;
        let idx = index.index(tail)?;
        let field = fields
            .get_mut(idx)
            .ok_or_else(|| error!("Inconsistent field {tail}"))?;

        Ok(field)
    }
}

fn get_fields_of_type_mut(data_type: &mut DataType) -> Result<&mut Vec<Field>> {
    match data_type {
        DataType::Struct(fields) => Ok(fields),
        _ => fail!("Unnested data type {data_type:?}"),
    }
}

fn get_event_data_type(event: Event<'_>) -> Result<DataType> {
    match event {
        Event::Bool(_) => Ok(DataType::Boolean),
        Event::I8(_) => Ok(DataType::Int8),
        Event::I16(_) => Ok(DataType::Int16),
        Event::I32(_) => Ok(DataType::Int32),
        Event::I64(_) => Ok(DataType::Int64),
        Event::U8(_) => Ok(DataType::UInt8),
        Event::U16(_) => Ok(DataType::UInt16),
        Event::U32(_) => Ok(DataType::UInt32),
        Event::U64(_) => Ok(DataType::UInt64),
        Event::Str(_) | Event::String(_) => Ok(DataType::Utf8),
        Event::F32(_) => Ok(DataType::Float32),
        Event::F64(_) => Ok(DataType::Float64),
        ev => fail!("Cannot determine arrow2 data type for {ev}"),
    }
}
