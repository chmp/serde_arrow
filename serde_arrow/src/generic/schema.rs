use std::{collections::HashMap, str::FromStr};

use crate::{
    base::{
        error::{error, fail},
        Event, EventSink,
    },
    Error, Result,
};

/// The metadata key under which to store the strategy (see [Strategy] for details)
///
pub const STRATEGY_KEY: &str = "SERDE_ARROW:strategy";

/// Strategies for handling types without direct match between arrow and serde
///
/// For the correct strategy both the field type and the field metadata must be
/// correctly configured. In particular, when determining the schema from the
/// Rust objects themselves, some field types are incorrectly recognized (e.g.,
/// datetimes).
///
/// For example, to let `serde_arrow` handle date time objects that are
/// serialized to strings (chrono's default), use
///
/// ```rust
/// # #[cfg(feature="arrow2")]
/// # fn main() {
/// # use arrow2::datatypes::{DataType, Field};
/// # use serde_arrow::{STRATEGY_KEY, Strategy};
/// # let mut field = Field::new("my_field", DataType::Null, false);
/// field.data_type = DataType::Date64;
/// field.metadata.insert(
///     STRATEGY_KEY.to_string(),
///     Strategy::UtcDateTimeStr.to_string(),
/// );
/// # }
/// # #[cfg(not(feature="arrow2"))]
/// # fn main() {}
/// ```
///
#[non_exhaustive]
pub enum Strategy {
    /// Arrow: Date64, serde: strings with UTC timezone
    UtcDateTimeStr,
    /// Arrow: Date64, serde: strings without a timezone
    NaiveDateTimeStr,
}

impl std::fmt::Display for Strategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UtcDateTimeStr => write!(f, "UtcDateTimeStr"),
            Self::NaiveDateTimeStr => write!(f, "NaiveDateTimeStr"),
        }
    }
}

impl FromStr for Strategy {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "UtcDateTimeStr" => Ok(Self::UtcDateTimeStr),
            "NaiveDateTimeStr" => Ok(Self::NaiveDateTimeStr),
            _ => fail!("Unknown strategy {s}"),
        }
    }
}

/// A schema traced from a sequence of rust objects
///
/// This object supports
pub struct TracedSchema<F> {
    next: State,
    path: Vec<PathFragment>,
    builder: SchemaBuilder<F>,
}

impl<F> TracedSchema<F> {
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
    pub fn into_fields(self) -> Result<Vec<F>> {
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

impl<F: GenericField> EventSink for TracedSchema<F> {
    fn accept(&mut self, event: Event<'_>) -> Result<()> {
        use State::*;

        self.next = match (self.next, event.to_self()) {
            (StartSequence, Event::StartSequence) => StartRecord,
            (StartRecord, Event::EndSequence) => Done,
            (StartRecord, Event::StartStruct) => Content(0),
            (Content(depth), Event::Some) => {
                self.builder.mark_nullable(&self.path)?;
                Content(depth)
            }
            (Content(depth), Event::Key(name)) => {
                self.path.push(PathFragment::Field(name.to_owned()));
                self.builder.ensure_field_exist(&self.path)?;
                Content(depth)
            }
            (Content(depth), Event::StartSequence) => {
                self.builder.mark_list(&self.path)?;
                self.path.push(PathFragment::Index(0));
                Content(depth + 1)
            }
            (Content(0), Event::EndSequence) => {
                fail!("Top level lists are not supported")
            }
            (Content(depth), Event::EndSequence) => {
                self.pop_required_index()?;
                self.pop_if_not_index();
                Content(depth - 1)
            }
            (Content(depth), Event::StartStruct) => {
                self.builder.mark_struct(&self.path)?;
                Content(depth + 1)
            }
            (Content(0), Event::EndStruct) => StartRecord,
            (Content(depth), Event::EndStruct) => {
                self.pop_if_not_index();
                Content(depth - 1)
            }
            (Content(depth), Event::Null) => {
                self.builder.mark_nullable(&self.path)?;
                self.pop_if_not_index();
                Content(depth)
            }
            (Content(depth), ev) => {
                self.builder.mark_primitive(&self.path, &ev)?;
                self.pop_if_not_index();
                Content(depth)
            }
            (Done, _) => fail!("Finished schema cannot handle extra events"),
            (state, ev) => fail!("Unexpected event {ev} in state {state:?}"),
        };
        Ok(())
    }
}

impl<F: GenericField> TracedSchema<F> {
    fn pop_required_index(&mut self) -> Result<()> {
        if let Some(PathFragment::Index(_)) = self.path.last() {
            self.path.pop();
            Ok(())
        } else {
            fail!("expected index on top");
        }
    }

    fn pop_if_not_index(&mut self) {
        if let Some(PathFragment::Field(_)) = self.path.last() {
            self.path.pop();
        }
    }
}

/// An abstraction over fields of a schema
///
pub trait GenericField: Sized {
    fn new_null(name: String) -> Self;
    fn new_struct(name: String) -> Self;
    fn new_list(name: String) -> Self;
    fn new_primitive(name: String, ev: &Event<'_>) -> Result<Self>;

    fn get_children_mut(&mut self) -> Result<&mut [Self]>;

    fn describe(&self) -> String;
    fn name(&self) -> &str;
    fn is_null(&self) -> bool;
    fn is_struct(&self) -> bool;
    fn is_list(&self) -> bool;
    fn is_primitive(&self, ev: &Event<'_>) -> bool;

    fn get_nullable(&self) -> bool;
    fn set_nullable(&mut self, nullable: bool);
    fn append_child(&mut self, child: Self) -> Result<()>;

    fn configure_serde_arrow_strategy(&mut self, strategy: Strategy) -> Result<()>;
}

#[derive(Debug)]
pub struct SchemaBuilder<F> {
    fields: Vec<F>,
    index: NestedFieldIndex,
}

impl<F> SchemaBuilder<F> {
    pub fn new() -> Self {
        Self {
            fields: Vec::new(),
            index: NestedFieldIndex::new(),
        }
    }
}

impl<F: GenericField> SchemaBuilder<F> {
    pub fn ensure_field_exist(&mut self, path: &[PathFragment]) -> Result<()> {
        let (head, tail) = match path {
            [] => fail!("Cannot handle empty paths"),
            [head @ .., tail] => (head, tail),
        };

        if !head.is_empty() {
            let (index, field) = self.index.lookup_mut(&mut self.fields, head)?;

            if index.contains(tail) {
                return Ok(());
            }

            index.insert(tail)?;
            field.append_child(F::new_null(tail.to_string()))?;
        } else if !self.index.contains(tail) {
            self.index.insert(tail)?;
            self.fields.push(F::new_null(tail.to_string()));
        }
        Ok(())
    }

    pub fn mark_struct(&mut self, path: &[PathFragment]) -> Result<()> {
        let field_name = match path {
            [] => fail!("Cannot mark the root as a struct"),
            [.., PathFragment::Field(name)] => name.as_str(),
            [.., PathFragment::Index(_)] => "element",
        };

        let (_, field) = self.index.lookup_mut(&mut self.fields, path)?;

        if field.is_null() {
            let prev_nullable = field.get_nullable();
            *field = F::new_struct(field_name.to_owned());
            field.set_nullable(prev_nullable);
        } else if !field.is_struct() {
            fail!(
                "Cannot mark field {:?} ({}) as a struct",
                field.describe(),
                PathDisplay(path)
            );
        }

        Ok(())
    }

    pub fn mark_list(&mut self, path: &[PathFragment]) -> Result<()> {
        let field_name = match path {
            [] => fail!("Cannot mark the root as a list"),
            [.., PathFragment::Field(name)] => name.as_str(),
            [.., PathFragment::Index(_)] => "element",
        };

        let (index, field) = self.index.lookup_mut(&mut self.fields, path)?;

        if field.is_null() {
            let prev_nullable = field.get_nullable();
            *field = F::new_list(field_name.to_owned());
            field.set_nullable(prev_nullable);
            index.insert(&PathFragment::Index(0))?;
        } else if !field.is_list() {
            fail!("Cannot mark field {} as a list", PathDisplay(path));
        }

        Ok(())
    }

    pub fn mark_primitive(&mut self, path: &[PathFragment], ev: &Event<'_>) -> Result<()> {
        let (_, field) = self.index.lookup_mut(&mut self.fields, path)?;
        let field_name = match path {
            [] => fail!("Cannot mark the root as primitive"),
            [.., PathFragment::Field(name)] => name.as_str(),
            [.., PathFragment::Index(_)] => "element",
        };

        if field.is_null() {
            let prev_nullable = field.get_nullable();
            *field = F::new_primitive(field_name.to_owned(), ev)?;
            field.set_nullable(prev_nullable);
        } else if !field.is_primitive(ev) {
            fail!("Cannot set field {} to primitive {}", PathDisplay(path), ev);
        }

        Ok(())
    }

    pub fn mark_nullable(&mut self, path: &[PathFragment]) -> Result<()> {
        let (_, field) = self.index.lookup_mut(&mut self.fields, path)?;
        field.set_nullable(true);
        Ok(())
    }
}

#[derive(Debug, Hash, PartialEq, Eq, Clone)]
pub enum PathFragment {
    Field(String),
    Index(usize),
}

impl std::fmt::Display for PathFragment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PathFragment::Field(field) => write!(f, "{field}"),
            PathFragment::Index(index) => write!(f, "{index}"),
        }
    }
}

pub struct PathDisplay<'a>(pub &'a [PathFragment]);

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
    fn from(index: usize) -> Self {
        Self::Index(index)
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

    fn lookup_mut<'field, F: GenericField>(
        &mut self,
        fields: &'field mut [F],
        path: &[PathFragment],
    ) -> Result<(&mut NestedFieldIndex, &'field mut F)> {
        let (head, tail) = match path {
            [head @ .., tail] => (head, tail),
            [] => fail!("Cannot lookup the root as a field"),
        };

        let mut current_index = self;
        let mut current_fields = fields;
        let mut current_path = head;

        while !current_path.is_empty() {
            let head = &current_path[0];
            let (idx, next_index) = current_index
                .fields
                .get_mut(head)
                .ok_or_else(|| error!("Unknown field {head} in path {}", PathDisplay(path)))?;
            let field = current_fields
                .get_mut(*idx)
                .ok_or_else(|| error!("Cannot lookup field"))?;

            current_index = next_index;
            current_fields = field.get_children_mut()?;
            current_path = &current_path[1..];
        }

        let (idx, field_index) = current_index
            .fields
            .get_mut(tail)
            .ok_or_else(|| error!("Unknown field {tail} in path {}", PathDisplay(path)))?;
        let field = &mut current_fields[*idx];

        Ok((field_index, field))
    }
}
