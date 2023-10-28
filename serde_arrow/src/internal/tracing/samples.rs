use std::{
    collections::{BTreeSet, HashMap},
    iter,
};

use crate::internal::{
    error::{fail, Result},
    event::Event,
    sink::EventSink,
    tracing::tracer::{PrimitiveTracer, UnknownTracer},
};

use super::{
    super::{
        schema::{GenericDataType, GenericField, Strategy},
        sink::macros,
    },
    TracingOptions,
};

pub enum SamplesTracer {
    Unknown(UnknownTracer),
    Primitive(PrimitiveTracer),
    List(ListTracer),
    Map(MapTracer),
    Struct(StructTracer),
    Union(UnionTracer),
    Tuple(TupleTracer),
}

impl SamplesTracer {
    pub fn new(path: String, options: TracingOptions) -> Self {
        Self::Unknown(UnknownTracer::new(path, options))
    }

    pub fn to_field(&self, name: &str) -> Result<GenericField> {
        use SamplesTracer::*;
        match self {
            Unknown(t) => t.to_field(name),
            List(t) => t.to_field(name),
            Map(t) => t.to_field(name),
            Primitive(t) => t.to_field(name),
            Tuple(t) => t.to_field(name),
            Union(t) => t.to_field(name),
            Struct(t) => t.to_field(name),
        }
    }

    pub fn mark_nullable(&mut self) {
        use SamplesTracer::*;
        match self {
            Unknown(t) => {
                t.nullable = true;
            }
            List(t) => {
                t.nullable = true;
            }
            Map(t) => {
                t.nullable = true;
            }
            Primitive(t) => {
                t.nullable = true;
            }
            Tuple(t) => {
                t.nullable = true;
            }
            Union(t) => {
                t.nullable = true;
            }
            Struct(t) => {
                t.nullable = true;
            }
        }
    }

    fn reset(&mut self) -> Result<()> {
        match self {
            Self::Unknown(tracer) => tracer.reset(),
            Self::List(tracer) => tracer.reset(),
            Self::Struct(tracer) => tracer.reset(),
            Self::Primitive(tracer) => tracer.reset(),
            Self::Tuple(tracer) => tracer.reset(),
            Self::Union(tracer) => tracer.reset(),
            Self::Map(tracer) => tracer.reset(),
        }
    }

    pub fn finish(&mut self) -> Result<()> {
        match self {
            Self::Unknown(tracer) => tracer.finish(),
            Self::List(tracer) => tracer.finish(),
            Self::Struct(tracer) => tracer.finish(),
            Self::Primitive(tracer) => tracer.finish(),
            Self::Tuple(tracer) => tracer.finish(),
            Self::Union(tracer) => tracer.finish(),
            Self::Map(tracer) => tracer.finish(),
        }
    }
}

impl EventSink for SamplesTracer {
    macros::forward_specialized_to_generic!();

    fn accept(&mut self, event: Event<'_>) -> Result<()> {
        match self {
            // NOTE: unknown tracer is the only tracer that change the internal type
            Self::Unknown(tracer) => match event {
                Event::Some | Event::Null => tracer.nullable = true,
                Event::Bool(_)
                | Event::I8(_)
                | Event::I16(_)
                | Event::I32(_)
                | Event::I64(_)
                | Event::U8(_)
                | Event::U16(_)
                | Event::U32(_)
                | Event::U64(_)
                | Event::F32(_)
                | Event::F64(_)
                | Event::Str(_)
                | Event::OwnedStr(_) => {
                    let mut tracer = PrimitiveTracer::new(
                        tracer.path.clone(),
                        tracer.options.clone(),
                        GenericDataType::Null,
                        tracer.nullable,
                    );
                    tracer.accept(event)?;
                    *self = SamplesTracer::Primitive(tracer)
                }
                Event::StartSequence => {
                    let mut tracer = ListTracer::new(
                        tracer.path.clone(),
                        tracer.options.clone(),
                        tracer.nullable,
                    );
                    tracer.accept(event)?;
                    *self = SamplesTracer::List(tracer);
                }
                Event::StartStruct => {
                    let mut tracer = StructTracer::new(
                        tracer.path.clone(),
                        tracer.options.clone(),
                        StructMode::Struct,
                        tracer.nullable,
                    );
                    tracer.accept(event)?;
                    *self = SamplesTracer::Struct(tracer);
                }
                Event::StartTuple => {
                    let mut tracer = TupleTracer::new(
                        tracer.path.clone(),
                        tracer.options.clone(),
                        tracer.nullable,
                    );
                    tracer.accept(event)?;
                    *self = SamplesTracer::Tuple(tracer);
                }
                Event::StartMap => {
                    if tracer.options.map_as_struct {
                        let mut tracer = StructTracer::new(
                            tracer.path.clone(),
                            tracer.options.clone(),
                            StructMode::Map,
                            tracer.nullable,
                        );
                        tracer.accept(event)?;
                        *self = SamplesTracer::Struct(tracer);
                    } else {
                        let mut tracer = MapTracer::new(
                            tracer.path.clone(),
                            tracer.options.clone(),
                            tracer.nullable,
                        );
                        tracer.accept(event)?;
                        *self = SamplesTracer::Map(tracer);
                    }
                }
                Event::Variant(_, _) => {
                    let mut tracer = UnionTracer::new(
                        tracer.path.clone(),
                        tracer.options.clone(),
                        tracer.nullable,
                    );
                    tracer.accept(event)?;
                    *self = SamplesTracer::Union(tracer)
                }
                ev if ev.is_end() => fail!(
                    "Invalid end nesting events for unknown tracer ({path})",
                    path = tracer.path
                ),
                ev => fail!(
                    "Internal error unmatched event {ev} in Tracer ({path})",
                    path = tracer.path
                ),
            },
            Self::List(tracer) => tracer.accept(event)?,
            Self::Struct(tracer) => tracer.accept(event)?,
            Self::Primitive(tracer) => tracer.accept(event)?,
            Self::Tuple(tracer) => tracer.accept(event)?,
            Self::Union(tracer) => tracer.accept(event)?,
            Self::Map(tracer) => tracer.accept(event)?,
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        SamplesTracer::finish(self)
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum StructMode {
    Struct,
    Map,
}

pub struct StructTracer {
    pub path: String,
    pub options: TracingOptions,
    pub nullable: bool,
    pub state: StructTracerState,
    pub mode: StructMode,
    pub fields: Vec<Field>,
    pub index: HashMap<String, usize>,
    pub current_sample: usize,
}

pub struct Field {
    pub name: String,
    pub tracer: SamplesTracer,
    pub last_seen_in_sample: usize,
}

#[derive(Debug, Clone, Copy)]
pub enum StructTracerState {
    /// The tracer is waiting for the next key
    WaitForKey,
    /// The tracer is currently processing the next key
    InKey,
    /// The tracer is currently tracing a value for `(field, depth)`
    InValue(usize, usize),
    /// The tracer is finished
    Finished,
}

impl StructTracer {
    pub fn new(path: String, options: TracingOptions, mode: StructMode, nullable: bool) -> Self {
        Self {
            path,
            options,
            mode,
            fields: Vec::new(),
            index: HashMap::new(),
            nullable,
            state: StructTracerState::WaitForKey,
            current_sample: 0,
        }
    }

    pub fn to_field(&self, name: &str) -> Result<GenericField> {
        if !matches!(self.state, StructTracerState::Finished) {
            fail!("Cannot build field {name} from unfinished tracer");
        }
        let mut res_field = GenericField::new(name, GenericDataType::Struct, self.nullable);
        for field in &self.fields {
            res_field.children.push(field.tracer.to_field(&field.name)?);
        }

        if let StructMode::Map = self.mode {
            res_field.children.sort_by(|a, b| a.name.cmp(&b.name));
            res_field.strategy = Some(Strategy::MapAsStruct);
        }
        Ok(res_field)
    }

    pub fn reset(&mut self) -> Result<()> {
        if !matches!(self.state, StructTracerState::Finished) {
            fail!("Cannot reset unfinished tracer");
        }
        for field in &mut self.fields {
            field.tracer.reset()?;
        }

        self.state = StructTracerState::WaitForKey;
        Ok(())
    }

    pub fn finish(&mut self) -> Result<()> {
        if !matches!(self.state, StructTracerState::WaitForKey) {
            fail!("Incomplete struct in schema tracing");
        }

        for field in &mut self.fields {
            field.tracer.finish()?;
        }

        self.state = StructTracerState::Finished;

        Ok(())
    }
}

impl EventSink for StructTracer {
    macros::forward_specialized_to_generic!();

    fn accept(&mut self, event: Event<'_>) -> Result<()> {
        use StructTracerState::*;
        type E<'a> = Event<'a>;

        self.state = match (self.state, event) {
            (WaitForKey, E::StartStruct | E::StartMap) => InKey,
            (WaitForKey, E::Null | E::Some) => {
                self.nullable = true;
                WaitForKey
            }
            (WaitForKey, ev) => fail!("Invalid event {ev} for struct tracer in state Start"),
            (InKey, E::Item) => InKey,
            (InKey, E::Str(key)) => {
                if let Some(&field_idx) = self.index.get(key) {
                    let Some(field) = self.fields.get_mut(field_idx) else {
                        fail!("invalid state");
                    };
                    field.last_seen_in_sample = self.current_sample;

                    InValue(field_idx, 0)
                } else {
                    let mut field = Field {
                        tracer: SamplesTracer::new(
                            format!("{path}.{key}", path = self.path),
                            self.options.clone(),
                        ),
                        name: key.to_owned(),
                        last_seen_in_sample: self.current_sample,
                    };

                    // field was missing in previous samples
                    if self.current_sample != 0 {
                        println!("{key}");
                        field.tracer.mark_nullable();
                    }

                    let field_idx = self.fields.len();
                    self.fields.push(field);
                    self.index.insert(key.to_owned(), field_idx);
                    InValue(field_idx, 0)
                }
            }
            (InKey, E::EndStruct | E::EndMap) => {
                for field in &mut self.fields {
                    // field. was not seen in this sample
                    if field.last_seen_in_sample != self.current_sample {
                        field.tracer.mark_nullable();
                    }
                }
                self.current_sample += 1;

                WaitForKey
            }
            (InKey, ev) => fail!("Invalid event {ev} for struct tracer in state Key"),
            (InValue(field, depth), ev) if ev.is_start() => {
                self.fields[field].tracer.accept(ev)?;
                InValue(field, depth + 1)
            }
            (InValue(field, depth), ev) if ev.is_end() => {
                self.fields[field].tracer.accept(ev)?;
                match depth {
                    0 => fail!("Invalid closing event in struct tracer in state Value"),
                    1 => InKey,
                    depth => InValue(field, depth - 1),
                }
            }
            (InValue(field, depth), ev) if ev.is_marker() => {
                self.fields[field].tracer.accept(ev)?;
                // markers are always followed by the actual  value
                InValue(field, depth)
            }
            (InValue(field, depth), ev) => {
                self.fields[field].tracer.accept(ev)?;
                match depth {
                    // Any event at depth == 0 that does not start a structure (is a complete value)
                    0 => InKey,
                    _ => InValue(field, depth),
                }
            }
            (Finished, _) => fail!("finished StructTracer cannot handle events"),
        };
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        StructTracer::finish(self)
    }
}

pub struct TupleTracer {
    pub field_tracers: Vec<SamplesTracer>,
    pub nullable: bool,
    pub state: TupleTracerState,
    pub path: String,
    pub options: TracingOptions,
}

#[derive(Debug, Clone, Copy)]
pub enum TupleTracerState {
    WaitForStart,
    /// Wait for the item with `(field_index)`
    WaitForItem(usize),
    /// Process the item at `(field_index, depth)`
    InItem(usize, usize),
    Finished,
}

impl TupleTracer {
    pub fn new(path: String, options: TracingOptions, nullable: bool) -> Self {
        Self {
            path,
            options,
            field_tracers: Vec::new(),
            nullable,
            state: TupleTracerState::WaitForStart,
        }
    }

    pub fn to_field(&self, name: &str) -> Result<GenericField> {
        if !matches!(self.state, TupleTracerState::Finished) {
            fail!("Cannot build field {name} from unfinished tracer");
        }

        let mut field = GenericField::new(name, GenericDataType::Struct, self.nullable);
        for (idx, tracer) in self.field_tracers.iter().enumerate() {
            field.children.push(tracer.to_field(&idx.to_string())?);
        }
        field.strategy = Some(Strategy::TupleAsStruct);

        Ok(field)
    }

    fn field_tracer(&mut self, idx: usize) -> &mut SamplesTracer {
        while self.field_tracers.len() <= idx {
            self.field_tracers.push(SamplesTracer::new(
                format!("{path}.{idx}", path = self.path),
                self.options.clone(),
            ));
        }
        &mut self.field_tracers[idx]
    }

    pub fn reset(&mut self) -> Result<()> {
        if !matches!(self.state, TupleTracerState::Finished) {
            fail!("Cannot reset unfinished tuple tracer");
        }
        for tracer in &mut self.field_tracers {
            tracer.reset()?;
        }
        self.state = TupleTracerState::WaitForStart;
        Ok(())
    }

    pub fn finish(&mut self) -> Result<()> {
        if !matches!(self.state, TupleTracerState::WaitForStart) {
            fail!("Incomplete tuple in schema tracing");
        }
        for tracer in &mut self.field_tracers {
            tracer.finish()?;
        }
        self.state = TupleTracerState::Finished;
        Ok(())
    }
}

impl EventSink for TupleTracer {
    macros::forward_specialized_to_generic!();

    fn accept(&mut self, event: Event<'_>) -> Result<()> {
        use TupleTracerState::*;
        type E<'a> = Event<'a>;

        self.state = match (self.state, event) {
            (WaitForStart, Event::StartTuple) => WaitForItem(0),
            (WaitForStart, E::Null | E::Some) => {
                self.nullable = true;
                WaitForStart
            }
            (WaitForStart, ev) => fail!(
                "Invalid event {ev} for TupleTracer in state Start [{path}]",
                path = self.path
            ),
            (WaitForItem(field), Event::Item) => InItem(field, 0),
            (WaitForItem(_), E::EndTuple) => WaitForStart,
            (WaitForItem(field), ev) => fail!(
                "Invalid event {ev} for TupleTracer in state WaitForItem({field}) [{path}]",
                path = self.path
            ),
            (InItem(field, depth), ev) if ev.is_start() => {
                self.field_tracer(field).accept(ev)?;
                InItem(field, depth + 1)
            }
            (InItem(field, depth), ev) if ev.is_end() => {
                self.field_tracer(field).accept(ev)?;
                match depth {
                    0 => fail!(
                        "Invalid closing event in TupleTracer in state Value [{path}]",
                        path = self.path
                    ),
                    1 => WaitForItem(field + 1),
                    depth => InItem(field, depth - 1),
                }
            }
            (InItem(field, depth), ev) if ev.is_marker() => {
                self.field_tracer(field).accept(ev)?;
                // markers are always followed by the actual  value
                InItem(field, depth)
            }
            (InItem(field, depth), ev) => {
                self.field_tracer(field).accept(ev)?;
                match depth {
                    // Any event at depth == 0 that does not start a structure (is a complete value)
                    0 => WaitForItem(field + 1),
                    _ => InItem(field, depth),
                }
            }
            (Finished, ev) => fail!("finished tuple tracer cannot handle event {ev}"),
        };
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        TupleTracer::finish(self)
    }
}

pub struct ListTracer {
    pub path: String,
    pub options: TracingOptions,
    pub item_tracer: Box<SamplesTracer>,
    pub nullable: bool,
    pub state: ListTracerState,
}

#[derive(Debug, Clone, Copy)]
pub enum ListTracerState {
    WaitForStart,
    WaitForItem,
    InItem(usize),
    Finished,
}

impl ListTracer {
    pub fn new(path: String, options: TracingOptions, nullable: bool) -> Self {
        Self {
            path: path.clone(),
            options: options.clone(),
            item_tracer: Box::new(SamplesTracer::new(path, options)),
            nullable,
            state: ListTracerState::WaitForStart,
        }
    }

    pub fn to_field(&self, name: &str) -> Result<GenericField> {
        if !matches!(self.state, ListTracerState::Finished) {
            fail!("Cannot build field {name} from unfinished tracer");
        }

        let mut field = GenericField::new(name, GenericDataType::LargeList, self.nullable);
        field.children.push(self.item_tracer.to_field("element")?);

        Ok(field)
    }

    pub fn reset(&mut self) -> Result<()> {
        if !matches!(self.state, ListTracerState::Finished) {
            fail!("Cannot reset unfinished list tracer");
        }
        self.item_tracer.reset()?;
        self.state = ListTracerState::Finished;
        Ok(())
    }

    pub fn finish(&mut self) -> Result<()> {
        if !matches!(self.state, ListTracerState::WaitForStart) {
            fail!("Incomplete list in schema tracing");
        }
        self.item_tracer.finish()?;
        self.state = ListTracerState::Finished;
        Ok(())
    }
}

impl EventSink for ListTracer {
    macros::forward_specialized_to_generic!();

    fn accept(&mut self, event: Event<'_>) -> Result<()> {
        use {Event as E, ListTracerState as S};

        self.state = match (self.state, event) {
            (S::WaitForStart, E::Null | E::Some) => {
                self.nullable = true;
                S::WaitForStart
            }
            (S::WaitForStart, E::StartSequence) => S::WaitForItem,
            (S::WaitForItem, E::EndSequence) => S::WaitForStart,
            (S::WaitForItem, E::Item) => S::InItem(0),
            (S::InItem(depth), ev) if ev.is_start() => {
                self.item_tracer.accept(ev)?;
                S::InItem(depth + 1)
            }
            (S::InItem(depth), ev) if ev.is_end() => match depth {
                0 => fail!(
                    "Invalid event {ev} for list tracer ({path}) in state Item(0)",
                    path = self.path
                ),
                1 => {
                    self.item_tracer.accept(ev)?;
                    S::WaitForItem
                }
                depth => {
                    self.item_tracer.accept(ev)?;
                    S::InItem(depth - 1)
                }
            },
            (S::InItem(0), ev) if ev.is_value() => {
                self.item_tracer.accept(ev)?;
                S::WaitForItem
            }
            (S::InItem(depth), ev) => {
                self.item_tracer.accept(ev)?;
                S::InItem(depth)
            }
            (state, ev) => fail!(
                "Invalid event {ev} for list tracer ({path}) in state {state:?}",
                path = self.path
            ),
        };
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        ListTracer::finish(self)
    }
}

pub struct UnionTracer {
    pub variants: Vec<Option<Variant>>,
    pub nullable: bool,
    pub state: UnionTracerState,
    pub path: String,
    pub options: TracingOptions,
}

pub struct Variant {
    name: String,
    tracer: SamplesTracer,
}

#[derive(Debug, Clone, Copy)]
pub enum UnionTracerState {
    /// Wait for the next variant
    WaitForVariant,
    /// Process the current variant at `(variant_index, depth)`
    InVariant(usize, usize),
    Finished,
}

impl UnionTracer {
    pub fn new(path: String, options: TracingOptions, nullable: bool) -> Self {
        Self {
            path,
            options,
            variants: Vec::new(),
            nullable,
            state: UnionTracerState::WaitForVariant,
        }
    }

    pub fn to_field(&self, name: &str) -> Result<GenericField> {
        if !matches!(self.state, UnionTracerState::Finished) {
            fail!("Cannot build field {name} from unfinished tracer");
        }

        let mut field = GenericField::new(name, GenericDataType::Union, self.nullable);
        for variant in &self.variants {
            if let Some(variant) = variant {
                field.children.push(variant.tracer.to_field(&variant.name)?);
            } else {
                field.children.push(
                    GenericField::new("", GenericDataType::Null, true)
                        .with_strategy(Strategy::UnknownVariant),
                );
            };
        }

        Ok(field)
    }

    fn ensure_variant<S: Into<String> + AsRef<str>>(
        &mut self,
        variant: S,
        idx: usize,
    ) -> Result<()> {
        while self.variants.len() <= idx {
            self.variants.push(None);
        }

        if let Some(prev) = self.variants[idx].as_mut() {
            let variant = variant.as_ref();
            if prev.name != variant {
                fail!(
                    "Incompatible names for variant {idx}: {prev}, {variant}",
                    prev = prev.name
                );
            }
        } else {
            let tracer = SamplesTracer::new(
                format!("{path}.{key}", path = self.path, key = variant.as_ref()),
                self.options.clone(),
            );
            let name = variant.into();

            self.variants[idx] = Some(Variant { name, tracer });
        }

        Ok(())
    }

    pub fn reset(&mut self) -> Result<()> {
        if !matches!(self.state, UnionTracerState::Finished) {
            fail!("Cannot reset unfinished union tracer");
        }
        for variant in &mut self.variants {
            let Some(variant) = variant.as_mut() else {
                continue;
            };
            variant.tracer.reset()?;
        }
        self.state = UnionTracerState::WaitForVariant;
        Ok(())
    }

    pub fn finish(&mut self) -> Result<()> {
        // TODO: fix me
        for variant in &mut self.variants {
            let Some(variant) = variant.as_mut() else {
                continue;
            };
            variant.tracer.finish()?;
        }
        self.state = UnionTracerState::Finished;
        Ok(())
    }
}

impl EventSink for UnionTracer {
    macros::forward_specialized_to_generic!();

    fn accept(&mut self, event: Event<'_>) -> Result<()> {
        type S = UnionTracerState;
        type E<'a> = Event<'a>;

        self.state = match self.state {
            S::WaitForVariant => match event {
                E::Variant(variant, idx) => {
                    self.ensure_variant(variant, idx)?;
                    S::InVariant(idx, 0)
                }
                E::Some => fail!("Nullable unions are not supported"),
                E::OwnedVariant(variant, idx) => {
                    self.ensure_variant(variant, idx)?;
                    S::InVariant(idx, 0)
                }
                ev => fail!("Invalid event {ev} for UnionTracer in State Inactive"),
            },
            S::InVariant(idx, depth) => match event {
                ev if ev.is_start() => {
                    self.variants[idx].as_mut().unwrap().tracer.accept(ev)?;
                    S::InVariant(idx, depth + 1)
                }
                ev if ev.is_end() => match depth {
                    0 => fail!("Invalid end event {ev} at depth 0 in UnionTracer"),
                    1 => {
                        self.variants[idx].as_mut().unwrap().tracer.accept(ev)?;
                        S::WaitForVariant
                    }
                    _ => {
                        self.variants[idx].as_mut().unwrap().tracer.accept(ev)?;
                        S::InVariant(idx, depth - 1)
                    }
                },
                ev if ev.is_marker() => {
                    self.variants[idx].as_mut().unwrap().tracer.accept(ev)?;
                    S::InVariant(idx, depth)
                }
                ev if ev.is_value() => {
                    self.variants[idx].as_mut().unwrap().tracer.accept(ev)?;
                    match depth {
                        0 => S::WaitForVariant,
                        _ => S::InVariant(idx, depth),
                    }
                }
                _ => unreachable!(),
            },
            S::Finished => fail!("finished union tracer cannot handle event"),
        };
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        UnionTracer::finish(self)
    }
}

pub struct MapTracer {
    pub path: String,
    pub options: TracingOptions,
    pub key_tracer: Box<SamplesTracer>,
    pub value_tracer: Box<SamplesTracer>,
    pub nullable: bool,
    pub state: MapTracerState,
}

#[derive(Debug, Clone, Copy)]
pub enum MapTracerState {
    WaitForKey,
    /// Process the current key at `(depth)`
    InKey(usize),
    /// Process the current value at `(depth)`
    InValue(usize),
    Finished,
}

impl MapTracer {
    pub fn new(path: String, options: TracingOptions, nullable: bool) -> Self {
        Self {
            nullable,
            options: options.clone(),
            key_tracer: Box::new(SamplesTracer::new(format!("{path}.$key"), options.clone())),
            value_tracer: Box::new(SamplesTracer::new(format!("{path}.$value"), options)),
            state: MapTracerState::WaitForKey,
            path,
        }
    }

    pub fn to_field(&self, name: &str) -> Result<GenericField> {
        if !matches!(self.state, MapTracerState::Finished) {
            fail!("Cannot build field {name} from unfinished tracer");
        }

        let mut entries = GenericField::new("entries", GenericDataType::Struct, false);
        entries.children.push(self.key_tracer.to_field("key")?);
        entries.children.push(self.value_tracer.to_field("value")?);

        let mut field = GenericField::new(name, GenericDataType::Map, self.nullable);
        field.children.push(entries);

        Ok(field)
    }

    pub fn reset(&mut self) -> Result<()> {
        self.key_tracer.reset()?;
        self.value_tracer.reset()?;
        self.state = MapTracerState::WaitForKey;
        Ok(())
    }

    pub fn finish(&mut self) -> Result<()> {
        self.key_tracer.finish()?;
        self.value_tracer.finish()?;
        self.state = MapTracerState::Finished;
        Ok(())
    }
}

impl EventSink for MapTracer {
    macros::forward_specialized_to_generic!();

    fn accept(&mut self, event: Event<'_>) -> Result<()> {
        type S = MapTracerState;
        type E<'a> = Event<'a>;

        self.state = match self.state {
            S::WaitForKey => match event {
                Event::StartMap => S::InKey(0),
                Event::Null | Event::Some => {
                    self.nullable = true;
                    S::WaitForKey
                }
                ev => fail!("Unexpected event {ev} in state Start of MapTracer"),
            },
            S::InKey(depth) => match event {
                Event::Item if depth == 0 => S::InKey(depth),
                ev if ev.is_end() => match depth {
                    0 => {
                        if !matches!(ev, E::EndMap) {
                            fail!("Unexpected event {ev} in State Key at depth 0 in MapTracer")
                        }
                        S::WaitForKey
                    }
                    1 => {
                        self.key_tracer.accept(ev)?;
                        S::InValue(0)
                    }
                    _ => {
                        self.key_tracer.accept(ev)?;
                        S::InKey(depth - 1)
                    }
                },
                ev if ev.is_start() => {
                    self.key_tracer.accept(ev)?;
                    S::InKey(depth + 1)
                }
                ev if ev.is_marker() => {
                    self.key_tracer.accept(ev)?;
                    S::InKey(depth)
                }
                ev if ev.is_value() => {
                    self.key_tracer.accept(ev)?;
                    if depth == 0 {
                        S::InValue(0)
                    } else {
                        S::InKey(depth)
                    }
                }
                _ => unreachable!(),
            },
            S::InValue(depth) => match event {
                ev if ev.is_end() => match depth {
                    0 => fail!("Unexpected event {ev} in State Value at depth 0 in MapTracer"),
                    1 => {
                        self.value_tracer.accept(ev)?;
                        S::InKey(0)
                    }
                    _ => {
                        self.value_tracer.accept(ev)?;
                        S::InValue(depth - 1)
                    }
                },
                ev if ev.is_start() => {
                    self.value_tracer.accept(ev)?;
                    S::InValue(depth + 1)
                }
                ev if ev.is_marker() => {
                    self.value_tracer.accept(ev)?;
                    S::InValue(depth)
                }
                ev if ev.is_value() => {
                    self.value_tracer.accept(ev)?;
                    if depth == 0 {
                        S::InKey(0)
                    } else {
                        S::InValue(depth)
                    }
                }
                _ => unreachable!(),
            },
            S::Finished => fail!("Finished map tracer cannot handle event"),
        };
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        MapTracer::finish(self)
    }
}

impl EventSink for PrimitiveTracer {
    macros::forward_specialized_to_generic!();

    fn accept(&mut self, event: Event<'_>) -> Result<()> {
        use GenericDataType::*;
        use Strategy as S;

        let (ev_type, ev_strategy) = match event {
            Event::Some | Event::Null => (Null, None),
            Event::Bool(_) => (Bool, None),
            Event::Str(s) => self.get_string_type_and_strategy(s),
            Event::OwnedStr(s) => self.get_string_type_and_strategy(&s),
            Event::U8(_) => (U8, None),
            Event::U16(_) => (U16, None),
            Event::U32(_) => (U32, None),
            Event::U64(_) => (U64, None),
            Event::I8(_) => (I8, None),
            Event::I16(_) => (I16, None),
            Event::I32(_) => (I32, None),
            Event::I64(_) => (I64, None),
            Event::F32(_) => (F32, None),
            Event::F64(_) => (F64, None),
            ev => fail!("Cannot handle event {ev} in primitive tracer"),
        };

        (self.item_type, self.strategy) = match (&self.item_type, ev_type) {
            (ty, Null) => {
                self.nullable = true;
                (ty.clone(), self.strategy.clone())
            }
            (Bool | Null, Bool) => (Bool, None),
            (I8 | Null, I8) => (I8, None),
            (I16 | Null, I16) => (I16, None),
            (I32 | Null, I32) => (I32, None),
            (I64 | Null, I64) => (I64, None),
            (U8 | Null, U8) => (U8, None),
            (U16 | Null, U16) => (U16, None),
            (U32 | Null, U32) => (U32, None),
            (U64 | Null, U64) => (U64, None),
            (F32 | Null, F32) => (F32, None),
            (F64 | Null, F64) => (F64, None),
            (Null, Date64) => (Date64, ev_strategy),
            (Date64, Date64) => match (&self.strategy, ev_strategy) {
                (Some(S::NaiveStrAsDate64), Some(S::NaiveStrAsDate64)) => {
                    (Date64, Some(S::NaiveStrAsDate64))
                }
                (Some(S::UtcStrAsDate64), Some(S::UtcStrAsDate64)) => {
                    (Date64, Some(S::UtcStrAsDate64))
                }
                _ => (LargeUtf8, None),
            },
            (LargeUtf8 | Null, LargeUtf8) | (Date64, LargeUtf8) | (LargeUtf8, Date64) => {
                (LargeUtf8, None)
            }
            (ty, ev) if self.options.coerce_numbers => match (ty, ev) {
                // unsigned x unsigned -> u64
                (U8 | U16 | U32 | U64, U8 | U16 | U32 | U64) => (U64, None),
                // signed x signed -> i64
                (I8 | I16 | I32 | I64, I8 | I16 | I32 | I64) => (I64, None),
                // signed x unsigned -> i64
                (I8 | I16 | I32 | I64, U8 | U16 | U32 | U64) => (I64, None),
                // unsigned x signed -> i64
                (U8 | U16 | U32 | U64, I8 | I16 | I32 | I64) => (I64, None),
                // float x float -> f64
                (F32 | F64, F32 | F64) => (F64, None),
                // int x float -> f64
                (I8 | I16 | I32 | I64 | U8 | U16 | U32 | U64, F32 | F64) => (F64, None),
                // float x int -> f64
                (F32 | F64, I8 | I16 | I32 | I64 | U8 | U16 | U32 | U64) => (F64, None),
                (ty, ev) => fail!("Cannot accept event {ev} for tracer of primitive type {ty}"),
            },
            (ty, ev) => fail!("Cannot accept event {ev} for tracer of primitive type {ty}"),
        };
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        PrimitiveTracer::finish(self)
    }
}

impl PrimitiveTracer {
    fn get_string_type_and_strategy(&self, s: &str) -> (GenericDataType, Option<Strategy>) {
        if !self.options.try_parse_dates {
            (GenericDataType::LargeUtf8, None)
        } else if matches_naive_datetime(s) {
            (GenericDataType::Date64, Some(Strategy::NaiveStrAsDate64))
        } else if matches_utc_datetime(s) {
            (GenericDataType::Date64, Some(Strategy::UtcStrAsDate64))
        } else {
            (GenericDataType::LargeUtf8, None)
        }
    }
}

mod parsing {
    pub const DIGIT: &[char] = &['0', '1', '2', '3', '4', '5', '6', '7', '8', '9'];

    pub fn match_optional_sign(s: &str) -> Result<&str, &str> {
        Ok(s.strip_prefix(['+', '-']).unwrap_or(s))
    }

    pub fn match_one_or_more_digits(s: &str) -> Result<&str, &str> {
        let mut s = s.strip_prefix(DIGIT).ok_or(s)?;
        while let Some(new_s) = s.strip_prefix(DIGIT) {
            s = new_s;
        }
        Ok(s)
    }

    pub fn match_one_or_two_digits(s: &str) -> Result<&str, &str> {
        let s = s.strip_prefix(DIGIT).ok_or(s)?;
        Ok(s.strip_prefix(DIGIT).unwrap_or(s))
    }

    pub fn match_char(s: &str, c: char) -> Result<&str, &str> {
        s.strip_prefix(c).ok_or(s)
    }

    pub fn matches_naive_datetime_with_sep<'a>(
        s: &'a str,
        sep: &'_ [char],
    ) -> Result<&'a str, &'a str> {
        let s = s.trim();
        let s = match_optional_sign(s)?;
        let s = match_one_or_more_digits(s)?;
        let s = match_char(s, '-')?;
        let s = match_one_or_two_digits(s)?;
        let s = match_char(s, '-')?;
        let s = match_one_or_two_digits(s)?;
        let s = s.strip_prefix(sep).ok_or(s)?;
        let s = match_one_or_two_digits(s)?;
        let s = match_char(s, ':')?;
        let s = match_one_or_two_digits(s)?;
        let s = match_char(s, ':')?;
        let s = match_one_or_two_digits(s)?;

        if let Some(s) = s.strip_prefix('.') {
            match_one_or_more_digits(s)
        } else {
            Ok(s)
        }
    }

    pub fn matches_naive_datetime(s: &str) -> Result<&str, &str> {
        matches_naive_datetime_with_sep(s, &['T'])
    }

    pub fn matches_utc_datetime(s: &str) -> Result<&str, &str> {
        let s = matches_naive_datetime_with_sep(s, &['T', ' '])?;

        if let Some(s) = s.strip_prefix('Z') {
            Ok(s)
        } else if let Some(s) = s.strip_prefix("+0000") {
            Ok(s)
        } else if let Some(s) = s.strip_prefix("+00:00") {
            Ok(s)
        } else {
            Err(s)
        }
    }
}

pub fn matches_naive_datetime(s: &str) -> bool {
    parsing::matches_naive_datetime(s)
        .map(|s| s.is_empty())
        .unwrap_or_default()
}

pub fn matches_utc_datetime(s: &str) -> bool {
    parsing::matches_utc_datetime(s)
        .map(|s| s.is_empty())
        .unwrap_or_default()
}

#[cfg(test)]
mod test_matches_naive_datetime {
    macro_rules! test {
        ($( ( $name:ident, $s:expr, $expected:expr ), )*) => {
            $(
                #[test]
                fn $name() {
                    if $expected {
                        assert_eq!(super::parsing::matches_naive_datetime($s), Ok(""));
                    }
                    assert_eq!(super::matches_naive_datetime($s), $expected);
                }
            )*
        };
    }

    test!(
        (example_chrono_docs_1, "2015-09-18T23:56:04", true),
        (example_chrono_docs_2, "+12345-6-7T7:59:60.5", true),
        (surrounding_space, "   2015-09-18T23:56:04   ", true),
    );
}

#[cfg(test)]
mod test_matches_utc_datetime {
    macro_rules! test {
        ($( ( $name:ident, $s:expr, $expected:expr ), )*) => {
            $(
                #[test]
                fn $name() {
                    if $expected {
                        assert_eq!(super::parsing::matches_utc_datetime($s), Ok(""));
                    }
                    assert_eq!(super::matches_utc_datetime($s), $expected);
                }
            )*
        };
    }

    test!(
        (example_chrono_docs_1, "2012-12-12T12:12:12Z", true),
        (example_chrono_docs_2, "2012-12-12 12:12:12Z", true),
        (example_chrono_docs_3, "2012-12-12 12:12:12+0000", true),
        (example_chrono_docs_4, "2012-12-12 12:12:12+00:00", true),
    );
}
