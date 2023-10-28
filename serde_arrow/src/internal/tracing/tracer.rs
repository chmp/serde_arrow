use std::collections::HashMap;

use crate::internal::{
    error::{fail, Result},
    schema::{GenericDataType, GenericField, Strategy},
    tracing::TracingOptions,
};

macro_rules! defined_tracer {
    ($($variant:ident($impl:ident)),* $(,)? ) => {
        #[derive(Debug, PartialEq, Clone)]
        pub enum Tracer {
            $($variant($impl),)*
        }

        macro_rules! dispatch_tracer {
            ($obj:expr, $item:ident => $block:expr) => {
                match $obj {
                    $(Tracer::$variant($item) => $block,)*
                }
            };
        }
    };
}

defined_tracer!(
    Unknown(UnknownTracer),
    Primitive(PrimitiveTracer),
    List(ListTracer),
    Map(MapTracer),
    Struct(StructTracer),
    Tuple(TupleTracer),
    Union(UnionTracer),
);

impl Tracer {
    pub fn new(path: String, options: TracingOptions) -> Self {
        Self::Unknown(UnknownTracer::new(path, options))
    }
}

impl Tracer {
    pub fn get_path(&self) -> &str {
        dispatch_tracer!(self, tracer => tracer.get_path())
    }

    pub fn is_unknown(&self) -> bool {
        matches!(self, Tracer::Unknown(_))
    }

    pub fn is_complete(&self) -> bool {
        dispatch_tracer!(self, tracer => tracer.is_complete())
    }

    pub fn get_type(&self) -> Option<&GenericDataType> {
        dispatch_tracer!(self, tracer => tracer.get_type())
    }

    pub fn get_nullable(&self) -> bool {
        dispatch_tracer!(self, tracer => tracer.nullable)
    }

    pub fn to_field(&self, name: &str) -> Result<GenericField> {
        dispatch_tracer!(self, tracer => tracer.to_field(name))
    }

    pub fn get_depth(&self) -> usize {
        self.get_path().chars().filter(|c| *c == '.').count()
    }

    pub fn get_options(&self) -> &TracingOptions {
        dispatch_tracer!(self, tracer => &tracer.options)
    }

    pub fn finish(&mut self) -> Result<()> {
        dispatch_tracer!(self, tracer => tracer.finish())
    }

    pub fn reset(&mut self) -> Result<()> {
        dispatch_tracer!(self, tracer => tracer.reset())
    }
}

// TODO: move into trace any?
impl Tracer {
    pub fn mark_nullable(&mut self) {
        dispatch_tracer!(self, tracer => { tracer.nullable = true; });
    }

    pub fn ensure_struct<S: std::fmt::Display>(&mut self, fields: &[S]) -> Result<()> {
        match self {
            this @ Self::Unknown(_) => {
                let field_names = fields
                    .iter()
                    .map(|field| field.to_string())
                    .collect::<Vec<_>>();
                let index = field_names
                    .iter()
                    .enumerate()
                    .map(|(idx, name)| (name.to_string(), idx))
                    .collect::<HashMap<_, _>>();

                let tracer = StructTracer {
                    path: this.get_path().to_owned(),
                    options: this.get_options().clone(),
                    fields: fields
                        .iter()
                        .map(|field| StructField {
                            tracer: Tracer::new(
                                format!("{}.{}", this.get_path(), field),
                                this.get_options().clone(),
                            ),
                            name: field.to_string(),
                            last_seen_in_sample: 0,
                        })
                        .collect(),
                    index,
                    nullable: this.get_nullable(),
                    mode: StructMode::Struct,
                    state: StructTracerState::WaitForKey,
                    current_sample: 0,
                };
                *this = Self::Struct(tracer);
                Ok(())
            }
            Self::Struct(_tracer) => {
                // TODO: check fields are equal
                Ok(())
            }
            _ => fail!(
                "mismatched types, previous {:?}, current struct",
                self.get_type()
            ),
        }
    }

    pub fn ensure_tuple(&mut self, num_fields: usize) -> Result<()> {
        match self {
            this @ Self::Unknown(_) => {
                let tracer = TupleTracer {
                    path: this.get_path().to_owned(),
                    options: this.get_options().clone(),
                    field_tracers: (0..num_fields)
                        .map(|i| {
                            Tracer::new(
                                format!("{}.{}", this.get_path(), i),
                                this.get_options().clone(),
                            )
                        })
                        .collect(),
                    nullable: this.get_nullable(),
                    state: TupleTracerState::WaitForStart,
                };
                *this = Self::Tuple(tracer);
                Ok(())
            }
            Self::Tuple(_tracer) => {
                // TODO: check fields are equal
                Ok(())
            }
            _ => fail!(
                "mismatched types, previous {:?}, current struct",
                self.get_type()
            ),
        }
    }

    pub fn ensure_union(&mut self, variants: &[&str]) -> Result<()> {
        match self {
            this @ Self::Unknown(_) => {
                let tracer = UnionTracer {
                    path: this.get_path().to_owned(),
                    options: this.get_options().clone(),
                    state: UnionTracerState::WaitForVariant,
                    variants: variants
                        .iter()
                        .map(|variant| {
                            Some(UnionVariant {
                                name: variant.to_string(),
                                tracer: Tracer::new(
                                    format!("{}.{}", this.get_path(), variant),
                                    this.get_options().clone(),
                                ),
                            })
                        })
                        .collect(),
                    nullable: this.get_nullable(),
                };
                *this = Self::Union(tracer);
                Ok(())
            }
            Self::Union(_tracer) => {
                // TODO: check fields are equal or fill missing fields
                Ok(())
            }
            _ => fail!(
                "mismatched types, previous {:?}, current union",
                self.get_type()
            ),
        }
    }

    pub fn ensure_list(&mut self) -> Result<()> {
        match self {
            this @ Self::Unknown(_) => {
                let tracer = ListTracer {
                    path: this.get_path().to_owned(),
                    options: this.get_options().clone(),
                    nullable: this.get_nullable(),
                    item_tracer: Box::new(Tracer::new(
                        format!("{}.item", this.get_path()),
                        this.get_options().clone(),
                    )),
                    state: ListTracerState::WaitForStart,
                };
                *this = Self::List(tracer);
                Ok(())
            }
            Self::List(_tracer) => Ok(()),
            _ => fail!(
                "mismatched types, previous {:?}, current list",
                self.get_type()
            ),
        }
    }

    pub fn ensure_map(&mut self) -> Result<()> {
        match self {
            this @ Self::Unknown(_) => {
                let tracer = MapTracer {
                    path: this.get_path().to_owned(),
                    options: this.get_options().clone(),
                    nullable: this.get_nullable(),
                    key_tracer: Box::new(Tracer::new(
                        format!("{}.key", this.get_path()),
                        this.get_options().clone(),
                    )),
                    value_tracer: Box::new(Tracer::new(
                        format!("{}.value", this.get_path()),
                        this.get_options().clone(),
                    )),
                    state: MapTracerState::WaitForKey,
                };
                *this = Self::Map(tracer);
                Ok(())
            }
            Self::Map(_tracer) => Ok(()),
            _ => fail!(
                "mismatched types, previous {:?}, current list",
                self.get_type()
            ),
        }
    }
}

macro_rules! impl_primitive_ensures {
    (
        $(
            ($func:ident, $variant:ident)
        ),*
        $(,)?
    ) => {
        impl Tracer {
            $(
                pub fn $func(&mut self) -> Result<()> {
                    match self {
                        this @ Self::Unknown(_) => {
                            let tracer = PrimitiveTracer::new(
                                this.get_path().to_owned(),
                                this.get_options().clone(),
                                GenericDataType::$variant,
                                this.get_nullable(),
                            );
                            *this = Self::Primitive(tracer);
                            Ok(())
                        }
                        Self::Primitive(tracer) if tracer.item_type == GenericDataType::$variant => {
                             Ok(())
                        }
                        _ => fail!("mismatched types, previous {:?}, current {:?}", self.get_type(), GenericDataType::$variant),
                    }
                }
            )*
        }
    };
}

impl_primitive_ensures!(
    (ensure_null, Null),
    (ensure_bool, Bool),
    (ensure_i8, I8),
    (ensure_i16, I16),
    (ensure_i32, I32),
    (ensure_i64, I64),
    (ensure_u8, U8),
    (ensure_u16, U16),
    (ensure_u32, U32),
    (ensure_u64, U64),
    (ensure_f32, F32),
    (ensure_f64, F64),
    (ensure_utf8, LargeUtf8),
);

#[derive(Debug, PartialEq, Clone)]
pub struct UnknownTracer {
    pub path: String,
    pub options: TracingOptions,
    pub nullable: bool,
    pub state: UnknownTracerState,
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum UnknownTracerState {
    Unfinished,
    Finished,
}

impl UnknownTracer {
    pub fn new(path: String, options: TracingOptions) -> Self {
        Self {
            path,
            options,
            nullable: false,
            state: UnknownTracerState::Unfinished,
        }
    }

    pub fn to_field(&self, name: &str) -> Result<GenericField> {
        if !matches!(self.state, UnknownTracerState::Finished) {
            fail!("Cannot build field {name} from unfinished tracer");
        }
        if !self.options.allow_null_fields {
            fail!(concat!(
                "Encountered null only or unknown field. This error can be ",
                "disabled by setting `allow_null_fields` to `true` in ",
                "`TracingOptions`",
            ));
        }

        Ok(GenericField::new(
            name,
            GenericDataType::Null,
            self.nullable,
        ))
    }

    pub fn reset(&mut self) -> Result<()> {
        if !matches!(self.state, UnknownTracerState::Finished) {
            fail!("cannot reset an unfinished tracer");
        }
        self.state = UnknownTracerState::Unfinished;
        Ok(())
    }

    pub fn finish(&mut self) -> Result<()> {
        if !matches!(self.state, UnknownTracerState::Unfinished) {
            fail!("Cannot finish an already finished tracer");
        }
        self.state = UnknownTracerState::Finished;
        Ok(())
    }

    pub fn get_path(&self) -> &str {
        &self.path
    }

    pub fn is_complete(&self) -> bool {
        false
    }

    pub fn get_type(&self) -> Option<&GenericDataType> {
        None
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct MapTracer {
    pub path: String,
    pub options: TracingOptions,
    pub nullable: bool,
    pub key_tracer: Box<Tracer>,
    pub value_tracer: Box<Tracer>,
    pub state: MapTracerState,
}

#[derive(Debug, Clone, Copy, PartialEq)]
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
            key_tracer: Box::new(Tracer::new(format!("{path}.$key"), options.clone())),
            value_tracer: Box::new(Tracer::new(format!("{path}.$value"), options)),
            state: MapTracerState::WaitForKey,
            path,
        }
    }

    pub fn get_path(&self) -> &str {
        &self.path
    }

    pub fn is_complete(&self) -> bool {
        self.key_tracer.is_complete() && self.value_tracer.is_complete()
    }

    pub fn get_type(&self) -> Option<&GenericDataType> {
        Some(&GenericDataType::Map)
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

#[derive(Debug, PartialEq, Clone)]
pub struct ListTracer {
    pub path: String,
    pub options: TracingOptions,
    pub nullable: bool,
    pub item_tracer: Box<Tracer>,
    pub state: ListTracerState,
}

#[derive(Debug, Clone, Copy, PartialEq)]
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
            item_tracer: Box::new(Tracer::new(path, options)),
            nullable,
            state: ListTracerState::WaitForStart,
        }
    }

    pub fn get_path(&self) -> &str {
        &self.path
    }

    pub fn is_complete(&self) -> bool {
        self.item_tracer.is_complete()
    }

    pub fn get_type(&self) -> Option<&GenericDataType> {
        Some(&GenericDataType::LargeList)
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

#[derive(Debug, PartialEq, Clone)]
pub struct TupleTracer {
    pub path: String,
    pub options: TracingOptions,
    pub nullable: bool,
    pub field_tracers: Vec<Tracer>,
    pub state: TupleTracerState,
}

#[derive(Debug, Clone, Copy, PartialEq)]
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

    pub fn get_path(&self) -> &str {
        &self.path
    }

    pub fn is_complete(&self) -> bool {
        self.field_tracers.iter().all(|tracer| tracer.is_complete())
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

    pub fn get_type(&self) -> Option<&GenericDataType> {
        Some(&GenericDataType::Struct)
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

    pub fn field_tracer(&mut self, idx: usize) -> &mut Tracer {
        while self.field_tracers.len() <= idx {
            self.field_tracers.push(Tracer::new(
                format!("{path}.{idx}", path = self.path),
                self.options.clone(),
            ));
        }
        &mut self.field_tracers[idx]
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct StructTracer {
    pub path: String,
    pub options: TracingOptions,
    pub nullable: bool,
    pub fields: Vec<StructField>,
    pub index: HashMap<String, usize>,
    pub mode: StructMode,
    pub state: StructTracerState,
    pub current_sample: usize,
}

#[derive(Debug, PartialEq, Clone)]
pub struct StructField {
    pub name: String,
    pub tracer: Tracer,
    pub last_seen_in_sample: usize,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum StructMode {
    Struct,
    Map,
}

#[derive(Debug, Clone, Copy, PartialEq)]
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
    pub fn get_path(&self) -> &str {
        &self.path
    }

    pub fn is_complete(&self) -> bool {
        self.fields.iter().all(|field| field.tracer.is_complete())
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

    pub fn get_type(&self) -> Option<&GenericDataType> {
        Some(&GenericDataType::Struct)
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

#[derive(Debug, PartialEq, Clone)]

pub struct UnionTracer {
    pub path: String,
    pub options: TracingOptions,
    pub nullable: bool,
    pub variants: Vec<Option<UnionVariant>>,
    pub state: UnionTracerState,
}

#[derive(Debug, PartialEq, Clone)]
pub struct UnionVariant {
    pub name: String,
    pub tracer: Tracer,
}

#[derive(Debug, Clone, Copy, PartialEq)]
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

    pub fn ensure_variant<S: Into<String> + AsRef<str>>(
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
            let tracer = Tracer::new(
                format!("{path}.{key}", path = self.path, key = variant.as_ref()),
                self.options.clone(),
            );
            let name = variant.into();

            self.variants[idx] = Some(UnionVariant { name, tracer });
        }

        Ok(())
    }

    pub fn get_path(&self) -> &str {
        &self.path
    }

    pub fn is_complete(&self) -> bool {
        self.variants
            .iter()
            .flat_map(|opt| opt.as_ref())
            .all(|variant| variant.tracer.is_complete())
    }

    pub fn get_type(&self) -> Option<&GenericDataType> {
        Some(&GenericDataType::Union)
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

#[derive(Debug, PartialEq, Clone)]
pub struct PrimitiveTracer {
    pub path: String,
    pub options: TracingOptions,
    pub nullable: bool,
    pub strategy: Option<Strategy>,
    pub item_type: GenericDataType,
    pub state: PrimitiveTracerState,
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum PrimitiveTracerState {
    Unfinished,
    Finished,
}

impl PrimitiveTracer {
    pub fn new(
        path: String,
        options: TracingOptions,
        item_type: GenericDataType,
        nullable: bool,
    ) -> Self {
        Self {
            path,
            options,
            item_type,
            nullable,
            strategy: None,
            state: PrimitiveTracerState::Unfinished,
        }
    }

    pub fn finish(&mut self) -> Result<()> {
        if matches!(self.state, PrimitiveTracerState::Finished) {
            fail!("Cannot finish an already finished tracer");
        }
        self.state = PrimitiveTracerState::Finished;
        Ok(())
    }

    pub fn reset(&mut self) -> Result<()> {
        if !matches!(self.state, PrimitiveTracerState::Finished) {
            fail!("Cannot reset an unfished tracer");
        }
        self.state = PrimitiveTracerState::Unfinished;
        Ok(())
    }

    pub fn to_field(&self, name: &str) -> Result<GenericField> {
        type D = GenericDataType;

        if !matches!(self.state, PrimitiveTracerState::Finished) {
            fail!("Cannot build field {name} from unfinished tracer");
        }

        if !self.options.allow_null_fields && matches!(self.item_type, D::Null) {
            fail!(concat!(
                "Encountered null only field. This error can be disabled by ",
                "setting `allow_null_fields` to `true` in `TracingOptions`",
            ));
        }

        match &self.item_type {
            dt @ (D::LargeUtf8 | D::Utf8) => {
                if !self.options.string_dictionary_encoding {
                    Ok(GenericField::new(name, dt.clone(), self.nullable))
                } else {
                    let field = GenericField::new(name, D::Dictionary, self.nullable)
                        .with_child(GenericField::new("key", D::U32, false))
                        .with_child(GenericField::new("value", dt.clone(), false));
                    Ok(field)
                }
            }
            dt => Ok(GenericField::new(name, dt.clone(), self.nullable)
                .with_optional_strategy(self.strategy.clone())),
        }
    }
}

impl PrimitiveTracer {
    pub fn get_path(&self) -> &str {
        &self.path
    }

    pub fn is_complete(&self) -> bool {
        true
    }

    pub fn get_type(&self) -> Option<&GenericDataType> {
        Some(&self.item_type)
    }
}
