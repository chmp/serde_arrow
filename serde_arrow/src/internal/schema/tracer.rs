use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
};

use crate::internal::{
    arrow::{DataType, Field, UnionMode},
    error::{fail, set_default, Context, Result},
    schema::{
        DataTypeDisplay, Overwrites, SerdeArrowSchema, Strategy, TracingMode, TracingOptions,
        STRATEGY_KEY,
    },
};

// TODO: allow to customize
const MAX_TYPE_DEPTH: usize = 20;
const RECURSIVE_TYPE_WARNING: &str =
    "Too deeply nested type detected: recursive types are not supported in schema tracing";

fn default_dictionary_field(name: &str, nullable: bool) -> Field {
    Field {
        name: name.to_owned(),
        nullable,
        metadata: HashMap::new(),
        data_type: DataType::Dictionary(
            Box::new(DataType::UInt32),
            Box::new(DataType::LargeUtf8),
            false,
        ),
    }
}

fn unknown_variant_field() -> Field {
    let mut metadata = HashMap::new();
    metadata.insert(STRATEGY_KEY.into(), Strategy::UnknownVariant.into());
    Field {
        name: String::from(""),
        nullable: true,
        data_type: DataType::Null,
        metadata,
    }
}

struct NullFieldMessage<'a>(&'a str);

impl<'a> std::fmt::Display for NullFieldMessage<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            concat!(
                "Encountered null only field {name}. ",
                "This error can be disabled by setting `allow_null_fields` to `true` in `TracingOptions`.",
            ),
            name = self.0
        )
    }
}

struct EnumWithoutDataMessage<'a>(&'a str);

impl<'a> std::fmt::Display for EnumWithoutDataMessage<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f,
            concat!(
                "Encountered enums without data {name}. ",
                "This error can be disabled by setting `enums_without_data_as_strings` to `true` in `TracingOptions`. ",
                "In this case the enum will be encoded as strings. ",
                "Alternatively, this error can be disabled by setting `allow_null_fields` to `true` in `TracingOptions`. ",
                "In this case the enum will encoded as a Union with Null children.",
            ),
            name=self.0,
        )
    }
}

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
    pub fn new(name: String, path: String, options: Arc<TracingOptions>) -> Self {
        Self::Unknown(UnknownTracer::new(name, path, options))
    }

    /// Convert the traced schema into a schema object
    pub fn to_schema(&self) -> Result<SerdeArrowSchema> {
        let root = self.to_field()?;

        if root.nullable {
            fail!("The root type cannot be nullable");
        }

        let tracing_mode = dispatch_tracer!(self, tracer => tracer.options.tracing_mode);

        let fields = match root.data_type {
            DataType::Struct(children) => children,
            DataType::Null => fail!("No records found to determine schema"),
            dt => fail!(
                concat!(
                    "Schema tracing is not directly supported for the root data type {dt}. ",
                    "Only struct-like types are supported as root types in schema tracing. ",
                    "{mitigation}",
                ),
                dt = DataTypeDisplay(&dt),
                mitigation = match tracing_mode {
                    TracingMode::FromType => "Consider using the `Item` wrapper, i.e., `::from_type<Item<T>>()`.",
                    TracingMode::FromSamples => "Consider using the `Items` wrapper, i.e., `::from_samples(Items(samples))`.",
                    TracingMode::Unknown => "Consider using the `Item` / `Items` wrappers.",
                },
            ),
        };

        Ok(SerdeArrowSchema { fields })
    }
}

impl Tracer {
    pub fn is_complete(&self) -> bool {
        dispatch_tracer!(self, tracer => tracer.is_complete())
    }

    pub fn get_type(&self) -> Option<&str> {
        dispatch_tracer!(self, tracer => tracer.get_type())
    }

    pub fn to_field(&self) -> Result<Field> {
        let path = dispatch_tracer!(self, tracer => &tracer.path);
        if let Some(overwrite) =
            dispatch_tracer!(self, tracer => tracer.options.get_overwrite(path))
        {
            let overwrite_name = &overwrite.name;
            let tracer_name = dispatch_tracer!(self, tracer => &tracer.name);
            if *overwrite_name != *tracer_name {
                let path = path.strip_prefix("$.").unwrap_or(path);
                fail!("Invalid name for overwritten field {path:?}: found {overwrite_name:?}, expected {tracer_name:?}");
            }
            Ok(overwrite.clone())
        } else {
            dispatch_tracer!(self, tracer => tracer.to_field())
        }
    }

    pub fn get_options(&self) -> &TracingOptions {
        dispatch_tracer!(self, tracer => &tracer.options)
    }

    pub fn finish(&mut self) -> Result<()> {
        dispatch_tracer!(self, tracer => tracer.finish())
    }

    pub fn get_depth(&self) -> usize {
        dispatch_tracer!(self, tracer => tracer.path.chars().filter(|c| *c == '.').count())
    }

    pub fn check(&self) -> Result<()> {
        if dispatch_tracer!(self, tracer => tracer.name != "$") {
            fail!("Check must be called on the root tracer");
        }
        let options = self.get_options();
        self.check_overwrites(&options.overwrites)
    }

    pub fn check_overwrites(&self, overwrites: &Overwrites) -> Result<()> {
        let mut paths = HashSet::new();
        self.collect_paths(&mut paths);

        let mut missing = Vec::new();
        for key in overwrites.0.keys() {
            if !paths.contains(key) {
                let key = key.strip_prefix("$.").unwrap_or(key);
                missing.push(key);
            }
        }

        if !missing.is_empty() {
            missing.sort();

            let mut paths = paths
                .into_iter()
                .map(|p| p.strip_prefix("$.").unwrap_or(p))
                .collect::<Vec<_>>();
            paths.sort();

            fail!("Overwritten fields could not be found: missing fields {missing:?}, known fields: {paths:?}");
        }

        Ok(())
    }

    pub fn collect_paths<'this>(&'this self, target: &mut HashSet<&'this String>) {
        match self {
            Self::Unknown(tracer) => {
                target.insert(&tracer.path);
            }
            Self::Primitive(tracer) => {
                target.insert(&tracer.path);
            }
            Self::Map(tracer) => {
                target.insert(&tracer.path);
                tracer.key_tracer.collect_paths(target);
                tracer.value_tracer.collect_paths(target)
            }
            Self::List(tracer) => {
                target.insert(&tracer.path);
                tracer.item_tracer.collect_paths(target);
            }
            Self::Struct(tracer) => {
                target.insert(&tracer.path);
                for field in &tracer.fields {
                    field.tracer.collect_paths(target);
                }
            }
            Self::Union(tracer) => {
                target.insert(&tracer.path);
                for variant in &tracer.variants {
                    let Some(variant) = variant else {
                        continue;
                    };
                    variant.tracer.collect_paths(target);
                }
            }
            Self::Tuple(tracer) => {
                target.insert(&tracer.path);
                for field in &tracer.field_tracers {
                    field.collect_paths(target);
                }
            }
        }
    }
}

impl Tracer {
    pub fn mark_nullable(&mut self) {
        dispatch_tracer!(self, tracer => { tracer.nullable = true; });
    }

    pub fn enforce_depth_limit(&self) -> Result<()> {
        if self.get_depth() >= MAX_TYPE_DEPTH {
            fail!("{RECURSIVE_TYPE_WARNING}");
        }
        Ok(())
    }

    pub fn ensure_struct<S: std::fmt::Display>(
        &mut self,
        fields: &[S],
        mode: StructMode,
    ) -> Result<()> {
        self.enforce_depth_limit()?;

        match self {
            this if matches!(this, Self::Unknown(_))
                || matches!(this, Self::Primitive(ref tracer) if tracer.item_type == DataType::Null) =>
            {
                let field_names = fields
                    .iter()
                    .map(|field| field.to_string())
                    .collect::<Vec<_>>();
                let index = field_names
                    .iter()
                    .enumerate()
                    .map(|(idx, name)| (name.to_string(), idx))
                    .collect::<HashMap<_, _>>();

                let tracer = dispatch_tracer!(this, tracer => StructTracer {
                    name: tracer.name.clone(),
                    path: tracer.path.clone(),
                    options: tracer.options.clone(),
                    fields: fields
                        .iter()
                        .map(|field| {
                            let field = field.to_string();
                            StructField {
                                tracer: Tracer::new(
                                    field.to_owned(),
                                    format!("{}.{}", tracer.path, &field),
                                    tracer.options.clone(),
                                ),
                                name: field,
                                last_seen_in_sample: 0,
                            }
                        })
                        .collect(),
                    index,
                    nullable: tracer.nullable,
                    mode,
                    seen_samples: 0,
                });
                *this = Self::Struct(tracer);
            }
            // TODO: check fields are equal
            Self::Struct(_tracer) => {}
            _ => fail!(
                "Mismatched types: previous {:?}, current struct",
                self.get_type()
            ),
        }
        Ok(())
    }

    pub fn ensure_tuple(&mut self, num_fields: usize) -> Result<()> {
        self.enforce_depth_limit()?;

        match self {
            this if matches!(this, Self::Unknown(_))
                || matches!(this, Self::Primitive(ref tracer) if tracer.item_type == DataType::Null) =>
            {
                let tracer = dispatch_tracer!(this, tracer => TupleTracer {
                    name: tracer.name.clone(),
                    path: tracer.path.clone(),
                    options: tracer.options.clone(),
                    field_tracers: (0..num_fields)
                        .map(|i| {
                            Tracer::new(
                                i.to_string(),
                                format!("{}.{}", tracer.path, i),
                                tracer.options.clone(),
                            )
                        })
                        .collect(),
                    nullable: tracer.nullable,
                });
                *this = Self::Tuple(tracer);
            }
            // TODO: check fields are equal
            Self::Tuple(_tracer) => {}
            _ => fail!(
                "Mismatched types, previous {:?}, current struct",
                self.get_type()
            ),
        }
        Ok(())
    }

    pub fn ensure_union(&mut self, variants: &[&str]) -> Result<()> {
        self.enforce_depth_limit()?;

        match self {
            this if matches!(this, Self::Unknown(_))
                || matches!(this, Self::Primitive(ref tracer) if tracer.item_type == DataType::Null) =>
            {
                let tracer = dispatch_tracer!(this, tracer => UnionTracer {
                    name: tracer.name.clone(),
                    path: tracer.path.clone(),
                    options: tracer.options.clone(),
                    variants: variants
                        .iter()
                        .map(|variant| {
                            Some(UnionVariant {
                                name: variant.to_string(),
                                tracer: Tracer::new(
                                    variant.to_string(),
                                    format!("{}.{}", tracer.path, variant),
                                    tracer.options.clone(),
                                ),
                            })
                        })
                        .collect(),
                    nullable: tracer.nullable,
                });
                *this = Self::Union(tracer);
            }
            // TODO: check fields are equal or fill missing fields
            Self::Union(_tracer) => {}
            _ => fail!(
                "Mismatched types: previous {:?}, current union",
                self.get_type()
            ),
        }
        Ok(())
    }

    pub fn ensure_list(&mut self) -> Result<()> {
        self.enforce_depth_limit()?;

        match self {
            this if matches!(this, Self::Unknown(_))
                || matches!(this, Self::Primitive(ref tracer) if tracer.item_type == DataType::Null) =>
            {
                let tracer = dispatch_tracer!(this, tracer => ListTracer {
                    name: tracer.name.clone(),
                    path: tracer.path.clone(),
                    options: tracer.options.clone(),
                    nullable: tracer.nullable,
                    item_tracer: Box::new(Tracer::new(
                        String::from("element"),
                        format!("{}.element", tracer.path),
                        tracer.options.clone(),
                    )),
                });
                *this = Self::List(tracer);
            }
            Self::List(_tracer) => {}
            _ => fail!(
                "Mismatched types: previous {:?}, current list",
                self.get_type()
            ),
        }
        Ok(())
    }

    pub fn ensure_map(&mut self) -> Result<()> {
        self.enforce_depth_limit()?;

        match self {
            this if matches!(this, Self::Unknown(_))
                || matches!(this, Self::Primitive(ref tracer) if tracer.item_type == DataType::Null) =>
            {
                let tracer = dispatch_tracer!(this, tracer => MapTracer {
                    name: tracer.name.clone(),
                    path: tracer.path.clone(),
                    options: tracer.options.clone(),
                    nullable: tracer.nullable,
                    key_tracer: Box::new(Tracer::new(
                        String::from("key"),
                        format!("{}.key", tracer.get_path()),
                        tracer.options.clone(),
                    )),
                    value_tracer: Box::new(Tracer::new(
                        String::from("value"),
                        format!("{}.value", tracer.get_path()),
                        tracer.options.clone(),
                    )),
                });
                *this = Self::Map(tracer);
            }
            Self::Map(_tracer) => {}
            _ => fail!(
                "Mismatched types: previous {:?}, current list",
                self.get_type()
            ),
        }
        Ok(())
    }

    pub fn ensure_utf8(&mut self, item_type: DataType, strategy: Option<Strategy>) -> Result<()> {
        self.ensure_primitive_with_strategy(item_type, strategy)
    }

    pub fn ensure_primitive(&mut self, item_type: DataType) -> Result<()> {
        self.ensure_primitive_with_strategy(item_type, None)
    }

    pub fn ensure_number(&mut self, item_type: DataType) -> Result<()> {
        self.ensure_primitive_with_strategy(item_type, None)
    }

    fn ensure_primitive_with_strategy(
        &mut self,
        item_type: DataType,
        strategy: Option<Strategy>,
    ) -> Result<()> {
        match self {
            this @ Self::Unknown(_) => {
                let is_null_type = matches!(item_type, DataType::Null);
                let tracer = dispatch_tracer!(this, tracer => PrimitiveTracer {
                    name: tracer.name.clone(),
                    path: tracer.path.clone(),
                    options: tracer.options.clone(),
                    nullable: tracer.nullable || is_null_type,
                    item_type,
                    strategy,
                });
                *this = Self::Primitive(tracer);
            }
            this @ (Self::List(_)
            | Self::Map(_)
            | Self::Struct(_)
            | Self::Tuple(_)
            | Self::Union(_)) => {
                if matches!(item_type, DataType::Null) {
                    dispatch_tracer!(this, tracer => { tracer.nullable = true });
                } else {
                    fail!(
                        "Cannot merge {ty:?} with {item_type:?}",
                        ty = this.get_type()
                    );
                }
            }
            Self::Primitive(tracer) => {
                let (item_type, nullable, strategy) = coerce_primitive_type(
                    (&tracer.item_type, tracer.nullable, tracer.strategy.as_ref()),
                    (item_type, strategy),
                    tracer.options.as_ref(),
                )?;

                tracer.item_type = item_type;
                tracer.strategy = strategy;
                tracer.nullable = nullable;
            }
        }
        Ok(())
    }
}

impl Context for Tracer {
    fn annotate(&self, annotations: &mut BTreeMap<String, String>) {
        dispatch_tracer!(self, tracer => tracer.annotate(annotations))
    }
}

fn coerce_primitive_type(
    prev: (&DataType, bool, Option<&Strategy>),
    curr: (DataType, Option<Strategy>),
    options: &TracingOptions,
) -> Result<(DataType, bool, Option<Strategy>)> {
    use DataType::{
        Date64, Float32, Float64, Int16, Int32, Int64, Int8, LargeUtf8, Null, UInt16, UInt32,
        UInt64, UInt8,
    };

    let res = match (prev, curr) {
        ((prev_ty, nullable, prev_st), (curr_ty, curr_st)) if prev_ty == &curr_ty && prev_st == curr_st.as_ref() => (curr_ty, nullable, curr_st),
        ((Null, _, _), (curr_ty, curr_st)) => (curr_ty, true, curr_st),
        ((prev_ty, _, prev_st), (Null, _)) => (prev_ty.clone(), true, prev_st.cloned()),
        // unsigned x unsigned -> u64
        ((UInt8 | UInt16 | UInt32 | UInt64, nullable, _), (UInt8 | UInt16 | UInt32 | UInt64, _,)) if options.coerce_numbers => (UInt64, nullable, None),
        // signed x signed -> i64
        ((Int8 | Int16 | Int32 | Int64, nullable, _), (Int8 | Int16 | Int32 | Int64, _)) if options.coerce_numbers => (Int64, nullable, None),
        // signed x unsigned -> i64
        ((Int8 | Int16 | Int32 | Int64, nullable, _), (UInt8 | UInt16 | UInt32 | UInt64, _)) if options.coerce_numbers => (Int64, nullable, None),
        // unsigned x signed -> i64
        ((UInt8 | UInt16 | UInt32 | UInt64, nullable, _), (Int8 | Int16 | Int32 | Int64, _)) if options.coerce_numbers => (Int64, nullable, None),
        // float x float -> f64
        ((Float32 | Float64, nullable, _), (Float32 | Float64, _)) if options.coerce_numbers=> (Float64, nullable, None),
        // int x float -> f64
        ((Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64, nullable, _), (Float32 | Float64, _)) if options.coerce_numbers => (Float64, nullable, None),
        // float x int -> f64
        ((Float32 | Float64, nullable, _), (Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64, _)) if options.coerce_numbers => (Float64, nullable, None),
        // incompatible formats, coerce to string
        ((Date64, nullable, _), (LargeUtf8, _)) => (LargeUtf8, nullable, None),
        ((LargeUtf8, nullable, _), (Date64, _)) => (LargeUtf8, nullable, None),
        ((Date64, nullable, prev_st), (Date64, curr_st)) if prev_st != curr_st.as_ref() => (LargeUtf8, nullable, None),
        ((prev_ty, _, prev_st), (curr_ty, curr_st)) => fail!("Cannot accept event {curr_ty:?} with strategy {curr_st:?} for tracer of primitive type {prev_ty:?} with strategy {prev_st:?}"),
    };
    Ok(res)
}

#[derive(Debug, PartialEq, Clone)]
pub struct UnknownTracer {
    pub name: String,
    pub path: String,
    pub options: Arc<TracingOptions>,
    pub nullable: bool,
}

impl Context for UnknownTracer {
    fn annotate(&self, annotations: &mut BTreeMap<String, String>) {
        set_default(annotations, "path", &self.path);
        set_default(annotations, "tracer_type", "Unknown");
    }
}

impl UnknownTracer {
    pub fn new(name: String, path: String, options: Arc<TracingOptions>) -> Self {
        Self {
            name,
            path,
            options,
            nullable: false,
        }
    }

    pub fn to_field(&self) -> Result<Field> {
        if !self.options.allow_null_fields {
            fail!("{}", NullFieldMessage(&self.name));
        }
        Ok(Field {
            name: self.name.to_owned(),
            data_type: DataType::Null,
            nullable: self.nullable,
            metadata: HashMap::new(),
        })
    }

    pub fn finish(&mut self) -> Result<()> {
        Ok(())
    }

    pub fn get_path(&self) -> &str {
        &self.path
    }

    pub fn is_complete(&self) -> bool {
        false
    }

    pub fn get_type(&self) -> Option<&str> {
        None
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct MapTracer {
    pub name: String,
    pub path: String,
    pub options: Arc<TracingOptions>,
    pub nullable: bool,
    pub key_tracer: Box<Tracer>,
    pub value_tracer: Box<Tracer>,
}

impl Context for MapTracer {
    fn annotate(&self, annotations: &mut BTreeMap<String, String>) {
        set_default(annotations, "path", &self.path);
        set_default(annotations, "tracer_type", "Map");
    }
}

impl MapTracer {
    pub fn get_path(&self) -> &str {
        &self.path
    }

    pub fn is_complete(&self) -> bool {
        self.key_tracer.is_complete() && self.value_tracer.is_complete()
    }

    pub fn get_type(&self) -> Option<&str> {
        Some("Map")
    }

    pub fn to_field(&self) -> Result<Field> {
        let entry = Field {
            name: String::from("entries"),
            nullable: false,
            metadata: HashMap::new(),
            data_type: DataType::Struct(vec![
                self.key_tracer.to_field()?,
                self.value_tracer.to_field()?,
            ]),
        };

        Ok(Field {
            name: self.name.to_owned(),
            data_type: DataType::Map(Box::new(entry), false),
            nullable: self.nullable,
            metadata: HashMap::new(),
        })
    }

    pub fn finish(&mut self) -> Result<()> {
        self.key_tracer.finish()?;
        self.value_tracer.finish()?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct ListTracer {
    pub name: String,
    pub path: String,
    pub options: Arc<TracingOptions>,
    pub nullable: bool,
    pub item_tracer: Box<Tracer>,
}

impl Context for ListTracer {
    fn annotate(&self, annotations: &mut BTreeMap<String, String>) {
        set_default(annotations, "path", &self.path);
        set_default(annotations, "tracer_type", "List");
    }
}

impl ListTracer {
    pub fn get_path(&self) -> &str {
        &self.path
    }

    pub fn is_complete(&self) -> bool {
        self.item_tracer.is_complete()
    }

    pub fn get_type(&self) -> Option<&str> {
        Some("List")
    }

    pub fn to_field(&self) -> Result<Field> {
        Ok(Field {
            name: self.name.to_owned(),
            nullable: self.nullable,
            metadata: HashMap::new(),
            data_type: DataType::LargeList(Box::new(self.item_tracer.to_field()?)),
        })
    }

    pub fn finish(&mut self) -> Result<()> {
        self.item_tracer.finish()
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct TupleTracer {
    pub name: String,
    pub path: String,
    pub options: Arc<TracingOptions>,
    pub nullable: bool,
    pub field_tracers: Vec<Tracer>,
}

impl Context for TupleTracer {
    fn annotate(&self, annotations: &mut BTreeMap<String, String>) {
        set_default(annotations, "path", &self.path);
        set_default(annotations, "tracer_type", "Tuple");
    }
}

impl TupleTracer {
    pub fn get_path(&self) -> &str {
        &self.path
    }

    pub fn is_complete(&self) -> bool {
        self.field_tracers.iter().all(|tracer| tracer.is_complete())
    }

    pub fn to_field(&self) -> Result<Field> {
        let mut metadata = HashMap::new();
        metadata.insert(
            STRATEGY_KEY.to_string(),
            Strategy::TupleAsStruct.to_string(),
        );

        let mut fields = Vec::new();
        for tracer in &self.field_tracers {
            fields.push(tracer.to_field()?);
        }

        Ok(Field {
            name: self.name.to_owned(),
            data_type: DataType::Struct(fields),
            nullable: self.nullable,
            metadata,
        })
    }

    pub fn get_type(&self) -> Option<&str> {
        Some("Struct")
    }

    pub fn finish(&mut self) -> Result<()> {
        for tracer in &mut self.field_tracers {
            tracer.finish()?;
        }
        Ok(())
    }

    pub fn field_tracer(&mut self, idx: usize) -> &mut Tracer {
        while self.field_tracers.len() <= idx {
            self.field_tracers.push(Tracer::new(
                idx.to_string(),
                format!("{path}.{idx}", path = self.path),
                self.options.clone(),
            ));
        }
        &mut self.field_tracers[idx]
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct StructTracer {
    pub name: String,
    pub path: String,
    pub options: Arc<TracingOptions>,
    pub nullable: bool,
    pub fields: Vec<StructField>,
    pub index: HashMap<String, usize>,
    pub mode: StructMode,
    /// Count how many samples were seen by this tracer
    pub seen_samples: usize,
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

impl Context for StructTracer {
    fn annotate(&self, annotations: &mut BTreeMap<String, String>) {
        set_default(annotations, "path", &self.path);
        set_default(annotations, "tracer_type", "Struct");
    }
}

impl StructTracer {
    pub fn get_field_tracer_mut(&mut self, idx: usize) -> Option<&mut Tracer> {
        Some(&mut self.fields.get_mut(idx)?.tracer)
    }

    pub fn ensure_field(&mut self, key: &str) -> Result<usize> {
        if let Some(&field_idx) = self.index.get(key) {
            let Some(field) = self.fields.get_mut(field_idx) else {
                fail!("Invalid state: no tracer found for field with name {key}");
            };
            field.last_seen_in_sample = self.seen_samples;

            Ok(field_idx)
        } else {
            let mut field = StructField {
                tracer: Tracer::new(
                    key.to_string(),
                    format!("{path}.{key}", path = self.path),
                    self.options.clone(),
                ),
                name: key.to_owned(),
                last_seen_in_sample: self.seen_samples,
            };

            // field was missing in previous samples
            if self.seen_samples != 0 {
                field.tracer.mark_nullable();
            }

            let field_idx = self.fields.len();
            self.fields.push(field);
            self.index.insert(key.to_owned(), field_idx);
            Ok(field_idx)
        }
    }

    pub fn end(&mut self) -> Result<()> {
        for field in &mut self.fields {
            // field. was not seen in this sample
            if field.last_seen_in_sample != self.seen_samples {
                field.tracer.mark_nullable();
            }
        }
        self.seen_samples += 1;
        Ok(())
    }

    pub fn get_path(&self) -> &str {
        &self.path
    }

    pub fn is_complete(&self) -> bool {
        self.fields.iter().all(|field| field.tracer.is_complete())
    }

    pub fn to_field(&self) -> Result<Field> {
        let mut fields = Vec::new();
        for field in &self.fields {
            fields.push(field.tracer.to_field()?);
        }

        let mut metadata = HashMap::new();
        if let StructMode::Map = self.mode {
            fields.sort_by(|a, b| a.name.cmp(&b.name));
            metadata.insert(STRATEGY_KEY.to_string(), Strategy::MapAsStruct.to_string());
        }

        Ok(Field {
            name: self.name.to_owned(),
            data_type: DataType::Struct(fields),
            nullable: self.nullable,
            metadata,
        })
    }

    pub fn get_type(&self) -> Option<&str> {
        Some("Struct")
    }

    pub fn finish(&mut self) -> Result<()> {
        for field in &mut self.fields {
            field.tracer.finish()?;
        }
        Ok(())
    }
}

#[derive(Debug, PartialEq, Clone)]

pub struct UnionTracer {
    pub name: String,
    pub path: String,
    pub options: Arc<TracingOptions>,
    pub nullable: bool,
    pub variants: Vec<Option<UnionVariant>>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct UnionVariant {
    pub name: String,
    pub tracer: Tracer,
}

impl UnionVariant {
    fn is_null_variant(&self) -> bool {
        match &self.tracer {
            Tracer::Unknown(_) => true,
            Tracer::Primitive(tracer) if matches!(tracer.item_type, DataType::Null) => true,
            _ => false,
        }
    }
}

impl Context for UnionTracer {
    fn annotate(&self, annotations: &mut BTreeMap<String, String>) {
        set_default(annotations, "path", &self.path);
        set_default(annotations, "tracer_type", "Union");
    }
}

impl UnionTracer {
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
                variant.as_ref().to_string(),
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

    pub fn get_type(&self) -> Option<&str> {
        Some("Union")
    }

    pub fn to_field(&self) -> Result<Field> {
        if self.is_without_data() {
            if self.options.enums_without_data_as_strings {
                return Ok(default_dictionary_field(&self.name, self.nullable));
            }
            if !self.options.allow_null_fields {
                fail!("{}", EnumWithoutDataMessage(&self.name));
            }
        }

        let mut fields = Vec::new();
        for (idx, variant) in self.variants.iter().enumerate() {
            if let Some(variant) = variant {
                fields.push((i8::try_from(idx)?, variant.tracer.to_field()?));
            } else {
                fields.push((i8::try_from(idx)?, unknown_variant_field()));
            };
        }

        Ok(Field {
            name: self.name.to_owned(),
            data_type: DataType::Union(fields, UnionMode::Dense),
            nullable: self.nullable,
            metadata: HashMap::new(),
        })
    }

    pub fn is_without_data(&self) -> bool {
        self.variants.iter().all(|v| {
            let Some(v) = v else {
                return false;
            };
            v.is_null_variant()
        })
    }

    pub fn finish(&mut self) -> Result<()> {
        // TODO: fix me
        for variant in &mut self.variants {
            let Some(variant) = variant.as_mut() else {
                continue;
            };
            variant.tracer.finish()?;
        }
        Ok(())
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct PrimitiveTracer {
    pub name: String,
    pub path: String,
    pub options: Arc<TracingOptions>,
    pub nullable: bool,
    pub strategy: Option<Strategy>,
    pub item_type: DataType,
}

impl Context for PrimitiveTracer {
    fn annotate(&self, annotations: &mut BTreeMap<String, String>) {
        set_default(annotations, "path", &self.path);
        set_default(annotations, "tracer_type", "Primitive");
    }
}

impl PrimitiveTracer {
    pub fn finish(&mut self) -> Result<()> {
        Ok(())
    }

    pub fn to_field(&self) -> Result<Field> {
        type D = DataType;

        if !self.options.allow_null_fields && matches!(self.item_type, D::Null) {
            fail!("{}", NullFieldMessage(&self.name));
        }

        match &self.item_type {
            D::Null => Ok(Field {
                name: self.name.to_owned(),
                data_type: DataType::Null,
                nullable: true,
                metadata: HashMap::new(),
            }),
            dt @ (D::LargeUtf8 | D::Utf8) => {
                if !self.options.string_dictionary_encoding {
                    Ok(Field {
                        name: self.name.to_owned(),
                        data_type: dt.clone(),
                        nullable: self.nullable,
                        metadata: HashMap::new(),
                    })
                } else {
                    Ok(default_dictionary_field(&self.name, self.nullable))
                }
            }
            dt => {
                let mut metadata = HashMap::new();
                if let Some(strategy) = self.strategy.as_ref() {
                    metadata.insert(STRATEGY_KEY.to_string(), strategy.to_string());
                }
                Ok(Field {
                    name: self.name.to_owned(),
                    data_type: dt.clone(),
                    nullable: self.nullable,
                    metadata,
                })
            }
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

    pub fn get_type(&self) -> Option<&str> {
        Some("Primitive")
    }
}
