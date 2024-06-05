use std::collections::HashMap;

use crate::internal::{
    error::{fail, Result},
    schema::{GenericDataType, GenericField, SerdeArrowSchema, Strategy},
};

use super::tracing_options::{TracingMode, TracingOptions};

// TODO: allow to customize
const MAX_TYPE_DEPTH: usize = 20;
const RECURSIVE_TYPE_WARNING: &str =
    "too deeply nested type detected. Recursive types are not supported in schema tracing";

fn default_dictionary_field(name: &str, nullable: bool) -> GenericField {
    GenericField::new(name, GenericDataType::Dictionary, nullable)
        .with_child(GenericField::new("key", GenericDataType::U32, nullable))
        .with_child(GenericField::new(
            "value",
            GenericDataType::LargeUtf8,
            false,
        ))
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
    pub fn new(path: String, options: TracingOptions) -> Self {
        Self::Unknown(UnknownTracer::new(path, options))
    }

    /// Convert the traced schema into a schema object
    pub fn to_schema(&self) -> Result<SerdeArrowSchema> {
        let root = self.to_field("root")?;

        if root.nullable {
            fail!("The root type cannot be nullable");
        }

        let tracing_mode = self.get_options().tracing_mode;

        let fields = match root.data_type {
            GenericDataType::Struct => root.children,
            GenericDataType::Null => fail!("No records found to determine schema"),
            dt => fail!(
                concat!(
                    "Schema tracing is not directly supported for the root data type {dt}. ",
                    "Only struct-like types are supported as root types in schema tracing. ",
                    "{mitigation}",
                ),
                dt = dt,
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

    pub fn get_options(&self) -> &TracingOptions {
        dispatch_tracer!(self, tracer => &tracer.options)
    }

    pub fn finish(&mut self) -> Result<()> {
        dispatch_tracer!(self, tracer => tracer.finish())
    }

    pub fn get_depth(&self) -> usize {
        self.get_path().chars().filter(|c| *c == '.').count()
    }
}

// TODO: move into trace any?
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
                    mode,
                    seen_samples: 0,
                };
                *this = Self::Struct(tracer);
            }
            // TODO: check fields are equal
            Self::Struct(_tracer) => {}
            _ => fail!(
                "mismatched types, previous {:?}, current struct",
                self.get_type()
            ),
        }
        Ok(())
    }

    pub fn ensure_tuple(&mut self, num_fields: usize) -> Result<()> {
        self.enforce_depth_limit()?;

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
                };
                *this = Self::Tuple(tracer);
            }
            // TODO: check fields are equal
            Self::Tuple(_tracer) => {}
            _ => fail!(
                "mismatched types, previous {:?}, current struct",
                self.get_type()
            ),
        }
        Ok(())
    }

    pub fn ensure_union(&mut self, variants: &[&str]) -> Result<()> {
        self.enforce_depth_limit()?;

        match self {
            this @ Self::Unknown(_) => {
                let tracer = UnionTracer {
                    path: this.get_path().to_owned(),
                    options: this.get_options().clone(),
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
            }
            // TODO: check fields are equal or fill missing fields
            Self::Union(_tracer) => {}
            _ => fail!(
                "mismatched types, previous {:?}, current union",
                self.get_type()
            ),
        }
        Ok(())
    }

    pub fn ensure_list(&mut self) -> Result<()> {
        self.enforce_depth_limit()?;

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
                };
                *this = Self::List(tracer);
            }
            Self::List(_tracer) => {}
            _ => fail!(
                "mismatched types, previous {:?}, current list",
                self.get_type()
            ),
        }
        Ok(())
    }

    pub fn ensure_map(&mut self) -> Result<()> {
        self.enforce_depth_limit()?;

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
                };
                *this = Self::Map(tracer);
            }
            Self::Map(_tracer) => {}
            _ => fail!(
                "mismatched types, previous {:?}, current list",
                self.get_type()
            ),
        }
        Ok(())
    }

    pub fn ensure_utf8(
        &mut self,
        item_type: GenericDataType,
        strategy: Option<Strategy>,
    ) -> Result<()> {
        if self.is_unknown() {
            let tracer = PrimitiveTracer::new(
                self.get_path().to_owned(),
                self.get_options().clone(),
                item_type,
                self.get_nullable(),
            )
            .with_strategy(strategy);
            *self = Self::Primitive(tracer);
        } else if let Tracer::Primitive(tracer) = self {
            use {
                GenericDataType::Date64, GenericDataType::LargeUtf8, Strategy::NaiveStrAsDate64,
                Strategy::UtcStrAsDate64,
            };
            let (item_type, strategy) = match ((&tracer.item_type), (item_type)) {
                (Date64, Date64) => match (&tracer.strategy, strategy) {
                    (Some(NaiveStrAsDate64), Some(NaiveStrAsDate64)) => {
                        (Date64, Some(NaiveStrAsDate64))
                    }
                    (Some(UtcStrAsDate64), Some(UtcStrAsDate64)) => (Date64, Some(UtcStrAsDate64)),
                    // incompatible strategies, coerce to string
                    (_, _) => (LargeUtf8, None),
                },
                (LargeUtf8, _) | (_, LargeUtf8) => (LargeUtf8, None),
                (prev_ty, new_ty) => {
                    fail!("mismatched types, previous {prev_ty}, current {new_ty}")
                }
            };
            tracer.item_type = item_type;
            tracer.strategy = strategy;
        } else {
            let Some(ty) = self.get_type() else {
                unreachable!("tracer cannot be unknown");
            };
            fail!("mismatched types, previous {ty}, current {item_type}");
        }
        Ok(())
    }

    pub fn ensure_primitive(&mut self, item_type: GenericDataType) -> Result<()> {
        match self {
            this @ Self::Unknown(_) => {
                let tracer = PrimitiveTracer::new(
                    this.get_path().to_owned(),
                    this.get_options().clone(),
                    item_type,
                    this.get_nullable(),
                );
                *this = Self::Primitive(tracer);
            }
            Self::Primitive(tracer) if tracer.item_type == item_type => {}
            _ => fail!(
                "mismatched types, previous {:?}, current {:?}",
                self.get_type(),
                item_type
            ),
        }
        Ok(())
    }

    pub fn ensure_number(&mut self, item_type: GenericDataType) -> Result<()> {
        match self {
            this @ Self::Unknown(_) => {
                let tracer = PrimitiveTracer::new(
                    this.get_path().to_owned(),
                    this.get_options().clone(),
                    item_type,
                    this.get_nullable(),
                );
                *this = Self::Primitive(tracer);
            }
            Self::Primitive(tracer) if tracer.options.coerce_numbers => {
                use GenericDataType::{F32, F64, I16, I32, I64, I8, U16, U32, U64, U8};
                let item_type = match (&tracer.item_type, item_type) {
                    // unsigned x unsigned -> u64
                    (U8 | U16 | U32 | U64, U8 | U16 | U32 | U64) => U64,
                    // signed x signed -> i64
                    (I8 | I16 | I32 | I64, I8 | I16 | I32 | I64) => I64,
                    // signed x unsigned -> i64
                    (I8 | I16 | I32 | I64, U8 | U16 | U32 | U64) => I64,
                    // unsigned x signed -> i64
                    (U8 | U16 | U32 | U64, I8 | I16 | I32 | I64) => I64,
                    // float x float -> f64
                    (F32 | F64, F32 | F64) => F64,
                    // int x float -> f64
                    (I8 | I16 | I32 | I64 | U8 | U16 | U32 | U64, F32 | F64) => F64,
                    // float x int -> f64
                    (F32 | F64, I8 | I16 | I32 | I64 | U8 | U16 | U32 | U64) => F64,
                    (ty, ev) => fail!("Cannot accept event {ev} for tracer of primitive type {ty}"),
                };
                tracer.item_type = item_type;
            }
            Self::Primitive(tracer) if tracer.item_type == item_type => {}
            _ => fail!(
                "mismatched types, previous {:?}, current {:?}",
                self.get_type(),
                item_type
            ),
        }
        Ok(())
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct UnknownTracer {
    pub path: String,
    pub options: TracingOptions,
    pub nullable: bool,
}

impl UnknownTracer {
    pub fn new(path: String, options: TracingOptions) -> Self {
        Self {
            path,
            options,
            nullable: false,
        }
    }

    pub fn to_field(&self, name: &str) -> Result<GenericField> {
        if !self.options.allow_null_fields {
            fail!("{}", NullFieldMessage(name));
        }
        Ok(GenericField::new(
            name,
            GenericDataType::Null,
            self.nullable,
        ))
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
}

impl MapTracer {
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
        let mut entries = GenericField::new("entries", GenericDataType::Struct, false);
        entries.children.push(self.key_tracer.to_field("key")?);
        entries.children.push(self.value_tracer.to_field("value")?);

        let mut field = GenericField::new(name, GenericDataType::Map, self.nullable);
        field.children.push(entries);

        Ok(field)
    }

    pub fn finish(&mut self) -> Result<()> {
        self.key_tracer.finish()?;
        self.value_tracer.finish()?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct ListTracer {
    pub path: String,
    pub options: TracingOptions,
    pub nullable: bool,
    pub item_tracer: Box<Tracer>,
}

impl ListTracer {
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
        let mut field = GenericField::new(name, GenericDataType::LargeList, self.nullable);
        field.children.push(self.item_tracer.to_field("element")?);

        Ok(field)
    }

    pub fn finish(&mut self) -> Result<()> {
        self.item_tracer.finish()
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct TupleTracer {
    pub path: String,
    pub options: TracingOptions,
    pub nullable: bool,
    pub field_tracers: Vec<Tracer>,
}

impl TupleTracer {
    pub fn get_path(&self) -> &str {
        &self.path
    }

    pub fn is_complete(&self) -> bool {
        self.field_tracers.iter().all(|tracer| tracer.is_complete())
    }

    pub fn to_field(&self, name: &str) -> Result<GenericField> {
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

    pub fn finish(&mut self) -> Result<()> {
        for tracer in &mut self.field_tracers {
            tracer.finish()?;
        }
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

impl StructTracer {
    pub fn get_field_tracer_mut(&mut self, idx: usize) -> Option<&mut Tracer> {
        Some(&mut self.fields.get_mut(idx)?.tracer)
    }

    pub fn ensure_field(&mut self, key: &str) -> Result<usize> {
        if let Some(&field_idx) = self.index.get(key) {
            let Some(field) = self.fields.get_mut(field_idx) else {
                fail!("invalid state");
            };
            field.last_seen_in_sample = self.seen_samples;

            Ok(field_idx)
        } else {
            let mut field = StructField {
                tracer: Tracer::new(
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

    pub fn to_field(&self, name: &str) -> Result<GenericField> {
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

    pub fn finish(&mut self) -> Result<()> {
        for field in &mut self.fields {
            field.tracer.finish()?;
        }
        Ok(())
    }
}

#[derive(Debug, PartialEq, Clone)]

pub struct UnionTracer {
    pub path: String,
    pub options: TracingOptions,
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
        // Note: unknown tracers are treated as Null tracers
        matches!(self.tracer.get_type(), None | Some(GenericDataType::Null))
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
        if self.is_without_data() {
            if self.options.enums_without_data_as_strings {
                return Ok(default_dictionary_field(name, self.nullable));
            }
            if !self.options.allow_null_fields {
                fail!("{}", EnumWithoutDataMessage(name));
            }
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
    pub path: String,
    pub options: TracingOptions,
    pub nullable: bool,
    pub strategy: Option<Strategy>,
    pub item_type: GenericDataType,
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
        }
    }

    pub fn with_strategy(mut self, strategy: Option<Strategy>) -> Self {
        self.strategy = strategy;
        self
    }

    pub fn finish(&mut self) -> Result<()> {
        Ok(())
    }

    pub fn to_field(&self, name: &str) -> Result<GenericField> {
        type D = GenericDataType;

        if !self.options.allow_null_fields && matches!(self.item_type, D::Null) {
            fail!("{}", NullFieldMessage(name));
        }

        match &self.item_type {
            D::Null => Ok(GenericField::new(name, D::Null, true)),
            dt @ (D::LargeUtf8 | D::Utf8) => {
                if !self.options.string_dictionary_encoding {
                    Ok(GenericField::new(name, dt.clone(), self.nullable))
                } else {
                    Ok(default_dictionary_field(name, self.nullable))
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
