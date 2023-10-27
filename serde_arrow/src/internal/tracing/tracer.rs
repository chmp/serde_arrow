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

impl Tracer {
    pub fn mark_nullable(&mut self) {
        dispatch_tracer!(self, tracer => { tracer.nullable = true; });
    }

    pub fn ensure_struct<S: std::fmt::Display>(&mut self, fields: &[S]) -> Result<()> {
        match self {
            this @ Self::Unknown(_) => {
                let tracer = StructTracer {
                    path: this.get_path().to_owned(),
                    options: this.get_options().clone(),
                    field_tracers: fields
                        .iter()
                        .map(|field| {
                            Tracer::new(
                                format!("{}.{}", this.get_path(), field),
                                this.get_options().clone(),
                            )
                        })
                        .collect(),
                    field_names: fields.iter().map(|field| field.to_string()).collect(),
                    nullable: this.get_nullable(),
                    strategy: None,
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

    pub fn ensure_union(&mut self, variants: &[&str]) -> Result<()> {
        match self {
            this @ Self::Unknown(_) => {
                let tracer = UnionTracer {
                    path: this.get_path().to_owned(),
                    options: this.get_options().clone(),
                    variant_tracers: variants
                        .iter()
                        .map(|variant| {
                            Tracer::new(
                                format!("{}.{}", this.get_path(), variant),
                                this.get_options().clone(),
                            )
                        })
                        .collect(),
                    variant_names: variants.iter().map(|s| s.to_string()).collect(),
                    nullable: this.get_nullable(),
                };
                *this = Self::Union(tracer);
                Ok(())
            }
            Self::Union(_tracer) => {
                // TODO: check fields are equal
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
}

impl MapTracer {
    pub fn get_path(&self) -> &str {
        &self.path
    }

    pub fn is_complete(&self) -> bool {
        self.key_tracer.is_complete() && self.value_tracer.is_complete()
    }

    pub fn to_field(&self, name: &str) -> Result<GenericField> {
        let key = self.key_tracer.to_field("key")?;
        let value = self.value_tracer.to_field("value")?;
        let res = GenericField::new(name, GenericDataType::Map, self.nullable)
            .with_child(key)
            .with_child(value);
        Ok(res)
    }

    pub fn get_type(&self) -> Option<&GenericDataType> {
        Some(&GenericDataType::Map)
    }

    pub fn reset(&mut self) -> Result<()> {
        self.key_tracer.reset()?;
        self.value_tracer.reset()?;
        Ok(())
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

    pub fn to_field(&self, name: &str) -> Result<GenericField> {
        let item = self.item_tracer.to_field("item")?;
        let res =
            GenericField::new(name, GenericDataType::LargeList, self.nullable).with_child(item);
        Ok(res)
    }

    pub fn get_type(&self) -> Option<&GenericDataType> {
        Some(&GenericDataType::LargeList)
    }

    pub fn reset(&mut self) -> Result<()> {
        self.item_tracer.reset()
    }

    pub fn finish(&mut self) -> Result<()> {
        self.item_tracer.finish()
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct StructTracer {
    pub path: String,
    pub options: TracingOptions,
    pub nullable: bool,
    pub field_names: Vec<String>,
    pub field_tracers: Vec<Tracer>,
    pub strategy: Option<Strategy>,
}

impl StructTracer {
    pub fn get_path(&self) -> &str {
        &self.path
    }

    pub fn is_complete(&self) -> bool {
        self.field_tracers.iter().all(Tracer::is_complete)
    }

    pub fn to_field(&self, name: &str) -> Result<GenericField> {
        let mut field = GenericField::new(name, GenericDataType::Struct, self.nullable);

        for (tracer, name) in self.field_tracers.iter().zip(&self.field_names) {
            field.children.push(tracer.to_field(name)?);
        }
        field.strategy = self.strategy.clone();

        Ok(field)
    }

    pub fn get_type(&self) -> Option<&GenericDataType> {
        Some(&GenericDataType::Struct)
    }

    pub fn reset(&mut self) -> Result<()> {
        for tracer in &mut self.field_tracers {
            tracer.reset()?;
        }
        Ok(())
    }

    pub fn finish(&mut self) -> Result<()> {
        for tracer in &mut self.field_tracers {
            tracer.finish()?;
        }
        Ok(())
    }
}

#[derive(Debug, PartialEq, Clone)]

pub struct UnionTracer {
    pub path: String,
    pub options: TracingOptions,
    pub nullable: bool,
    pub variant_names: Vec<String>,
    pub variant_tracers: Vec<Tracer>,
}

impl UnionTracer {
    pub fn get_path(&self) -> &str {
        &self.path
    }

    pub fn is_complete(&self) -> bool {
        self.variant_tracers.iter().all(Tracer::is_complete)
    }

    pub fn to_field(&self, name: &str) -> Result<GenericField> {
        let mut field = GenericField::new(name, GenericDataType::Union, self.nullable);
        for (tracer, name) in self.variant_tracers.iter().zip(&self.variant_names) {
            field.children.push(tracer.to_field(name)?);
        }
        Ok(field)
    }

    pub fn get_type(&self) -> Option<&GenericDataType> {
        Some(&GenericDataType::Union)
    }

    pub fn reset(&mut self) -> Result<()> {
        for tracer in &mut self.variant_tracers {
            tracer.reset()?;
        }
        Ok(())
    }

    pub fn finish(&mut self) -> Result<()> {
        for tracer in &mut self.variant_tracers {
            tracer.finish()?;
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
