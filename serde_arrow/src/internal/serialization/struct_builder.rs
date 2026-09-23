use std::collections::{BTreeMap, HashMap};

use marrow::{
    array::{Array, StructArray},
    datatypes::{Field, FieldMeta},
};
use serde::{Serialize, Serializer};

use crate::internal::{
    error::{fail, prepend, set_default, try_, Context, ContextSupport, Error, ErrorKind, Result},
    serialization::{construction::build_struct, utils::impl_serializer},
    utils::array_ext::{ArrayExt, CountArray, SeqArrayExt},
};

use super::array_builder::ArrayBuilder;

const UNKNOWN_KEY: usize = usize::MAX;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CanonicalMode {
    /// Determine whether this record uses schema-ordered static field names.
    Discover,
    /// Serialize a record using a previously discovered canonical layout.
    Active,
    /// Serialize through the regular field-lookup path.
    Disabled,
}

#[derive(Debug, Clone)]
pub struct StructBuilder {
    pub name: String,
    pub fields: Vec<ArrayBuilder>,
    // Note: for the complex_1000 benchmark this optimization reduces the relative
    // runtime by 1.26
    lookup_cache: CachedNameLookup,
    pub next: usize,
    pub seen: Vec<bool>,
    /// Number of fields written in the struct currently being serialized.
    seen_count: usize,
    /// Whether a prior struct established that its static field names arrive in
    /// schema order.
    discovered_canonical_layout: bool,
    /// How the struct currently being serialized uses or discovers a canonical
    /// field layout.
    canonical_mode: CanonicalMode,
    field_names_unique: bool,
    pub seq: CountArray,
    pub metadata: HashMap<String, String>,
}

impl StructBuilder {
    pub fn from_fields(fields: Vec<Field>) -> Result<Self> {
        build_struct(String::from("$"), fields, false, Default::default())
    }

    pub fn new(
        name: String,
        fields: Vec<ArrayBuilder>,
        is_nullable: bool,
        metadata: HashMap<String, String>,
    ) -> Result<Self> {
        Ok(Self {
            name,
            seq: CountArray::new(is_nullable),
            seen: vec![false; fields.len()],
            seen_count: 0,
            discovered_canonical_layout: false,
            canonical_mode: CanonicalMode::Disabled,
            field_names_unique: field_names_are_unique(&fields),
            next: 0,
            lookup_cache: CachedNameLookup::new(fields.len()),
            fields,
            metadata,
        })
    }

    pub fn take_self(&mut self) -> Self {
        Self {
            name: self.name.clone(),
            metadata: self.metadata.clone(),
            fields: self
                .fields
                .iter_mut()
                .map(|builder| builder.take())
                .collect(),
            lookup_cache: std::mem::replace(
                &mut self.lookup_cache,
                CachedNameLookup::new(self.fields.len()),
            ),
            seen: std::mem::replace(&mut self.seen, vec![false; self.fields.len()]),
            seen_count: std::mem::take(&mut self.seen_count),
            discovered_canonical_layout: std::mem::take(&mut self.discovered_canonical_layout),
            canonical_mode: CanonicalMode::Disabled,
            field_names_unique: self.field_names_unique,
            seq: self.seq.take(),
            next: std::mem::take(&mut self.next),
        }
    }

    pub fn take(&mut self) -> ArrayBuilder {
        ArrayBuilder::Struct(self.take_self())
    }

    pub fn is_nullable(&self) -> bool {
        self.seq.validity.is_some()
    }

    pub fn into_array_and_field_meta(self) -> Result<(Array, FieldMeta)> {
        let meta = FieldMeta {
            name: self.name,
            metadata: self.metadata,
            nullable: self.seq.validity.is_some(),
        };

        let mut fields = Vec::with_capacity(self.fields.len());
        for builder in self.fields {
            let (array, meta) = builder.into_array_and_field_meta()?;
            fields.push((meta, array));
        }

        let array = Array::Struct(StructArray {
            len: self.seq.len,
            validity: self.seq.validity,
            fields,
        });
        Ok((array, meta))
    }

    pub fn reserve(&mut self, additional: usize) {
        self.seq.reserve(additional);
        for builder in &mut self.fields {
            builder.reserve(additional);
        }
    }

    pub fn serialize_default_value(&mut self) -> Result<()> {
        try_(|| {
            self.seq.push_seq_default()?;
            for builder in &mut self.fields {
                builder.serialize_default_value()?;
            }

            Ok(())
        })
        .ctx(self)
    }

    pub fn serialize_value<V: Serialize>(&mut self, value: V) -> Result<()> {
        value.serialize(&mut *self).ctx(self)
    }

    pub fn num_fields(&self) -> usize {
        self.fields.len()
    }
}

impl StructBuilder {
    fn start_with_mode(&mut self, canonical_mode: CanonicalMode) -> Result<()> {
        self.seq.start_seq()?;
        if canonical_mode != CanonicalMode::Active {
            self.seen.fill(false);
            self.seen_count = 0;
        }
        self.canonical_mode = canonical_mode;
        self.next = 0;
        Ok(())
    }

    fn start_struct(&mut self) -> Result<()> {
        let canonical_mode = if self.discovered_canonical_layout {
            CanonicalMode::Active
        } else {
            CanonicalMode::Discover
        };
        self.start_with_mode(canonical_mode)
    }

    fn leave_canonical_layout(&mut self) {
        if self.canonical_mode != CanonicalMode::Active {
            return;
        }

        self.seen.fill(false);
        self.seen[..self.next].fill(true);
        self.seen_count = self.next;
        self.canonical_mode = CanonicalMode::Disabled;
    }

    pub fn end(&mut self) -> Result<()> {
        self.seq.end_seq()?;
        match self.canonical_mode {
            CanonicalMode::Active if self.next == self.fields.len() => return Ok(()),
            CanonicalMode::Active => self.leave_canonical_layout(),
            CanonicalMode::Discover if self.seen_count == self.fields.len() => {
                self.discovered_canonical_layout = true;
                return Ok(());
            }
            CanonicalMode::Discover | CanonicalMode::Disabled => {}
        }
        for (seen, field) in std::iter::zip(&self.seen, &mut self.fields) {
            if !*seen {
                if !field.is_nullable() {
                    return Err(Error::new(
                        ErrorKind::MissingField {
                            field: field.get_name().into(),
                        },
                        format!(
                            "missing non-nullable field {:?} in struct",
                            field.get_name()
                        ),
                    ));
                }

                field.serialize_none()?;
            }
        }
        Ok(())
    }

    pub fn element<T: Serialize + ?Sized>(&mut self, idx: usize, value: &T) -> Result<()> {
        self.seq.push_seq_elements(1)?;
        let Some(seen) = self.seen.get_mut(idx) else {
            fail!(
                "field index {idx} is out of bounds for struct with {} fields",
                self.fields.len()
            );
        };
        let num_fields = self.fields.len();
        let Some(field) = self.fields.get_mut(idx) else {
            fail!(
                "field index {idx} is out of bounds for struct with {} fields",
                num_fields
            );
        };

        if *seen {
            fail!("duplicate field {key:?}", key = field.get_name());
        }

        field.serialize_value(value)?;
        *seen = true;
        self.seen_count += 1;
        self.next = idx + 1;
        Ok(())
    }

    fn canonical_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        self.seq.push_seq_elements(1)?;
        let Some(field) = self.fields.get_mut(self.next) else {
            fail!(
                "field index {} is out of bounds for struct with {} fields",
                self.next,
                self.fields.len()
            );
        };
        field.serialize_value(value)?;
        self.next += 1;
        Ok(())
    }
}

impl Context for StructBuilder {
    fn annotate(&self, annotations: &mut BTreeMap<String, String>) {
        prepend(annotations, "field", &self.name);
        set_default(annotations, "data_type", "Struct");
    }
}

impl<'a> Serializer for &'a mut StructBuilder {
    impl_serializer!(
        'a, StructBuilder;
        override serialize_map,
        override serialize_none,
        override serialize_struct,
        override serialize_tuple,
        override serialize_seq,
    );

    fn serialize_none(self) -> Result<()> {
        self.seq.push_seq_none()?;
        for builder in &mut self.fields {
            builder.serialize_default_value()?;
        }
        Ok(())
    }

    fn serialize_struct(self, _name: &'static str, _len: usize) -> Result<Self::SerializeStruct> {
        self.start_struct()?;
        Ok(Self::SerializeStruct::Struct(self))
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap> {
        self.start_with_mode(CanonicalMode::Disabled)?;
        // always re-set to an invalid field to force that `_key()` is called before `_value()`.
        self.next = UNKNOWN_KEY;
        Ok(Self::SerializeMap::Struct(self))
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple> {
        self.start_with_mode(CanonicalMode::Disabled)?;
        Ok(Self::SerializeTuple::Struct(self))
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq> {
        self.start_with_mode(CanonicalMode::Disabled)?;
        Ok(Self::SerializeSeq::Struct(self))
    }
}

impl serde::ser::SerializeStruct for &mut StructBuilder {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T: ?Sized + serde::Serialize>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<()> {
        if self.canonical_mode == CanonicalMode::Active && self.lookup_cache.matches(self.next, key)
        {
            return self.canonical_element(value);
        }

        if self.canonical_mode == CanonicalMode::Active {
            self.leave_canonical_layout();
        }

        if let Some(idx) = self.lookup_cache.lookup(self.next, key, &self.fields) {
            if self.canonical_mode == CanonicalMode::Discover && idx != self.next {
                self.canonical_mode = CanonicalMode::Disabled;
            }
            self.element(idx, value)
        } else {
            // ignore unknown fields
            self.canonical_mode = CanonicalMode::Disabled;
            Ok(())
        }
    }

    fn end(self) -> Result<()> {
        StructBuilder::end(self)
    }
}

impl serde::ser::SerializeMap for &mut StructBuilder {
    type Ok = ();
    type Error = Error;

    fn serialize_key<T: ?Sized + Serialize>(&mut self, key: &T) -> Result<()> {
        self.next = KeyLookupSerializer::lookup(&self.fields, self.field_names_unique, key)?
            .unwrap_or(UNKNOWN_KEY);
        Ok(())
    }

    fn serialize_value<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<()> {
        if self.next != UNKNOWN_KEY {
            self.element(self.next, value)?;
        }
        self.next = UNKNOWN_KEY;
        Ok(())
    }

    fn end(self) -> Result<()> {
        StructBuilder::end(self)
    }
}

impl serde::ser::SerializeSeq for &mut StructBuilder {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<()> {
        // ignore extra tuple fields
        if self.next < self.fields.len() {
            self.element(self.next, value)?;
        }
        Ok(())
    }

    fn end(self) -> Result<()> {
        StructBuilder::end(self)
    }
}

impl serde::ser::SerializeTuple for &mut StructBuilder {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<()> {
        // ignore extra tuple fields
        if self.next < self.fields.len() {
            self.element(self.next, value)?;
        }
        Ok(())
    }

    fn end(self) -> Result<()> {
        StructBuilder::end(self)
    }
}

/// A wrapper around a static field name that compares using ptr and length
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StaticFieldName(*const u8, usize);

// SAFETY: StaticFieldName is always constructed from a &static str and never read
unsafe impl Send for StaticFieldName {}

// SAFETY: StaticFieldName is always constructed from a &static str and never read
unsafe impl Sync for StaticFieldName {}

impl StaticFieldName {
    const EMPTY: Self = Self(std::ptr::null(), 0);

    pub fn new(s: &'static str) -> Self {
        Self(s.as_ptr(), s.len())
    }

    fn is_empty(self) -> bool {
        self == Self::EMPTY
    }
}

trait Named {
    fn get_name(&self) -> &str;
}

fn field_names_are_unique(fields: &[ArrayBuilder]) -> bool {
    fields.iter().enumerate().all(|(idx, field)| {
        fields[..idx]
            .iter()
            .all(|previous| previous.get_name() != field.get_name())
    })
}

impl Named for ArrayBuilder {
    fn get_name(&self) -> &str {
        ArrayBuilder::get_name(self)
    }
}

impl Named for &str {
    fn get_name(&self) -> &str {
        self
    }
}

#[derive(Debug, Clone)]
struct CachedNameLookup {
    cache: Vec<StaticFieldName>,
}

impl CachedNameLookup {
    fn new(n_fields: usize) -> Self {
        Self {
            cache: vec![StaticFieldName::EMPTY; n_fields],
        }
    }

    fn lookup(&mut self, guess: usize, name: &'static str, fields: &[impl Named]) -> Option<usize> {
        let static_name = StaticFieldName::new(name);
        if self.cache.get(guess) == Some(&static_name) {
            Some(guess)
        } else if fields.get(guess).map(|field| field.get_name()) == Some(name) {
            if let Some(cached) = self.cache.get_mut(guess) {
                if cached.is_empty() {
                    *cached = static_name;
                }
            }
            Some(guess)
        } else if let Some(idx) = self.lookup_field_loop(name, fields) {
            if let Some(cached) = self.cache.get_mut(idx) {
                if cached.is_empty() {
                    *cached = static_name;
                }
            }
            Some(idx)
        } else {
            None
        }
    }

    fn matches(&self, idx: usize, name: &'static str) -> bool {
        self.cache.get(idx) == Some(&StaticFieldName::new(name))
    }

    fn lookup_field_loop(&self, name: &str, fields: &[impl Named]) -> Option<usize> {
        fields.iter().position(|field| field.get_name() == name)
    }
}

#[derive(Debug)]
pub struct KeyLookupSerializer<'a> {
    fields: &'a [ArrayBuilder],
    field_names_unique: bool,
    result: Option<usize>,
}

impl<'a> KeyLookupSerializer<'a> {
    pub fn lookup<K: Serialize + ?Sized>(
        fields: &'a [ArrayBuilder],
        field_names_unique: bool,
        key: &K,
    ) -> Result<Option<usize>> {
        let mut this = Self {
            fields,
            field_names_unique,
            result: None,
        };
        key.serialize(&mut this)?;
        Ok(this.result)
    }
}

impl Context for KeyLookupSerializer<'_> {
    fn annotate(&self, _: &mut BTreeMap<String, String>) {}
}

impl<'a> Serializer for &'a mut KeyLookupSerializer<'_> {
    impl_serializer!(
        'a, KeyLookupSerializer;
        override serialize_str,
    );

    fn serialize_str(self, v: &str) -> Result<()> {
        for (idx, builder) in self.fields.iter().enumerate() {
            if builder.get_name() == v {
                self.result = Some(idx);
                if self.field_names_unique {
                    break;
                }
            }
        }
        Ok(())
    }
}

#[test]
fn example() {
    let mut lookup = CachedNameLookup::new(3);

    const FOO: &str = "foo";
    const BAR: &str = "bar";
    const BAZ: &str = "baz";

    assert_eq!(lookup.lookup(0, FOO, &["foo", "bar", "baz"]), Some(0));
    assert_eq!(lookup.lookup(1, BAR, &["foo", "bar", "baz"]), Some(1));
    assert_eq!(lookup.lookup(2, BAZ, &["foo", "bar", "baz"]), Some(2));

    assert_eq!(lookup.cache[0], StaticFieldName::new(FOO));

    assert_eq!(lookup.cache[1], StaticFieldName::new(BAR));

    assert_eq!(lookup.cache[2], StaticFieldName::new(BAZ));

    assert_eq!(lookup.lookup(0, FOO, &["foo", "bar", "baz"]), Some(0));
    assert_eq!(lookup.lookup(1, BAR, &["foo", "bar", "baz"]), Some(1));
    assert_eq!(lookup.lookup(2, BAZ, &["foo", "bar", "baz"]), Some(2));

    assert_eq!(lookup.lookup(0, FOO, &["foo", "bar", "baz"]), Some(0));
    assert_eq!(lookup.lookup(1, FOO, &["foo", "bar", "baz"]), Some(0));
    assert_eq!(lookup.lookup(2, FOO, &["foo", "bar", "baz"]), Some(0));

    assert_eq!(lookup.lookup(0, BAR, &["foo", "bar", "baz"]), Some(1));
    assert_eq!(lookup.lookup(1, BAR, &["foo", "bar", "baz"]), Some(1));
    assert_eq!(lookup.lookup(2, BAR, &["foo", "bar", "baz"]), Some(1));

    assert_eq!(lookup.lookup(0, BAZ, &["foo", "bar", "baz"]), Some(2));
    assert_eq!(lookup.lookup(1, BAZ, &["foo", "bar", "baz"]), Some(2));
    assert_eq!(lookup.lookup(2, BAZ, &["foo", "bar", "baz"]), Some(2));
}

#[test]
fn canonical_layout_deopts_for_reordered_fields() {
    use marrow::datatypes::DataType;
    use serde::ser::SerializeStruct;

    #[derive(Serialize)]
    struct Canonical {
        a: i8,
        b: i8,
    }

    struct Reordered {
        a: i8,
        b: i8,
    }

    impl Serialize for Reordered {
        fn serialize<S: serde::Serializer>(
            &self,
            serializer: S,
        ) -> std::result::Result<S::Ok, S::Error> {
            let mut state = serializer.serialize_struct("Reordered", 2)?;
            state.serialize_field("b", &self.b)?;
            state.serialize_field("a", &self.a)?;
            state.end()
        }
    }

    let mut builder = StructBuilder::from_fields(vec![
        Field {
            name: "a".into(),
            data_type: DataType::Int8,
            nullable: false,
            metadata: Default::default(),
        },
        Field {
            name: "b".into(),
            data_type: DataType::Int8,
            nullable: false,
            metadata: Default::default(),
        },
    ])
    .unwrap();

    builder.serialize_value((1i8, 2i8)).unwrap();
    assert!(!builder.discovered_canonical_layout);

    builder.serialize_value(Canonical { a: 3, b: 4 }).unwrap();
    assert!(builder.discovered_canonical_layout);

    builder.serialize_value(Reordered { a: 5, b: 6 }).unwrap();

    let (array, _) = builder.into_array_and_field_meta().unwrap();
    let Array::Struct(array) = array else {
        panic!("expected struct array");
    };
    let Array::Int8(a) = &array.fields[0].1 else {
        panic!("expected i8 field");
    };
    let Array::Int8(b) = &array.fields[1].1 else {
        panic!("expected i8 field");
    };
    assert_eq!(a.values, vec![1, 3, 5]);
    assert_eq!(b.values, vec![2, 4, 6]);
}
