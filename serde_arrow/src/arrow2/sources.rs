use std::marker::PhantomData;

use arrow2::{
    array::{Array, BooleanArray, ListArray, PrimitiveArray, StructArray, UnionArray, Utf8Array},
    datatypes::{DataType, Field},
    types::{NativeType, Offset},
};

use crate::{
    base::{
        error::{error, fail},
        source::DynamicSource,
        Event, EventSource,
    },
    generic::{
        chrono::{NaiveDateTimeStrSource, UtcDateTimeStrSource},
        sources::{ListSource, RecordSource, StructSource, UnionSource},
    },
    generic::{
        schema::{Strategy, STRATEGY_KEY},
        sources::TupleSource,
    },
    Result,
};

use super::schema::check_strategy;

pub fn build_record_source<'a, A: AsRef<dyn Array> + 'a>(
    fields: &'a [Field],
    arrays: &'a [A],
) -> Result<RecordSource<'a, DynamicSource<'a>>> {
    let mut columns = Vec::new();
    let mut sources = Vec::new();

    for i in 0..fields.len() {
        columns.push(fields[i].name.as_str());
        sources.push(build_dynamic_source(&fields[i], arrays[i].as_ref())?);
    }

    Ok(RecordSource::new(columns, sources))
}

pub fn build_dynamic_source<'a>(
    field: &'a Field,
    array: &'a dyn Array,
) -> Result<DynamicSource<'a>> {
    check_strategy(field)?;

    let source = match field.data_type() {
        DataType::Null => DynamicSource::new(NullArraySource::new(array.len())),
        DataType::Int8 => build_dynamic_primitive_source::<i8>(field, array)?,
        DataType::Int16 => build_dynamic_primitive_source::<i16>(field, array)?,
        DataType::Int32 => build_dynamic_primitive_source::<i32>(field, array)?,
        DataType::Int64 => build_dynamic_primitive_source::<i64>(field, array)?,
        DataType::UInt8 => build_dynamic_primitive_source::<u8>(field, array)?,
        DataType::UInt16 => build_dynamic_primitive_source::<u16>(field, array)?,
        DataType::UInt32 => build_dynamic_primitive_source::<u32>(field, array)?,
        DataType::UInt64 => build_dynamic_primitive_source::<u64>(field, array)?,
        DataType::Float32 => build_dynamic_primitive_source::<f32>(field, array)?,
        DataType::Float64 => build_dynamic_primitive_source::<f64>(field, array)?,
        DataType::Boolean => DynamicSource::new(BooleanEventSource::new(
            array
                .as_any()
                .downcast_ref()
                .ok_or_else(|| error!("mismatched types"))?,
        )),
        DataType::Utf8 => DynamicSource::new(Utf8EventSource::new(
            array
                .as_any()
                .downcast_ref::<Utf8Array<i32>>()
                .ok_or_else(|| error!("mismatched types"))?,
        )),
        DataType::LargeUtf8 => DynamicSource::new(Utf8EventSource::new(
            array
                .as_any()
                .downcast_ref::<Utf8Array<i64>>()
                .ok_or_else(|| error!("mismatched types"))?,
        )),
        DataType::Date64 => {
            if let Some(strategy) = field.metadata.get(STRATEGY_KEY) {
                let strategy: Strategy = strategy.parse()?;
                match strategy {
                    Strategy::NaiveDateTimeStr => DynamicSource::new(NaiveDateTimeStrSource(
                        PrimitiveEventSource::<i64>::from_array(array)?,
                    )),
                    Strategy::UtcDateTimeStr => DynamicSource::new(UtcDateTimeStrSource(
                        PrimitiveEventSource::<i64>::from_array(array)?,
                    )),
                    s => fail!("Invalid strategy {s} for Date64 column"),
                }
            } else {
                build_dynamic_primitive_source::<i64>(field, array)?
            }
        }
        DataType::Struct(fields) => {
            if let Some(strategy) = field.metadata.get(STRATEGY_KEY) {
                let strategy: Strategy = strategy.parse()?;
                if !matches!(strategy, Strategy::Tuple) {
                    fail!("Invalid strategy {strategy} for Struct column");
                }
                build_dynamic_struct_source(fields, array, true)?
            } else {
                build_dynamic_struct_source(fields, array, false)?
            }
        }
        DataType::List(field) => build_dynamic_list_source::<i32>(field.as_ref(), array)?,
        DataType::LargeList(field) => build_dynamic_list_source::<i64>(field.as_ref(), array)?,
        DataType::Union(fields, field_indices, mode) => {
            if !mode.is_dense() {
                fail!("Only dense unions are supported at the moment");
            }
            if field_indices.is_some() {
                fail!("Explicit field indices are currently not supported for unions");
            }
            build_dynamic_union_source(fields, array)?
        }
        dt => fail!("Sources of type {dt:?} not yet supported"),
    };
    Ok(source)
}

pub fn build_dynamic_primitive_source<'a, T: Into<Event<'static>> + NativeType>(
    field: &'a Field,
    array: &'a dyn Array,
) -> Result<DynamicSource<'a>> {
    let source =
        PrimitiveEventSource::<'a, T>::new(array.as_any().downcast_ref().ok_or_else(|| {
            error!(
                "Mismatched type. Expected {:?}, found: {:?}",
                field.data_type,
                array.data_type()
            )
        })?);
    Ok(DynamicSource::new(source))
}

pub fn build_dynamic_struct_source<'a>(
    fields: &'a [Field],
    array: &'a dyn Array,
    as_tuple: bool,
) -> Result<DynamicSource<'a>> {
    let array = array
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| error!("mismatched type"))?;
    let children = array.values();

    let mut names: Vec<&'a str> = Vec::new();
    let mut values: Vec<DynamicSource<'a>> = Vec::new();

    for i in 0..fields.len() {
        names.push(fields[i].name.as_str());
        values.push(build_dynamic_source(&fields[i], children[i].as_ref())?);
    }

    let validity = if let Some(validity) = array.validity() {
        validity.iter().collect()
    } else {
        vec![true; array.len()]
    };

    if !as_tuple {
        let source = StructSource::new(names, validity, values);
        Ok(DynamicSource::new(source))
    } else {
        let source = TupleSource::new(validity, values);
        Ok(DynamicSource::new(source))
    }
}

pub fn build_dynamic_list_source<'a, T: Offset>(
    field: &'a Field,
    array: &'a dyn Array,
) -> Result<DynamicSource<'a>> {
    let array = array
        .as_any()
        .downcast_ref::<ListArray<T>>()
        .ok_or_else(|| error!("invalid array type {:?} for LargeList", array.data_type()))?;

    let values = build_dynamic_source(field, array.values().as_ref())?;
    let offsets: Vec<usize> = array.offsets().iter().map(|o| o.to_usize()).collect();
    let validity: Vec<bool> = if let Some(validity) = array.validity() {
        validity.iter().collect()
    } else {
        vec![true; array.len()]
    };

    let source = ListSource::new(values, offsets, validity);
    Ok(DynamicSource::new(source))
}

pub fn build_dynamic_union_source<'a>(
    fields: &'a [Field],
    array: &'a dyn Array,
) -> Result<DynamicSource<'a>> {
    let array = array
        .as_any()
        .downcast_ref::<UnionArray>()
        .ok_or_else(|| error!("invalid array type {:?} for UnionArray", array.data_type()))?;

    let mut names = Vec::new();
    let mut sources = Vec::new();

    for (field, array) in std::iter::zip(fields, array.fields()) {
        names.push(field.name.as_str());
        sources.push(build_dynamic_source(field, array.as_ref())?);
    }

    let mut types = Vec::new();
    for ty in array.types().iter() {
        types.push(u8::try_from(*ty)?);
    }

    // TODO: test that the offsets are dense

    let source = UnionSource::new(names, sources, types);
    Ok(DynamicSource::new(source))
}

pub struct NullArraySource<'a> {
    length: usize,
    next: usize,
    phantom: PhantomData<&'a ()>,
}

impl<'a> NullArraySource<'a> {
    fn new(length: usize) -> Self {
        Self {
            length,
            next: 0,
            phantom: Default::default(),
        }
    }
}

impl<'a> EventSource<'a> for NullArraySource<'a> {
    fn next(&mut self) -> Result<Option<Event<'a>>> {
        if self.next < self.length {
            self.next += 1;
            Ok(Some(Event::Null))
        } else {
            Ok(None)
        }
    }
}

pub struct PrimitiveEventSource<'a, T: Into<Event<'static>> + NativeType> {
    array: &'a PrimitiveArray<T>,
    next: usize,
}

impl<'a, T: Into<Event<'static>> + NativeType> PrimitiveEventSource<'a, T> {
    pub fn new(array: &'a PrimitiveArray<T>) -> Self {
        Self { array, next: 0 }
    }

    pub fn from_array(array: &'a dyn Array) -> Result<Self> {
        Ok(Self::new(
            array
                .as_any()
                .downcast_ref::<PrimitiveArray<T>>()
                .ok_or_else(|| error!("Mismatched type"))?,
        ))
    }
}

impl<'a, T: Into<Event<'static>> + NativeType> EventSource<'a> for PrimitiveEventSource<'a, T> {
    fn next(&mut self) -> Result<Option<Event<'a>>> {
        let ev = if self.next >= self.array.len() {
            return Ok(None);
        } else if !self.array.is_valid(self.next) {
            Event::Null
        } else {
            self.array.value(self.next).into()
        };
        self.next += 1;
        Ok(Some(ev))
    }
}

pub struct BooleanEventSource<'a> {
    array: &'a BooleanArray,
    next: usize,
}

impl<'a> BooleanEventSource<'a> {
    pub fn new(array: &'a BooleanArray) -> Self {
        Self { array, next: 0 }
    }
}

impl<'a> EventSource<'a> for BooleanEventSource<'a> {
    fn next(&mut self) -> Result<Option<Event<'a>>> {
        let ev = if self.next >= self.array.len() {
            return Ok(None);
        } else if !self.array.is_valid(self.next) {
            Event::Null
        } else {
            self.array.value(self.next).into()
        };
        self.next += 1;
        Ok(Some(ev))
    }
}

pub struct Utf8EventSource<'a, O: Offset> {
    array: &'a Utf8Array<O>,
    next: usize,
}

impl<'a, O: Offset> Utf8EventSource<'a, O> {
    pub fn new(array: &'a Utf8Array<O>) -> Self {
        Self { array, next: 0 }
    }
}

impl<'a, O: Offset> EventSource<'a> for Utf8EventSource<'a, O> {
    fn next(&mut self) -> Result<Option<Event<'a>>> {
        let ev = if self.next >= self.array.len() {
            return Ok(None);
        } else if !self.array.is_valid(self.next) {
            Event::Null
        } else {
            self.array.value(self.next).into()
        };
        self.next += 1;
        Ok(Some(ev))
    }
}
