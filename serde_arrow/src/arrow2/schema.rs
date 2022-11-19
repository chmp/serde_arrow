use std::slice;

use arrow2::datatypes::{DataType, Field};

use crate::{
    base::Event,
    fail,
    generic::schema::{GenericField, Strategy, STRATEGY_KEY},
    Result,
};

impl GenericField for Field {
    fn new_null(name: String) -> Self {
        Field::new(name, DataType::Null, false)
    }

    fn new_struct(name: String) -> Self {
        Field::new(name, DataType::Struct(Vec::new()), false)
    }

    fn new_list(name: String) -> Self {
        let inner = Field::new(&name, DataType::Null, false);
        Field::new(name, DataType::LargeList(Box::new(inner)), false)
    }

    fn new_primitive(name: String, ev: &Event<'_>) -> Result<Self> {
        Ok(Field::new(name, get_event_data_type(ev)?, false))
    }

    fn get_children_mut(&mut self) -> Result<&mut [Self]> {
        match &mut self.data_type {
            DataType::Struct(fields) => Ok(fields),
            DataType::List(field) => Ok(slice::from_mut(field.as_mut())),
            DataType::LargeList(field) => Ok(slice::from_mut(field.as_mut())),
            dt => fail!("Unnested data type {dt:?}"),
        }
    }

    fn describe(&self) -> String {
        format!("{self:?}")
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn is_null(&self) -> bool {
        matches!(self.data_type, DataType::Null)
    }

    fn is_struct(&self) -> bool {
        matches!(self.data_type, DataType::Struct(_))
    }

    fn is_list(&self) -> bool {
        matches!(self.data_type, DataType::LargeList(_) | DataType::List(_))
    }

    fn is_primitive(&self, ev: &Event<'_>) -> bool {
        if let Ok(dt) = get_event_data_type(ev) {
            self.data_type == dt
        } else {
            false
        }
    }

    fn set_nullable(&mut self, nullable: bool) {
        self.is_nullable = nullable;
    }

    fn append_child(&mut self, child: Self) -> Result<()> {
        if let DataType::Struct(fields) = &mut self.data_type {
            fields.push(child);
            Ok(())
        } else {
            fail!("Cannot append a child to a non-struct");
        }
    }

    fn configure_serde_arrow_strategy(&mut self, strategy: Strategy) -> Result<()> {
        match strategy {
            Strategy::DateTimeStr | Strategy::NaiveDateTimeStr => {
                if !matches!(
                    self.data_type,
                    DataType::Null | DataType::Utf8 | DataType::LargeUtf8
                ) {
                    fail!(
                        "Cannot configure DateTimeStr for field of type {:?}",
                        self.data_type
                    );
                }
                self.data_type = DataType::Date64;
                self.metadata
                    .insert(String::from(STRATEGY_KEY), strategy.to_string());
            }
        }

        Ok(())
    }
}

fn get_event_data_type(event: &Event<'_>) -> Result<DataType> {
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
