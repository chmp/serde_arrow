use std::collections::HashSet;

use arrow2::{array::{Array, StructArray}, datatypes::{DataType, Field}};

use crate::{event::{EventSink, Event}, fail, Result, error};

use super::base::ArrayBuilder;


pub struct StructArrayBuilder<B: ArrayBuilder> {
    columns: Vec<String>,
    builders: Vec<B>,
    nullable: Vec<bool>,
    state: StructArrayBuilderState,
    // TODO: use bit mask here?
    seen: HashSet<usize>,
}

impl<B: ArrayBuilder> StructArrayBuilder<B> {
    pub fn new(columns: Vec<String>, builders: Vec<B>, nullable: Vec<bool>) -> Self {
        Self {
            columns,
            builders,
            nullable,
            state: StructArrayBuilderState::WaitForStart,
            seen: HashSet::new(),
        }
    }
}

impl<B: ArrayBuilder> EventSink for StructArrayBuilder<B> {
    fn accept(&mut self, event: Event<'_>) -> Result<()> {
        use StructArrayBuilderState::*;

        match self.state {
            WaitForStart => match event {
                Event::StartMap => {
                    self.state = WaitForField;
                    self.seen.clear();
                }
                _ => fail!("Expected start map"),
            }
            WaitForField => {
                let key = match event {
                    Event::Key(key) => key,
                    Event::OwnedKey(ref key) => key,
                    Event::EndMap => {
                        if self.seen.len() != self.columns.len() {
                            // TODO: improve error message
                            fail!("Missing fields");        
                        }
                        self.state = WaitForStart;
                        return Ok(())
                    }
                    event => fail!("Unexpected event while waiting for field: {event}"),
                };
                let idx = self.columns.iter().position(|col| col == key).ok_or_else(|| error!("unknown field {key}"))?;
                if self.seen.contains(&idx) {
                    fail!("Duplicate field {}", self.columns[idx]);
                }
                self.seen.insert(idx);
                self.state = WaitForValue(idx, 0);
            }
            WaitForValue(active, depth) => {
                self.state = match &event {
                    Event::StartMap | Event::StartSequence => WaitForValue(active, depth + 1),
                    Event::EndMap | Event::EndSequence => match depth {
                        // the last closing event for the current value
                        1 => WaitForField,
                        // TODO: check is this event possible?
                        0 => fail!("Unbalanced opening / close events"),
                        _ => WaitForValue(active, depth - 1),
                    }
                    _ if depth == 0 => WaitForField,
                    _ => self.state,
                };
                self.builders[active].accept(event)?;
            }
        }
        Ok(())
    }    
}

impl<B: ArrayBuilder> ArrayBuilder for StructArrayBuilder<B> {
    fn box_into_array(self: Box<Self>) -> Result<Box<dyn Array>> {
        (*self).into_array()
    }

    fn into_array(self) -> Result<Box<dyn Array>> where Self: Sized {
        if !matches!(self.state, StructArrayBuilderState::WaitForStart) {
            fail!("Invalid state at array construction");
        } 

        let values: Result<Vec<Box<dyn Array>>> = self.builders.into_iter().map(|b| b.into_array()).collect();
        let values = values?;

        let mut fields = Vec::new();
        for (i, column) in self.columns.into_iter().enumerate() {
            fields.push(Field::new(column, values[i].data_type().clone(), self.nullable[i]));
        }
        let data_type = DataType::Struct(fields);

        Ok(Box::new(StructArray::new(data_type, values, None)))
    }
}


#[derive(Debug, Clone, Copy)]
enum StructArrayBuilderState {
    WaitForStart,
    WaitForField,
    WaitForValue(usize, usize),
}
