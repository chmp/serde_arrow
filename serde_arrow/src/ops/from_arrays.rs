use crate::event::Source;
use crate::{
    error,
    event::{self, Event},
    Result,
};

use std::cell::Cell;

use serde::Deserialize;

pub fn from_arrays<'de, T: Deserialize<'de>, A: ArraySource>(
    arrays: Vec<A>,
    num_rows: usize,
) -> Result<T> {
    let state = Cell::new(State::StartSequence);
    let source = ArraysSource {
        arrays,
        num_rows,
        state,
    };
    event::from_source(source)
}

pub trait ArraySource {
    fn name(&self) -> &str;
    fn emit<'this, 'event>(&'this self, idx: usize) -> Event<'event>;
}

pub struct ArraysSource<A> {
    arrays: Vec<A>,
    num_rows: usize,
    state: Cell<State>,
}

#[derive(Clone, Copy)]
enum State {
    StartSequence,
    StartMap(usize),
    Key(usize, usize),
    Value(usize, usize),
    Done,
}

impl<A> ArraysSource<A> {
    fn next_state(&self) -> Option<State> {
        match self.state.get() {
            State::StartSequence => Some(State::StartMap(0)),
            State::StartMap(row) if row >= self.num_rows => Some(State::Done),
            State::StartMap(row) => Some(State::Key(row, 0)),
            State::Key(row, col) if col >= self.arrays.len() => Some(State::StartMap(row + 1)),
            State::Key(row, col) => Some(State::Value(row, col)),
            State::Value(row, col) => Some(State::Key(row, col + 1)),
            State::Done => None,
        }
    }
}

impl<A: ArraySource> Source for ArraysSource<A> {
    fn is_done(&self) -> bool {
        matches!(self.state.get(), State::Done)
    }

    fn peek(&self) -> Option<Event<'_>> {
        match self.state.get() {
            State::StartSequence => Some(Event::StartSequence),
            State::StartMap(row) if row >= self.num_rows => Some(Event::EndSequence),
            State::StartMap(_) => Some(Event::StartMap),
            State::Key(_, col) if col >= self.arrays.len() => Some(Event::EndMap),
            State::Key(_, col) => Some(Event::Key(self.arrays[col].name())),
            State::Value(row, col) => Some(self.arrays[col].emit(row)),
            State::Done => None,
        }
    }

    fn next_event(&mut self) -> Result<Event<'_>> {
        let next_event = self
            .peek()
            .ok_or_else(|| error!("Invalid call to next on exhausted EventSource"))?;
        let next_state = self.next_state().expect("next_event: Inconsistent state");
        self.state.set(next_state);
        Ok(next_event)
    }
}
