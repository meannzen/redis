use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use bytes::Bytes;

#[derive(Debug)]
pub struct Store {
    pub db: Db,
}

#[derive(Debug, Clone)]
pub struct Db {
    shared: Arc<Shared>,
}
#[derive(Debug)]
struct Shared {
    state: Mutex<State>,
}

#[derive(Debug)]
struct State {
    entries: HashMap<String, Entry>,
}

#[derive(Debug)]
struct Entry {
    data: Bytes,
}

impl Db {
    pub fn new() -> Self {
        Db {
            shared: Arc::new(Shared {
                state: Mutex::new(State {
                    entries: HashMap::new(),
                }),
            }),
        }
    }
    pub fn get(&self, key: &str) -> Option<Bytes> {
        let state = self.shared.state.lock().unwrap();
        state.entries.get(key).map(|entry| entry.data.clone())
    }

    pub fn set(&self, key: String, value: Bytes) {
        let mut state = self.shared.state.lock().unwrap();
        state.entries.insert(key.clone(), Entry { data: value });
        drop(state);
    }
}
impl Default for Db {
    fn default() -> Self {
        Self::new()
    }
}
impl Store {
    pub fn new() -> Self {
        Store { db: Db::new() }
    }
}

impl Default for Store {
    fn default() -> Self {
        Store::new()
    }
}
