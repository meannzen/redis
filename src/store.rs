use std::{
    collections::{BTreeSet, HashMap},
    sync::{Arc, Mutex},
};

use bytes::Bytes;
use tokio::{
    sync::Notify,
    time::{self, Duration, Instant},
};

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
    background_task: Notify,
}

#[derive(Debug)]
struct State {
    entries: HashMap<String, Entry>,
    expirations: BTreeSet<(Instant, String)>,
    shutdown: bool,
}

#[derive(Debug)]
struct Entry {
    data: Bytes,
    expires_at: Option<Instant>,
}
impl Shared {
    fn pure_expire_keys(&self) -> Option<Instant> {
        let mut state = self.state.lock().unwrap();
        if state.shutdown {
            return None;
        }
        let state = &mut *state;
        let now = Instant::now();
        while let Some(&(when, ref key)) = state.expirations.iter().next() {
            if when > now {
                return Some(when);
            }

            state.entries.remove(key);
            state.expirations.remove(&(when, key.clone()));
        }

        None
    }
    pub fn is_shutdown(&self) -> bool {
        self.state.lock().unwrap().shutdown
    }
}
impl Db {
    pub fn new() -> Self {
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                entries: HashMap::new(),
                expirations: BTreeSet::new(),
                shutdown: false,
            }),

            background_task: Notify::new(),
        });
        tokio::spawn(pure_expire_task(shared.clone()));

        Db { shared }
    }
    pub fn get(&self, key: &str) -> Option<Bytes> {
        let state = self.shared.state.lock().unwrap();
        state.entries.get(key).map(|entry| entry.data.clone())
    }

    pub fn get_keys(&self, key: &str) -> Vec<Bytes> {
        let state = self.shared.state.lock().unwrap();
        let mut result_keys: Vec<Bytes> = Vec::new();

        if key == "*" {
            result_keys = state.entries.keys().cloned().map(|x| x.into()).collect();
        } else if key.ends_with('*') {
            let prefix = &key[0..key.len() - 1];
            for (k, _) in state.entries.iter() {
                if k.starts_with(prefix) {
                    result_keys.push(k.clone().into());
                }
            }
        } else if let Some(suffix) = key.strip_suffix('*') {
            for (k, _) in state.entries.iter() {
                if k.ends_with(suffix) {
                    result_keys.push(k.clone().into());
                }
            }
        }

        result_keys
    }

    pub fn set(&self, key: String, value: Bytes, expire: Option<Duration>) {
        let mut state = self.shared.state.lock().unwrap();
        let mut notify = false;
        let expires_at = expire.map(|duration| {
            let when = Instant::now() + duration;
            notify = state
                .next_expiration()
                .map(|expiration| expiration > when)
                .unwrap_or(true);
            when
        });

        let prev = state.entries.insert(
            key.clone(),
            Entry {
                data: value,
                expires_at,
            },
        );
        if let Some(prev) = prev {
            if let Some(when) = prev.expires_at {
                state.expirations.remove(&(when, key.clone()));
            }
        }

        if let Some(when) = expires_at {
            state.expirations.insert((when, key.clone()));
        }
        drop(state);
        if notify {
            self.shared.background_task.notify_one();
        }
    }

    fn shutdown_purge_task(&self) {
        let mut state = self.shared.state.lock().unwrap();
        state.shutdown = true;

        drop(state);
        self.shared.background_task.notify_one();
    }
}

impl Drop for Store {
    fn drop(&mut self) {
        self.db.shutdown_purge_task();
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

impl State {
    pub fn next_expiration(&self) -> Option<Instant> {
        self.expirations
            .iter()
            .next()
            .map(|expiration| expiration.0)
    }
}

async fn pure_expire_task(shared: Arc<Shared>) {
    while !shared.is_shutdown() {
        if let Some(when) = shared.pure_expire_keys() {
            tokio::select! {
                _= time::sleep_until(when)=> {},
                _= shared.background_task.notified() => {}
            }
        } else {
            shared.background_task.notified().await;
        }
    }
}
