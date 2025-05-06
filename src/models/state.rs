use std::fmt::format;
use std::{collections::HashMap, time::SystemTime};

use crate::models::base::*;
use crate::models::raft::*;

use crate::consts::LOCK_TIMEOUT;

pub struct InnerStateLock {
    timestep: SystemTime,
    owner: NodeId,
}

impl InnerStateLock {
    pub fn new(owner: NodeId) -> InnerStateLock {
        InnerStateLock {
            timestep: SystemTime::now(),
            owner,
        }
    }

    pub fn has_access(&self, id: NodeId, access_time: SystemTime) -> bool {
        return self.owner == id
            || access_time.duration_since(self.timestep).unwrap() > LOCK_TIMEOUT;
    }
}

pub struct InnerState {
    hmap: HashMap<String, String>,
    lock: Option<InnerStateLock>,
}

impl InnerState {
    pub fn new() -> InnerState {
        InnerState {
            hmap: HashMap::new(),
            lock: None,
        }
    }

    pub fn apply_entry(&mut self, entry: &Entry) {
        match &entry.command {
            Command::SetValue {
                key,
                value,
                id,
                timestamp,
            } => {
                if self.is_unlocked(id, timestamp) {
                    self.hmap.insert(key.clone(), value.clone());
                }
            }
            Command::DelValue { key, id, timestamp } => {
                if self.is_unlocked(id, timestamp) {
                    self.hmap.remove(key);
                }
            }
            Command::Lock { id, timestamp } => {
                if self.is_unlocked(id, timestamp) {
                    self.lock = Some(InnerStateLock::new(*id));
                } else if let Some(lock) = &self.lock {
                    if lock.owner == *id {
                        self.lock = Some(InnerStateLock::new(*id));
                    }
                }
            }
            Command::Unlock { id } => {
                if let Some(lock) = &self.lock {
                    if lock.owner == *id {
                        self.lock = None;
                    }
                }
            }
        }
    }

    pub fn get_value(&self, key: &String) -> Option<String> {
        self.hmap.get(key).cloned()
    }

    pub fn is_unlocked(&self, id: &NodeId, timestamp: &SystemTime) -> bool {
        return self.lock.is_none()
            || self
                .lock
                .as_ref()
                .is_some_and(|lock| lock.has_access(*id, *timestamp));
    }

    pub fn get_store(&self) -> String {
        format!("{:#?}", self.hmap)
    }
}
