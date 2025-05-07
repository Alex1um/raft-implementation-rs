use std::time::SystemTime;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub type Term = u64;
pub type Index = i64;
pub type NodeId = Uuid;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum Command {
    SetValue {
        key: String,
        value: String,
        id: NodeId,
        timestamp: SystemTime,
    },
    DelValue {
        key: String,
        id: NodeId,
        timestamp: SystemTime,
    },
    Lock {
        id: NodeId,
        timestamp: SystemTime,
    },
    Unlock {
        id: NodeId,
    },
}

impl Command {
    pub fn new_set_value(key: String, value: String, id: NodeId) -> Command {
        Command::SetValue {
            key,
            value,
            id,
            timestamp: SystemTime::now(),
        }
    }
    
    pub fn new_del_value(key: String, id: NodeId) -> Command {
        Command::DelValue {
            key,
            id,
            timestamp: SystemTime::now(),
        }
    }

    pub fn new_lock(id: NodeId) -> Command {
        Command::Lock {
            id,
            timestamp: SystemTime::now(),
        }
    }

    pub fn new_unlock(id: NodeId) -> Command {
        Command::Unlock { id }
    }

}
