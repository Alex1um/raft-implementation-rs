use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub type Term = u64;
pub type Index = i64;
pub type NodeId = Uuid;

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub enum ServerState {
    Follower,
    Candidate,
    Leader,
}

pub struct InnerState {
    hmap: HashMap<String, String>,
}

impl InnerState {
    pub fn new() -> InnerState {
        InnerState {
            hmap: HashMap::new(),
        }
    }

    pub fn apply_entry(&mut self, entry: &Entry) {
        match &entry.command {
            Command::SetValue { key, value } => {
                self.hmap.insert(key.clone(), value.clone());
            }
        }
    }

    pub fn get_value(&self, key: &String) -> Option<String> {
        self.hmap.get(key).cloned()
    }
}

pub enum FrontEndCommand {
    SetValue { key: String, value: String },
    GetValue { key: String },
}

#[derive(Debug)]
pub enum FrontEndCommandResponse {
    Ok,
    Value(String),
    NotFound,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum Command {
    SetValue { key: String, value: String },
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Entry {
    pub term: Term,
    command: Command,
    pub index: Index,
}

impl Entry {
    pub fn new_command(term: Term, command: Command, index: Index) -> Entry {
        Entry {
            term,
            command,
            index,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub enum MessageBody {
    RequestVote {
        term: Term,
        candidate_id: NodeId,
        last_log_index: Index,
        last_log_term: Term,
    },
    AppendEntry {
        term: Term,
        leader_id: NodeId,
        prev_log_index: Option<Index>,
        prev_log_term: Option<Term>,
        entries: Vec<Entry>,
        leader_commit: Index,
    },
    Command {
        command: Command,
    },
    RequestVoteResponse {
        success: bool,
    },
    AppendEntryResponse {
        success: bool,
        last_log_index: Option<Index>,
    },
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Message {
    pub term: Term,
    pub body: MessageBody,
}

impl Message {
    pub fn new(term: Term, body: MessageBody) -> Message {
        Message { term, body }
    }

    pub fn new_heartbeat(
        term: Term,
        leader_id: Uuid,
        leader_commit: Index,
    ) -> Message {
        Message {
            term,
            body: MessageBody::AppendEntry {
                term,
                leader_id,
                prev_log_index: None,
                prev_log_term: None,
                entries: Vec::new(),
                leader_commit,
            },
        }
    }

    pub fn new_append_entry(
        term: Term,
        leader_id: Uuid,
        prev_log_index: Option<Index>,
        prev_log_term: Option<Term>,
        entries: Vec<Entry>,
        leader_commit: Index
    ) -> Message {
        Message {
            term,
            body: MessageBody::AppendEntry { term, leader_id, prev_log_index, prev_log_term, entries, leader_commit }
        }
    }

    pub fn new_request_vote(
        term: Term,
        candidate_id: Uuid,
        last_log_index: Index,
        last_log_term: Term,
    ) -> Message {
        Message {
            term,
            body: MessageBody::RequestVote {
                term,
                candidate_id,
                last_log_index,
                last_log_term,
            },
        }
    }

    pub fn new_request_vote_response(term: Term, success: bool) -> Message {
        Message {
            term,
            body: MessageBody::RequestVoteResponse { success },
        }
    }

    pub fn new_append_entry_response(term: Term, success: bool, last_log_index: Option<Index>) -> Message {
        Message {
            term,
            body: MessageBody::AppendEntryResponse { success, last_log_index },
        }
    }

    pub fn new_command(term: Term, command: Command) -> Message {
        Message {
            term,
            body: MessageBody::Command { command },
        }
    }
}
