use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub type Term = u64;
pub type Index = u64;
pub type NodeId = Uuid;

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub enum ServerState {
    Follower,
    Candidate,
    Leader,
}

#[derive(Serialize, Deserialize)]
pub enum Command {
    Append(Vec<u8>),
}

#[derive(Serialize, Deserialize)]
struct Entry {}

#[derive(Serialize, Deserialize)]
pub struct RequestVote {
    pub term: Term,
    pub candidate_id: NodeId,
    pub last_log_index: Index,
    pub last_log_term: Term,
}

impl RequestVote {
    pub fn new(
        term: Term,
        candidate_id: Uuid,
        last_log_index: Index,
        last_log_term: Term,
    ) -> RequestVote {
        RequestVote {
            term,
            candidate_id,
            last_log_index,
            last_log_term,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct AppendEntry {
    pub term: Term,
    pub leader_id: NodeId,
    pub prev_log_index: Index,
    pub prev_log_term: Term,
    pub entries: Vec<Entry>,
    pub leader_commit: Index,
}

impl AppendEntry {
    pub fn heartbeat(
        term: Term,
        leader_id: Uuid,
        prev_log_index: Index,
        prev_log_term: Term,
        leader_commit: Index,
    ) -> AppendEntry {
        AppendEntry {
            term,
            leader_id,
            prev_log_index,
            prev_log_term,
            entries: Vec::new(),
            leader_commit,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub enum MessageBody {
    RequestVote(RequestVote),
    AppendEntry(AppendEntry),
}

#[derive(Serialize, Deserialize)]
pub struct Message {
    pub term: Term,
    pub body: MessageBody,
}


impl Message {
    pub fn new(term: Term, body: MessageBody) -> Message {
        Message { term, body }
    }
}