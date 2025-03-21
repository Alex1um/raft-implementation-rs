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
pub struct Entry {
    term: Term,
    command: Command,
}


#[derive(Serialize, Deserialize)]
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
        prev_log_index: Index,
        prev_log_term: Term,
        entries: Vec<Entry>,
        leader_commit: Index,
    },
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

    pub fn new_heartbeat(
        term: Term,
        leader_id: Uuid,
        prev_log_index: Index,
        prev_log_term: Term,
        leader_commit: Index,
    ) -> Message {
        Message {
            term,
            body: MessageBody::AppendEntry {
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries: Vec::new(),
                leader_commit,
            },
        }
    }

    pub fn new_request_vote(term: Term, candidate_id: Uuid, last_log_index: Index, last_log_term: Term) -> Message {
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
}

struct EntryToReplicate {
    
}
