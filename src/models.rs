use serde::{Deserialize, Serialize};


type Term = u64;
type Index = u64;


pub enum ServerState {
    Follower,
    Candidate,
    Leader,
}


pub enum Command {
    Append(Vec<u8>),
}


struct LogEntry {
    term: Term,
    command: Command,
}


struct NodeState {
    current_term: Term,
    voted_for: Option<u64>,
    log: Vec<LogEntry>,
    commit_index: Index,
    last_applied: Index,
    next_index: Vec<Index>,
    match_index: Vec<Index>
}

impl NodeState {
    fn new() -> NodeState {
        NodeState {
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
            commit_index: 0,
            last_applied: 0,
            next_index: Vec::new(),
            match_index: Vec::new(),
        }
    }
}
