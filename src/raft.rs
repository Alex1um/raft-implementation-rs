use std::net::IpAddr;
use std::time::Duration;
use std::{net::SocketAddr, sync::Arc};

use crate::consts::{HEARTBEAT_INTERVAL, MAX_ELECTION_TIMEOUT, MIN_ELECTION_TIMEOUT, MIN_NODES};
use crate::models::*;
use rand::seq::index;
use rand::{TryCryptoRng, prelude::*};
use rocket::http::Iter;
use serde::de::Visitor;
use std::collections::{HashMap, HashSet};
use std::time::Instant;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::sync::mpsc::{Receiver, Sender, channel};
use uuid::Uuid;
use std::collections::VecDeque;
use serde::Deserialize;

async fn create_peers(
    mut receiver: Receiver<SocketAddr>,
    peers: Arc<Mutex<HashSet<IpAddr>>>,
    sender: Sender<(TcpStream, IpAddr)>,
) {
    while let Some(addr) = receiver.recv().await {
        let ip = addr.ip();
        let addr = SocketAddr::from((ip, 48667));
        let mut peers = peers.lock().await;
        if peers.contains(&ip) {
            continue;
        }
        if let Ok(stream) = TcpStream::connect(addr).await {
            sender
                .send((stream, ip))
                .await
                .unwrap_or_else(|e| print!("error: {}", e));
            peers.insert(ip);
            println!("Connected to {}", addr);
        }
    }
}

async fn listen_for_peers(peers: Arc<Mutex<HashSet<IpAddr>>>, sender: Sender<(TcpStream, IpAddr)>) {
    let listener = TcpListener::bind("0.0.0.0:48667").await.unwrap();
    loop {
        let (socket, _) = listener.accept().await.unwrap();
        let addr = socket.peer_addr().unwrap();
        let ip = addr.ip();
        let mut peers = peers.lock().await;
        if peers.contains(&ip) {
            continue;
        }
        peers.insert(ip);
        sender
            .send((socket, ip))
            .await
            .unwrap_or_else(|e| print!("error: {}", e));
        println!("Connected to {}", addr);
    }
}

fn receive_messages(
    connections: &mut HashMap<IpAddr, TcpStream>,
    connections_to_remove: &mut HashSet<IpAddr>,
    messages_buffer: &mut VecDeque<(IpAddr, Message)>,
) {
    let mut buf = [0u8; 1024];
    for (ip, connection) in connections.iter_mut() {
        let received = connection.try_read(&mut buf);
        match received {
            Ok(0) => {
                println!("Closing connection to {}", ip);
                connections_to_remove.insert(*ip);
            }
            Ok(readed) => {
                let mut deserializer = serde_json::Deserializer::from_slice(&buf[0..readed]);
                loop {
                    match Message::deserialize(&mut deserializer) {
                        Ok(message) => {
                            messages_buffer.push_back((*ip, message));
                        }
                        Err(e) => {
                            if !e.is_eof() {
                                println!("Error Deserializing message: {}", e);
                                // connections_to_remove.insert(*ip);
                            }
                            break;
                        }
                    }
                }
                while let Ok(message) = Message::deserialize(&mut deserializer) {
                    messages_buffer.push_back((*ip, message));
                    // match message {
                    //     Ok(message) => {
                    //         messages_buffer.push_back((*ip, message));
                    //     }
                    //     Err(e) => {
                    //         println!("Error Deserializing message: {}", e);
                    //         // connections_to_remove.insert(*ip);
                    //     }
                    // }
                }
            }
            Err(e) => match e.kind() {
                std::io::ErrorKind::WouldBlock => continue,
                _ => {
                    println!("Error receiving message: {}", e);
                    connections_to_remove.insert(*ip);
                }
            }
        }
    }
}

async fn send_all(
    connections: &mut HashMap<IpAddr, TcpStream>,
    message: &Message,
    connections_to_remove: &mut HashSet<IpAddr>,
) {
    let data = serde_json::to_vec(message).unwrap();
    for (ip, connection) in connections.iter_mut() {
        match connection.write_all(&data).await {
            Ok(_) => {}
            Err(e) => {
                println!("Error sending message: {}", e);
                connections_to_remove.insert(*ip);
            }
        }
    }
}

async fn remove_peers(
    ips: &mut HashSet<IpAddr>,
    connections: &mut HashMap<IpAddr, TcpStream>,
    peers: &Arc<Mutex<HashSet<IpAddr>>>,
) {
    for ip in ips.iter() {
        let mut peers_locked = peers.lock().await;
        peers_locked.remove(ip);
        connections.remove(ip);
    }
    ips.clear();
}

async fn process_replication<'a>(
    log: &Vec<Entry>,
    connections: &mut HashMap<IpAddr, TcpStream>,
    connections_to_remove: &mut HashSet<IpAddr>,
    last_heartbeats: &mut HashMap<IpAddr, Instant>,
    next_index: &mut HashMap<IpAddr, Index>,
    leader_id: Uuid,
    current_term: Term,
    prev_log_term: Term,
    prev_log_index: Index,
    commit_index: Index,
) {
    let last_log_index = log.len() as Index - 1;
    let mut heartbeat: Option<Vec<u8>> = None;
    for (ip, connection) in connections.iter_mut() {
        let index = next_index.entry(*ip).or_insert(0);
        if last_log_index > *index {
            let entries = log[*index as usize..].to_vec();
            let message = Message::new(
                current_term,
                MessageBody::AppendEntry {
                    term: current_term,
                    leader_id,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit: commit_index,
                },
            );
            let data = serde_json::to_vec(&message).unwrap();
            match connection.write_all(&data).await {
                Ok(_) => {}
                Err(e) => {
                    println!("Error sending message: {}", e);
                    connections_to_remove.insert(*ip);
                }
            }
        } else if last_heartbeats
            .entry(*ip)
            .or_insert(Instant::now())
            .elapsed()
            > HEARTBEAT_INTERVAL
        {
            let data = heartbeat.get_or_insert(
                serde_json::to_vec(&Message::new_heartbeat(
                    current_term,
                    leader_id,
                    prev_log_index,
                    prev_log_term,
                    commit_index,
                ))
                .unwrap(),
            );
            if let Err(e) = connection.write_all(data).await {
                println!("Error sending message: {}", e);
                connections_to_remove.insert(*ip);
            } else {
                last_heartbeats.insert(*ip, Instant::now());
            }
        }
    }
}

async fn reply_request_vote(
    connection: &mut TcpStream,
    connection_ip: &IpAddr,
    term: Term,
    success: bool,
    connections_to_remove: &mut HashSet<IpAddr>,
) {
    let data = serde_json::to_vec(&Message::new_request_vote_response(term, success)).unwrap();
    if let Err(_) = connection.write_all(&data).await {
        connections_to_remove.insert(*connection_ip);
    }
}

async fn reply_append_entry(
    connection: &mut TcpStream,
    connection_ip: &IpAddr,
    term: Term,
    success: bool,
    connections_to_remove: &mut HashSet<IpAddr>,
) {
    let data = serde_json::to_vec(&Message::new_append_entry_response(term, success)).unwrap();
    if let Err(_) = connection.write_all(&data).await {
        connections_to_remove.insert(*connection_ip);
    }
}

pub fn check_log_consistency(log: &Vec<Entry>, prev_log_index: Index, prev_log_term: Term) -> bool {
    if prev_log_index >= log.len() as Index {
        return false;
    }
    if prev_log_index > 0 && log[prev_log_index as usize].term != prev_log_term {
        return false;
    }
    return true;
}


pub fn is_log_up_to_date(log: &Vec<Entry>, prev_log_index: Index, prev_log_term: Term) -> bool {
    if let Some(last_entry) = log.last() {
        if prev_log_term > last_entry.term {
            return true;
        }
        if prev_log_term == last_entry.term && prev_log_index >= log.len() as Index {
            return true;
        }
        return false;
    } else {
        return true;
    }
}


pub fn apply_to_state_machine(log: &Vec<Entry>, commit_index: &Index, last_applied: &mut Index, inner_state: &mut InnerState) {
    if commit_index > last_applied {
        *last_applied += 1;
        println!("Applying commit: {}", commit_index);
        let entry = log.get(*last_applied as usize).unwrap();
        inner_state.apply_entry(entry);
    }
}

pub async fn grant_vote(
    new_vote: &NodeId, 
    ip: &IpAddr, 
    connection: &mut TcpStream, 
    term: Term, 
    voted_for: &mut Option<NodeId>, 
    election_instant: &mut Instant,
    connections_to_remove: &mut HashSet<IpAddr>
) {
    *voted_for = Some(*new_vote);
    *election_instant = Instant::now();
    reply_request_vote(connection, ip, term, true, connections_to_remove).await
}


pub async fn start_election(
    current_term: &mut Term,
    voted_for: &mut Option<NodeId>,
    election_instant: &mut Instant,
    vote_count: &mut usize,
    my_id: NodeId,
    log: &Vec<Entry>,
    connections: &mut HashMap<IpAddr, TcpStream>,
    connections_to_remove: &mut HashSet<IpAddr>,
) {
    *current_term += 1;
    *voted_for = Some(my_id);
    *election_instant = Instant::now();
    let msg = Message::new_request_vote(
        *current_term, 
        my_id, 
        (log.len() - 1) as Index, 
        log
        .last()
        .and_then(|l| Some(l.term as Index))
        .unwrap_or(0 as Index)
    );
    send_all(connections, &msg, connections_to_remove).await;
    *vote_count = 1;
}


pub async fn run_raft(receiver: Receiver<SocketAddr>, mut signal_receiver: Receiver<()>) {
    let peers = Arc::new(Mutex::new(HashSet::new()));
    let (stream_sender, mut new_connections) = channel::<(TcpStream, IpAddr)>(3);
    tokio::spawn(listen_for_peers(peers.clone(), stream_sender.clone()));
    tokio::spawn(create_peers(receiver, peers.clone(), stream_sender));
    let mut messages_buffer = VecDeque::<(IpAddr, Message)>::new();

    let mut connections = HashMap::<IpAddr, TcpStream>::new();
    let mut connections_to_remove = HashSet::<IpAddr>::new();
    let mut rng = rand::rng();
    let mut inner_state = InnerState::new();
    let mut last_heartbeats: HashMap<IpAddr, Instant> = HashMap::new();

    let election_timeout = rng.random_range(MIN_ELECTION_TIMEOUT..=MAX_ELECTION_TIMEOUT);
    let mut state = ServerState::Follower;
    let my_id = uuid::Uuid::new_v4();

    // raft basic state
    let mut current_term = 0;
    let mut voted_for: Option<NodeId> = None;
    let mut log = Vec::<Entry>::new();

    let mut commit_index = 0; // index of highest log entry known to be commited
    let mut last_applied = 0; // index of highest log entry applied to state machine

    let mut election_instant = Instant::now();
    println!("My election timeout: {:#?}", election_timeout);

    let mut next_index = HashMap::<IpAddr, Index>::new();
    let mut match_index = HashMap::<IpAddr, Index>::new();

    let mut vote_count = 0;

    'main: loop {
        tokio::time::sleep(Duration::from_millis(50)).await;
        if let Ok(_) = signal_receiver.try_recv() {
            break;
        }
        // setup new connections
        loop {
            match new_connections.try_recv() {
                Ok((stream, ip)) => {
                    println!("new connection {}", ip);
                    connections.insert(ip, stream);
                }
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                    break 'main;
                }
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                    break;
                }
            }
        }
        if connections.len() < MIN_NODES - 1 {
            println!("not enough connections: {}", connections.len());
            continue;
        }
        // receive messages
        receive_messages(&mut connections, &mut connections_to_remove, &mut messages_buffer);
        // Do my work
        match state {
            ServerState::Follower => {
                while let Some((ip, message)) = messages_buffer.pop_front() {
                    match message.body {
                        MessageBody::AppendEntry { term, leader_id, prev_log_index, prev_log_term, mut entries, leader_commit } => {
                            election_instant = Instant::now();
                            if message.term < current_term || check_log_consistency(&log, prev_log_index, prev_log_term) {
                                reply_append_entry(connections.get_mut(&ip).unwrap(), &ip, term, false, &mut connections_to_remove).await;
                                continue;
                            }
                            // append entries if any
                            log.append(&mut entries);
                            // update commit index
                            if leader_commit > commit_index {
                                commit_index = leader_commit;
                            }
                            // apply state
                            apply_to_state_machine(&log, &commit_index, &mut last_applied, &mut inner_state);
                            reply_append_entry(connections.get_mut(&ip).unwrap(), &ip, term, true, &mut connections_to_remove).await;
                        }
                        MessageBody::RequestVote { term , candidate_id, last_log_index, last_log_term } => {
                            if message.term >= current_term && (voted_for.is_none() || voted_for.unwrap() == candidate_id) && is_log_up_to_date(&log, last_log_index, last_log_term) {
                                grant_vote(&candidate_id, &ip, connections.get_mut(&ip).unwrap(), message.term, &mut voted_for, &mut election_instant, &mut connections_to_remove).await
                            }
                            reply_request_vote(connections.get_mut(&ip).unwrap(), &ip, message.term, false, &mut connections_to_remove).await;
                        }
                        _ => (),
                    }
                }
                if Instant::now().duration_since(election_instant) > election_timeout {
                    println!("I am the candidate!");
                    state = ServerState::Candidate;
                }
            }
            ServerState::Candidate => {
                if Instant::now().duration_since(election_instant) > election_timeout {
                    start_election(&mut current_term, &mut voted_for, &mut election_instant, &mut vote_count, my_id, &log, &mut connections, &mut connections_to_remove).await;
                }
                while let Some((ip, message)) = messages_buffer.pop_front() {
                    match message.body {
                        MessageBody::AppendEntry { .. } => {
                            if message.term > current_term {
                                state = ServerState::Follower;
                                continue 'main;
                            }
                        }
                        MessageBody::RequestVote { .. } => {
                            if message.term > current_term {
                                state = ServerState::Follower;
                                continue 'main;
                            }
                        }
                        MessageBody::RequestVoteResponse { success: true } => {
                            if message.term != current_term {
                                continue;
                            }
                            vote_count += 1;
                            if vote_count >= (MIN_NODES - 1) / 2 {
                                state = ServerState::Leader;
                                continue 'main;
                            }
                        }
                        _ => {}
                    }
                }
            }
            ServerState::Leader => {
                while let Some((ip, message)) = messages_buffer.pop_front() {
                    match message.body {
                        MessageBody::AppendEntry { term, leader_id, prev_log_index, prev_log_term, mut entries, leader_commit } => {
                            if message.term < current_term {
                                reply_append_entry(connections.get_mut(&ip).unwrap(), &ip, term, false, &mut connections_to_remove).await;
                                continue;
                            } else if message.term > current_term {
                                state = ServerState::Follower;
                                continue 'main;
                            }
                        }
                        MessageBody::RequestVote { term, candidate_id, last_log_index, last_log_term } => {
                            if message.term < current_term {
                                reply_request_vote(connections.get_mut(&ip).unwrap(), &ip, term, false, &mut connections_to_remove).await;
                                continue;
                            } else if message.term > current_term {
                                state = ServerState::Follower;
                                continue 'main;
                            }
                        }
                        MessageBody::Command { command } => {
                            let command_index = log.len() as Index;
                            log.append(&mut vec![Entry::new_command(current_term, command, command_index as Index)]);
                            apply_to_state_machine(&log, &command_index, &mut last_applied, &mut inner_state);
                        }
                        MessageBody::AppendEntryResponse { success } => {
                            if success {
                                next_index.entry(ip).and_modify(|index| *index += 1 as Index);
                                match_index.entry(ip).and_modify(|index| *index += 1 as Index);
                            } else {
                                next_index.entry(ip).and_modify(|index| *index -= 1 as Index);
                                continue;
                            }
                            let mut match_index_is_bigger_count = 0;
                            for (ip, index) in match_index.iter() {
                                if index > &commit_index {
                                    match_index_is_bigger_count += 1;
                                }
                            }
                            if match_index_is_bigger_count >= (MIN_NODES - 1) / 2 {
                                commit_index = std::cmp::min(*match_index.values().max().unwrap(), log.len() as Index);
                            }
                            apply_to_state_machine(&log, &commit_index, &mut last_applied, &mut inner_state);

                        }
                        _ => {}
                    }
                }
                process_replication(
                    &log,
                    &mut connections,
                    &mut connections_to_remove,
                    &mut last_heartbeats,
                    &mut next_index,
                    my_id,
                    current_term,
                    current_term - 1,
                    last_applied,
                    commit_index,
                )
                .await;
            }
        }
        remove_peers(&mut connections_to_remove, &mut connections, &peers).await;
    }
    println!("Done.");
}
