use std::net::IpAddr;
use std::time::Duration;
use std::{net::SocketAddr, sync::Arc};

use crate::consts::{HEARTBEAT_INTERVAL, MAX_ELECTION_TIMEOUT, MIN_ELECTION_TIMEOUT, MIN_NODES};
use crate::models::*;
use rand::seq::index;
use rand::{TryCryptoRng, prelude::*};
use rocket::http::Iter;
use std::collections::{HashMap, HashSet};
use std::time::Instant;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::sync::mpsc::{Receiver, Sender, channel};
use uuid::Uuid;

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
    messages_buffer: &mut Vec<(IpAddr, Message)>,
) {
    let mut buf = [0u8; 1024];
    for (ip, connection) in connections.iter_mut() {
        let received = connection.try_read(&mut buf);
        match received {
            Ok(0) => {
                println!("Closing connection to {}", ip);
                connections_to_remove.insert(*ip);
            }
            Ok(readed) => match (serde_json::from_slice::<Message>(&buf[0..readed])) {
                Ok(message) => {
                    messages_buffer.push((*ip, message));
                }
                Err(e) => {
                    println!("Error Deserializing message: {}", e);
                    // connections_to_remove.insert(*ip);
                }
            },
            Err(e) => match e.kind() {
                std::io::ErrorKind::WouldBlock => continue,
                _ => {
                    println!("Error receiving message: {}", e);
                    connections_to_remove.insert(*ip);
                }
            },
        }
    }
}

async fn send_all(
    connections: &mut HashMap<IpAddr, TcpStream>,
    message: &Message,
    connections_to_delete: &mut HashSet<IpAddr>,
) {
    let data = serde_json::to_vec(message).unwrap();
    for (ip, connection) in connections.iter_mut() {
        match connection.write_all(&data).await {
            Ok(_) => {}
            Err(e) => {
                println!("Error sending message: {}", e);
                connections_to_delete.insert(*ip);
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
    connections_to_delete: &mut HashSet<IpAddr>,
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
                    connections_to_delete.insert(*ip);
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
                connections_to_delete.insert(*ip);
            } else {
                last_heartbeats.insert(*ip, Instant::now());
            }
        }
    }
}

async fn reply(
    connection: &mut TcpStream,
    connection_ip: &IpAddr,
    term: Term,
    success: bool,
    connections_to_delete: &mut HashSet<IpAddr>,
) {
    let data = serde_json::to_vec(&Message::new_response(term, success)).unwrap();
    if let Err(e) = connection.write_all(&data).await {
        connections_to_delete.insert(*connection_ip);
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


pub fn apply_to_state_machine(log: &Vec<Entry>, commit_index: &Index, last_applied: &mut Index, inner_state: &mut InnerState) {
    if commit_index > last_applied {
        *last_applied += 1;
        println!("Applying commit: {}", commit_index);
        let entry = log.get(*last_applied as usize).unwrap();
        inner_state.apply_entry(entry);
    }
}


pub async fn run_raft(receiver: Receiver<SocketAddr>, mut signal_receiver: Receiver<()>) {
    let peers = Arc::new(Mutex::new(HashSet::new()));
    let (stream_sender, mut new_connections) = channel::<(TcpStream, IpAddr)>(3);
    tokio::spawn(listen_for_peers(peers.clone(), stream_sender.clone()));
    tokio::spawn(create_peers(receiver, peers.clone(), stream_sender));
    let mut messages_buffer = Vec::<(IpAddr, Message)>::new();

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
                for (ip, message) in messages_buffer.drain(..) {
                    match message.body {
                        MessageBody::AppendEntry { term, leader_id, prev_log_index, prev_log_term, mut entries, leader_commit } => {
                            election_instant = Instant::now();
                            if message.term < current_term || check_log_consistency(&log, prev_log_index, prev_log_term) {
                                reply(&mut connections[&ip], &ip, term, false, &mut connections_to_remove).await;
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
                            reply(&mut connections[&ip], &ip, term, true, &mut connections_to_remove).await;
                        }
                        MessageBody::RequestVote { term , candidate_id, last_log_index, last_log_term } => {
                            if message.term < current_term {
                                reply(&mut connections[&ip], &ip, message.term, false, &mut connections_to_remove).await;
                                continue;
                            }
                            if (voted_for.is_none() || voted_for.unwrap() == candidate_id) && check_log_consistency(&log, last_log_index, last_log_term) {
                                reply(&mut connections[&ip], &ip, message.term, true, &mut connections_to_remove).await;
                                voted_for = Some(candidate_id);
                                election_instant = Instant::now();
                            }
                            reply(&mut connections[&ip], &ip, message.term, false, &mut connections_to_remove).await;
                        }
                        _ => (),
                    }
                    if message.term > current_term {
                        current_term = message.term;
                        election_instant = Instant::now();
                    }
                }
                if Instant::now().duration_since(election_instant) > election_timeout {
                    println!("I am the candidate!");
                    state = ServerState::Candidate;
                }
            }
            ServerState::Candidate => {
                for message in messages_buffer.drain(..) {
                    if message.term > current_term {
                        current_term = message.term;
                        election_instant = Instant::now();
                        state = ServerState::Follower;
                        continue;
                    }
                }
                if self_votes > total_votes / 2 {
                    println!("I am the leader!");
                    state = ServerState::Leader;
                } else if Instant::now().duration_since(election_instant) > election_timeout {
                    current_term += 1;
                    election_instant = Instant::now();
                    let msg = Message::new_request_vote(current_term, my_id, 0, 0);
                    total_votes += 1;
                    self_votes += 1;
                    send_all(&mut connections, &msg, &mut connections_to_remove).await;
                }
            }
            ServerState::Leader => {
                for message in messages_buffer.drain(..) {
                    if message.term > current_term {
                        current_term = message.term;
                        election_instant = Instant::now();
                        state = ServerState::Follower;
                        continue;
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
