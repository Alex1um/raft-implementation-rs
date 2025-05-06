use std::net::IpAddr;
use std::time::{Duration, SystemTime};
use std::{net::SocketAddr, sync::Arc};

use crate::consts::{HEARTBEAT_INTERVAL, MAX_ELECTION_TIMEOUT, MIN_ELECTION_TIMEOUT, MIN_NODES};
use crate::models::*;
use rand::prelude::*;
use serde::Deserialize;
use std::collections::VecDeque;
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
    messages_buffer: &mut VecDeque<(IpAddr, Message)>,
) {
    let mut buf = [0u8; 4096];
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
                                println!(
                                    "Error Deserializing message: {}; raw: {}",
                                    e,
                                    String::from_utf8_lossy(&buf[0..readed])
                                );
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
            },
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
    commit_index: Option<Index>,
) {
    let last_log_index = log.len() as Index - 1;
    let mut heartbeat: Option<Vec<u8>> = None;
    for (ip, connection) in connections.iter_mut() {
        let index = next_index.entry(*ip).or_insert(0);
        if !log.is_empty() && last_log_index >= *index {
            let message = {
                if *index == 0 {
                    Message::new_append_entry(
                        current_term,
                        leader_id,
                        None,
                        None,
                        log[*index as usize..].to_vec(),
                        commit_index,
                    )
                } else {
                    Message::new_append_entry(
                        current_term,
                        leader_id,
                        Some(*index - 1),
                        Some(log[(*index - 1) as usize].term),
                        log[*index as usize..].to_vec(),
                        commit_index,
                    )
                }
            };
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
    last_entry_index: Option<Index>,
    connections_to_remove: &mut HashSet<IpAddr>,
) {
    let data = serde_json::to_vec(&Message::new_append_entry_response(
        term,
        success,
        last_entry_index,
    ))
    .unwrap();
    if let Err(_) = connection.write_all(&data).await {
        connections_to_remove.insert(*connection_ip);
    }
}

pub fn check_log_consistency(
    log: &Vec<Entry>,
    prev_log_index: Option<Index>,
    prev_log_term: Option<Term>,
) -> bool {
    if let (Some(prev_log_index), Some(prev_log_term)) = (prev_log_index, prev_log_term) {
        if prev_log_index >= log.len() as Index {
            return false;
        }
        if prev_log_index > 0 && log[prev_log_index as usize].term != prev_log_term {
            return false;
        }
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

pub fn apply_to_state_machine(
    log: &Vec<Entry>,
    commit_index: &Option<Index>,
    last_applied: &mut Option<Index>,
    inner_state: &mut InnerState,
) {
    let mut applied_logs = last_applied.unwrap_or(0);
    let log_iter = match (*last_applied, *commit_index) {
        (Some(last_applied), Some(commit_index)) => log
            .iter()
            .skip(last_applied as usize + 1)
            .take(commit_index as usize - last_applied as usize + 1),
        (None, Some(commit_index)) => log.iter().skip(0).take(commit_index as usize + 1),
        (_, None) => return,
    };
    for log in log_iter {
        println!("Applying log to state: {:?}", log);
        inner_state.apply_entry(log);
        applied_logs += 1;
    }
    if last_applied.is_none() {
        applied_logs -= 1;
    }
    last_applied.replace(applied_logs);
}

pub async fn grant_vote(
    new_vote: &NodeId,
    ip: &IpAddr,
    connection: &mut TcpStream,
    term: Term,
    voted_for: &mut Option<NodeId>,
    election_instant: &mut Instant,
    connections_to_remove: &mut HashSet<IpAddr>,
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
    *voted_for = None;
    *election_instant = Instant::now();
    let msg = Message::new_request_vote(
        *current_term,
        my_id,
        (log.len() - 1) as Index,
        log.last()
            .and_then(|l| Some(l.term as Term))
            .unwrap_or(0 as Term),
    );
    send_all(connections, &msg, connections_to_remove).await;
    *vote_count = 1;
}

async fn send_command_to_leader(
    leader_ip: IpAddr,
    term: Term,
    command: Command,
    connections: &mut HashMap<IpAddr, TcpStream>,
) -> Result<(), std::io::Error> {
    if let Some(connection) = connections.get_mut(&leader_ip) {
        let data = serde_json::to_vec(&Message::new_command(term, command)).unwrap();
        return connection.write_all(&data).await;
    }
    Ok(())
}

pub async fn run_raft(
    receiver: Receiver<SocketAddr>,
    mut signal_receiver: Receiver<()>,
    mut front_end_command_receiver: Receiver<FrontEndCommand>,
    mut front_end_response_sender: Sender<FrontEndCommandResponse>,
) {
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
    let mut leader_ip: Option<IpAddr> = None;

    let election_timeout = rng.random_range(MIN_ELECTION_TIMEOUT..=MAX_ELECTION_TIMEOUT);
    let mut state = ServerState::Follower;
    let my_id = uuid::Uuid::new_v4();
    println!("My uuid: {}", my_id);

    // raft basic state
    let mut current_term = 0;
    let mut voted_for: Option<NodeId> = None;
    let mut log = Vec::<Entry>::new();

    let mut commit_index: Option<Index> = None; // index of highest log entry known to be commited on all
    let mut last_applied: Option<Index> = None; // index of highest log entry applied to state machine

    let mut election_instant = Instant::now();
    println!("My election timeout: {:#?}", election_timeout);

    let mut next_index = HashMap::<IpAddr, Index>::new(); // the index of the next log entry the leader should send to that follower.
    let mut match_index = HashMap::<IpAddr, Index>::new(); // the index of the highest log entry known to be replicated on that follower.

    let mut vote_count = 0;

    'main: loop {
        tokio::time::sleep(Duration::from_millis(20)).await;
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
        receive_messages(
            &mut connections,
            &mut connections_to_remove,
            &mut messages_buffer,
        );
        // Do my work
        match state {
            ServerState::Follower => {
                while let Some((ip, message)) = messages_buffer.pop_front() {
                    match message.body {
                        MessageBody::AppendEntry {
                            term,
                            leader_id,
                            prev_log_index,
                            prev_log_term,
                            mut entries,
                            leader_commit,
                        } => {
                            election_instant = Instant::now();
                            leader_ip = Some(ip);
                            // update commit index
                            match (leader_commit, commit_index) {
                                (Some(leader_commit_val), Some(commit_index_val))
                                    if leader_commit_val > commit_index_val =>
                                {
                                    commit_index = leader_commit
                                }
                                (Some(leader_commit), None) => commit_index = Some(leader_commit),
                                _ => (),
                            }
                            // apply state
                            apply_to_state_machine(
                                &log,
                                &commit_index,
                                &mut last_applied,
                                &mut inner_state,
                            );
                            if entries.is_empty() {
                                continue;
                            }
                            if message.term < current_term
                                || !check_log_consistency(&log, prev_log_index, prev_log_term)
                            {
                                reply_append_entry(
                                    connections.get_mut(&ip).unwrap(),
                                    &ip,
                                    term,
                                    false,
                                    None,
                                    &mut connections_to_remove,
                                )
                                .await;
                                continue;
                            }
                            // append entries if any
                            if let Some(prev_log_index) = prev_log_index {
                                log.truncate(prev_log_index as usize + 1);
                            }
                            if entries.len() > 0 {
                                println!("Appending entries: {}", entries.len());
                                log.append(&mut entries);
                            }
                            reply_append_entry(
                                connections.get_mut(&ip).unwrap(),
                                &ip,
                                term,
                                true,
                                Some(log.len() as Index - 1),
                                &mut connections_to_remove,
                            )
                            .await;
                        }
                        MessageBody::RequestVote {
                            term,
                            candidate_id,
                            last_log_index,
                            last_log_term,
                        } => {
                            if message.term >= current_term
                                && (voted_for.is_none() || voted_for.unwrap() == candidate_id)
                                && is_log_up_to_date(&log, last_log_index, last_log_term)
                            {
                                grant_vote(
                                    &candidate_id,
                                    &ip,
                                    connections.get_mut(&ip).unwrap(),
                                    message.term,
                                    &mut voted_for,
                                    &mut election_instant,
                                    &mut connections_to_remove,
                                )
                                .await;
                                println!("voted for {}", candidate_id);
                            }
                            reply_request_vote(
                                connections.get_mut(&ip).unwrap(),
                                &ip,
                                message.term,
                                false,
                                &mut connections_to_remove,
                            )
                            .await;
                        }
                        _ => (),
                    }
                }
                while let Some(command) = front_end_command_receiver.try_recv().ok() {
                    match command {
                        FrontEndCommand::SetValue { key, value } => {
                            if let Some(lead_ip) = leader_ip {
                                let command = Command::new_set_value(key, value, my_id);
                                println!("sending command to leader: {:?}", command);
                                let _ = send_command_to_leader(
                                    lead_ip,
                                    current_term,
                                    command,
                                    &mut connections,
                                )
                                .await;
                                front_end_response_sender
                                    .send(FrontEndCommandResponse::Ok)
                                    .await
                                    .unwrap();
                            } else {
                                println!("No leader ip");
                            }
                        }
                        FrontEndCommand::GetValue { key } => {
                            let value = inner_state.get_value(&key);
                            let current_time = SystemTime::now();
                            if inner_state.is_unlocked(&my_id, &current_time) {
                                if let Some(value) = value {
                                    front_end_response_sender
                                        .send(FrontEndCommandResponse::Value(value))
                                        .await
                                        .unwrap();
                                } else {
                                    front_end_response_sender
                                        .send(FrontEndCommandResponse::NotFound)
                                        .await
                                        .unwrap();
                                }
                            } else {
                                front_end_response_sender
                                    .send(FrontEndCommandResponse::Locked)
                                    .await
                                    .unwrap();
                            }
                        }
                        FrontEndCommand::Lock => {
                            if let Some(lead_ip) = leader_ip {
                                let command = Command::new_lock(my_id);
                                println!("sending command to leader: {:?}", command);
                                let _ = send_command_to_leader(
                                    lead_ip,
                                    current_term,
                                    command,
                                    &mut connections,
                                )
                                .await;
                                front_end_response_sender
                                    .send(FrontEndCommandResponse::Ok)
                                    .await
                                    .unwrap();
                            } else {
                                front_end_response_sender
                                    .send(FrontEndCommandResponse::Error("No leader ip".to_owned()))
                                    .await
                                    .unwrap();
                                println!("No leader ip");
                            }
                        }
                        FrontEndCommand::Unlock => {
                            if let Some(lead_ip) = leader_ip {
                                let command = Command::new_unlock(my_id);
                                println!("sending command to leader: {:?}", command);
                                let _ = send_command_to_leader(
                                    lead_ip,
                                    current_term,
                                    command,
                                    &mut connections,
                                )
                                .await;
                                front_end_response_sender
                                    .send(FrontEndCommandResponse::Ok)
                                    .await
                                    .unwrap();
                            } else {
                                front_end_response_sender
                                    .send(FrontEndCommandResponse::Error("No leader ip".to_owned()))
                                    .await
                                    .unwrap();
                                println!("No leader ip");
                            }
                        }
                        FrontEndCommand::DelValue { key } => {
                            if let Some(lead_ip) = leader_ip {
                                let command = Command::new_del_value(key, my_id);
                                println!("sending command to leader: {:?}", command);
                                let _ = send_command_to_leader(
                                    lead_ip,
                                    current_term,
                                    command,
                                    &mut connections,
                                )
                                .await;
                                front_end_response_sender
                                    .send(FrontEndCommandResponse::Ok)
                                    .await
                                    .unwrap();
                            } else {
                                front_end_response_sender
                                    .send(FrontEndCommandResponse::Error("No leader ip".to_owned()))
                                    .await
                                    .unwrap();
                                println!("No leader ip");
                            }
                        }
                        FrontEndCommand::GetStore => {
                            let store = inner_state.get_store();
                            front_end_response_sender
                                .send(FrontEndCommandResponse::Store(store))
                                .await
                                .unwrap();
                        }
                    }
                }
                if Instant::now().duration_since(election_instant) > election_timeout {
                    println!("Timeout... I am the candidate!");
                    state = ServerState::Candidate;
                }
            }
            ServerState::Candidate => {
                if Instant::now().duration_since(election_instant) > election_timeout {
                    println!("Timeout... I am the candidate!");
                    start_election(
                        &mut current_term,
                        &mut voted_for,
                        &mut election_instant,
                        &mut vote_count,
                        my_id,
                        &log,
                        &mut connections,
                        &mut connections_to_remove,
                    )
                    .await;
                }
                while let Some((ip, message)) = messages_buffer.pop_front() {
                    match message.body {
                        MessageBody::AppendEntry { .. } => {
                            if message.term > current_term {
                                println!("Timeout... I am the follower!");
                                state = ServerState::Follower;
                                leader_ip = Some(ip);
                                continue 'main;
                            }
                        }
                        MessageBody::RequestVote {
                            term,
                            candidate_id,
                            last_log_index,
                            last_log_term,
                        } => {
                            if message.term > current_term {
                                println!("Timeout... I am the follower!");
                                state = ServerState::Follower;
                                leader_ip = Some(ip);
                                if message.term >= current_term
                                    && (voted_for.is_none() || voted_for.unwrap() == candidate_id)
                                    && is_log_up_to_date(&log, last_log_index, last_log_term)
                                {
                                    grant_vote(
                                        &candidate_id,
                                        &ip,
                                        connections.get_mut(&ip).unwrap(),
                                        message.term,
                                        &mut voted_for,
                                        &mut election_instant,
                                        &mut connections_to_remove,
                                    )
                                    .await;
                                    println!("voted for {}", candidate_id);
                                }
                                reply_request_vote(
                                    connections.get_mut(&ip).unwrap(),
                                    &ip,
                                    message.term,
                                    false,
                                    &mut connections_to_remove,
                                )
                                .await;
                                continue 'main;
                            }
                        }
                        MessageBody::RequestVoteResponse { success: true } => {
                            if message.term != current_term {
                                continue;
                            }
                            vote_count += 1;
                            println!("Vote granted!");
                            if vote_count >= (MIN_NODES - 1) / 2 {
                                println!("Timeout... I am the leader!");
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
                        MessageBody::AppendEntry { term, .. } => {
                            if message.term < current_term {
                                reply_append_entry(
                                    connections.get_mut(&ip).unwrap(),
                                    &ip,
                                    term,
                                    false,
                                    None,
                                    &mut connections_to_remove,
                                )
                                .await;
                                continue;
                            } else if message.term > current_term {
                                println!("Timeout... I am the follower!");
                                state = ServerState::Follower;
                                leader_ip = Some(ip);
                                continue 'main;
                            }
                        }
                        MessageBody::RequestVote {
                            term,
                            candidate_id,
                            last_log_index,
                            last_log_term,
                        } => {
                            if message.term < current_term {
                                reply_request_vote(
                                    connections.get_mut(&ip).unwrap(),
                                    &ip,
                                    term,
                                    false,
                                    &mut connections_to_remove,
                                )
                                .await;
                                continue;
                            } else if message.term > current_term {
                                println!("Timeout... I am the follower!");
                                state = ServerState::Follower;
                                if message.term >= current_term
                                    && (voted_for.is_none() || voted_for.unwrap() == candidate_id)
                                    && is_log_up_to_date(&log, last_log_index, last_log_term)
                                {
                                    grant_vote(
                                        &candidate_id,
                                        &ip,
                                        connections.get_mut(&ip).unwrap(),
                                        message.term,
                                        &mut voted_for,
                                        &mut election_instant,
                                        &mut connections_to_remove,
                                    )
                                    .await;
                                    println!("voted for {}", candidate_id);
                                }
                                reply_request_vote(
                                    connections.get_mut(&ip).unwrap(),
                                    &ip,
                                    message.term,
                                    false,
                                    &mut connections_to_remove,
                                )
                                .await;
                                continue 'main;
                            }
                        }
                        MessageBody::Command { command } => {
                            let command_index = log.len() as Index;
                            println!("New command: {:?}", command);
                            log.append(&mut vec![Entry::new_command(
                                current_term,
                                command,
                                command_index,
                            )]);
                        }
                        MessageBody::AppendEntryResponse {
                            success,
                            last_log_index: node_last_applied,
                        } => {
                            if let Some(last_applied_index) = node_last_applied {
                                next_index
                                    .entry(ip)
                                    .and_modify(|index| *index = last_applied_index + 1)
                                    .or_insert(last_applied_index + 1);
                                match_index
                                    .entry(ip)
                                    .and_modify(|index| *index = last_applied_index)
                                    .or_insert(last_applied_index);
                                println!("none last applid: {}", node_last_applied.unwrap());
                            } else {
                                next_index.entry(ip).and_modify(|index| {
                                    if *index > 0 {
                                        *index -= 1 as Index
                                    }
                                });
                                continue;
                            }
                            let mut match_index_is_bigger_count = 0;
                            let mut min_bigger_index = 0;
                            for (ip, index) in match_index.iter() {
                                if commit_index.is_none() || index > &commit_index.unwrap() {
                                    match_index_is_bigger_count += 1;
                                    if *index > min_bigger_index {
                                        min_bigger_index = *index;
                                    }
                                }
                            }
                            println!(
                                "match_index_is_bigger_count: {}",
                                match_index_is_bigger_count
                            );
                            if match_index_is_bigger_count >= (MIN_NODES - 1) / 2 {
                                commit_index = Some(min_bigger_index);
                                println!(
                                    "new commit_index: {:?}; last_applied: {:?}",
                                    commit_index, last_applied
                                );
                            }
                            apply_to_state_machine(
                                &log,
                                &commit_index,
                                &mut last_applied,
                                &mut inner_state,
                            );
                        }
                        _ => {}
                    }
                }
                while let Some(command) = front_end_command_receiver.try_recv().ok() {
                    match command {
                        FrontEndCommand::SetValue { key, value } => {
                            let index = log.len() as Index;
                            let entry = Entry::new_command(
                                current_term,
                                Command::new_set_value(key, value, my_id),
                                index,
                            );
                            log.push(entry);
                            front_end_response_sender
                                .send(FrontEndCommandResponse::Ok)
                                .await
                                .unwrap();
                        }
                        FrontEndCommand::GetValue { key } => {
                            let value = inner_state.get_value(&key);
                            if let Some(value) = value {
                                front_end_response_sender
                                    .send(FrontEndCommandResponse::Value(value))
                                    .await
                                    .unwrap();
                            } else {
                                front_end_response_sender
                                    .send(FrontEndCommandResponse::NotFound)
                                    .await
                                    .unwrap();
                            }
                        }
                        FrontEndCommand::DelValue { key } => {
                            let index = log.len() as Index;
                            let entry = Entry::new_command(
                                current_term,
                                Command::new_del_value(key.clone(), my_id),
                                index,
                            );
                            log.push(entry);
                            front_end_response_sender
                                .send(FrontEndCommandResponse::Ok)
                                .await
                                .unwrap();
                        }
                        FrontEndCommand::Lock => {
                            let index = log.len() as Index;
                            let entry = Entry::new_command(
                                current_term,
                                Command::new_lock(my_id),
                                index,
                            );
                            log.push(entry);
                            front_end_response_sender
                                .send(FrontEndCommandResponse::Ok)
                                .await
                                .unwrap();
                        }
                        FrontEndCommand::Unlock => {
                            let index = log.len() as Index;
                            let entry = Entry::new_command(
                                current_term,
                                Command::new_unlock(my_id),
                                index,
                            );
                            log.push(entry);
                            front_end_response_sender
                                .send(FrontEndCommandResponse::Ok)
                                .await
                                .unwrap();
                        }
                        FrontEndCommand::GetStore => {
                            let store = inner_state.get_store();
                            front_end_response_sender
                                .send(FrontEndCommandResponse::Store(store))
                                .await
                                .unwrap();
                        }
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
                    commit_index,
                )
                .await;
            }
        }
        remove_peers(&mut connections_to_remove, &mut connections, &peers).await;
    }
    println!("Done.");
}
