use std::net::IpAddr;
use std::time::Duration;
use std::{net::SocketAddr, sync::Arc};

use crate::consts::{MAX_ELECTION_TIMEOUT, MIN_ELECTION_TIMEOUT, MIN_NODES};
use crate::models::*;
use rand::prelude::*;
use rand::seq::index;
use rocket::http::Iter;
use uuid::Uuid;
use std::collections::{HashMap, HashSet};
use std::time::Instant;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::sync::mpsc::{Receiver, Sender, channel};

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
    next_index: &mut HashMap<IpAddr, Index>,
    leader_id: Uuid,
    term: Term,
    prev_term: Term,
    prev_index: Index,
    commit_index: Index
) {
    let last_log_index = log.len() as Index - 1;
    for (ip, connection) in connections.iter_mut() {
        let index = next_index.entry(*ip).or_insert(0);
        if last_log_index > *index {
            let entries = log[*index as usize..].to_vec();
            let message = Message::new(term, MessageBody::AppendEntry {
                term,
                leader_id,
                prev_log_index: prev_index,
                prev_log_term: prev_term,
                entries,
                leader_commit: commit_index
            });
            let data = serde_json::to_vec(&message).unwrap();
            match connection.write_all(&data).await {
                Ok(_) => {}
                Err(e) => {
                    println!("Error sending message: {}", e);
                    connections_to_delete.insert(*ip);
                }
            }
        }
    }
}

pub async fn run_raft(receiver: Receiver<SocketAddr>, mut signal_receiver: Receiver<()>) {
    let peers = Arc::new(Mutex::new(HashSet::new()));
    let (stream_sender, mut new_connections) = channel::<(TcpStream, IpAddr)>(3);
    tokio::spawn(listen_for_peers(peers.clone(), stream_sender.clone()));
    tokio::spawn(create_peers(receiver, peers.clone(), stream_sender));

    let mut connections = HashMap::<IpAddr, TcpStream>::new();
    let mut connections_to_remove = HashSet::<IpAddr>::new();
    let mut buf = [0u8; 1024];
    let mut rng = rand::rng();

    let election_timeout = rng.random_range(MIN_ELECTION_TIMEOUT..=MAX_ELECTION_TIMEOUT);
    let mut state = ServerState::Follower;
    let my_id = uuid::Uuid::new_v4();

    let mut current_term = 0;
    let mut election_instant = Instant::now();
    let mut log = Vec::<Entry>::new();
    println!("My election timeout: {:#?}", election_timeout);
    
    let mut commit_index = 0; // index of highest log entry known to be commited
    let mut last_applied = 0; // index of highest log entry applied to state machine

    let mut next_index = HashMap::<IpAddr, Index>::new();
    let mut match_index = HashMap::<IpAddr, Index>::new();

    let mut self_votes = 0;
    let mut total_votes = 0;

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
        for (ip, connection) in connections.iter_mut() {
            let received = connection.try_read(&mut buf);
            match received {
                Ok(0) => {
                    println!("Closing connection to {}", ip);
                    connections_to_remove.insert(*ip);
                }
                Ok(readed) => match serde_json::from_slice::<Message>(&buf[0..readed]) {
                    Ok(message) => {
                        if message.term > current_term {
                            current_term = message.term;
                            state = ServerState::Follower;
                            election_instant = Instant::now();
                            println!("I am the follower!");
                        } else if message.term < current_term {
                            continue;
                        }
                        match message.body {
                            MessageBody::AppendEntry { mut entries, .. } => {
                                match state {
                                    ServerState::Leader => {
                                        commit_index = log.len() as Index - 1;
                                        log.append(&mut entries);
                                        println!("I am the leader!");
                                    }
                                    ServerState::Candidate => {
                                        println!("I am the candidate!");
                                        state = ServerState::Follower;
                                    }
                                    _ => {}
                                }
                                election_instant = Instant::now();
                            }
                            MessageBody::RequestVote { candidate_id, .. } => {
                                if candidate_id == my_id {
                                    self_votes += 1;
                                }
                                total_votes += 1;
                            }
                            MessageBody::AppendEntryResponse { success} => {
                                if state == ServerState::Leader {
                                    if success {
                                        next_index.insert(*ip, log.len() as Index);
                                        match_index.insert(*ip, log.len() as Index);
                                    } else {
                                        next_index.entry(*ip).and_modify(|e| *e -= 1);
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => println!("Error deserializing message: {}", e),
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
        // Do my work
        match state {
            ServerState::Follower => {
                if Instant::now().duration_since(election_instant) > election_timeout {
                    println!("I am the candidate!");
                    state = ServerState::Candidate;
                    total_votes = 0;
                    self_votes = 0;
                }
            }
            ServerState::Candidate => {
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
                let heartbeat = Message::new_heartbeat(current_term, my_id, 0, 0, 0);
                send_all(&mut connections, &heartbeat, &mut connections_to_remove).await;
            }
        }
        process_replication(
            &log,
            &mut connections,
            &mut connections_to_remove,
            &mut next_index,
            my_id,
            current_term,
            current_term - 1,
            last_applied,
            commit_index,
        ).await;
        remove_peers(&mut connections_to_remove, &mut connections, &peers).await;
    }
    println!("Done.");
}
