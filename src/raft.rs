use std::net::IpAddr;
use std::time::Duration;
use std::{net::SocketAddr, sync::Arc};

use crate::consts::{MAX_ELECTION_TIMEOUT, MIN_ELECTION_TIMEOUT, MIN_NODES};
use crate::models::*;
use rand::prelude::*;
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
    println!("My election timeout: {:#?}", election_timeout);

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
                            MessageBody::AppendEntry { .. } => {
                                if state == ServerState::Candidate {
                                    state = ServerState::Follower;
                                    println!("I am the follower!");
                                }
                                election_instant = Instant::now();
                            }
                            MessageBody::RequestVote { candidate_id, .. } => {
                                if candidate_id == my_id {
                                    self_votes += 1;
                                }
                                total_votes += 1;
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
        remove_peers(&mut connections_to_remove, &mut connections, &peers).await;
    }
    println!("Done.");
}
