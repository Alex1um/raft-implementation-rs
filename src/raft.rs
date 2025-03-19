use std::net::IpAddr;
use std::time::Duration;
use std::{net::SocketAddr, sync::Arc};

use crate::consts::{HEARTBEAT_TIMEOUT, MAX_ELECTION_TIMEOUT, MIN_ELECTION_TIMEOUT};
use crate::models::*;
use std::collections::HashSet;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::net::TcpSocket;
use tokio::net::TcpStream;
use tokio::sync::broadcast::error::TryRecvError;
use tokio::sync::Mutex;
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio::time::Instant;
use uuid::Uuid;
use rand::prelude::*;

async fn create_peers(
    mut receiver: Receiver<SocketAddr>,
    peers: Arc<Mutex<HashSet<IpAddr>>>,
    sender: Sender<TcpStream>,
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
                .send(stream)
                .await
                .unwrap_or_else(|e| print!("error: {}", e));
            peers.insert(ip);
            println!("Connected to {}", addr);
        }
    }
}

async fn listen_for_peers(peers: Arc<Mutex<HashSet<IpAddr>>>, sender: Sender<TcpStream>) {
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
            .send(socket)
            .await
            .unwrap_or_else(|e| print!("error: {}", e));
        println!("Connected to {}", addr);
    }
}


async fn receive_messages(
    connection: &TcpStream,
    buf: &mut [u8; 1024],
) -> Result<Message, std::io::Error> {
    connection.readable().await?;
    let readed = connection.try_read(buf)?;
    Ok(serde_json::from_slice::<Message>(&buf[0..readed]).unwrap())
}

pub async fn run_raft(receiver: Receiver<SocketAddr>) {
    let peers = Arc::new(Mutex::new(HashSet::new()));
    let (stream_sender, mut new_connections) = channel::<TcpStream>(3);
    tokio::spawn(listen_for_peers(peers.clone(), stream_sender.clone()));
    tokio::spawn(create_peers(receiver, peers.clone(), stream_sender));

    let mut connections = Vec::<TcpStream>::new();
    let mut buf = [0u8; 1024];
    let mut rng = rand::rng();

    let election_timeout = rng.random_range(MIN_ELECTION_TIMEOUT..=MAX_ELECTION_TIMEOUT);
    let mut state = ServerState::Follower;
    let my_id = uuid::Uuid::new_v4();

    let mut current_term = 0;
    let mut election_instant = Instant::now();
    let mut sended_messages = Vec::<Message>::new();

    let mut self_votes = 0;
    let mut total_votes = 0;

    'main: loop {
        // setup new connections
        loop {
            match new_connections.try_recv() {
                Ok(stream) => {
                    connections.push(stream);
                }
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                    break 'main;
                }
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                    break;
                }
            } 
        }
        // receive messages
        self_votes = 0;
        for connection in &connections {
            match receive_messages(connection, &mut buf).await {
                std::io::Result::Ok(msg) => {
                    if msg.term > current_term {
                        current_term = msg.term;
                        state = ServerState::Follower;
                        election_instant = Instant::now();
                    } else if msg.term < current_term {
                        continue;
                    }
                    match msg.body {
                        MessageBody::AppendEntry(AppendEntry { .. }) => {
                            if state == ServerState::Candidate {
                                state = ServerState::Follower;
                            }
                            election_instant = Instant::now();
                        }
                        MessageBody::RequestVote(RequestVote { candidate_id, .. }) => {
                            if candidate_id == my_id {
                                self_votes += 1;
                            }
                            total_votes += 1;
                        }
                    }
                }
                std::io::Result::Err(e) => match e.kind() {
                    std::io::ErrorKind::WouldBlock => {
                        continue;
                    }
                    _ => {
                        println!("error: {}", e);
                        continue;
                    }
                },
            }
        }
        // Do my work
        match state {
            ServerState::Follower => {
                if Instant::now()
                    .duration_since(election_instant)
                    > election_timeout
                {
                    println!("I am the candidate!");
                    state = ServerState::Candidate;
                }
            }
            ServerState::Candidate => {
                if self_votes > total_votes / 2 {
                    println!("I am the leader!");
                    state = ServerState::Leader;
                } else if Instant::now()
                    .duration_since(election_instant)
                    > election_timeout
                {
                    current_term += 1;
                    election_instant = Instant::now();
                    let vote = RequestVote {
                        term: current_term,
                        candidate_id: my_id,
                        last_log_index: 0,
                        last_log_term: 0,
                    };
                    let serialized = serde_json::to_vec(&vote).unwrap();
                    for connection in &mut connections {
                        match connection.write_all(&serialized).await {
                            std::io::Result::Ok(_) => {}
                            std::io::Result::Err(e) => {
                                println!("error while sending vote: {}", e);
                                continue;
                            }
                        }
                    }
                }
            }
            ServerState::Leader => {
                let heartbeat = AppendEntry::heartbeat(current_term, my_id, 0, 0, 0);
                let serialized = serde_json::to_vec(&heartbeat).unwrap();
                for connection in &mut connections {
                    match connection.write_all(&serialized).await {
                        std::io::Result::Ok(_) => {}
                        std::io::Result::Err(e) => {
                            println!("error while sending heartbeat: {}", e);
                            continue;
                        }
                    }
                }
            }
        }
    }
}
