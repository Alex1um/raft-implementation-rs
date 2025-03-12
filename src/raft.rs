use std::net::IpAddr;
use std::time::Duration;
use std::{net::SocketAddr, sync::Arc};

use tokio::sync::mpsc::Receiver;
use tokio::net::TcpStream;
use tokio::net::TcpListener;
use tokio::net::TcpSocket;
use tokio::sync::Mutex;
use std::collections::HashMap;


async fn create_peers(mut receiver: Receiver<SocketAddr>, peers: Arc<Mutex<HashMap<IpAddr, TcpStream>>>) {
    while let Some(addr) = receiver.recv().await {
        let ip = addr.ip();
        let addr = SocketAddr::from((ip, 48667));
        let mut peers = peers.lock().await;
        if peers.contains_key(&ip) {
            continue;
        }
        if let Ok(stream) = TcpStream::connect(addr).await {
            peers.insert(ip, stream);
            println!("Connected to {}", addr);
        }
    }
}

async fn listen_for_peers(peers: Arc<Mutex<HashMap<IpAddr, TcpStream>>>) {
    let listener = TcpListener::bind("0.0.0.0:48667").await.unwrap();
    loop {
        let (socket, _) = listener.accept().await.unwrap();
        let addr = socket.peer_addr().unwrap();
        let ip = addr.ip();
        let mut peers = peers.lock().await;
        if peers.contains_key(&ip) {
            continue;
        }
        peers.insert(ip, socket);
        println!("Connected to {}", addr);
    }
}

pub async fn run_raft(receiver: Receiver<SocketAddr>) {
    let peers = Arc::new(Mutex::new(HashMap::new()));
    tokio::spawn(listen_for_peers(peers.clone()));
    tokio::spawn(create_peers(receiver, peers.clone()));
    loop {
        let mut peers = peers.lock().await;
        println!("Peers: {:?}", peers);
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
