use crate::consts::{INTERVAL, TIMEOUT, MULTICAST_ADDR, MULTICAST_PORT, BIND_ADDR, BIND_PORT};
use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::time::Duration;
use tokio;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::time::Instant;

fn check_hashmap(copies: &mut HashMap<SocketAddr, Instant>) {
    let prev_len = copies.len();
    copies.retain(|k, v| {
        let is_ok = v.elapsed().as_secs_f32() < TIMEOUT;
        if !is_ok {
            println!("copy {} timed out", k);
        }
        is_ok
    });
    if copies.len() < prev_len {
        println!("current copies: {:?}", copies.keys());
    }
}

async fn send_self(send_socket: Arc<UdpSocket>) {
    loop {
        send_socket
            .send_to("".as_bytes(), (MULTICAST_ADDR, MULTICAST_PORT))
            .await
            .expect("valid send");
        tokio::time::sleep(Duration::from_secs_f32(INTERVAL)).await;
    }
}

async fn receive_self(receive_socket: Arc<UdpSocket>, sender: mpsc::Sender<SocketAddr>) {
    let mut buffer = [0u8; 64];
    let mut copies = HashMap::<SocketAddr, Instant>::new();
    loop {
        match receive_socket.recv_from(&mut buffer).await {
            Ok((_, addr)) => match copies.entry(addr) {
                std::collections::hash_map::Entry::Vacant(e) => {
                    println!("New copy found: {:?}", addr);
                    sender.send(addr).await.expect("valid send");
                    e.insert(Instant::now());
                    println!("current copies: {:?}", copies.keys());
                }
                std::collections::hash_map::Entry::Occupied(mut e) => {
                    e.insert(Instant::now());
                }
            },
            Err(e) => match e.kind() {
                std::io::ErrorKind::TimedOut
                | std::io::ErrorKind::WouldBlock
                | std::io::ErrorKind::Interrupted => {}
                _ => {
                    println!("recv error: {}", e);
                    break;
                }
            },
        }
        check_hashmap(&mut copies)
    }
}

pub async fn run_self_finder() -> (tokio::sync::mpsc::Receiver<std::net::SocketAddr>, tokio::task::JoinHandle<()>, tokio::task::JoinHandle<()>) {
    let socket = UdpSocket::bind((BIND_ADDR, BIND_PORT)).await
                .expect("Failed to bind ipv4");
    socket
        .join_multicast_v4(MULTICAST_ADDR, BIND_ADDR)
        .expect("valid join IPv4 multicast group");
    socket
        .set_multicast_loop_v4(true)
        .expect("setted loop option");
    let send_socket = Arc::new(socket);
    let receive_socket = send_socket.clone();
    let (sender, receiver) = mpsc::channel::<SocketAddr>(2);
    let send_handle = tokio::spawn(async move {
        send_self(send_socket).await;
    });
    let recv_handle = tokio::spawn(async move {
        receive_self(receive_socket, sender).await;
    });
    return (receiver, send_handle, recv_handle);
}
