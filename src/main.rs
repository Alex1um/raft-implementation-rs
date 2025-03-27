mod consts;
mod self_finder;
mod raft;
mod models;
mod http_routes;
mod signal_handler;

use self_finder::run_self_finder;
use tokio;
use raft::run_raft;
use http_routes::rocket;
use signal_handler::setup_signal_handler;

#[tokio::main]
async fn main() {
    let (_, signal_receiver) = setup_signal_handler().await;
    let (receiver, send_handle, recv_handle) = run_self_finder().await;
    let http_server_handle = rocket().await;
    run_raft(receiver, signal_receiver).await;
    let _ = send_handle.abort();
    let _ = recv_handle.abort();
    let _ = http_server_handle.await;
    println!("Done.");
}
