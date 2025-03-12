mod consts;
mod self_finder;
mod raft;
mod models;

use self_finder::run_self_finder;
use tokio;
use raft::run_raft;

#[tokio::main]
async fn main() {
    let (receiver, send_handle, recv_handle) = run_self_finder().await;
    run_raft(receiver).await;
    let _ = send_handle.await;
    let _ = recv_handle.await;
    println!("Done.");
}
