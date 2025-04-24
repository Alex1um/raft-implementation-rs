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
    let (front_end_command_receiver, front_end_response_sender, http_server_handle) = rocket().await;
    run_raft(receiver, signal_receiver, front_end_command_receiver, front_end_response_sender).await;
    let _ = send_handle.abort();
    let _ = recv_handle.abort();
    let _ = http_server_handle.await;
    println!("Done.");
}

#[cfg(test)]
mod tests {
    use super::models::*;
    use serde_json;
    use serde::{Deserialize};

    #[test]
    fn dessearlize_double() {
        let double_json = r#"{"term":8,"body":{"RequestVote":{"term":8,"candidate_id":"83abf631-f9b6-43e9-91ae-32f5ccc5bb7e","last_log_index":18446744073709551615,"last_log_term":0}}}{"term":9,"body":{"RequestVoteResponse":{"success":false}}}"#;
        let mut deserializer = serde_json::Deserializer::from_str(double_json);
        let message1 = Message::deserialize(&mut deserializer).unwrap();
        let message2 = Message::deserialize(&mut deserializer).unwrap();
        match Message::deserialize(&mut deserializer) {
            Ok(_) => {}
            Err(e) if e.is_eof() => { println!("no message3 left") }
            Err(e) => {}
        }
        println!("msg1: {:?}", message1);
        println!("msg2: {:?}", message2);
    }

    #[test]
    fn desserialize_apend_entries() {
        let append_entries = r#"{"AppendEntry":{"term":3,"leader_id":"397fdc1d-67ed-4e76-9ea4-a0611aa8b6ba","prev_log_index":null,"prev_log_term":null,"entries":[{"term":1,"command":{"SetValue":{"key":"1","value":"1"}},"index":0},{"term":1,"command":{"SetValue":{"key":"1","value":"1"}},"index":1}],"leader_commit":0}}"#;
        let mut deserializer = serde_json::Deserializer::from_str(append_entries);
        let message = Message::deserialize(&mut deserializer).unwrap();
        println!("{:?}", message);
    }
}