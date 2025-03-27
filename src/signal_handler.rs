use tokio::sync::mpsc::{Receiver, Sender, channel};


pub async fn setup_signal_handler() -> (tokio::task::JoinHandle<()>, Receiver<()>) {
    let (sender, receiver) = channel(1);
    let ctrl_c = async move {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
        sender.send(()).await.unwrap();
    };
    return (tokio::spawn(ctrl_c), receiver);
}