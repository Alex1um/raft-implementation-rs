use rocket::{get, launch, routes};
use std::process;
use tokio;

#[get("/")]
fn index() -> &'static str {
    "Hello, world!"
}

#[get("/stop")]
fn stop() -> &'static str {
    process::exit(0);
}

pub async fn rocket() -> tokio::task::JoinHandle<()> {
    let send_handle = tokio::spawn(async move {
        rocket::build().mount("/", routes![index, stop]).launch().await.unwrap();
    });
    return send_handle;
}

