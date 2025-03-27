use rocket::{get, launch, routes};
use std::process;
use tokio;
use rocket::response::content;


#[get("/")]
fn index() -> content::RawHtml<&'static str> {
    content::RawHtml(
        r#"Hello, world!
        <a href="/stop">stop</a>"#
    )
}

#[get("/stop")]
fn stop() -> &'static str {
    process::exit(0);
    "Ok"
}

pub async fn rocket() -> tokio::task::JoinHandle<()> {
    let send_handle = tokio::spawn(async move {
        rocket::build().mount("/", routes![index, stop]).launch().await.unwrap();
    });
    return send_handle;
}

