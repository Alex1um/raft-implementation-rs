use crate::models::*;
use rocket::form::Form;
use rocket::form::FromForm;
use rocket::response::content;
use rocket::{State, get, post, routes};
use std::process;
use tokio;
use tokio::sync::Mutex;
use tokio::sync::mpsc;

struct CommandCommunicator {
    command_sender: mpsc::Sender<FrontEndCommand>,
    response_receiver: Mutex<mpsc::Receiver<FrontEndCommandResponse>>,
}

#[derive(FromForm)]
struct SetValueForm {
    name: String,
    value: String,
}

#[derive(FromForm)]
struct GetValueForm {
    name: String,
}

#[get("/")]
fn index() -> content::RawHtml<&'static str> {
    content::RawHtml(
        r#"
        Hello, world!
        <a href="/stop">stop</a>
        <hr>
        <form action="/set" method="post">
            <label for="name">Name:</label>
            <input type="text" id="name" name="name"><br><br>
            <label for="value">Value:</label>
            <input type="text" id="value" name="value"><br><br>
            <input type="submit" value="Submit">
        </form>
        <hr>
        <form action="/get" method="post">
            <label for="name">Name:</label>
            <input type="text" id="name" name="name"><br><br>
            <input type="submit" value="Submit">
        </form>
        <hr>
        <a href="/lock">lock</a>
        <hr>
        <a href="/unlock">unlock</a>
        "#,
    )
}

#[post("/set", data = "<command>")]
async fn set(command: Form<SetValueForm>, communicator: &State<CommandCommunicator>) -> String {
    let command = FrontEndCommand::SetValue {
        key: command.name.clone(),
        value: command.value.clone(),
    };
    communicator.command_sender.send(command).await.unwrap();
    let response = communicator
        .response_receiver
        .lock()
        .await
        .recv()
        .await
        .unwrap();
    return format!("{:?}", response);
}

#[post("/get", data = "<command>")]
async fn get(command: Form<GetValueForm>, communicator: &State<CommandCommunicator>) -> String {
    let command = FrontEndCommand::GetValue {
        key: command.name.clone(),
    };
    communicator.command_sender.send(command).await.unwrap();
    let response = communicator
        .response_receiver
        .lock()
        .await
        .recv()
        .await
        .unwrap();
    return format!("{:?}", response);
}

#[get("/lock")]
async fn lock(communicator: &State<CommandCommunicator>) -> String {
    let command = FrontEndCommand::Lock;
    communicator.command_sender.send(command).await.unwrap();
    let response = communicator
        .response_receiver
        .lock()
        .await
        .recv()
        .await
        .unwrap();
    return format!("{:?}", response);
}

#[get("/unlock")]
async fn unlock(communicator: &State<CommandCommunicator>) -> String {
    let command = FrontEndCommand::Unlock;
    communicator.command_sender.send(command).await.unwrap();
    let response = communicator
        .response_receiver
        .lock()
        .await
        .recv()
        .await
        .unwrap();
    return format!("{:?}", response);
}

#[get("/stop")]
fn stop() -> &'static str {
    process::exit(0);
}

pub async fn rocket() -> (
    mpsc::Receiver<FrontEndCommand>,
    mpsc::Sender<FrontEndCommandResponse>,
    tokio::task::JoinHandle<()>,
) {
    let (command_sender, command_receiver) = mpsc::channel::<FrontEndCommand>(2);
    let (response_sender, response_receiver) = mpsc::channel::<FrontEndCommandResponse>(2);
    let communicator = CommandCommunicator {
        command_sender,
        response_receiver: Mutex::new(response_receiver),
    };
    let send_handle = tokio::spawn(async move {
        rocket::build()
            .mount("/", routes![index, stop, set, get, lock, unlock])
            .manage(communicator)
            .launch()
            .await
            .unwrap();
    });
    return (command_receiver, response_sender, send_handle);
}
