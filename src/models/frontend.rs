pub enum FrontEndCommand {
    SetValue { key: String, value: String },
    GetValue { key: String },
    DelValue { key: String },
    GetStore,
    Lock,
    Unlock,
}

#[derive(Debug)]
pub enum FrontEndCommandResponse {
    Ok,
    Value(String),
    Store(String),
    Error(String),
    NotFound,
    Locked,
}
