use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct Definition {
    name: String,
    settings: Settings,
}

#[derive(Serialize, Deserialize)]
pub struct Settings {
    time_field: String,
    #[serde(default)]
    index_all: bool,
    fields: Vec<Field>,
}
