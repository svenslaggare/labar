use std::path::Path;
use serde::{Deserialize, Serialize};

use crate::image::Image;
use crate::reference::ImageId;

#[derive(Debug, Serialize, Deserialize)]
pub struct State {
    pub layers: Vec<ImageId>,
    pub images: Vec<Image>
}

impl State {
    pub fn new() -> State {
        State {
            layers: Vec::new(),
            images: Vec::new()
        }
    }

    pub fn from_file(path: &Path) -> Result<State, String> {
        let state_content = std::fs::read_to_string(path)
            .map_err(|err| format!("{}", err))?;

        let state: State = serde_json::from_str(&state_content)
            .map_err(|err| format!("{}", err))?;

        Ok(state)
    }

    pub fn save_to_file(&self, path: &Path) -> Result<(), String> {
        std::fs::write(
            path,
            serde_json::to_string_pretty(self).map_err(|err| format!("{}", err))?
        ).map_err(|err| format!("{}", err))?;

        Ok(())
    }
}