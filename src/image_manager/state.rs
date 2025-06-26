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
}