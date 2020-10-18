use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize)]
pub enum LinkType {
    Soft,
    Hard
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum LayerOperation {
    Image { hash: String },
    File { path: String, source_path: String, link_type: LinkType },
    Directory { path: String }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Layer {
    pub parent_hash: Option<String>,
    pub hash: String,
    pub operations: Vec<LayerOperation>,
    pub created: std::time::SystemTime,
}

impl Layer {
    pub fn new(parent_hash: Option<String>, hash: String, operations: Vec<LayerOperation>) -> Layer {
        Layer {
            parent_hash,
            hash,
            operations,
            created: std::time::SystemTime::now()
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Image {
    pub hash: String,
    pub tag: String
}

impl Image {
    pub fn new(hash: String, tag: String) -> Image {
        Image {
            hash,
            tag
        }
    }
}