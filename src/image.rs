use std::fmt::{Display, Formatter};
use std::time::SystemTime;

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize)]
pub enum LinkType {
    Soft,
    Hard
}

impl Display for LinkType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            LinkType::Soft => write!(f, "Soft"),
            LinkType::Hard => write!(f, "Hard")
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum LayerOperation {
    Image { hash: String },
    File { path: String, source_path: String, link_type: LinkType },
    Directory { path: String }
}

impl Display for LayerOperation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            LayerOperation::Image { hash } => {
                write!(f, "Image: {}", hash)
            }
            LayerOperation::Directory { path } => {
                write!(f, "Create directory: {}", path)
            }
            LayerOperation::File { path, source_path, link_type } => {
                write!(f, "Copy file {} -> {} ({})", source_path, path, link_type)
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Layer {
    pub parent_hash: Option<String>,
    pub hash: String,
    pub operations: Vec<LayerOperation>,
    pub created: SystemTime,
}

impl Layer {
    pub fn new(parent_hash: Option<String>, hash: String, operations: Vec<LayerOperation>) -> Layer {
        Layer {
            parent_hash,
            hash,
            operations,
            created: SystemTime::now()
        }
    }

    pub fn get_file_operation(&self, index: usize) -> Option<&LayerOperation> {
        let mut current_index = 0;
        for operation in &self.operations {
            if let LayerOperation::File { .. } = operation {
                if current_index == index {
                    return Some(operation);
                }

                current_index += 1;
            }
        }

        None
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