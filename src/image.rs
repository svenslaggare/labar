use std::fmt::{Display, Formatter};
use std::path::Path;
use std::time::SystemTime;
use chrono::{DateTime, Local};
use serde::{Deserialize, Serialize};

use crate::helpers::DataSize;
use crate::image_manager::ImageManagerError;
use crate::reference::{ImageId, ImageTag};

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
    Image { hash: ImageId },
    File { path: String, source_path: String, link_type: LinkType, writable: bool },
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
            LayerOperation::File { path, source_path, link_type, .. } => {
                write!(f, "Copy file {} -> {} ({})", source_path, path, link_type)
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Layer {
    pub parent_hash: Option<ImageId>,
    pub hash: ImageId,
    pub operations: Vec<LayerOperation>,
    pub created: SystemTime,
}

impl Layer {
    pub fn new(parent_hash: Option<ImageId>, hash: ImageId, operations: Vec<LayerOperation>) -> Layer {
        Layer {
            parent_hash,
            hash,
            operations,
            created: SystemTime::now()
        }
    }

    pub fn from_file(path: &Path) -> Result<Layer, String> {
        let layer_content = std::fs::read_to_string(path)
            .map_err(|err| format!("{}", err))?;

        let layer: Layer = serde_json::from_str(&layer_content)
            .map_err(|err| format!("{}", err))?;

        Ok(layer)
    }

    pub fn save_to_file(&self, base_path: &Path) -> Result<(), ImageManagerError> {
        std::fs::write(
            base_path.join("manifest.json"),
            serde_json::to_string_pretty(&self)
                .map_err(|err|
                    ImageManagerError::OtherError {
                        message: format!("Failed to write manifest due to: {}", err)
                    }
                )?
        )?;

        Ok(())
    }

    pub async fn save_to_file_async(&self, base_path: &Path) -> Result<(), ImageManagerError> {
        tokio::fs::write(
            base_path.join("manifest.json"),
            serde_json::to_string_pretty(&self)
                .map_err(|err|
                    ImageManagerError::OtherError {
                        message: format!("Failed to write manifest due to: {}", err)
                    }
                )?
        ).await?;

        Ok(())
    }

    pub fn created_datetime(&self) -> DateTime<Local> {
        self.created.into()
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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Image {
    pub hash: ImageId,
    pub tag: ImageTag
}

impl Image {
    pub fn new(hash: ImageId, tag: ImageTag) -> Image {
        Image {
            hash,
            tag
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ImageMetadata {
    pub image: Image,
    pub created: SystemTime,
    pub size: DataSize
}