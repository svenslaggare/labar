use std::fmt::{Display, Formatter};
use std::path::Path;

use chrono::{DateTime, Local};
use serde::{Deserialize, Serialize};

use rusqlite::Row;
use rusqlite::types::{FromSql, FromSqlError, FromSqlResult, ValueRef};

use crate::helpers::{clean_path, DataSize};
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
    File { path: String, source_path: String, content_hash: String, link_type: LinkType, writable: bool },
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
    pub created: DateTime<Local>,
}

impl Layer {
    pub fn new(parent_hash: Option<ImageId>, hash: ImageId, operations: Vec<LayerOperation>) -> Layer {
        Layer {
            parent_hash,
            hash,
            operations,
            created: Local::now()
        }
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

    pub fn verify_paths_exists(&self, base_folder: &Path) -> bool {
        for operation in &self.operations {
            match operation {
                LayerOperation::File { source_path, .. } => {
                    if !base_folder.join(source_path).exists() {
                        return false;
                    }
                }
                _ => {}
            }
        }

        true
    }

    pub fn verify_valid_paths(&self, base_folder: &Path) -> bool {
        fn inner(base_folder: &Path, layer: &Layer) -> std::io::Result<bool> {
            let base_folder = base_folder.canonicalize()?;

            for operation in &layer.operations {
                match operation {
                    LayerOperation::File { source_path, .. } => {
                        let abs_source_path = base_folder.join(source_path);
                        if abs_source_path != clean_path(&abs_source_path) {
                            return Ok(false);
                        }
                    }
                    _ => {}
                }
            }

            Ok(true)
        }

        inner(base_folder, self).unwrap_or(false)
    }
}

impl FromSql for Layer {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        let value = serde_json::Value::column_result(value)?;
        serde_json::from_value::<Layer>(value).map_err(|_| FromSqlError::InvalidType)
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

    pub fn from_row(row: &Row) -> rusqlite::Result<Image> {
        Ok(Image::new(row.get(0)?, row.get(1)?))
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ImageMetadata {
    pub image: Image,
    pub created: DateTime<Local>,
    pub size: DataSize
}