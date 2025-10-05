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
    Directory { path: String },
    File {
        path: String,
        source_path: String,
        original_source_path: String,
        content_hash: String,
        link_type: LinkType,
        writable: bool
    },
    CompressedFile {
        path: String,
        source_path: String,
        original_source_path: String,
        content_hash: String,
        link_type: LinkType,
        writable: bool,
        compressed_content_hash: String
    },
}

impl LayerOperation {
    pub fn compressed_content_hash(&self) -> Option<&str> {
        if let LayerOperation::CompressedFile { compressed_content_hash, .. } = self {
            Some(compressed_content_hash)
        } else {
            None
        }
    }
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
            LayerOperation::File { path, source_path, .. } => {
                write!(f, "File {} -> {}", source_path, path)
            }
            LayerOperation::CompressedFile { path, source_path, .. } => {
                write!(f, "File (compressed) {} -> {}", source_path, path)
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Layer {
    pub parent_hash: Option<ImageId>,
    pub hash: ImageId,
    pub operations: Vec<LayerOperation>,
    pub storage_size: DataSize,
    pub created: DateTime<Local>,
}

impl Layer {
    pub fn new(parent_hash: Option<ImageId>,
               hash: ImageId,
               operations: Vec<LayerOperation>,
               storage_size: DataSize) -> Layer {
        Layer {
            parent_hash,
            hash,
            operations,
            storage_size,
            created: Local::now()
        }
    }

    pub fn get_file_operation(&self, index: usize) -> Option<&LayerOperation> {
        let mut current_index = 0;
        for operation in &self.operations {
            match operation {
                LayerOperation::Image { .. } => {}
                LayerOperation::Directory { .. } => {}
                LayerOperation::File { .. } | LayerOperation::CompressedFile { .. } => {
                    if current_index == index {
                        return Some(operation);
                    }

                    current_index += 1;
                }
            }
        }

        None
    }

    pub fn storage_modes(&self) -> (usize, usize) {
        let mut uncompressed = 0;
        let mut compressed = 0;
        for operation in &self.operations {
            match operation {
                LayerOperation::Image { .. } => {}
                LayerOperation::Directory { .. } => {}
                LayerOperation::File { .. } => {
                    uncompressed += 1;
                }
                LayerOperation::CompressedFile { .. } => {
                    compressed += 1;
                }
            }
        }

        (uncompressed, compressed)
    }

    pub fn verify_path_exists(&self, base_folder: &Path) -> bool {
        for operation in &self.operations {
            match operation {
                LayerOperation::Image { .. } => {}
                LayerOperation::Directory { .. } => {}
                LayerOperation::File { source_path, .. } | LayerOperation::CompressedFile { source_path, .. } => {
                    if !base_folder.join(source_path).exists() {
                        return false;
                    }
                }
            }
        }

        true
    }

    pub fn verify_valid_paths(&self, base_folder: &Path) -> bool {
        fn inner(base_folder: &Path, layer: &Layer) -> std::io::Result<bool> {
            let base_folder = base_folder.canonicalize()?;

            for operation in &layer.operations {
                match operation {
                    LayerOperation::Image { .. } => {}
                    LayerOperation::Directory { .. } => {}
                    LayerOperation::File { source_path, .. } | LayerOperation::CompressedFile { source_path, .. } => {
                        let abs_source_path = base_folder.join(source_path);
                        if abs_source_path != clean_path(&abs_source_path) {
                            return Ok(false);
                        }
                    }
                }
            }

            Ok(true)
        }

        inner(base_folder, self).unwrap_or(false)
    }

    pub fn visit_image_ids<F: FnMut(&ImageId)>(&self, mut f: F) {
        if let Some(parent_hash) = self.parent_hash.as_ref() {
            f(parent_hash);
        }

        for operation in &self.operations {
            match operation {
                LayerOperation::Image { hash } => {
                    f(hash);
                },
                _ => {}
            }
        }
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

    pub fn replace_tag(mut self, tag: ImageTag) -> Image {
        self.tag = tag;
        self
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ImageMetadata {
    pub image: Image,
    pub created: DateTime<Local>,
    pub size: DataSize
}