use std::fmt::{Display, Formatter};
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;

use crate::image_manager::ImageId;

pub type ArcImageStorage = Arc<dyn ImageStorage + Send + Sync>;

#[async_trait]
pub trait ImageStorage {
    async fn commit_downloaded_file(&self, data_path: &Path, path: &str) -> ImageStorageResult<()>;
    async fn remove_layer(&self, hash: &ImageId) -> ImageStorageResult<usize>;
}

pub type ImageStorageResult<T> = Result<T, ImageStorageError>;

#[derive(Debug)]
pub enum ImageStorageError {
    IO(String)
}

impl Display for ImageStorageError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ImageStorageError::IO(err) => write!(f, "I/O: {}", err)
        }
    }
}