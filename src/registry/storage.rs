use std::fmt::{Display, Formatter};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;

use tokio_util::io::ReaderStream;

use axum::body::Body;
use axum::response::Response;

use crate::image::{Layer, LayerOperation};

#[async_trait]
pub trait RegistryStorage {
    async fn download(&self, path: &str) -> RegistryStorageResult<Response>;
    async fn upload(&self, data_path: &Path, path: &str) -> RegistryStorageResult<bool>;
    async fn exists(&self, path: &str) -> RegistryStorageResult<bool>;
}

pub type ArcRegistryStorage = Arc<dyn RegistryStorage + Send + Sync>;

pub type RegistryStorageResult<T> = Result<T, RegistryStorageError>;

#[derive(Debug)]
pub enum RegistryStorageError {
    LayerFileNotFound,
    FailedToUploadLayerFile(String),
    IO(std::io::Error)
}

impl From<std::io::Error> for RegistryStorageError {
    fn from(error: std::io::Error) -> Self {
        RegistryStorageError::IO(error)
    }
}

impl Display for RegistryStorageError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RegistryStorageError::LayerFileNotFound => write!(f, "Layer file not found"),
            RegistryStorageError::FailedToUploadLayerFile(err) => write!(f, "Failed to upload layer file due to: {}", err),
            RegistryStorageError::IO(err) => write!(f, "I/O: {}", err)
        }
    }
}

pub async fn verify_path_exists(storage: &ArcRegistryStorage, layer: &Layer) -> RegistryStorageResult<bool> {
    for operation in &layer.operations {
        match operation {
            LayerOperation::Image { .. } => {}
            LayerOperation::ImageAlias { .. } => {}
            LayerOperation::Directory { .. } => {}
            LayerOperation::File { source_path, .. } | LayerOperation::CompressedFile { source_path, .. } => {
                if !storage.exists(source_path).await? {
                    return Ok(false);
                }
            }
            LayerOperation::Label { .. } => {}
        }
    }

    Ok(true)
}

pub struct InternalRegistryStorage {
    base_folder: PathBuf
}

impl InternalRegistryStorage {
    pub fn new(base_folder: &Path) -> InternalRegistryStorage {
        InternalRegistryStorage {
            base_folder: base_folder.to_path_buf()
        }
    }
}

#[async_trait]
impl RegistryStorage for InternalRegistryStorage {
    async fn download(&self, path: &str) -> RegistryStorageResult<Response> {
        let source_path = Path::new(path);
        let abs_source_path = self.base_folder.join(source_path);

        let file = tokio::fs::File::open(&abs_source_path).await?;
        let stream = ReaderStream::new(file);
        let body = Body::from_stream(stream);

        Ok(
            Response::builder()
                .header("Content-Type", "application/octet-stream")
                .header(
                    "Content-Disposition",
                    format!("attachment; filename={}", abs_source_path.components().last().unwrap().as_os_str().display())
                )
                .body(body)
                .unwrap()
        )
    }

    async fn upload(&self, data_path: &Path, path: &str) -> RegistryStorageResult<bool> {
        let abs_source_path = self.base_folder.join(path);

        if let Some(parent) = abs_source_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        tokio::fs::rename(&data_path, abs_source_path).await?;
        Ok(true)
    }

    async fn exists(&self, path: &str) -> RegistryStorageResult<bool> {
        Ok(self.base_folder.join(path).exists())
    }
}
