use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use log::{debug, info};

use tokio::sync::RwLock;

use async_trait::async_trait;

use axum::body::{Body, Bytes};
use axum::response::{IntoResponse, Redirect, Response};

use aws_config::Region;
use aws_credential_types::Credentials;
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_sdk_s3::presigning::PresigningConfig;
use aws_sdk_s3::primitives::ByteStream;

use crate::image::{Layer, LayerOperation};
use crate::image_manager::{RegistryStorage, RegistryStorageError, RegistryStorageResult};
use crate::reference::{ImageId};
use crate::registry::config::{InMemoryStorageConfig, S3StorageConfig};

pub type ArcExternalStorage = Arc<dyn ExternalStorage + Send + Sync>;

#[async_trait]
pub trait ExternalStorage: RegistryStorage {
    async fn download(&self, path: &str) -> ExternalStorageResult<Response>;
    async fn upload(&self, data_path: &Path, path: &str) -> ExternalStorageResult<()>;
    async fn exists(&self, path: &str) -> ExternalStorageResult<bool>;
    async fn remove_layer(&self, hash: &ImageId) -> ExternalStorageResult<usize>;
}

pub type ExternalStorageResult<T> = Result<T, ExternalStorageError>;

#[derive(Debug)]
pub enum ExternalStorageError {
    LayerFileNotFound,
    FailedToUploadLayerFile(String),
    IO(std::io::Error)
}

impl Display for ExternalStorageError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ExternalStorageError::LayerFileNotFound => write!(f, "Layer file not found"),
            ExternalStorageError::FailedToUploadLayerFile(err) => write!(f, "Failed to upload layer file due to: {}", err),
            ExternalStorageError::IO(err) => write!(f, "I/O: {}", err)
        }
    }
}

#[async_trait]
impl<T: ExternalStorage + Send + Sync> RegistryStorage for T {
    async fn commit_downloaded_file(&self, data_path: &Path, path: &str) -> RegistryStorageResult<()> {
        self.upload(data_path, path).await.map_err(|err| RegistryStorageError::IO(format!("{}", err)))
    }

    async fn remove_layer(&self, hash: &ImageId) -> RegistryStorageResult<usize> {
        ExternalStorage::remove_layer(self, hash).await.map_err(|err| RegistryStorageError::IO(format!("{}", err)))
    }
}

pub async fn verify_path_exists(external_storage: &ArcExternalStorage, layer: &Layer) -> ExternalStorageResult<bool> {
    for operation in &layer.operations {
        match operation {
            LayerOperation::Image { .. } => {}
            LayerOperation::ImageAlias { .. } => {}
            LayerOperation::Directory { .. } => {}
            LayerOperation::File { source_path, .. } | LayerOperation::CompressedFile { source_path, .. } => {
                if !external_storage.exists(source_path).await? {
                    return Ok(false);
                }
            }
            LayerOperation::Label { .. } => {}
        }
    }

    Ok(true)
}

pub struct S3Storage {
    client: aws_sdk_s3::Client,
    bucket: String
}

impl S3Storage {
    pub fn new(config: &S3StorageConfig) -> S3Storage {
        let credentials = Credentials::from_keys(config.access_key_id.clone(), config.secret_access_key.clone(), None);

        let mut aws_config = aws_config::SdkConfig::builder()
            .credentials_provider(SharedCredentialsProvider::new(credentials))
            .region(Region::new(config.region.clone()));

        if let Some(endpoint_url) = config.endpoint_url.as_ref() {
            aws_config = aws_config.endpoint_url(endpoint_url.clone());
        }

        info!("Using S3 storage mode - bucket: {}.", config.bucket);

        S3Storage {
            client: aws_sdk_s3::Client::new(&aws_config.build()),
            bucket: config.bucket.clone()
        }
    }
}

#[async_trait]
impl ExternalStorage for S3Storage {
    async fn download(&self, path: &str) -> ExternalStorageResult<Response> {
        let object = self.client
            .get_object()
            .bucket(self.bucket.clone())
            .key(path)
            .presigned(PresigningConfig::builder().expires_in(Duration::from_secs(3600)).build().unwrap()).await
            .map_err(|_| ExternalStorageError::LayerFileNotFound)?;
        debug!("Download URL: {}", object.uri());

        Ok(Redirect::permanent(object.uri()).into_response())
    }

    async fn upload(&self, data_path: &Path, path: &str) -> ExternalStorageResult<()> {
        debug!("Uploading {} to bucket {}", path, self.bucket);
        self.client.put_object()
            .bucket(self.bucket.clone())
            .key(path.to_owned())
            .body(ByteStream::from_path(data_path).await.unwrap())
            .send().await
            .map_err(|err| ExternalStorageError::FailedToUploadLayerFile(err.to_string()))?;

        Ok(())
    }

    async fn exists(&self, path: &str) -> ExternalStorageResult<bool> {
        let exists = self.client.get_object()
            .bucket(self.bucket.to_owned())
            .key(path.to_owned())
            .send().await
            .is_ok();

        Ok(exists)
    }

    async fn remove_layer(&self, hash: &ImageId) -> ExternalStorageResult<usize> {
        let objects = self.client.list_objects()
            .bucket(self.bucket.clone())
            .prefix(format!("layers/{}", hash))
            .send().await
            .map_err(|_| ExternalStorageError::LayerFileNotFound)?;

        let delete_requests = objects.contents()
            .iter()
            .flat_map(|object| object.key.as_ref())
            .map(|key| self.client
                .delete_object()
                .bucket(self.bucket.clone())
                .key(key)
                .send()
            );

        let mut num_deleted = 0;
        for result in futures::future::join_all(delete_requests).await {
            result.map_err(|_| ExternalStorageError::LayerFileNotFound)?;
            num_deleted += 1;
        }

        Ok(num_deleted)
    }
}

pub struct InMemoryStorage {
    files: RwLock<HashMap<String, Arc<[u8]>>>
}

impl InMemoryStorage {
    pub fn new(_config: &InMemoryStorageConfig) -> InMemoryStorage {
        InMemoryStorage {
            files: RwLock::new(HashMap::new())
        }
    }
}

#[async_trait]
impl ExternalStorage for InMemoryStorage {
    async fn download(&self, path: &str) -> ExternalStorageResult<Response> {
        let files = self.files.read().await;
        let file = files.get(path).ok_or_else(|| ExternalStorageError::LayerFileNotFound)?;

        let body = Body::from(Body::from(Bytes::from_owner(file.clone())));

        Ok(
            Response::builder()
                .header("Content-Type", "application/octet-stream")
                .header(
                    "Content-Disposition",
                    format!("attachment; filename={}", "file")
                )
                .body(body)
                .unwrap()
        )
    }

    async fn upload(&self, data_path: &Path, path: &str) -> ExternalStorageResult<()> {
        let buffer = std::fs::read(data_path).map_err(|err| ExternalStorageError::IO(err))?;
        self.files.write().await.insert(path.to_owned(), Arc::from(buffer));
        Ok(())
    }

    async fn exists(&self, path: &str) -> ExternalStorageResult<bool> {
        Ok(self.files.read().await.contains_key(path))
    }

    async fn remove_layer(&self, hash: &ImageId) -> ExternalStorageResult<usize> {
        let mut files = self.files.write().await;
        let mut num_deleted = 0;

        files.retain(|key, _| {
            if !key.starts_with(&format!("layers/{}", hash)) {
                true
            } else {
                num_deleted += 1;
                false
            }
        });

        Ok(num_deleted)
    }
}