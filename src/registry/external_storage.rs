use std::path::Path;
use std::time::Duration;

use async_trait::async_trait;
use aws_config::Region;
use aws_credential_types::Credentials;
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_sdk_s3::presigning::PresigningConfig;
use aws_sdk_s3::primitives::ByteStream;

use axum::response::{IntoResponse, Redirect, Response};

use log::{debug, info};

use crate::reference::{ImageId};
use crate::registry::config::S3StorageConfig;
use crate::registry::model::{AppError, AppResult};

pub type BoxExternalStorage = Box<dyn ExternalStorage + Send + Sync>;

#[async_trait]
pub trait ExternalStorage {
    async fn download(&self, path: &str) -> AppResult<Response>;
    async fn upload(&self, path: &str, data_path: &Path) -> AppResult<()>;
    async fn exists(&self, path: &str) -> AppResult<bool>;
    async fn remove_layer(&self, hash: &ImageId) -> AppResult<()>;
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
    async fn download(&self, path: &str) -> AppResult<Response> {
        let object = self.client
            .get_object()
            .bucket(self.bucket.clone())
            .key(path)
            .presigned(PresigningConfig::builder().expires_in(Duration::from_secs(3600)).build().unwrap()).await
            .map_err(|_| AppError::LayerFileNotFound)?;
        debug!("Download URL: {}", object.uri());

        Ok(Redirect::permanent(object.uri()).into_response())
    }

    async fn upload(&self, path: &str, data_path: &Path) -> AppResult<()> {
        debug!("Uploading {} to bucket {}", path, self.bucket);
        self.client.put_object()
            .bucket(self.bucket.clone())
            .key(path.to_owned())
            .body(ByteStream::from_path(data_path).await.unwrap())
            .send().await
            .map_err(|err| AppError::FailedToUploadLayerFile(err.to_string()))?;

        Ok(())
    }

    async fn exists(&self, path: &str) -> AppResult<bool> {
        let exists = self.client.get_object()
            .bucket(self.bucket.to_owned())
            .key(path.to_owned())
            .send().await
            .is_ok();

        Ok(exists)
    }

    async fn remove_layer(&self, hash: &ImageId) -> AppResult<()> {
        let objects = self.client.list_objects()
            .bucket(self.bucket.clone())
            .prefix(format!("layers/{}", hash))
            .send().await
            .map_err(|_| AppError::LayerFileNotFound)?;

        let delete_requests = objects.contents()
            .iter()
            .flat_map(|object| object.key.as_ref())
            .map(|key| self.client
                .delete_object()
                .bucket(self.bucket.clone())
                .key(key)
                .send()
            );

        for result in futures::future::join_all(delete_requests).await {
            result.map_err(|_| AppError::LayerFileNotFound)?;
        }

        Ok(())
    }
}