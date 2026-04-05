use std::fmt::{Display, Formatter};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::time::{Duration};

use futures::{future, StreamExt};
use tokio::io::AsyncWriteExt;

use reqwest::{Body, Client, Request, Response, StatusCode};
use reqwest::header::{HeaderMap, HeaderValue};

use rand::distr::{Alphanumeric, SampleString};

use crate::content::{ContentHash};
use crate::helpers::{clean_path, DeferredFileDelete};
use crate::image::{ImageMetadata, Layer, LayerOperation};
use crate::image_manager::{PrinterRef, ImageManagerConfig};
use crate::image_manager::build::LayerHash;
use crate::image_manager::compression::CompressionManager;
use crate::image_manager::state::{StateSession};
use crate::reference::{ImageId, ImageTag};
use crate::registry::model;
use crate::registry::model::{AppErrorResponse, AuthenticationFailureResponse, ImageSpec, LayerExists, UploadLayerResponse, UploadStatus};

pub type RegistryResult<T> = Result<T, RegistryError>;

pub struct RegistryManager {
    config: ImageManagerConfig,
    printer: PrinterRef
}

impl RegistryManager {
    pub fn new(config: ImageManagerConfig, printer: PrinterRef) -> RegistryManager {
        RegistryManager {
            config: config.clone(),
            printer
        }
    }

    #[allow(dead_code)]
    pub async fn is_reachable(&self, registry: &str) -> RegistryResult<bool> {
        let http_client = create_http_client(&self.config)?;

        let full_url = format!("https://{}/", registry);

        match http_client.execute(http_client.get(full_url).build()?).await {
            Ok(response) => Ok(response.status().is_success()),
            _ => Ok(false)
        }
    }

    pub async fn sign_in(&self, registry: &str, username: &str, password: &str) -> RegistryResult<String> {
        let http_client = create_http_client(&self.config)?;

        let request = http_client
            .get(format!("https://{}/sign-in", registry))
            .basic_auth(username, Some(password))
            .build()?;
        let response = http_client.execute(request).await?;
        let token = RegistryError::from_response(response, "url: /sign-in".to_owned()).await?;

        Ok(token)
    }

    pub async fn list_images(&self, registry: &RegistrySession) -> RegistryResult<Vec<ImageMetadata>> {
        let client = RegistryClient::new(&self.config, &registry)?;

        let (response, _) = client.get_registry_text_response("images").await?;
        let images: Vec<ImageMetadata> = serde_json::from_str(&response)?;
        Ok(images)
    }

    pub async fn resolve_image(&self, registry: &RegistrySession, tag: &ImageTag, can_pull_through: bool) -> RegistryResult<ImageMetadata> {
        let client = RegistryClient::new(&self.config, registry)?;

        let url = if can_pull_through {
            format!("images/{}?can_pull_through=true", tag)
        } else {
            format!("images/{}", tag)
        };

        let (response, _) = client.get_registry_text_response(&url).await?;
        let image: ImageMetadata = serde_json::from_str(&response)?;
        Ok(image)
    }

    pub async fn get_layer_definition(&self, registry: &RegistrySession, hash: &ImageId) -> RegistryResult<Layer> {
        let client = RegistryClient::new(&self.config, registry)?;
        let (layer, _) = Self::get_layer_definition_internal(&client, &hash, false).await?;
        Ok(layer)
    }

    pub async fn download_layer(&self,
                                compression_manager: &CompressionManager,
                                registry: &RegistrySession,
                                hash: &ImageId,
                                verbose_output: bool) -> RegistryResult<Layer> {
        let client = RegistryClient::new(&self.config, registry)?;

        let (layer, pull_through) = Self::get_layer_definition_internal(&client, &hash, true).await?;

        let computed_hash = LayerHash::from_layer(&layer);
        if &computed_hash != hash {
            return Err(RegistryError::IncorrectLayer { expected: hash.clone(), actual: computed_hash });
        }

        if pull_through {
            self.wait_for_pull_through(&client, hash).await?;
        }

        let download_folder = self.config.base_folder.join("tmp-download");
        tokio::fs::create_dir_all(&download_folder).await?;

        let mut file_index = 0;
        let mut file_operations = Vec::new();
        for operation in &layer.operations {
            match operation {
                LayerOperation::File { source_path, content_hash, .. } => {
                    file_operations.push((operation, source_path.clone(), content_hash.clone(), file_index));
                    file_index += 1;
                }
                LayerOperation::CompressedFile { source_path, compressed_content_hash, .. } => {
                    file_operations.push((operation, source_path.clone(), compressed_content_hash.clone(), file_index));
                    file_index += 1;
                }
                LayerOperation::Image { .. } => {}
                LayerOperation::ImageAlias { .. } => {}
                LayerOperation::Directory { .. } => {}
                LayerOperation::Label { .. } => {}
            }
        }

        let num_parallel = {
            if file_operations.len() > 0 {
                let bytes_per_operation = layer.storage_size.0 / file_operations.len();
                let target_throughput = 1 * 1024 * 1024;
                (target_throughput / bytes_per_operation).clamp(4, 32)
            } else {
                1
            }
        };

        let mut done_operations = Vec::new();
        for chunk in file_operations.chunks(num_parallel) {
            let mut download_operations = Vec::new();
            for (operation, source_path, content_hash, file_index) in chunk {
                let local_source_path = self.config.base_folder.canonicalize()?.join(Path::new(source_path));
                if local_source_path != clean_path(&local_source_path) {
                    return Err(RegistryError::InvalidLayer);
                }

                if !local_source_path.exists() {
                    if verbose_output {
                        self.printer.refresh_latest_line(&format!("\t\t* Downloading file '{}'...", source_path));
                    }

                    download_operations.push(self.download_file(
                        compression_manager,
                        &download_folder,
                        &client,
                        hash,
                        operation,
                        local_source_path,
                        *file_index,
                        content_hash
                    ));
                } else {
                    if verbose_output {
                        self.printer.refresh_latest_line(&format!("\t\t* Skipping downloading file {}.", source_path));
                    }
                }
            }

            for result in future::join_all(download_operations).await {
                done_operations.push(result?);
            }
        }

        let mut layer = layer;
        for result in done_operations {
            if let Some((file_index, new_operation)) = result {
                if let Some(operation) = layer.get_file_operation_mut(file_index) {
                    *operation = new_operation;
                }
            }
        }

        Ok(layer)
    }

    async fn download_file(&self,
                           compression_manager: &CompressionManager,
                           download_folder: &Path,
                           client: &RegistryClient<'_>,
                           hash: &ImageId,
                           operation: &LayerOperation,
                           local_source_path: PathBuf,
                           file_index: usize,
                           content_hash: &str) -> RegistryResult<Option<(usize, LayerOperation)>> {
        let download_path = download_folder.join(Alphanumeric.sample_string(&mut rand::rng(), 64));

        let mut download_delete = DeferredFileDelete::new(download_path.clone());

        let response = client.get_registry_response(&format!("layers/{}/download/{}", hash, file_index)).await?;
        let mut content_hasher = ContentHash::new();

        {
            let mut byte_stream = response.bytes_stream();
            let mut file = tokio::fs::File::create(&download_path).await?;
            while let Some(item) = byte_stream.next().await {
                let data = item?;
                let data = data.as_ref();
                content_hasher.add(data);
                file.write_all(data).await?;
            }
            file.flush().await?;
        }

        if &content_hasher.finalize() != &content_hash {
            return Err(RegistryError::InvalidContentHash);
        }

        let result = compression_manager.handle_compression(
            &self.config.storage_mode,
            operation,
            &download_path
        ).await.map_err(|_| RegistryError::FailedToUnpack)?;

        self.commit_downloaded_file(
            &download_path,
            result.as_ref().map(|(compressed_path, _)| compressed_path.as_path()),
            &local_source_path,
            &mut download_delete
        ).await?;

        Ok(result.map(|(_, operation)| (file_index, operation)))
    }

    async fn commit_downloaded_file(&self,
                                    download_path: &Path,
                                    compressed_path: Option<&Path>,
                                    local_source_path: &Path,
                                    download_delete: &mut DeferredFileDelete) -> RegistryResult<()> {
        if let Some(layer_folder) = local_source_path.parent() {
            tokio::fs::create_dir_all(&layer_folder).await?;
        }

        if let Some(compressed_path) = compressed_path {
            tokio::fs::rename(&compressed_path, &local_source_path).await?;
        } else {
            tokio::fs::rename(&download_path, &local_source_path).await?;
            download_delete.skip();
        }

        Ok(())
    }

    async fn get_layer_definition_internal(client: &RegistryClient<'_>,
                                           hash: &ImageId,
                                           can_pull_through: bool) -> Result<(Layer, bool), RegistryError> {
        let url = if can_pull_through {
            format!("layers/{}/manifest?can_pull_through=true", hash)
        } else {
            format!("layers/{}/manifest", hash)
        };

        let (response, headers) = client.get_registry_text_response(&url).await?;
        let layer: Layer = serde_json::from_str(&response)?;

        if &layer.hash != hash {
            return Err(RegistryError::IncorrectLayer { expected: hash.clone(), actual: layer.hash.clone() })
        }

        let pull_through = headers.get(model::PULL_THROUGH_HEADER) == Some(&HeaderValue::from_static("true"));

        Ok((layer, pull_through))
    }

    async fn wait_for_pull_through(&self,
                                   client: &RegistryClient<'_>,
                                   hash: &ImageId) -> Result<(), RegistryError> {
        self.printer.println("\t\t* Waiting for upstream to pull...");
        let t0 = std::time::Instant::now();
        while t0.elapsed().as_secs_f64() < self.config.max_wait_for_upstream_pull {
            match Self::get_layer_exists(&client, &hash).await {
                Ok(exists) => {
                    if exists {
                        return Ok(());
                    } else {
                        tokio::time::sleep(Duration::from_secs_f64(self.config.upstream_pull_check)).await;
                    }
                }
                Err(err) => {
                    return Err(err);
                }
            }
        }

        Err(RegistryError::FailToPullThrough)
    }

    async fn get_layer_exists(client: &RegistryClient<'_>, hash: &ImageId) -> Result<bool, RegistryError> {
        let (response, _) = client.get_registry_text_response(&format!("layers/{}/exists", hash)).await?;
        let layer_exists: LayerExists = serde_json::from_str(&response)?;

        Ok(layer_exists.exists)
    }

    pub async fn upload_layer(&self, registry: &RegistrySession, layer: &Layer) -> RegistryResult<bool> {
        let client = RegistryClient::new(&self.config, registry)?;

        // Begin upload
        let mut request = client.json_post_registry_response("layers/begin-upload").build()?;
        *request.body_mut() = Some(Body::from(serde_json::to_string(&layer)?));
        let response = client.execute(request).await?;

        let upload_response = RegistryError::from_response(response, "begin-upload".to_owned()).await?;
        let upload_response: UploadLayerResponse = serde_json::from_str(&upload_response)?;
        let upload_id = upload_response.upload_id.unwrap_or(String::new()).clone();

        match upload_response.status {
            UploadStatus::Started => {}
            UploadStatus::InvalidPaths => {
                return Err(
                    RegistryError::FailedToUpload {
                        layer: layer.hash.clone(),
                        reason: upload_response.status
                    }
                );
            }
            _ => {
                self.printer.println("\t\t* Layer already exist.");
                return Ok(false);
            }
        }

        // Upload every file
        let mut file_index = 0;
        for operation in &layer.operations {
            match operation {
                LayerOperation::File { source_path, .. } | LayerOperation::CompressedFile { source_path, .. } => {
                    let mut request = client.build_post_request(
                        &format!("layers/{}/upload/{}", layer.hash, file_index)
                    )
                        .header(model::UPLOAD_ID_HEADER, upload_id.clone())
                        .build()?;

                    let file = tokio::fs::File::open(self.config.base_folder.join(source_path)).await?;
                    let body = Body::from(file);
                    *request.body_mut() = Some(body);

                    let response = client.execute(request).await?;
                    RegistryError::from_response(response, format!("file {}", file_index)).await?;

                    file_index += 1;
                }
                LayerOperation::Image { .. } => {}
                LayerOperation::ImageAlias { .. } => {}
                LayerOperation::Directory { .. } => {}
                LayerOperation::Label { .. } => {}
            }
        }

        // Commit upload
        let request = client.json_post_registry_response("layers/end-upload")
            .header(model::UPLOAD_ID_HEADER, upload_id.clone())
            .build()?;
        let response = client.execute(request).await?;

        let upload_response = RegistryError::from_response(response, "begin-upload".to_owned()).await?;
        let upload_response: UploadLayerResponse = serde_json::from_str(&upload_response)?;

        if UploadStatus::Finished != upload_response.status {
            return Err(
                RegistryError::FailedToUpload {
                    layer: layer.hash.clone(),
                    reason: upload_response.status
                }
            );
        }

        Ok(true)
    }

    pub async fn upload_image(&self, registry: &RegistrySession, hash: &ImageId, tag: &ImageTag) -> RegistryResult<()> {
        let client = RegistryClient::new(&self.config, registry)?;

        let mut request = client.json_post_registry_response("images").build()?;

        *request.body_mut() = Some(Body::from(serde_json::to_string(
            &ImageSpec {
                hash: hash.clone(),
                tag: tag.clone()
            }
        )?));

        let response = client.execute(request).await?;
        RegistryError::from_response(response, "image".to_owned()).await?;

        Ok(())
    }

    pub async fn remove_image(&self, registry: &RegistrySession, tag: &ImageTag) -> RegistryResult<()> {
        let client = RegistryClient::new(&self.config, registry)?;
        let response = client.delete_registry_response(&format!("images/{}", tag)).await?;
        RegistryError::from_response(response, "remove".to_owned()).await?;
        Ok(())
    }
}

pub struct RegistrySession {
    registry: String,
    auth_token: String
}

impl RegistrySession {
    pub fn new(session: &StateSession, registry: &str) -> RegistryResult<RegistrySession> {
        let auth_token = session.get_login(registry)
            .ok().flatten()
            .ok_or_else(|| RegistryError::InvalidAuthentication(None))?;

        Ok(
            RegistrySession {
                registry: registry.to_owned(),
                auth_token
            }
        )
    }
}

struct RegistryClient<'a> {
    http_client: Client,
    session: &'a RegistrySession
}

impl<'a> RegistryClient<'a> {
    pub fn new(config: &ImageManagerConfig, registry: &'a RegistrySession) -> RegistryResult<RegistryClient<'a>> {
        Ok(
            RegistryClient {
                http_client: create_http_client(config)?,
                session: registry
            }
        )
    }

    pub fn execute(&self, request: Request) -> impl Future<Output = Result<Response, reqwest::Error>> {
        self.http_client.execute(request)
    }

    pub async fn get_registry_text_response(&self, url: &str) -> RegistryResult<(String, HeaderMap)> {
        let response = self.get_registry_response(url).await?;
        let headers = response.headers().clone();
        let response = RegistryError::from_response(response, format!("url: /{}", url)).await?;
        Ok((response, headers))
    }

    pub fn json_post_registry_response(&self, url: &str) -> reqwest::RequestBuilder {
        let request = self.build_post_request(url);
        request.header(reqwest::header::CONTENT_TYPE, "application/json")
    }

    pub async fn get_registry_response(&self, url: &str) -> RegistryResult<Response> {
        let request = self.build_get_request(url);
        let response = self.http_client.execute(request.build()?).await.map_err(|err| RegistryError::Unavailable(err))?;
        Ok(response)
    }

    pub async fn delete_registry_response(&self, url: &str) -> RegistryResult<Response> {
        let request = self.build_delete_request(url);
        let response = self.http_client.execute(request.build()?).await.map_err(|err| RegistryError::Unavailable(err))?;
        Ok(response)
    }

    pub fn build_get_request(&self, url: &str) -> reqwest::RequestBuilder {
        let full_url = format!("https://{}/{}", self.session.registry, url);
        self.http_client
            .get(full_url)
            .bearer_auth(&self.session.auth_token)
    }

    pub fn build_post_request(&self, url: &str) -> reqwest::RequestBuilder {
        let full_url = format!("https://{}/{}", self.session.registry, url);
        self.http_client
            .post(full_url)
            .bearer_auth(&self.session.auth_token)
    }

    pub fn build_delete_request(&self, url: &str) -> reqwest::RequestBuilder {
        let full_url = format!("https://{}/{}", self.session.registry, url);
        self.http_client
            .delete(full_url)
            .bearer_auth(&self.session.auth_token)
    }
}

#[derive(Debug)]
pub enum RegistryError {
    Unavailable(reqwest::Error),
    InvalidAuthentication(Option<String>),
    ReferenceNotFound,
    FailedToUpload { layer: ImageId, reason: UploadStatus },
    InvalidLayer,
    InvalidContentHash,
    FailedToUnpack,
    IncorrectLayer { expected: ImageId, actual: ImageId },
    FailToPullThrough,
    TooLargePayload,
    Operation { status_code: StatusCode, message: String, operation: String },
    Http(reqwest::Error),
    Serialization(serde_json::Error),
    IO(std::io::Error)
}

impl RegistryError {
    pub async fn from_response(response: Response, operation: String) -> RegistryResult<String> {
        if response.status().is_success() {
            Ok(response.text().await?)
        } else if response.status() == StatusCode::UNAUTHORIZED {
            let text = response.text().await?;
            let error: AuthenticationFailureResponse = serde_json::from_str(&text)?;

            Err(RegistryError::InvalidAuthentication(error.reason))
        } else if response.status() == StatusCode::NOT_FOUND {
            Err(RegistryError::ReferenceNotFound)
        } else if response.status() == StatusCode::PAYLOAD_TOO_LARGE {
            Err(RegistryError::TooLargePayload)
        } else {
            let status_code = response.status();
            let text = response.text().await?;
            let error: AppErrorResponse = serde_json::from_str(&text)?;

            Err(
                RegistryError::Operation {
                    status_code,
                    message: error.error,
                    operation
                }
            )
        }
    }
}

impl Display for RegistryError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RegistryError::Unavailable(err) => write!(f, "Registry unavailable due to: {}", err),
            RegistryError::InvalidAuthentication(reason) => {
                match reason {
                    Some(reason) => write!(f, "Invalid authentication due to: {}", reason),
                    None => write!(f, "Invalid authentication"),
                }
            },
            RegistryError::ReferenceNotFound => write!(f, "Could not find the reference"),
            RegistryError::FailedToUpload { layer, reason } => write!(f, "Failed to upload layer {} due to: {}", layer, reason),
            RegistryError::InvalidLayer => write!(f, "Invalid layer"),
            RegistryError::InvalidContentHash => write!(f, "Incorrect downloaded content"),
            RegistryError::FailedToUnpack => write!(f, "Failed to unpack downloaded content"),
            RegistryError::IncorrectLayer { expected, actual } => write!(f, "Expected layer {} but got layer {}", expected, actual),
            RegistryError::FailToPullThrough => write!(f, "Failed to pull through upstream in enough time"),
            RegistryError::TooLargePayload => write!(f, "The payload is too large to be uploaded"),
            RegistryError::Operation { status_code, message, operation } => write!(f, "Operation ({}) failed due to: {} ({})", operation, message, status_code),
            RegistryError::Http(err) => write!(f, "Http error: {}", err),
            RegistryError::Serialization(err) => write!(f, "Serialization error: {}", err),
            RegistryError::IO(err) => write!(f, "I/O error: {}", err)
        }
    }
}

impl From<reqwest::Error> for RegistryError {
    fn from(value: reqwest::Error) -> Self {
        RegistryError::Http(value)
    }
}

impl From<serde_json::Error> for RegistryError {
    fn from(value: serde_json::Error) -> Self {
        RegistryError::Serialization(value)
    }
}

impl From<std::io::Error> for RegistryError {
    fn from(value: std::io::Error) -> Self {
        RegistryError::IO(value)
    }
}

fn create_http_client(config: &ImageManagerConfig) -> reqwest::Result<Client> {
    Client::builder()
        .danger_accept_invalid_certs(config.accept_self_signed)
        .build()
}