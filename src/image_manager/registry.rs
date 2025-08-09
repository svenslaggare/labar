use std::fmt::{Display, Formatter};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use futures::{future, StreamExt};
use reqwest::{Body, Client, Request, Response, StatusCode};

use crate::content::{ContentHash};
use crate::helpers::{clean_path, DeferredFileDelete};
use crate::image::{ImageMetadata, Layer, LayerOperation};
use crate::image_manager::{BoxPrinter, ImageManagerConfig};
use crate::image_manager::state::{StateSession};
use crate::reference::{ImageId, ImageTag, Reference};
use crate::registry::model::{AppErrorResponse, ImageSpec, UploadLayerResponse, UploadStatus};

pub type RegistryResult<T> = Result<T, RegistryError>;

pub struct RegistryManager {
    config: ImageManagerConfig,
    printer: BoxPrinter
}

impl RegistryManager {
    pub fn new(config: ImageManagerConfig, printer: BoxPrinter) -> RegistryManager {
        RegistryManager {
            config: config.clone(),
            printer
        }
    }

    pub async fn verify_login(&self, registry: &str, username: &str, password: &str) -> RegistryResult<()> {
        let http_client = Client::builder()
            .danger_accept_invalid_certs(self.config.accept_self_signed)
            .build()?;

        let full_url = format!("https://{}/verify_login", registry);
        let response = http_client.execute(
            http_client
                .get(full_url)
                .basic_auth(&username, Some(&password)).build()?
        ).await?;
        RegistryError::from_response(response, "url: /verify".to_owned()).await?;
        Ok(())
    }

    pub async fn list_images(&self, session: &StateSession, registry: &str) -> RegistryResult<Vec<ImageMetadata>> {
        let client = RegistryClient::new(&self.config, session, registry)?;

        let response = client.get_registry_text_response(registry, "images").await?;
        let images: Vec<ImageMetadata> = serde_json::from_str(&response)?;
        Ok(images)
    }

    pub async fn resolve_image(&self, session: &StateSession, registry: &str, tag: &ImageTag) -> RegistryResult<ImageMetadata> {
        let client = RegistryClient::new(&self.config, session, registry)?;

        let response = client.get_registry_text_response(registry, &format!("images/{}", tag)).await?;
        let image: ImageMetadata = serde_json::from_str(&response)?;
        Ok(image)
    }

    pub async fn download_layer(&self,
                                session: &StateSession,
                                registry: &str,
                                reference: &Reference) -> RegistryResult<Layer> {
        let client = Arc::new(RegistryClient::new(&self.config, session, registry)?);

        let response = client.get_registry_text_response(registry, &format!("layers/{}/manifest", reference)).await?;
        let mut layer: Layer = serde_json::from_str(&response)?;

        let layer_folder = self.config.get_layer_folder(&layer.hash);

        tokio::fs::create_dir_all(&layer_folder).await?;

        async fn download_file(client: &RegistryClient,
                               registry: &str,
                               reference: &Reference,
                               local_source_path: PathBuf,
                               file_index: usize,
                               content_hash: &str) -> RegistryResult<()> {
            let tmp_local_source_path = Path::new(&(local_source_path.to_str().unwrap().to_owned() + ".tmp")).to_owned();

            let mut deferred_delete = DeferredFileDelete::new(tmp_local_source_path.clone());

            let response = client.get_registry_response(&registry, &format!("layers/{}/download/{}", reference, file_index)).await?;
            let mut byte_stream = response.bytes_stream();

            let mut file = tokio::fs::File::create(&tmp_local_source_path).await?;
            let mut content_hasher = ContentHash::new();
            while let Some(item) = byte_stream.next().await {
                let data = item?;
                let mut data = data.as_ref();
                content_hasher.add(data);
                tokio::io::copy(&mut data, &mut file).await?;
            }

            if &content_hasher.finalize() != &content_hash {
                return Err(RegistryError::InvalidContentHash);
            }

            tokio::fs::rename(&tmp_local_source_path, &local_source_path).await?;
            deferred_delete.skip();

            Ok(())
        }

        let mut file_index = 0;
        let mut file_operations = Vec::new();
        for operation in &mut layer.operations {
            if let LayerOperation::File { source_path, content_hash, .. } = operation {
                file_operations.push((source_path.clone(), content_hash.clone(), file_index));
                file_index += 1;
            }
        }

        for chunk in file_operations.chunks(4) {
            let mut download_operations = Vec::new();
            for (source_path, content_hash, file_index) in chunk {
                let local_source_path = self.config.base_folder.canonicalize()?.join(Path::new(source_path));
                if local_source_path != clean_path(&local_source_path) {
                    return Err(RegistryError::InvalidLayer);
                }

                if !local_source_path.exists() {
                    self.printer.println(&format!("\t\t* Downloading file {}...", source_path));
                    download_operations.push(download_file(
                        &client,
                        registry,
                        reference,
                        local_source_path,
                        *file_index,
                        content_hash
                    ));
                } else {
                    self.printer.println(&format!("\t\t* Skipping downloading file {}.", source_path));
                }
            }

            for result in future::join_all(download_operations).await {
                result?;
            }
        }

        Ok(layer)
    }

    pub async fn upload_layer(&self,
                              session: &StateSession,
                              registry: &str,
                              layer: &Layer) -> RegistryResult<()> {
        let client = RegistryClient::new(&self.config, session, registry)?;

        // Begin upload
        let mut request = client.json_post_registry_response(registry, "layers/begin-upload").build()?;
        *request.body_mut() = Some(Body::from(serde_json::to_string(&layer)?));
        let response = client.execute(request).await?;

        let upload_response = RegistryError::from_response(response, "begin-upload".to_owned()).await?;
        let upload_response: UploadLayerResponse = serde_json::from_str(&upload_response)?;
        let upload_id = upload_response.upload_id.unwrap_or(String::new()).clone();

        match upload_response.status {
            UploadStatus::Started => {}
            UploadStatus::InvalidPaths => {
                return Err(RegistryError::FailedToUpload);
            }
            _ => {
                self.printer.println("\t\t* Layer already exist.");
                return Ok(());
            }
        }

        // Upload every file
        let mut file_index = 0;
        for operation in &layer.operations {
            if let LayerOperation::File { source_path, .. } = operation {
                let mut request = client.build_post_request(registry, &format!("layers/{}/upload/{}", layer.hash, file_index))
                    .header("UPLOAD_ID", upload_id.clone())
                    .build()?;

                let file = tokio::fs::File::open(self.config.base_folder.join(source_path)).await?;
                let body = Body::from(file);
                *request.body_mut() = Some(body);

                let response = client.execute(request).await?;
                RegistryError::from_response(response, format!("file {}", file_index)).await?;

                file_index += 1;
            }
        }

        // Commit upload
        let request = client.json_post_registry_response(registry, "layers/end-upload")
            .header("UPLOAD_ID", upload_id.clone())
            .build()?;
        let response = client.execute(request).await?;

        let upload_response = RegistryError::from_response(response, "begin-upload".to_owned()).await?;
        let upload_response: UploadLayerResponse = serde_json::from_str(&upload_response)?;

        if UploadStatus::Finished != upload_response.status {
            return Err(RegistryError::FailedToUpload);
        }

        Ok(())
    }

    pub async fn upload_image(&self,
                              session: &StateSession,
                              registry: &str, hash: &ImageId, tag: &ImageTag) -> RegistryResult<()> {
        let client = RegistryClient::new(&self.config, session, registry)?;

        let mut request = client.json_post_registry_response(registry, "images").build()?;

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
}

struct RegistryClient {
    http_client: Client,
    username: String,
    password: String
}

impl RegistryClient {
    pub fn new(config: &ImageManagerConfig, session: &StateSession, registry: &str) -> RegistryResult<RegistryClient> {
        let (username, password) = session.get_login(registry)
            .ok().flatten()
            .ok_or_else(|| RegistryError::InvalidAuthentication)?;

        Ok(
            RegistryClient {
                http_client: Client::builder()
                    .danger_accept_invalid_certs(config.accept_self_signed)
                    .build()?,
                username,
                password
            }
        )
    }

    pub fn execute(&self, request: Request) -> impl Future<Output = Result<Response, reqwest::Error>> {
        self.http_client.execute(request)
    }

    pub async fn get_registry_text_response(&self, registry: &str, url: &str) -> RegistryResult<String> {
        let response = self.get_registry_response(registry, url).await?;
        let response = RegistryError::from_response(response, format!("url: /{}", url)).await?;
        Ok(response)
    }

    pub fn json_post_registry_response(&self, registry: &str, url: &str) -> reqwest::RequestBuilder {
        let request = self.build_post_request(registry, url);
        request.header(reqwest::header::CONTENT_TYPE, "application/json")
    }

    pub async fn get_registry_response(&self, registry: &str, url: &str) -> RegistryResult<Response> {
        let request = self.build_get_request(registry, url);
        let response = self.http_client.execute(request.build()?).await.map_err(|err| RegistryError::FailedToContactRegistry(err))?;
        Ok(response)
    }

    pub fn build_get_request(&self, registry: &str, url: &str) -> reqwest::RequestBuilder {
        let full_url = format!("https://{}/{}", registry, url);
        self.http_client.
            get(full_url)
            .basic_auth(&self.username, Some(&self.password))
    }

    pub fn build_post_request(&self, registry: &str, url: &str) -> reqwest::RequestBuilder {
        let full_url = format!("https://{}/{}", registry, url);
        self.http_client
            .post(full_url)
            .basic_auth(&self.username, Some(&self.password))
    }
}

#[derive(Debug)]
pub enum RegistryError {
    FailedToContactRegistry(reqwest::Error),
    InvalidAuthentication,
    FailedToUpload,
    InvalidLayer,
    InvalidContentHash,
    Operation { status_code: StatusCode, message: String, operation: String },
    Http(reqwest::Error),
    Deserialize(serde_json::Error),
    IO(std::io::Error)
}

impl RegistryError {
    pub async fn from_response(response: Response, operation: String) -> RegistryResult<String> {
        if response.status().is_success() {
            Ok(response.text().await?)
        } else if response.status() == StatusCode::UNAUTHORIZED {
            Err(RegistryError::InvalidAuthentication)
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
            RegistryError::FailedToContactRegistry(err) => write!(f, "Failed to contact the registry due to: {}", err),
            RegistryError::InvalidAuthentication => write!(f, "Invalid authentication"),
            RegistryError::FailedToUpload => write!(f, "Failed to upload layer"),
            RegistryError::InvalidLayer => write!(f, "Invalid layer"),
            RegistryError::InvalidContentHash => write!(f, "The content has of the downloaded file is incorrect"),
            RegistryError::Operation { status_code, message, operation } => write!(f, "Operation ({}) failed due to: {} ({})", operation, message, status_code),
            RegistryError::Http(err) => write!(f, "Http: {}", err),
            RegistryError::Deserialize(err) => write!(f, "Deserialize: {}", err),
            RegistryError::IO(err) => write!(f, "I/O: {}", err)
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
        RegistryError::Deserialize(value)
    }
}

impl From<std::io::Error> for RegistryError {
    fn from(value: std::io::Error) -> Self {
        RegistryError::IO(value)
    }
}