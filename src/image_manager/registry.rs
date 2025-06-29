use std::fmt::{Display, Formatter};
use std::path::Path;

use futures::StreamExt;
use reqwest::{Body, Client, StatusCode};

use crate::image::{ImageMetadata, Layer, LayerOperation};
use crate::image_manager::{BoxPrinter, ImageManagerConfig};
use crate::reference::{ImageId, ImageTag, Reference};
use crate::registry::model::{ImageSpec, UploadLayerManifestResult, UploadLayerManifestStatus};

pub type RegistryResult<T> = Result<T, RegistryError>;

pub struct RegistryManager {
    config: ImageManagerConfig,
    printer: BoxPrinter,
    http_client: Client,
}

impl RegistryManager {
    pub fn new(config: ImageManagerConfig, printer: BoxPrinter) -> RegistryManager {
        RegistryManager {
            config,
            printer,
            http_client: Client::builder()
                .danger_accept_invalid_certs(true)
                .build().unwrap(),
        }
    }

    pub async fn list_images(&self, registry: &str) -> RegistryResult<Vec<ImageMetadata>> {
        let response = self.get_registry_text_response(registry, "images").await?;
        let images: Vec<ImageMetadata> = serde_json::from_str(&response)?;
        Ok(images)
    }

    pub async fn resolve_image(&self, registry: &str, tag: &ImageTag) -> RegistryResult<ImageMetadata> {
        let response = self.get_registry_text_response(registry, &format!("images/{}", tag)).await?;
        let image: ImageMetadata = serde_json::from_str(&response)?;
        Ok(image)
    }

    pub async fn download_layer(&self,
                                config: &ImageManagerConfig,
                                registry: &str,
                                reference: &Reference) -> RegistryResult<Layer> {
        let response = self.get_registry_text_response(registry, &format!("layers/{}/manifest", reference)).await?;
        let mut layer: Layer = serde_json::from_str(&response)?;

        let layer_folder = config.get_layer_folder(&layer.hash);
        let manifest_file_path = layer_folder.join("manifest.json");
        if manifest_file_path.exists() {
            return Ok(layer);
        }

        tokio::fs::create_dir_all(&layer_folder).await?;

        let mut file_index = 0;
        for operation in &mut layer.operations {
            if let LayerOperation::File { source_path, .. } = operation {
                let local_source_path = config.base_folder.join(Path::new(source_path));
                self.printer.println(&format!("\t\t* Downloading file {}...", source_path));

                let response = self.get_registry_response(registry, &format!("layers/{}/download/{}", reference, file_index)).await?;
                let mut byte_stream = response.bytes_stream();

                let mut file = tokio::fs::File::create(local_source_path).await?;
                while let Some(item) = byte_stream.next().await {
                    tokio::io::copy(&mut item?.as_ref(), &mut file).await?;
                }

                file_index += 1;
            }
        }

        // This will commit the layer to local file system
        tokio::fs::write(&manifest_file_path, serde_json::to_string_pretty(&layer)?.as_bytes()).await?;
        Ok(layer)
    }

    pub async fn upload_layer(&self,
                              config: &ImageManagerConfig,
                              registry: &str,
                              layer: &Layer) -> RegistryResult<()> {
        let mut request = self.json_post_registry_response(registry, "layers/manifest").build()?;

        *request.body_mut() = Some(Body::from(serde_json::to_string(&layer)?));
        let response = self.http_client.execute(request).await?;
        RegistryError::from_status_code(response.status(), "manifest".to_owned())?;

        let upload_result: UploadLayerManifestResult = serde_json::from_str(&response.text().await?)?;
        if let UploadLayerManifestStatus::AlreadyExist = upload_result.status {
            self.printer.println("\t\t* Layer already exist.");
            return Ok(());
        }

        let mut file_index = 0;
        for operation in &layer.operations {
            if let LayerOperation::File { source_path, .. } = operation {
                let mut request = self.build_post_request(registry, &format!("layers/{}/upload/{}", layer.hash, file_index)).build()?;

                let file = tokio::fs::File::open(config.base_folder.join(source_path)).await?;
                let body = Body::from(file);
                *request.body_mut() = Some(body);

                let response = self.http_client.execute(request).await?;
                RegistryError::from_status_code(response.status(), format!("file {}", file_index))?;

                file_index += 1;
            }
        }

        Ok(())
    }

    pub async fn upload_image(&self, registry: &str, hash: &ImageId, tag: &ImageTag) -> RegistryResult<()> {
        let mut request = self.json_post_registry_response(registry, "images").build()?;

        *request.body_mut() = Some(Body::from(serde_json::to_string(
            &ImageSpec {
                hash: hash.clone(),
                tag: tag.clone()
            }
        )?));

        let response = self.http_client.execute(request).await?;
        RegistryError::from_status_code(response.status(), "image".to_owned())?;

        Ok(())
    }

    async fn get_registry_text_response(&self, registry: &str, url: &str) -> RegistryResult<String> {
        let response = self.get_registry_response(registry, url).await?;
        RegistryError::from_status_code(response.status(), format!("url: {}", url))?;
        let response = response.text().await?;
        Ok(response)
    }

    fn json_post_registry_response(&self, registry: &str, url: &str) -> reqwest::RequestBuilder {
        let request = self.build_post_request(registry, url);
        request.header(reqwest::header::CONTENT_TYPE, "application/json")
    }

    async fn get_registry_response(&self, registry: &str, url: &str) -> RegistryResult<reqwest::Response> {
        let request = self.build_get_request(registry, url);
        let response = self.http_client.execute(request.build()?).await?;
        Ok(response)
    }

    fn build_get_request(&self, registry: &str, url: &str) -> reqwest::RequestBuilder {
        let full_url = format!("{}://{}/{}", self.http_mode(), registry, url);
        self.http_client.
            get(full_url)
            .basic_auth(&self.config.registry_username, Some(&self.config.registry_password))
    }

    fn build_post_request(&self, registry: &str, url: &str) -> reqwest::RequestBuilder {
        let full_url = format!("{}://{}/{}", self.http_mode(), registry, url);
        self.http_client
            .post(full_url)
            .basic_auth(&self.config.registry_username, Some(&self.config.registry_password))
    }

    fn http_mode(&self) -> &str {
        if self.config.registry_use_ssl {
            "https"
        } else {
            "http"
        }
    }
}

#[derive(Debug)]
pub enum RegistryError {
    Http(reqwest::Error),
    HttpFailed { status_code: StatusCode, description: String },
    Deserialize(serde_json::Error),
    IO(std::io::Error)
}

impl RegistryError {
    pub fn from_status_code(status_code: StatusCode, description: String) -> RegistryResult<()> {
        if status_code.is_success() {
            Ok(())
        } else {
            Err(RegistryError::HttpFailed { status_code, description })
        }
    }
}

impl Display for RegistryError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RegistryError::Http(err) => write!(f, "Http: {}", err),
            RegistryError::HttpFailed { status_code, description } => write!(f, "Http failed with status code: {} ({})", status_code, description),
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