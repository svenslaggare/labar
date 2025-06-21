use std::fmt::{Display, Formatter};
use std::path::Path;

use futures::StreamExt;
use reqwest::{Body, Client, StatusCode};

use crate::image::{Layer, LayerOperation};
use crate::image_manager::{BoxPrinter, ImageManagerConfig, ImageMetadata};
use crate::registry::model::{ImageSpec, UploadLayerManifestResult, UploadLayerManifestStatus};

pub type RegistryResult<T> = Result<T, RegistryError>;

pub struct RegistryManager {
    http_client: Client,
    printer: BoxPrinter
}

impl RegistryManager {
    pub fn new(printer: BoxPrinter) -> RegistryManager {
        RegistryManager {
            http_client: Client::new(),
            printer
        }
    }

    pub async fn list_images(&self, registry: &str) -> RegistryResult<Vec<ImageMetadata>> {
        let response = self.get_registry_text_response(registry, "images").await?;
        let images: Vec<ImageMetadata> = serde_json::from_str(&response)?;
        Ok(images)
    }

    pub async fn download_layer(&self,
                                config: &ImageManagerConfig,
                                registry: &str,
                                reference: &str) -> RegistryResult<Layer> {
        let response = self.get_registry_text_response(registry, &format!("layers/{}/manifest", reference)).await?;
        let mut layer: Layer = serde_json::from_str(&response)?;

        let manifest_file_path = config.get_layer_folder(&layer.hash).join("manifest.json");
        if manifest_file_path.exists() {
            return Ok(layer);
        }

        let mut file_index = 0;
        for operation in &mut layer.operations {
            if let LayerOperation::File { source_path, .. } = operation {
                let local_source_path = config.base_folder.join(Path::new(source_path));
                *source_path = local_source_path.to_str().unwrap().to_owned();
                self.printer.println(&format!("\t\t* Downloading file {}...", source_path));

                if let Some(parent) = local_source_path.parent() {
                    tokio::fs::create_dir_all(parent).await?;
                }

                let mut file = tokio::fs::File::create(local_source_path).await?;

                let response = self.get_registry_response(registry, &format!("layers/{}/download/{}", reference, file_index)).await?;
                let mut byte_stream = response.bytes_stream();
                while let Some(item) = byte_stream.next().await {
                    tokio::io::copy(&mut item?.as_ref(), &mut file).await?;
                }

                file_index += 1;
            }
        }

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
        RegistryError::from_status_code(response.status())?;

        let upload_result: UploadLayerManifestResult = serde_json::from_str(&response.text().await?)?;
        if let UploadLayerManifestStatus::AlreadyExist = upload_result.status {
            self.printer.println("\t\t* Layer already exist.");
            return Ok(());
        }

        let mut file_index = 0;
        for operation in &layer.operations {
            if let LayerOperation::File { source_path, .. } = operation {
                let mut request = self.post_registry_response(registry, &format!("layers/{}/upload/{}", layer.hash, file_index)).build()?;

                let file = tokio::fs::File::open(config.base_folder.join(source_path)).await?;
                let body = Body::from(file);
                *request.body_mut() = Some(body);

                let response = self.http_client.execute(request).await?;
                RegistryError::from_status_code(response.status())?;

                file_index += 1;
            }
        }

        Ok(())
    }

    pub async fn upload_image(&self, registry: &str, hash: &str, tag: &str) -> RegistryResult<()> {
        let mut request = self.json_post_registry_response(registry, "images").build()?;

        *request.body_mut() = Some(Body::from(serde_json::to_string(
            &ImageSpec {
                hash: hash.to_owned(),
                tag: tag.to_owned()
            }
        )?));

        let response = self.http_client.execute(request).await?;
        RegistryError::from_status_code(response.status())?;

        Ok(())
    }

    async fn get_registry_text_response(&self, registry: &str, url: &str) -> RegistryResult<String> {
        let response = self.get_registry_response(registry, url).await?;
        let response = response.text().await?;
        Ok(response)
    }

    async fn get_registry_response(&self, registry: &str, url: &str) -> RegistryResult<reqwest::Response> {
        let response = self.http_client.execute(self.http_client.get(format!("http://{}/{}", registry, url)).build()?).await?;
        Ok(response)
    }

    fn post_registry_response(&self, registry: &str, url: &str) -> reqwest::RequestBuilder {
        self.http_client.post(format!("http://{}/{}", registry, url))
    }

    fn json_post_registry_response(&self, registry: &str, url: &str) -> reqwest::RequestBuilder {
        let request = self.post_registry_response(registry, url);
        request.header(reqwest::header::CONTENT_TYPE, "application/json")
    }
}

#[derive(Debug)]
pub enum RegistryError {
    Http(reqwest::Error),
    HttpFailed(StatusCode),
    Deserialize(serde_json::Error),
    IO(std::io::Error)
}

impl RegistryError {
    pub fn from_status_code(status_code: StatusCode) -> RegistryResult<()> {
        if status_code.is_success() {
            Ok(())
        } else {
            Err(RegistryError::HttpFailed(status_code))
        }
    }
}

impl Display for RegistryError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RegistryError::Http(err) => write!(f, "Http: {}", err),
            RegistryError::HttpFailed(status_code) => write!(f, "Http failed with status code: {}", status_code),
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