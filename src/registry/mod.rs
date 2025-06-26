use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::str::FromStr;

use serde::Deserialize;
use serde_json::json;

use axum::response::{IntoResponse, Response};
use axum::{Json, Router};
use axum::body::Body;
use axum::extract::{Path, State};
use axum::extract::Request;
use axum::routing::{get, post};

use futures::StreamExt;
use reqwest::StatusCode;
use tokio_util::io::ReaderStream;
use tokio::io::AsyncWriteExt;

pub mod model;

use crate::image::{Layer, LayerOperation};
use crate::image_manager::{ImageManager, EmptyPrinter, ImageManagerConfig, ImageManagerError};
use crate::lock::FileLock;
use crate::reference::{ImageId, ImageTag, Reference};
use crate::registry::model::{ImageSpec, UploadLayerManifestResult, UploadLayerManifestStatus};

#[derive(Debug, Deserialize)]
pub struct RegistryConfig {
    pub data_path: PathBuf,
    pub address: SocketAddr
}

impl RegistryConfig {
    pub fn load(filename: &std::path::Path) -> Result<RegistryConfig, String> {
        let content = std::fs::read_to_string(filename).map_err(|err| format!("{}", err))?;
        serde_yaml::from_str(&content).map_err(|err| format!("{}", err))
    }

    pub fn image_manager_config(&self) -> ImageManagerConfig {
        ImageManagerConfig::with_base_dir(self.data_path.clone())
    }
}

pub async fn run(config: RegistryConfig) {
    let state = AppState::new(config);

    let app = Router::new()
        .route("/images", get(list_images))
        .route("/images", post(set_image))
        .route("/images/{*tag}", get(resolve_image))
        .route("/layers/{layer}/manifest", get(get_layer_manifest))
        .route("/layers/{layer}/download/{index}", get(download_layer))
        .route("/layers/manifest", post(upload_layer_manifest))
        .route("/layers/{layer}/upload/{index}", post(upload_layer_file))
        .with_state(state.clone())
    ;

    let listener = tokio::net::TcpListener::bind(state.config.address).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

pub struct AppState {
    config: RegistryConfig
}

impl AppState {
    pub fn new(config: RegistryConfig) -> Arc<AppState> {
        Arc::new(
            AppState {
                config,
            }
        )
    }
}

async fn list_images(State(state): State<Arc<AppState>>) -> AppResult<impl IntoResponse> {
    let image_manager = create_image_manager(&state);
    let images = image_manager.list_images()?;

    Ok(Json(json!(images)))
}

async fn set_image(State(state): State<Arc<AppState>>,
                   Json(spec): Json<ImageSpec>) -> AppResult<impl IntoResponse> {
    let _lock = create_write_lock(&state);
    let mut image_manager = create_image_manager(&state);

    image_manager.tag_image(&Reference::ImageId(spec.hash.clone()), &spec.tag)?;

    Ok(())
}

async fn resolve_image(State(state): State<Arc<AppState>>, Path(tag): Path<String>) -> AppResult<impl IntoResponse> {
    let image_manager = create_image_manager(&state);

    let tag = ImageTag::from_str(&tag).map_err(|err| AppError::InvalidImageReference(err))?;
    let image = image_manager.resolve_image(&tag)?;

    Ok(Json(json!(image)))
}

async fn get_layer_manifest(State(state): State<Arc<AppState>>,
                            Path(reference): Path<String>) -> AppResult<impl IntoResponse> {
    let image_manager = create_image_manager(&state);

    let layer_reference = ImageId::from_str(&reference).map_err(|err| AppError::InvalidImageReference(err))?;
    let layer_reference = Reference::ImageId(layer_reference);
    let layer = image_manager.get_layer(&layer_reference)?;
    Ok(Json(json!(layer)))
}

async fn download_layer(State(state): State<Arc<AppState>>,
                        Path((layer, file_index)): Path<(String, usize)>) -> AppResult<impl IntoResponse> {
    let image_manager = create_image_manager(&state);

    let layer_reference = ImageId::from_str(&layer).map_err(|err| AppError::InvalidImageReference(err))?;
    let layer_reference = Reference::ImageId(layer_reference);
    let layer = image_manager.get_layer(&layer_reference)?;

    if let Some(operation) = layer.get_file_operation(file_index) {
        if let LayerOperation::File { source_path, .. } = operation {
            let source_path = std::path::Path::new(source_path);
            let abs_source_path = image_manager.config().base_folder().join(source_path);

            let file = tokio::fs::File::open(&abs_source_path).await?;
            let stream = ReaderStream::with_capacity(file, 4096);
            let body = Body::from_stream(stream);

            return Ok(
                Response::builder()
                    .header("Content-Type", "application/octet-stream")
                    .header(
                        "Content-Disposition",
                        format!("attachment; filename={}", abs_source_path.components().last().unwrap().as_os_str().display())
                    )
                    .body(body)
                    .unwrap()
            );
        }
    }

    Err(AppError::LayerFileNotFound)
}

async fn upload_layer_manifest(State(state): State<Arc<AppState>>,
                               Json(layer): Json<Layer>) -> AppResult<impl IntoResponse> {
    let _lock = create_write_lock(&state);
    let mut image_manager = create_image_manager(&state);

    if image_manager.get_layer(&Reference::ImageId(layer.hash.clone())).is_ok() {
        return Ok(
            Json(json!(
                UploadLayerManifestResult {
                    status: UploadLayerManifestStatus::AlreadyExist
                }
            ))
        );
    }

    let folder = image_manager.config().get_layer_folder(&layer.hash);
    tokio::fs::create_dir_all(&folder).await?;

    let mut file = tokio::fs::File::create(folder.join("manifest.json")).await?;
    file.write_all(serde_json::to_string_pretty(&layer).unwrap().as_bytes()).await?;

    image_manager.add_layer(layer);
    Ok(
        Json(
            json!(
                UploadLayerManifestResult {
                    status: UploadLayerManifestStatus::Uploaded
                }
            )
        )
    )
}

async fn upload_layer_file(State(state): State<Arc<AppState>>,
                           Path((layer, file_index)): Path<(String, usize)>,
                           request: Request) -> AppResult<impl IntoResponse> {
    let _lock = create_write_lock(&state);
    let image_manager = create_image_manager(&state);

    let layer_reference = Reference::from_str(&layer).map_err(|err| AppError::InvalidImageReference(err))?;
    let layer = image_manager.get_layer(&layer_reference)?;

    if let Some(operation) = layer.get_file_operation(file_index) {
        if let LayerOperation::File { source_path, .. } = operation {
            let abs_source_path = image_manager.config().base_folder().join(source_path).to_str().unwrap().to_owned();
            let temp_file_path = abs_source_path.to_owned() + ".tmp";
            let mut file = tokio::fs::File::create(&temp_file_path).await?;

            let mut stream = request.into_body().into_data_stream();
            while let Some(chunk) = stream.next().await {
                match chunk {
                    Ok(chunk) => {
                        file.write_all(chunk.as_ref()).await?;
                    }
                    Err(err) => {
                        return Err(AppError::FailedToUploadLayerFile(err.to_string()));
                    }
                }
            }

            tokio::fs::rename(&temp_file_path, abs_source_path).await?;
            return Ok(Json(json!({ "status": "uploaded" })));
        }
    }

    Err(AppError::LayerFileNotFound)
}

type AppResult<T> = Result<T, AppError>;

enum AppError {
    ImagerManager(ImageManagerError),
    LayerFileNotFound,
    FailedToUploadLayerFile(String),
    InvalidImageReference(String),
    IO(std::io::Error),
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        match self {
            AppError::ImagerManager(err) => {
                match err {
                    err @ ImageManagerError::ImageNotFound { .. } => {
                        (
                            StatusCode::NOT_FOUND,
                            Json(json!({ "error": format!("image not found due to: {}", err) }))
                        ).into_response()
                    }
                    err => {
                        (
                            StatusCode::BAD_REQUEST,
                            Json(json!({ "error": format!("{}", err) }))
                        ).into_response()
                    }
                }
            }
            AppError::LayerFileNotFound => {
                (
                    StatusCode::NOT_FOUND,
                    Json(json!({ "error": "Layer file not found" }))
                ).into_response()
            }
            AppError::FailedToUploadLayerFile(err) => {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({ "error": format!("Failed to upload layer file: {}", err) }))
                ).into_response()
            }
            AppError::InvalidImageReference(err) => {
                (
                    StatusCode::BAD_REQUEST,
                    Json(json!({ "error": format!("{}", err) }))
                ).into_response()
            }
            AppError::IO(err) => {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({ "error": format!("I/O error: {}", err) }))
                ).into_response()
            }
        }
    }
}

impl From<ImageManagerError> for AppError {
    fn from(value: ImageManagerError) -> Self {
        AppError::ImagerManager(value)
    }
}

impl From<std::io::Error> for AppError {
    fn from(value: std::io::Error) -> Self {
        AppError::IO(value)
    }
}

fn create_write_lock(state: &AppState) -> FileLock {
    FileLock::new(state.config.image_manager_config().base_folder().join("write_lock"))
}

fn create_image_manager(state: &AppState) -> ImageManager {
    let mut image_manager = ImageManager::with_config(
        state.config.image_manager_config(),
        EmptyPrinter::new()
    );

    if image_manager.state_exists() {
        image_manager.load_state().unwrap();
    } else {
        image_manager.save_state().unwrap();
    }

    image_manager
}