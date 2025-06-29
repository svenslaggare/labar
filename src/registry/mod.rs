use std::net::SocketAddr;
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::Arc;
use std::str::FromStr;

use chrono::Local;
use log::info;

use serde::Deserialize;
use serde_json::json;
use serde::de::DeserializeOwned;

use futures::StreamExt;
use tokio_util::io::ReaderStream;
use tokio::io::AsyncWriteExt;

use axum::response::{IntoResponse, Response};
use axum::{Json, Router};
use axum::body::Body;
use axum::extract::{Path, State, Request, FromRequest};
use axum::routing::{delete, get, post};
use axum_server::tls_rustls::RustlsConfig;

pub mod model;
pub mod auth;

use crate::image::{Layer, LayerOperation};
use crate::image_manager::{ImageManager, EmptyPrinter, ImageManagerConfig};
use crate::lock::FileLock;
use crate::reference::{ImageId, ImageTag, Reference};
use crate::registry::auth::{check_access_right, AccessRight, AuthProvider, AuthToken, MemoryAuthProvider, UsersSpec};
use crate::registry::model::{AppError, AppResult, ImageSpec, UploadLayerManifestResult, UploadLayerManifestStatus};

#[derive(Debug, Deserialize)]
pub struct RegistryConfig {
    pub data_path: PathBuf,

    pub address: SocketAddr,

    #[serde(default)]
    pub use_ssl: bool,
    pub ssl_cert_path: Option<PathBuf>,
    pub ssl_key_path: Option<PathBuf>,

    #[serde(default)]
    pub users: UsersSpec
}

impl RegistryConfig {
    pub fn load(filename: &std::path::Path) -> Result<RegistryConfig, String> {
        let content = std::fs::read_to_string(filename).map_err(|err| format!("{}", err))?;
        toml::from_str(&content).map_err(|err| format!("{}", err))
    }

    pub fn image_manager_config(&self) -> ImageManagerConfig {
        ImageManagerConfig::with_base_folder(self.data_path.clone())
    }
}

pub async fn run(config: RegistryConfig) {
    setup_logging().unwrap();

    let tls_config = if config.use_ssl {
        Some(
            RustlsConfig::from_pem_chain_file(
                config.ssl_cert_path.as_ref().expect("Specify ssl_cert_path argument."),
                config.ssl_key_path.as_ref().expect("Specify ssl_key_path argument."),
            ).await.unwrap()
        )
    } else {
        None
    };

    let state = AppState::new(config);

    let app = Router::new()
        .route("/images", get(list_images))
        .route("/images", post(set_image))
        .route("/images/{*tag}", get(resolve_image))
        .route("/images/{*tag}", delete(remove_image))
        .route("/layers/{layer}/manifest", get(get_layer_manifest))
        .route("/layers/{layer}/download/{index}", get(download_layer))
        .route("/layers/manifest", post(upload_layer_manifest))
        .route("/layers/{layer}/upload/{index}", post(upload_layer_file))
        .with_state(state.clone())
    ;

    if let Some(tls_config) = tls_config {
        info!("Running https://{}", state.config.address);
        axum_server::bind_rustls(state.config.address, tls_config)
            .serve(app.into_make_service())
            .await
            .unwrap();
    } else {
        info!("Running http://{}", state.config.address);
        axum_server::bind(state.config.address)
            .serve(app.into_make_service())
            .await
            .unwrap();
    }
}

pub struct AppState {
    config: RegistryConfig,
    access_provider: Box<dyn AuthProvider + Send + Sync>
}

impl AppState {
    pub fn new(mut config: RegistryConfig) -> Arc<AppState> {
        let access_provider = Box::new(MemoryAuthProvider::new(std::mem::take(&mut config.users)));

        Arc::new(
            AppState {
                config,
                access_provider
            }
        )
    }
}

async fn list_images(State(state): State<Arc<AppState>>,
                     request: Request) -> AppResult<impl IntoResponse> {
    let token = check_access_right(state.access_provider.deref(), &request, AccessRight::List)?;

    let image_manager = create_image_manager(&state, &token);
    let images = image_manager.list_images()?;

    Ok(Json(json!(images)).into_response())
}

async fn set_image(State(state): State<Arc<AppState>>,
                   request: Request) -> AppResult<impl IntoResponse> {
    let token = check_access_right(state.access_provider.deref(), &request, AccessRight::Upload)?;

    let spec: ImageSpec = decode_json(request).await?;

    let _lock = create_write_lock(&state).await;
    let mut image_manager = create_image_manager(&state, &token);

    image_manager.tag_image(&Reference::ImageId(spec.hash.clone()), &spec.tag)?;
    info!("Uploaded image: {} ({})", spec.tag, spec.hash);
    Ok(())
}

async fn resolve_image(State(state): State<Arc<AppState>>,
                       Path(tag): Path<String>,
                       request: Request) -> AppResult<impl IntoResponse> {
    let token = check_access_right(state.access_provider.deref(), &request, AccessRight::List)?;

    let image_manager = create_image_manager(&state, &token);

    let tag = ImageTag::from_str(&tag).map_err(|err| AppError::InvalidImageReference(err))?;
    let image = image_manager.resolve_image(&tag)?;

    Ok(Json(json!(image)))
}

async fn remove_image(State(state): State<Arc<AppState>>,
                      Path(tag): Path<String>,
                      request: Request) -> AppResult<impl IntoResponse> {
    let token = check_access_right(state.access_provider.deref(), &request, AccessRight::Delete)?;

    let _lock = create_write_lock(&state).await;
    let mut image_manager = create_image_manager(&state, &token);

    let tag = ImageTag::from_str(&tag).map_err(|err| AppError::InvalidImageReference(err))?;
    image_manager.remove_image(&tag)?;
    info!("Removed image: {}", tag);

    Ok(Json(json!({ "status": "success" })))
}

async fn get_layer_manifest(State(state): State<Arc<AppState>>,
                            Path(reference): Path<String>,
                            request: Request) -> AppResult<impl IntoResponse> {
    let token = check_access_right(state.access_provider.deref(), &request, AccessRight::Download)?;

    let image_manager = create_image_manager(&state, &token);

    let layer_reference = ImageId::from_str(&reference).map_err(|err| AppError::InvalidImageReference(err))?.to_ref();
    let layer = image_manager.get_layer(&layer_reference)?;
    Ok(Json(json!(layer)))
}

async fn download_layer(State(state): State<Arc<AppState>>,
                        Path((layer, file_index)): Path<(String, usize)>,
                        request: Request) -> AppResult<impl IntoResponse> {
    let token = check_access_right(state.access_provider.deref(), &request, AccessRight::Download)?;

    let image_manager = create_image_manager(&state, &token);

    let layer_reference = ImageId::from_str(&layer).map_err(|err| AppError::InvalidImageReference(err))?.to_ref();
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
                               request: Request) -> AppResult<impl IntoResponse> {
    let token = check_access_right(state.access_provider.deref(), &request, AccessRight::Upload)?;

    let layer: Layer = decode_json(request).await?;

    let _lock = create_write_lock(&state).await;
    let mut image_manager = create_image_manager(&state, &token);

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
    layer.save_to_file_async(&folder).await?;

    info!("Uploaded layer manifest: {}", layer.hash);
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
    let token = check_access_right(state.access_provider.deref(), &request, AccessRight::Upload)?;

    let _lock = create_write_lock(&state).await;
    let image_manager = create_image_manager(&state, &token);

    let layer_reference = ImageId::from_str(&layer).map_err(|err| AppError::InvalidImageReference(err))?.to_ref();
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
                        tokio::fs::remove_file(&temp_file_path).await?;
                        return Err(AppError::FailedToUploadLayerFile(err.to_string()));
                    }
                }
            }

            tokio::fs::rename(&temp_file_path, abs_source_path).await?;
            info!("Uploaded layer file: {}:{}", layer.hash, file_index);
            return Ok(Json(json!({ "status": "uploaded" })));
        }
    }

    Err(AppError::LayerFileNotFound)
}

async fn create_write_lock(state: &AppState) -> FileLock {
    FileLock::new_async(state.config.image_manager_config().base_folder().join("write_lock")).await
}

fn create_image_manager(state: &AppState, _token: &AuthToken) -> ImageManager {
    let mut image_manager = ImageManager::with_config(
        state.config.image_manager_config(),
        EmptyPrinter::new()
    );

    let result = if image_manager.state_exists() {
        image_manager.load_state()
    } else {
        image_manager.save_state()
    };

    if let Err(err) = result {
        println!("Failed loading state: {}", err);
    }

    image_manager
}

async fn decode_json<T: DeserializeOwned>(request: Request) -> AppResult<T> {
    let value = Json::<T>::from_request(request, &()).await.map_err(|err| AppError::Other(err.into_response()))?;
    Ok(value.0)
}

fn setup_logging() -> Result<(), fern::InitError> {
    fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "[{} {} {}] {}",
                 Local::now().format("%Y-%m-%d %T"),
                record.level(),
                record.target(),
                message
            ))
        })
        .level(log::LevelFilter::Info)
        .chain(std::io::stdout())
        .apply()?;
    Ok(())
}