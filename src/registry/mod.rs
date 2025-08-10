use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;
use std::time::{Instant};

use chrono::Local;
use log::{error, info};

use serde_json::json;
use serde::de::DeserializeOwned;

use futures::StreamExt;
use tokio_util::io::ReaderStream;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

use axum::response::{IntoResponse, Response};
use axum::{Json, Router};
use axum::body::Body;
use axum::extract::{Path, State, Request, FromRequest};
use axum::routing::{delete, get, post};
use axum_server::tls_rustls::RustlsConfig;

use rcgen::CertifiedKey;

pub mod model;
pub mod auth;
pub mod config;

pub use crate::registry::config::RegistryConfig;

use crate::content::ContentHash;
use crate::image::{Layer, LayerOperation};
use crate::image_manager::{ImageManager, EmptyPrinter};
use crate::reference::{ImageId, ImageTag, Reference};
use crate::registry::auth::{check_access_right, AccessRight, AuthProvider, AuthToken, MemoryAuthProvider};
use crate::registry::config::RegistryUpstreamConfig;
use crate::registry::model::{AppError, AppResult, ImageSpec, UploadLayerResponse, UploadStatus};

pub async fn run(config: RegistryConfig) {
    if let Err(err) = setup_logging() {
        println!("Failed to setting up logging: {}", err);
    }

    let (cert_path, key_path) = match (config.ssl_cert_path.as_ref(), config.ssl_key_path.as_ref()) {
        (Some(cert_path), Some(key_path)) => {
            info!("Using specified SSL certificate.");
            (cert_path.clone(), key_path.clone())
        }
        _ => {
            let cert_path = config.data_path.join("cert.pem");
            let key_path = config.data_path.join("key.pem");

            if !cert_path.exists() {
                info!("Generating SSL certificate...");
                let subject_alt_names = vec!["localhost".to_string()];
                let CertifiedKey { cert, signing_key } = rcgen::generate_simple_self_signed(subject_alt_names).unwrap();

                std::fs::create_dir_all(&config.data_path).unwrap();
                std::fs::write(&cert_path, cert.pem()).unwrap();
                std::fs::write(&key_path, signing_key.serialize_pem()).unwrap();
            }

            info!("Using auto-generated SSL certificate.");

            (cert_path, key_path)
        }
    };

    let tls_config = RustlsConfig::from_pem_chain_file(cert_path, key_path).await.unwrap();

    let state = AppState::new(config);

    let app = Router::new()
        .route("/verify_login", get(verify_login))
        .route("/images", get(list_images))
        .route("/images", post(set_image))
        .route("/images/{*tag}", get(resolve_image))
        .route("/images/{*tag}", delete(remove_image))
        .route("/layers/{layer}/manifest", get(get_layer_manifest))
        .route("/layers/{layer}/download/{index}", get(download_layer))
        .route("/layers/begin-upload", post(begin_layer_upload))
        .route("/layers/end-upload", post(end_layer_upload))
        .route("/layers/{layer}/upload/{index}", post(upload_layer_file))
        .with_state(state.clone())
    ;

    let state_clone = state.clone();
    tokio::spawn(async move {
        if let Some(upstream_config) = state_clone.config.upstream.as_ref() {
            let mut is_first = true;
            loop {
                if (is_first && upstream_config.sync_at_startup) || !is_first {
                    sync_with_upstream(state_clone.clone(), &upstream_config).await;
                }

                let current = Local::now();
                if let Ok(next) = upstream_config.sync_interval.find_next_occurrence(&current, false) {
                    tokio::time::sleep((next - current).to_std().unwrap()).await;
                }

                is_first = false;
            }
        }
    });

    info!("Running at https://{}", state.config.address);
    axum_server::bind_rustls(state.config.address, tls_config)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

pub struct AppState {
    config: RegistryConfig,
    access_provider: Box<dyn AuthProvider + Send + Sync>,
    pending_uploads: Mutex<PendingUploads>
}

impl AppState {
    pub fn new(mut config: RegistryConfig) -> Arc<AppState> {
        let access_provider = Box::new(MemoryAuthProvider::new(std::mem::take(&mut config.users)));

        Arc::new(
            AppState {
                config,
                access_provider,
                pending_uploads: Mutex::new(HashMap::new())
            }
        )
    }
}

type PendingUploads = HashMap<ImageId, PendingUpload>;

struct PendingUpload {
    upload_id: String,
    layer: Layer,
    started: Instant
}

async fn verify_login(State(state): State<Arc<AppState>>,
                      request: Request) -> AppResult<impl IntoResponse> {
    let _token = check_access_right(state.access_provider.deref(), &request, AccessRight::Access)?;
    Ok("")
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

    let mut image_manager = create_image_manager(&state, &token);
    image_manager.tag_image(&Reference::ImageId(spec.hash.clone()), &spec.tag)?;

    info!("Uploaded image: {} ({})", spec.tag, spec.hash);
    Ok(())
}

async fn resolve_image(State(state): State<Arc<AppState>>,
                       Path(tag): Path<ImageTag>,
                       request: Request) -> AppResult<impl IntoResponse> {
    let token = check_access_right(state.access_provider.deref(), &request, AccessRight::List)?;

    let image_manager = create_image_manager(&state, &token);
    let image = image_manager.resolve_image(&tag)?;

    Ok(Json(json!(image)))
}

async fn remove_image(State(state): State<Arc<AppState>>,
                      Path(tag): Path<ImageTag>,
                      request: Request) -> AppResult<impl IntoResponse> {
    let token = check_access_right(state.access_provider.deref(), &request, AccessRight::Delete)?;

    let mut image_manager = create_image_manager(&state, &token);
    image_manager.remove_image(&tag)?;

    info!("Removed image: {}", tag);

    Ok(Json(json!({ "status": "success" })))
}

async fn get_layer_manifest(State(state): State<Arc<AppState>>,
                            Path(hash): Path<ImageId>,
                            request: Request) -> AppResult<impl IntoResponse> {
    let token = check_access_right(state.access_provider.deref(), &request, AccessRight::Download)?;

    let image_manager = create_image_manager(&state, &token);

    let layer_reference = hash.to_ref();
    let layer = image_manager.get_layer(&layer_reference)?;
    Ok(Json(json!(layer)))
}

async fn download_layer(State(state): State<Arc<AppState>>,
                        Path((hash, file_index)): Path<(ImageId, usize)>,
                        request: Request) -> AppResult<impl IntoResponse> {
    let token = check_access_right(state.access_provider.deref(), &request, AccessRight::Download)?;

    let image_manager = create_image_manager(&state, &token);

    let layer_reference = hash.to_ref();
    let layer = image_manager.get_layer(&layer_reference)?;
    let base_folder = image_manager.config().base_folder().to_path_buf();

    if let Some(operation) = layer.get_file_operation(file_index) {
        if let LayerOperation::File { source_path, .. } = operation {
            let source_path = std::path::Path::new(source_path);
            let abs_source_path = base_folder.join(source_path);

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

async fn begin_layer_upload(State(state): State<Arc<AppState>>,
                            request: Request) -> AppResult<impl IntoResponse> {
    let token = check_access_right(state.access_provider.deref(), &request, AccessRight::Upload)?;

    let layer: Layer = decode_json(request).await?;

    let mut pending_uploads = state.pending_uploads.lock().await;

    let image_manager = create_image_manager(&state, &token);
    if image_manager.get_layer(&Reference::ImageId(layer.hash.clone())).is_ok() {
        return Ok(
            Json(json!(
                UploadLayerResponse {
                    status: UploadStatus::AlreadyExist,
                    upload_id: None
                }
            ))
        );
    }

    if let Some(existing) = pending_uploads.get(&layer.hash) {
        if existing.started.elapsed().as_secs_f64() < state.config.pending_upload_expiration {
            return Ok(
                Json(json!(
                    UploadLayerResponse {
                        status: UploadStatus::UploadingPending,
                        upload_id: None
                    }
                ))
            );
        }
    }

    if !layer.verify_valid_paths(image_manager.config().base_folder()) {
        return Ok(
            Json(json!(
                UploadLayerResponse {
                    status: UploadStatus::InvalidPaths,
                    upload_id: None
                }
            ))
        );
    }

    let upload_id = uuid::Uuid::new_v4().to_string();
    let hash = layer.hash.clone();
    pending_uploads.insert(
        layer.hash.clone(),
        PendingUpload {
            upload_id: upload_id.clone(),
            layer,
            started: Instant::now()
        }
    );

    info!("Beginning upload of layer {} (id: {})", hash, upload_id);

    Ok(
        Json(
            json!(
                UploadLayerResponse {
                    status: UploadStatus::Started,
                    upload_id: Some(upload_id.clone())
                }
            )
        )
    )
}

async fn end_layer_upload(State(state): State<Arc<AppState>>,
                          request: Request) -> AppResult<impl IntoResponse> {
    let token = check_access_right(state.access_provider.deref(), &request, AccessRight::Upload)?;
    let upload_id = get_upload_id(&request, &token)?;

    let mut pending_uploads = state.pending_uploads.lock().await;
    let pending_upload = remove_pending_upload_by_id(&mut pending_uploads, &upload_id)?;

    let mut image_manager = create_image_manager(&state, &token);

    if !pending_upload.layer.verify_path_exists(image_manager.config().base_folder()) {
        info!("Incomplete upload of layer {} (id: {}) - clearing pending.", pending_upload.layer.hash, upload_id);

        return Ok(
            Json(json!(
                UploadLayerResponse {
                    status: UploadStatus::IncompleteUpload,
                    upload_id: None
                }
            ))
        );
    }

    image_manager.insert_layer(pending_upload.layer.clone())?;

    info!("Finished upload of layer {} (id: {})", pending_upload.layer.hash, upload_id);

    Ok(
        Json(
            json!(
                UploadLayerResponse {
                    status: UploadStatus::Finished,
                    upload_id: None
                }
            )
        )
    )
}

async fn upload_layer_file(State(state): State<Arc<AppState>>,
                           Path((layer_hash, file_index)): Path<(ImageId, usize)>,
                           request: Request) -> AppResult<impl IntoResponse> {
    let token = check_access_right(state.access_provider.deref(), &request, AccessRight::Upload)?;
    let upload_id = get_upload_id(&request, &token)?;

    let (layer, base_folder) = {
        let pending_uploads = state.pending_uploads.lock().await;
        let pending_upload = get_pending_upload_by_id(&pending_uploads, &upload_id)?;

        if layer_hash != pending_upload.layer.hash {
            return Err(AppError::LayerFileNotFound);
        }

        let layer = pending_upload.layer.clone();
        let base_folder = state.config.image_manager_config().base_folder().to_path_buf();

        (layer, base_folder)
    };

    if let Some(operation) = layer.get_file_operation(file_index) {
        if let LayerOperation::File { source_path, content_hash, .. } = operation {
            let abs_source_path = base_folder.join(source_path);

            let temp_file_path = abs_source_path.to_str().unwrap().to_owned() + ".tmp";
            let temp_file_path = std::path::Path::new(&temp_file_path).to_path_buf();
            if let Some(parent) = temp_file_path.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }

            let mut file = tokio::fs::File::create(&temp_file_path).await?;

            let mut stream = request.into_body().into_data_stream();
            let mut content_hasher = ContentHash::new();
            while let Some(chunk) = stream.next().await {
                match chunk {
                    Ok(chunk) => {
                        content_hasher.add(chunk.as_ref());
                        file.write_all(chunk.as_ref()).await?;
                    }
                    Err(err) => {
                        tokio::fs::remove_file(&temp_file_path).await?;
                        return Err(AppError::FailedToUploadLayerFile(err.to_string()));
                    }
                }
            }

            if &content_hasher.finalize() != content_hash {
                return Err(AppError::FailedToUploadLayerFile("Invalid content hash".to_string()));
            }

            tokio::fs::rename(&temp_file_path, abs_source_path).await?;

            info!("Uploaded layer file: {}:{}", layer.hash, file_index);
            return Ok(Json(json!({ "status": "uploaded" })));
        }
    }

    Err(AppError::LayerFileNotFound)
}

async fn sync_with_upstream(state: Arc<AppState>, upstream_config: &RegistryUpstreamConfig) {
    let upstream_address = upstream_config.address.to_string();
    info!("Syncing with upstream {}...", upstream_address);

    async fn internal(state: Arc<AppState>,
                      upstream_config: &RegistryUpstreamConfig,
                      upstream_address: &str) -> Result<(), AppError> {
        let t0 = Instant::now();
        let mut image_manager = create_image_manager(&state, &AuthToken);
        image_manager.login(&upstream_address, &upstream_config.username, &upstream_config.password).await?;
        let result = image_manager.sync(&upstream_address, Some(&state.config.address.to_string())).await?;
        info!(
            "Downloaded {} images, {} layers from upstream in {:.1} seconds.",
            result.downloaded_images,
            result.downloaded_layers,
            t0.elapsed().as_secs_f64()
        );

        Ok(())
    }

    if let Err(err) = internal(state.clone(), &upstream_config, &upstream_address).await {
        error!("Syncing with upstream failed due to: {:?}.", err);
    }
}

fn get_upload_id(request: &Request, _token: &AuthToken) -> AppResult<String> {
    request.headers()
        .get("UPLOAD_ID").map(|x| x.to_str().ok()).flatten()
        .map(|x| x.to_owned())
        .ok_or_else(|| AppError::UploadIdNotSpecified)
}

fn get_pending_upload_by_id<'a>(pending_uploads: &'a PendingUploads, upload_id: &str) -> AppResult<&'a PendingUpload> {
    for pending_upload in pending_uploads.values() {
        if pending_upload.upload_id == upload_id {
            return Ok(pending_upload);
        }
    }

    Err(AppError::InvalidUploadId)
}

fn remove_pending_upload_by_id(pending_uploads: &mut PendingUploads, upload_id: &str) -> AppResult<PendingUpload> {
    for pending_upload in pending_uploads.values() {
        if pending_upload.upload_id == upload_id {
            let hash = pending_upload.layer.hash.clone();
            return pending_uploads.remove(&hash).ok_or_else(|| AppError::InvalidUploadId);
        }
    }

    Err(AppError::InvalidUploadId)
}

fn create_image_manager(state: &AppState, _token: &AuthToken) -> ImageManager {
    ImageManager::with_config(
        state.config.image_manager_config(),
        EmptyPrinter::new()
    ).unwrap()
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