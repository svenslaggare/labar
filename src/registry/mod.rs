use std::fmt::{Display, Formatter};
use std::ops::Deref;
use std::sync::Arc;
use std::time::Instant;

use chrono::Local;
use log::{debug, error, info};

use rand::distr::{Alphanumeric, SampleString};

use serde_json::json;
use serde::Deserialize;

use futures::StreamExt;
use tokio_util::io::ReaderStream;
use tokio::io::AsyncWriteExt;

use axum::response::{IntoResponse, Response};
use axum::{Json, Router};
use axum::body::Body;
use axum::extract::{DefaultBodyLimit, Path, Query, Request, State};
use axum::http::HeaderValue;
use axum::routing::{delete, get, post};
use axum_server::tls_rustls::RustlsConfig;

pub mod model;
pub mod auth;
pub mod config;
mod helpers;
mod external_storage;
mod state;

use crate::content::ContentHash;
use crate::helpers::{DeferredFileDelete};
use crate::image::{Layer, LayerOperation};
use crate::image_manager::{ImageManagerError, StorageMode};
use crate::reference::{ImageId, ImageTag, Reference};

use crate::registry::state::AppState;
use crate::registry::config::RegistryConfig;
use crate::registry::auth::{authenticate, check_access_right, generate_jwt_token, AccessRight, AuthToken};
use crate::registry::config::RegistryUpstreamConfig;
use crate::registry::external_storage::{BoxExternalStorage};
use crate::registry::model::{AppError, AppResult, ImageSpec, LayerExists, UploadLayerResponse, UploadStatus};

pub async fn run(config: RegistryConfig) -> Result<(), RunRegistryError> {
    if let Err(err) = setup_logging() {
        println!("Failed to setting up logging: {}", err);
    }

    let (cert_path, key_path) = helpers::get_certificate(&config)?;
    let tls_config = RustlsConfig::from_pem_chain_file(cert_path, key_path).await
        .map_err(|err| RunRegistryError::InvalidCertificate { reason: err.to_string() })?;

    let sign_in_path = config.data_path.join("sign_key");
    let sign_key = if !sign_in_path.exists() {
        let sign_key = Alphanumeric.sample_string(&mut rand::rng(), 32);
        std::fs::write(
            sign_in_path,
            &sign_key
        ).map_err(|err| RunRegistryError::AuthSetup { reason: err.to_string() })?;
        sign_key
    } else {
        std::fs::read_to_string(
            sign_in_path
        ).map_err(|err| RunRegistryError::AuthSetup { reason: err.to_string() })?
    };

    let payload_max_size = config.payload_max_size;
    let state = AppState::new(config, &sign_key)?;

    let app = Router::new()
        .route("/", get(index))

        .route("/sign-in", get(sign_in))

        .route("/images", get(list_images))
        .route("/images", post(set_image))
        .route("/images/{*tag}", get(resolve_image))
        .route("/images/{*tag}", delete(remove_image))

        .route("/layers/{layer}/exists", get(get_layer_exists))
        .route("/layers/{layer}/manifest", get(get_layer_manifest))
        .route("/layers/{layer}/download/{index}", get(download_layer))
        .route("/layers/begin-upload", post(begin_layer_upload))
        .route("/layers/end-upload", post(end_layer_upload))
        .route("/layers/{layer}/upload/{index}", post(upload_layer_file))
        .layer(DefaultBodyLimit::max(payload_max_size))
        .with_state(state.clone())
    ;

    if let Some(upstream_config) = state.config.upstream.as_ref() {
        let mut image_manager = helpers::create_image_manager(&state, &AuthToken);
        image_manager.login(
            &upstream_config.hostname,
            &upstream_config.username,
            &upstream_config.password
        ).await.map_err(|err| RunRegistryError::LoginUpstream { reason: err.to_string() })?;
    }

    let state_clone = state.clone();
    tokio::spawn(async move {
        if let Some(upstream_config) = state_clone.config.upstream.as_ref() {
            if upstream_config.sync {
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
        }
    });

    info!("Running at https://{}", state.config.address);
    axum_server::bind_rustls(state.config.address, tls_config)
        .serve(app.into_make_service())
        .await
        .map_err(|err| RunRegistryError::FailedRunServer { reason: err.to_string() })?;

    Ok(())
}

#[derive(Debug, Clone)]
pub enum RunRegistryError {
    FailedGenerateCertificate { reason: String },
    InvalidCertificate { reason: String },
    AuthSetup { reason: String },
    LoginUpstream { reason: String },
    FailedRunServer { reason: String }
}

impl Display for RunRegistryError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RunRegistryError::FailedGenerateCertificate { reason } => write!(f, "Failed to generate certificate due to: {}", reason),
            RunRegistryError::InvalidCertificate { reason } => write!(f, "Invalid certificate specified: {}", reason),
            RunRegistryError::AuthSetup { reason } => write!(f, "Failed to setup authentication due to: {}", reason),
            RunRegistryError::LoginUpstream { reason } => write!(f, "Failed to login upstream due to: {}", reason),
            RunRegistryError::FailedRunServer { reason } => write!(f, "Failed to run server due to: {}", reason)
        }
    }
}

struct ChangedUploadLayerFileOperation {
    file_index: usize,
    operation: LayerOperation
}

async fn index() -> AppResult<impl IntoResponse> {
    Ok("Labar registry")
}

async fn sign_in(State(state): State<Arc<AppState>>,
                 request: Request) -> AppResult<impl IntoResponse> {
    let authenticated = authenticate(state.access_provider.deref(), &request)?;
    generate_jwt_token(&state.config, &state.sign_key, authenticated)
}

async fn list_images(State(state): State<Arc<AppState>>,
                     request: Request) -> AppResult<impl IntoResponse> {
    let token = check_access_right(&request, &state.sign_key, AccessRight::List)?;

    let image_manager = state.pooled_image_manager(&token);
    let images = image_manager.list_images(None)?;

    Ok(Json(json!(images)).into_response())
}

async fn set_image(State(state): State<Arc<AppState>>,
                   request: Request) -> AppResult<impl IntoResponse> {
    let token = check_access_right(&request, &state.sign_key, AccessRight::Upload)?;

    let spec: ImageSpec = helpers::decode_json(request).await?;

    let mut image_manager = state.pooled_image_manager(&token);
    image_manager.tag_image(&Reference::ImageId(spec.hash.clone()), &spec.tag)?;

    info!("Uploaded image: {} ({})", spec.tag, spec.hash);
    Ok(())
}

#[derive(Deserialize)]
struct ResolveImageQuery {
    #[serde(default)]
    can_pull_through: bool
}

async fn resolve_image(State(state): State<Arc<AppState>>,
                       Path(tag): Path<ImageTag>,
                       Query(query): Query<ResolveImageQuery>,
                       request: Request) -> AppResult<impl IntoResponse> {
    let token = check_access_right(&request, &state.sign_key, AccessRight::List)?;

    let image_manager = state.pooled_image_manager(&token);
    let image = match image_manager.resolve_image(&tag) {
        Ok(image) => image,
        Err(ImageManagerError::ReferenceNotFound { .. }) if state.config.can_pull_through_upstream() && query.can_pull_through => {
            let upstream_config = state.config.upstream.as_ref().unwrap();

            info!("Pulling image {} from upstream.", &tag);
            let upstream_tag = tag.clone().set_registry(&upstream_config.hostname);
            let image = image_manager.resolve_image_in_registry(&upstream_config.hostname, &upstream_tag).await?;

            // Delay image insert until layer has been pulled
            state.delayed_image_inserts.lock().await.entry(image.image.hash.clone())
                .or_insert_with(|| Vec::new())
                .push(image.image.clone().replace_tag(tag));

            image
        }
        err => {
            err?
        }
    };

    Ok(Json(json!(image)))
}

async fn remove_image(State(state): State<Arc<AppState>>,
                      Path(tag): Path<ImageTag>,
                      request: Request) -> AppResult<impl IntoResponse> {
    let token = check_access_right(&request, &state.sign_key, AccessRight::Delete)?;

    let mut image_manager = state.pooled_image_manager(&token);
    let removed_layers = image_manager.remove_image(&tag)?;
    let mut num_deleted_layers = 0;
    if let Some(external_storage) = state.external_storage.as_ref() {
        for hash in removed_layers {
            if external_storage.remove_layer(&hash).await? > 0 {
                num_deleted_layers += 1;
            }
        }
    } else {
        num_deleted_layers = removed_layers.len();
    }

    state.clear_layer_cache().await;

    info!("Removed image: {} ({} layers removed)", tag, num_deleted_layers);

    Ok(Json(json!({ "status": "success", "num_deleted_layers": num_deleted_layers })))
}

async fn get_layer_exists(State(state): State<Arc<AppState>>,
                          Path(hash): Path<ImageId>,
                          request: Request) -> AppResult<impl IntoResponse> {
    let token = check_access_right(&request, &state.sign_key, AccessRight::Download)?;

    let image_manager = state.pooled_image_manager(&token);
    let exists = match state.get_layer(&image_manager, &hash).await {
        Ok(_) => true,
        Err(ImageManagerError::ReferenceNotFound { .. }) => false,
        Err(err) => { return Err(err.into()); }
    };

    Ok(Json(json!(LayerExists { exists })))
}

#[derive(Deserialize)]
struct LayerManifestQuery {
    #[serde(default)]
    can_pull_through: bool
}

async fn get_layer_manifest(State(state): State<Arc<AppState>>,
                            Path(hash): Path<ImageId>,
                            Query(query): Query<LayerManifestQuery>,
                            request: Request) -> AppResult<Response> {
    let token = check_access_right(&request, &state.sign_key, AccessRight::Download)?;

    let image_manager = state.pooled_image_manager(&token);
    match state.get_layer(&image_manager, &hash).await {
        Ok(layer) => Ok(Json(json!(layer.as_ref())).into_response()),
        Err(ImageManagerError::ReferenceNotFound { .. }) if query.can_pull_through && state.config.can_pull_through_upstream() => {
            let upstream_config = state.config.upstream.as_ref().unwrap();
            let layer = image_manager.get_layer_from_registry(&upstream_config.hostname, &hash).await?;

            // Download from upstream in the background
            tokio::spawn(pull_from_upstream(state.clone(), layer.clone()));

            let mut response = Json(json!(layer)).into_response();
            response.headers_mut().insert(model::PULL_THROUGH_HEADER, HeaderValue::from_static("true"));
            Ok(response)
        },
        Err(err) => Err(err.into())
    }
}

async fn download_layer(State(state): State<Arc<AppState>>,
                        Path((hash, file_index)): Path<(ImageId, usize)>,
                        request: Request) -> AppResult<impl IntoResponse> {
    let token = check_access_right(&request, &state.sign_key, AccessRight::Download)?;

    let (layer, base_folder) = {
        let image_manager = state.pooled_image_manager(&token);
        let layer = state.get_layer(&image_manager, &hash).await?;
        let base_folder = image_manager.config().base_folder().to_path_buf();

        (layer, base_folder)
    };

    if let Some(operation) = layer.get_file_operation(file_index) {
        match operation {
            LayerOperation::File { source_path, .. } | LayerOperation::CompressedFile { source_path, .. } => {
                return match state.external_storage.as_ref() {
                    Some(external_storage) => {
                        external_storage.download(source_path).await
                    }
                    None => {
                        let source_path = std::path::Path::new(source_path);
                        let abs_source_path = base_folder.join(source_path);

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
                }
            }
            LayerOperation::Image { .. } => {}
            LayerOperation::Directory { .. } => {}
            LayerOperation::Label { .. } => {}
        }
    }

    Err(AppError::LayerFileNotFound)
}

async fn begin_layer_upload(State(state): State<Arc<AppState>>,
                            request: Request) -> AppResult<impl IntoResponse> {
    let token = check_access_right(&request, &state.sign_key, AccessRight::Upload)?;

    let layer: Layer = helpers::decode_json(request).await?;

    let image_manager = state.pooled_image_manager(&token);
    if state.get_layer(&image_manager, &layer.hash).await.is_ok() {
        return Ok(
            Json(json!(
                UploadLayerResponse {
                    status: UploadStatus::AlreadyExist,
                    upload_id: None
                }
            ))
        );
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

    let mut state_session = image_manager.pooled_state_session()?;
    let upload_result = state_session.registry_try_start_layer_upload(
        Local::now(),
        &layer,
        &upload_id,
        state.config.pending_upload_expiration
    ).map_err(|err| ImageManagerError::Sql(err))?;

    if !upload_result {
        return Ok(
            Json(json!(
                UploadLayerResponse {
                    status: UploadStatus::UploadingPending,
                    upload_id: None
                }
            ))
        );
    }

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
    let token = check_access_right(&request, &state.sign_key, AccessRight::Upload)?;
    let upload_id = helpers::get_upload_id(&request, &token)?;

    let image_manager = state.pooled_image_manager(&token);
    let mut state_session = image_manager.pooled_state_session()?;

    let mut pending_upload_layer = helpers::get_pending_upload_layer_by_id(&state_session, &upload_id)?;
    let pending_upload_layer_hash = pending_upload_layer.hash.clone();

    let exists = match state.external_storage.as_ref() {
        Some(external_storage) => {
            verify_path_exists_external_storage(external_storage, &pending_upload_layer).await?
        }
        None => {
            pending_upload_layer.verify_path_exists(image_manager.config().base_folder())
        }
    };

    if !exists {
        info!("Incomplete upload of layer {} (id: {}) - clearing pending.", pending_upload_layer_hash, upload_id);
        state_session.registry_remove_upload(
            &upload_id
        ).map_err(|err| ImageManagerError::Sql(err))?;

        state.remove_pending_upload_layer_by_id(&upload_id).await;

        return Ok(
            Json(
                json!(
                    UploadLayerResponse {
                        status: UploadStatus::IncompleteUpload,
                        upload_id: None
                    }
                )
            )
        );
    }


    if let Some(changed_operations) = state.take_changed_pending_upload_layer_operations(&upload_id).await {
        for new_operation in changed_operations {
            if let Some(existing_operation) = pending_upload_layer.get_file_operation_mut(new_operation.file_index) {
                *existing_operation = new_operation.operation;
            }
        }
    }

    let pending_upload_layer_hash = pending_upload_layer.hash.clone();
    state_session.registry_end_layer_upload(
        pending_upload_layer
    ).map_err(|err| ImageManagerError::Sql(err))?;

    state.remove_pending_upload_layer_by_id(&upload_id).await;

    info!("Finished upload of layer {} (id: {})", pending_upload_layer_hash, upload_id);

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

async fn verify_path_exists_external_storage(external_storage: &BoxExternalStorage, layer: &Layer) -> AppResult<bool> {
    for operation in &layer.operations {
        match operation {
            LayerOperation::Image { .. } => {}
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

async fn upload_layer_file(State(state): State<Arc<AppState>>,
                           Path((layer_hash, file_index)): Path<(ImageId, usize)>,
                           request: Request) -> AppResult<impl IntoResponse> {
    let token = check_access_right(&request, &state.sign_key, AccessRight::Upload)?;
    let upload_id = helpers::get_upload_id(&request, &token)?;

    let (layer, base_folder) = {
        let image_manager = state.pooled_image_manager(&token);
        let state_session = image_manager.pooled_state_session()?;

        let pending_upload_layer = state.get_pending_upload_layer_by_id(state_session, &upload_id).await?;
        if layer_hash != pending_upload_layer.hash {
            return Err(AppError::LayerFileNotFound);
        }

        let base_folder = state.config.image_manager_config().base_folder().to_path_buf();

        (pending_upload_layer, base_folder)
    };

    if let Some(operation) = layer.get_file_operation(file_index) {
        match operation {
            LayerOperation::File { source_path, content_hash, .. } | LayerOperation::CompressedFile { source_path, content_hash, .. } => {
                let abs_source_path = base_folder.join(source_path);

                let temp_file_path = base_folder.join("tmp-upload").join(Alphanumeric.sample_string(&mut rand::rng(), 64));
                if let Some(parent) = temp_file_path.parent() {
                    tokio::fs::create_dir_all(parent).await?;
                }

                let compressed_content_hash = operation.compressed_content_hash();

                let mut deferred_delete = DeferredFileDelete::new(temp_file_path.clone());
                let mut content_hasher = ContentHash::new();

                {
                    let mut file = tokio::fs::File::create(&temp_file_path).await?;
                    let mut stream = request.into_body().into_data_stream();
                    while let Some(chunk) = stream.next().await {
                        match chunk {
                            Ok(chunk) => {
                                content_hasher.add(chunk.as_ref());
                                file.write_all(chunk.as_ref()).await?;
                            }
                            Err(err) => {
                                return Err(AppError::FailedToUploadLayerFile(err.to_string()));
                            }
                        }
                    }

                    file.flush().await?;
                }

                let check_content_hash = compressed_content_hash.unwrap_or(content_hash);
                if &content_hasher.finalize() != check_content_hash {
                    return Err(AppError::FailedToUploadLayerFile("Invalid content hash".to_string()));
                }

                let compress_result = match state.config.storage_mode {
                    StorageMode::AlwaysCompressed | StorageMode::PreferCompressed => {
                        state.pooled_image_manager(&token).compress_operation(
                            operation,
                            Some(temp_file_path.clone()),
                            state.config.storage_mode == StorageMode::AlwaysCompressed,
                        )?
                    }
                    StorageMode::AlwaysUncompressed => {
                        state.pooled_image_manager(&token).decompress_operation(
                            operation,
                            Some(temp_file_path.clone())
                        )?
                    }
                    StorageMode::PreferUncompressed => None
                };

                if let Some((temp_compress_path, _, new_operation)) = compress_result {
                    tokio::fs::rename(&temp_compress_path, &temp_file_path).await?;
                    state.changed_pending_upload_layer_operations(
                        &upload_id,
                        file_index,
                        new_operation
                    ).await;
                }

                if let Some(external_storage) = state.external_storage.as_ref() {
                    external_storage.upload(source_path, &temp_file_path).await?;
                } else {
                    if let Some(parent) = abs_source_path.parent() {
                        tokio::fs::create_dir_all(parent).await?;
                    }

                    tokio::fs::rename(&temp_file_path, abs_source_path).await?;
                    deferred_delete.skip();
                }

                debug!("Uploaded layer file: {}:{}", layer.hash, file_index);
                return Ok(Json(json!({ "status": "uploaded" })));
            }
            LayerOperation::Image { .. } => {}
            LayerOperation::Directory { .. } => {}
            LayerOperation::Label { .. } => {}
        }
    }

    Err(AppError::LayerFileNotFound)
}

async fn sync_with_upstream(state: Arc<AppState>, upstream_config: &RegistryUpstreamConfig) {
    async fn internal(state: Arc<AppState>, upstream_config: &RegistryUpstreamConfig) -> Result<(), AppError> {
        let t0 = Instant::now();
        let mut image_manager = helpers::create_image_manager(&state, &AuthToken);

        let result = image_manager.sync(
            &upstream_config.hostname,
            Some(&state.config.address.to_string()),
            |state_session, layer| {
                state_session.registry_try_start_layer_upload(
                    Local::now(),
                    layer,
                    &uuid::Uuid::new_v4().to_string(),
                    state.config.pending_upload_expiration
                ).unwrap_or(false)
            },
            |state_session, layer| {
                state_session.registry_end_layer_upload(layer).unwrap_or(false)
            }
        ).await?;

        info!(
            "Downloaded {} images, {} layers from upstream in {:.1} seconds.",
            result.downloaded_images,
            result.downloaded_layers,
            t0.elapsed().as_secs_f64()
        );

        Ok(())
    }

    info!("Syncing with upstream {}...", &upstream_config.hostname);

    if let Err(err) = internal(state.clone(), &upstream_config).await {
        error!("Syncing with upstream failed due to: {:?}.", err);
    }
}

async fn pull_from_upstream(state: Arc<AppState>, layer: Layer) {
    async fn internal(state: Arc<AppState>,
                      upstream_config: &RegistryUpstreamConfig,
                      layer: Layer) -> Result<(), AppError> {
        let mut image_manager = helpers::create_image_manager(&state, &AuthToken);

        let mut state_session = image_manager.pooled_state_session()?;
        let started = state_session.registry_try_start_layer_upload(
            Local::now(),
            &layer,
            &uuid::Uuid::new_v4().to_string(),
            state.config.pending_upload_expiration
        ).map_err(|err| ImageManagerError::Sql(err))?;

        if !started {
            info!("Layer {} already being pulled from upstream.", &layer.hash);
        }

        let layer = image_manager.pull_layer(&upstream_config.hostname, &layer.hash).await?;
        let layer_hash = layer.hash.clone();
        state_session.registry_end_layer_upload(layer).map_err(|err| ImageManagerError::Sql(err))?;

        if let Some(images) = state.delayed_image_inserts.lock().await.remove(&layer_hash) {
            for image in images {
                if let Err(err) = image_manager.insert_or_replace_image(image) {
                    error!("Failed to insert image after pulling from upstream due to: {}.", err);
                }
            }
        }

        Ok(())
    }

    if let Some(upstream_config) = state.config.upstream.as_ref() {
        info!("Pulling layer {} from upstream.", layer.hash);

        if let Err(err) = internal(state.clone(), upstream_config, layer).await {
            error!("Pulling from upstream failed due to: {:?}.", err);
        }
    }
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