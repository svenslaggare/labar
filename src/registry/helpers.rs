use std::path::PathBuf;

use serde::de::DeserializeOwned;

use axum::extract::{FromRequest, Request};
use axum::Json;
use axum::response::IntoResponse;

use crate::image::Layer;
use crate::image_manager::{EmptyPrinter, ImageManager, ImageManagerError, StateSession};
use crate::registry::{model, AppState, RegistryConfig, RunRegistryError};
use crate::registry::auth::AuthToken;
use crate::registry::model::{AppError, AppResult};

pub fn get_certificate(config: &RegistryConfig) -> Result<(PathBuf, PathBuf), RunRegistryError> {
    use log::info;
    use rcgen::CertifiedKey;
    use crate::registry::RunRegistryError::*;

    match (config.ssl_cert_path.as_ref(), config.ssl_key_path.as_ref()) {
        (Some(cert_path), Some(key_path)) => {
            info!("Using specified SSL certificate.");
            Ok((cert_path.clone(), key_path.clone()))
        }
        _ => {
            let cert_path = config.data_path.join("cert.pem");
            let key_path = config.data_path.join("key.pem");

            if !cert_path.exists() || !key_path.exists() {
                info!("Generating SSL certificate...");
                let subject_alt_names = vec!["localhost".to_string()];
                let CertifiedKey { cert, signing_key } = rcgen::generate_simple_self_signed(subject_alt_names)
                    .map_err(|err| FailedGenerateCertificate { reason: err.to_string() })?;

                std::fs::create_dir_all(&config.data_path).map_err(|err| FailedGenerateCertificate { reason: err.to_string() })?;
                std::fs::write(&cert_path, cert.pem()).map_err(|err| FailedGenerateCertificate { reason: err.to_string() })?;
                std::fs::write(&key_path, signing_key.serialize_pem()).map_err(|err| FailedGenerateCertificate { reason: err.to_string() })?;
            }

            info!("Using auto-generated SSL certificate.");

            Ok((cert_path, key_path))
        }
    }
}

pub fn get_upload_id(request: &Request, _token: &AuthToken) -> AppResult<String> {
    request.headers()
        .get(model::UPLOAD_ID_HEADER).map(|x| x.to_str().ok()).flatten()
        .map(|x| x.to_owned())
        .ok_or_else(|| AppError::UploadIdNotSpecified)
}

pub fn get_pending_upload_layer_by_id(state_session: &StateSession, upload_id: &str) -> AppResult<Layer> {
    let pending_upload_layer = state_session.registry_get_layer_upload_by_id(
        upload_id
    ).map_err(|err| ImageManagerError::Sql(err))?;
    pending_upload_layer.ok_or_else(|| AppError::InvalidUploadId)
}

pub fn create_image_manager(state: &AppState, _token: &AuthToken) -> ImageManager {
    ImageManager::new(
        state.config.image_manager_config(),
        EmptyPrinter::new()
    ).unwrap()
}

pub async fn decode_json<T: DeserializeOwned>(request: Request) -> AppResult<T> {
    let value = Json::<T>::from_request(request, &()).await.map_err(|err| AppError::Other(err.into_response()))?;
    Ok(value.0)
}