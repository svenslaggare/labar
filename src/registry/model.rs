use axum::body::Body;
use axum::http::StatusCode;
use axum::Json;
use axum::response::{IntoResponse, Response};

use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::image_manager::ImageManagerError;
use crate::reference::{ImageId, ImageTag};

pub type AppResult<T> = Result<T, AppError>;

pub enum AppError {
    ImagerManager(ImageManagerError),
    LayerFileNotFound,
    LayerFileAlreadyExists,
    FailedToUploadLayerFile(String),
    InvalidImageReference(String),
    Unauthorized,
    IO(std::io::Error),
    Other(Response)
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        match self {
            AppError::ImagerManager(err) => {
                match err {
                    err @ ImageManagerError::ImageNotFound { .. } => {
                        (
                            StatusCode::NOT_FOUND,
                            Json(json!({ "error": format!("Image not found due to: {}", err) }))
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
            AppError::LayerFileAlreadyExists => {
                (
                    StatusCode::BAD_REQUEST,
                    Json(json!({ "error": "The layer file already exists" }))
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
            AppError::Unauthorized => {
                Response::builder()
                    .status(StatusCode::UNAUTHORIZED)
                    .header(reqwest::header::WWW_AUTHENTICATE, "Basic realm=registry")
                    .body(Body::empty()).unwrap()
            }
            AppError::IO(err) => {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({ "error": format!("I/O error: {}", err) }))
                ).into_response()
            }
            AppError::Other(response) => {
                response
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

#[derive(Serialize, Deserialize)]
pub struct UploadLayerManifestResult {
    pub status: UploadLayerManifestStatus
}

#[derive(Serialize, Deserialize)]
pub enum UploadLayerManifestStatus {
    #[serde(rename="already_exist")]
    AlreadyExist,
    #[serde(rename="uploaded")]
    Uploaded
}

#[derive(Serialize, Deserialize)]
pub struct ImageSpec {
    pub hash: ImageId,
    pub tag: ImageTag
}