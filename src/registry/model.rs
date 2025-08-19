use axum::body::Body;
use axum::http::StatusCode;
use axum::Json;
use axum::response::{IntoResponse, Response};

use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::image_manager::ImageManagerError;
use crate::reference::{ImageId, ImageTag};

pub type AppResult<T> = Result<T, AppError>;

#[derive(Debug)]
pub enum AppError {
    ImagerManager(ImageManagerError),
    LayerFileNotFound,
    LayerFileAlreadyExists,
    FailedToUploadLayerFile(String),
    UploadIdNotSpecified,
    InvalidUploadId,
    InvalidImageReference(String),
    Unauthorized,
    IO(std::io::Error),
    Other(Response)
}

#[derive(Serialize, Deserialize)]
pub struct AppErrorResponse {
    pub error: String
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        match self {
            AppError::ImagerManager(err) => {
                match err {
                    err @ ImageManagerError::ReferenceNotFound { .. } => {
                        (
                            StatusCode::NOT_FOUND,
                            Json(json!(AppErrorResponse { error: format!("{}", err) }))
                        ).into_response()
                    }
                    err => {
                        (
                            StatusCode::BAD_REQUEST,
                            Json(json!(AppErrorResponse { error: format!("{}", err) }))
                        ).into_response()
                    }
                }
            }
            AppError::LayerFileNotFound => {
                (
                    StatusCode::NOT_FOUND,
                    Json(json!(AppErrorResponse { error: "Layer file not found".to_owned() }))
                ).into_response()
            }
            AppError::LayerFileAlreadyExists => {
                (
                    StatusCode::BAD_REQUEST,
                    Json(json!(AppErrorResponse { error: "The layer file already exists".to_owned() }))
                ).into_response()
            }
            AppError::FailedToUploadLayerFile(err) => {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!(AppErrorResponse { error: format!("Failed to upload layer file: {}", err) }))
                ).into_response()
            }
            AppError::UploadIdNotSpecified => {
                (
                    StatusCode::BAD_REQUEST,
                    Json(json!(AppErrorResponse { error: "Upload id not specified".to_owned() }))
                ).into_response()
            }
            AppError::InvalidUploadId => {
                (
                    StatusCode::BAD_REQUEST,
                    Json(json!(AppErrorResponse { error: "Invalid upload id".to_owned() }))
                ).into_response()
            }
            AppError::InvalidImageReference(err) => {
                (
                    StatusCode::BAD_REQUEST,
                    Json(json!(AppErrorResponse { error: format!("{}", err) }))
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
                    Json(json!(AppErrorResponse { error: format!("I/O error: {}", err)}))
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
pub struct LayerExists {
    pub exists: bool
}

#[derive(Serialize, Deserialize)]
pub struct UploadLayerResponse {
    pub status: UploadStatus,
    pub upload_id: Option<String>
}

#[derive(Serialize, Deserialize, PartialEq, Eq)]
pub enum UploadStatus {
    #[serde(rename="already_exist")]
    AlreadyExist,
    #[serde(rename="upload_pending")]
    UploadingPending,
    #[serde(rename="invalid_paths")]
    InvalidPaths,
    #[serde(rename="started")]
    Started,
    #[serde(rename="finished")]
    Finished,
    #[serde(rename="incomplete_upload")]
    IncompleteUpload
}

#[derive(Serialize, Deserialize)]
pub struct ImageSpec {
    pub hash: ImageId,
    pub tag: ImageTag
}

pub const UPLOAD_ID_HEADER: &str = "UPLOAD-ID";
pub const PULL_THROUGH_HEADER: &str = "PULL-THROUGH";