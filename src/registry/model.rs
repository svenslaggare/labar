use serde::{Deserialize, Serialize};

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
    pub hash: String,
    pub tag: String
}