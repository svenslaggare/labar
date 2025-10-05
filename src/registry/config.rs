use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use croner::Cron;
use serde::{Deserialize, Serialize};

use crate::image_manager::ImageManagerConfig;
use crate::registry::auth::{UsersSpec};

#[derive(Debug, Serialize,  Deserialize)]
pub struct RegistryConfig {
    pub data_path: PathBuf,
    #[serde(default="default_storage_mode")]
    pub storage_mode: StorageMode,
    
    pub address: SocketAddr,

    #[serde(default="default_pending_upload_expiration")]
    pub pending_upload_expiration: f64,

    pub ssl_cert_path: Option<PathBuf>,
    pub ssl_key_path: Option<PathBuf>,

    pub upstream: Option<RegistryUpstreamConfig>,

    #[serde(default)]
    pub initial_users: UsersSpec
}

impl RegistryConfig {
    pub fn can_pull_through_upstream(&self) -> bool {
        match self.upstream.as_ref() {
            Some(upstream) => upstream.pull_through,
            None => false,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum StorageMode {
    Uncompressed,
    Compressed
}

fn default_storage_mode() -> StorageMode {
    StorageMode::Uncompressed
}

fn default_pending_upload_expiration() -> f64 {
    30.0 * 60.0
}

#[derive(Debug, Serialize,  Deserialize)]
pub struct RegistryUpstreamConfig {
    pub hostname: String,
    pub username: String,
    pub password: String,

    #[serde(default="default_sync")]
    pub sync: bool,
    #[serde(default="default_sync_at_startup")]
    pub sync_at_startup: bool,
    #[serde(default="default_sync_interval")]
    pub sync_interval: Cron,

    #[serde(default)]
    pub pull_through: bool
}

fn default_sync() -> bool {
    true
}

fn default_sync_at_startup() -> bool {
    true
}

fn default_sync_interval() -> Cron {
    Cron::from_str("* * * * *").unwrap()
}

impl RegistryConfig {
    pub fn load_from_file(path: &Path) -> Result<RegistryConfig, String> {
        let content = std::fs::read_to_string(path).map_err(|err| format!("{}", err))?;
        toml::from_str(&content).map_err(|err| format!("{}", err))
    }

    pub fn image_manager_config(&self) -> ImageManagerConfig {
        ImageManagerConfig::with_base_folder(self.data_path.clone())
    }
}