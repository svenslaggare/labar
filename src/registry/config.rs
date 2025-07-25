use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use toml_edit::DocumentMut;

use crate::image_manager::ImageManagerConfig;
use crate::registry::auth::{AccessRight, Password, UsersSpec};

#[derive(Debug, Serialize,  Deserialize)]
pub struct RegistryConfig {
    pub data_path: PathBuf,

    pub address: SocketAddr,

    #[serde(default="default_pending_upload_expiration")]
    pub pending_upload_expiration: f64,

    pub ssl_cert_path: Option<PathBuf>,
    pub ssl_key_path: Option<PathBuf>,

    #[serde(default)]
    pub users: UsersSpec
}

fn default_pending_upload_expiration() -> f64 {
    30.0 * 60.0
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

pub fn config_file_add_user(path: &Path, username: String, password: Password, access_rights: Vec<AccessRight>) -> Result<(), String> {
    let content = std::fs::read_to_string(&path).map_err(|err| format!("{}", err))?;
    let mut doc = content.parse::<DocumentMut>().expect("Not a valid TOML document.");

    let mut user = toml_edit::Array::new();
    user.push(username.clone());
    user.push(password.to_string());
    user.push(toml_edit::Array::from_iter(access_rights.iter().map(|x| x.to_string())));

    match doc["users"].as_array_mut() {
        Some(users) => {
            remove_user(users, &username);
            users.push(user);
        }
        None => {
            doc.insert("users", toml_edit::value(toml_edit::Array::from_iter([user].into_iter())));
        }
    }

    std::fs::write(&path, doc.to_string()).map_err(|err| format!("{}", err))?;
    Ok(())
}

pub fn config_file_remove_user(path: &Path, username: String) -> Result<(), String> {
    let content = std::fs::read_to_string(&path).map_err(|err| format!("{}", err))?;
    let mut doc = content.parse::<DocumentMut>().expect("Not a valid TOML document.");

    if let Some(users) = doc["users"].as_array_mut() {
        remove_user(users, &username);
    }

    std::fs::write(&path, doc.to_string()).map_err(|err| format!("{}", err))?;
    Ok(())
}

fn remove_user(users: &mut toml_edit::Array, username: &str){
    users.retain(|user| {
        if let Some(user) = user.as_array() {
            if let Some(current_username) = user.get(0).map(|x| x.as_str()).flatten() {
                if current_username == username {
                    return false;
                }
            }
        }

        true
    });
}