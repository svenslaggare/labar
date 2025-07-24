use std::collections::{HashMap, HashSet};

use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use sha2::{Digest, Sha256};
use log::info;

use serde::Deserialize;

use axum::extract::Request;

use crate::registry::model::{AppError, AppResult};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize)]
pub enum AccessRight {
    Access,
    List,
    Download,
    Upload,
    Delete
}

pub struct AuthToken;

pub fn check_access_right(access_provider: &(dyn AuthProvider + Send + Sync),
                          request: &Request,
                          access_right: AccessRight) -> AppResult<AuthToken> {
    fn internal(access_provider: &(dyn AuthProvider + Send + Sync),
                request: &Request,
                access_right: AccessRight) -> AppResult<bool> {
        let auth_header = match request.headers().get(reqwest::header::AUTHORIZATION) {
            Some(header) => header,
            None => return Ok(false)
        };

        let auth_header = match auth_header.to_str().ok() {
            Some(header) => header,
            None => return Ok(false)
        };

        let parts = auth_header.split(' ').collect::<Vec<_>>();
        if parts.len() != 2 {
            return Ok(false);
        }

        let auth_type = parts[0];
        let auth_data = parts[1];
        if auth_type != "Basic" {
            return Ok(false);
        }

        let auth_data = match BASE64_STANDARD.decode(auth_data).ok() {
            Some(auth_data) => auth_data,
            None => return Ok(false)
        };

        let auth_data = match String::from_utf8(auth_data).ok() {
            Some(auth_data) => auth_data,
            None => return Ok(false)
        };

        let parts = auth_data.split(':').collect::<Vec<_>>();
        if parts.len() != 2 {
            return Ok(false);
        }

        if parts.len() != 2 {
            return Ok(false);
        }

        let username = parts[0];
        let password = create_password_hash(parts[1]);

        Ok(access_provider.has_access(username, &password, access_right)?)
    }

    if internal(access_provider, request, access_right.clone())? {
        Ok(AuthToken)
    } else {
        info!("Not authorized for {:?} access.", access_right);
        Err(AppError::Unauthorized)
    }
}

pub trait AuthProvider {
    fn has_access(&self, username: &str, password: &str, requested_access_right: AccessRight) -> AppResult<bool>;
}

pub type UsersSpec = Vec<(String, String, Vec<AccessRight>)>;

pub struct MemoryAuthProvider {
    users: HashMap<String, MemoryAuthProviderEntry>
}

impl MemoryAuthProvider {
    pub fn new(users: UsersSpec) -> MemoryAuthProvider {
        let mut users_map = HashMap::new();
        for (username, password, access_rights) in users {
            users_map.insert(
                username,
                MemoryAuthProviderEntry {
                    password,
                    access_rights: HashSet::from_iter(access_rights.into_iter())
                }
            );
        }

        MemoryAuthProvider {
            users: users_map
        }
    }
}

struct MemoryAuthProviderEntry {
    password: String,
    access_rights: HashSet<AccessRight>
}

impl AuthProvider for MemoryAuthProvider {
    fn has_access(&self, username: &str, password: &str, requested_access_right: AccessRight) -> AppResult<bool> {
        let entry = match self.users.get(username) {
            Some(user) => user,
            None => {
                info!("User '{}' does not exist.", username);
                return Ok(false);
            }
        };

        if entry.password != password {
            info!("Invalid password for user: {}.", username);
            return Ok(false);
        }

        if requested_access_right == AccessRight::Access {
            return Ok(true);
        }

        if entry.access_rights.contains(&requested_access_right) {
            Ok(true)
        } else {
            info!("User '{}' does not have {:?} access rights.", username, requested_access_right);
            Ok(false)
        }
    }
}

pub fn create_password_hash(password: &str) -> String {
    base16ct::lower::encode_string(&Sha256::digest(password.as_bytes()))
}

#[test]
fn test_access_rights1() {
    let provider = MemoryAuthProvider::new(vec![
        (
            "guest".to_owned(),
            create_password_hash("guest"),
            vec![AccessRight::List, AccessRight::Download]
        )
    ]);

    assert_eq!(Some(true), provider.has_access("guest", &create_password_hash("guest"), AccessRight::Access).ok());
    assert_eq!(Some(true), provider.has_access("guest", &create_password_hash("guest"), AccessRight::List).ok());
    assert_eq!(Some(false), provider.has_access("guest", &create_password_hash("guest"), AccessRight::Delete).ok());
    assert_eq!(Some(false), provider.has_access("guest", &create_password_hash("gueste"), AccessRight::List).ok());
    assert_eq!(Some(false), provider.has_access("gueste", &create_password_hash("guest"), AccessRight::List).ok());
}

#[test]
fn test_access_rights2() {
    let provider = MemoryAuthProvider::new(vec![
        (
            "guest".to_owned(),
            create_password_hash("guest"),
            vec![]
        )
    ]);

    assert_eq!(Some(true), provider.has_access("guest", &create_password_hash("guest"), AccessRight::Access).ok());
}