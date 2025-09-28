use std::fmt::{Debug, Display, Formatter};
use std::path::Path;
use std::str::FromStr;

use log::{error, info};

use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use sha2::{Digest, Sha256};

use serde::{Deserialize, Serialize};

use axum::extract::Request;
use rusqlite::OptionalExtension;
use rusqlite::types::FromSqlError;

use crate::image_manager::{SqlResult, StateManager, StateSession};
use crate::registry::model::{AppError, AppResult};
use crate::registry::RegistryConfig;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AccessRight {
    Access,
    List,
    Download,
    Upload,
    Delete
}

impl Display for AccessRight {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            AccessRight::Access => write!(f, "Access"),
            AccessRight::List => write!(f, "List"),
            AccessRight::Download => write!(f, "Download"),
            AccessRight::Upload => write!(f, "Upload"),
            AccessRight::Delete => write!(f, "Delete"),
        }
    }
}

impl FromStr for AccessRight {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        match text {
            "access" => Ok(AccessRight::Access),
            "list" => Ok(AccessRight::List),
            "download" => Ok(AccessRight::Download),
            "upload" => Ok(AccessRight::Upload),
            "delete" => Ok(AccessRight::Delete),
            _ => Err(format!("Unknown access right: {}", text)),
        }
    }
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
        let password = Password::from_plain_text(parts[1]);

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
    fn has_access(&self, username: &str, password: &Password, requested_access_right: AccessRight) -> AppResult<bool>;
}

pub type UsersSpec = Vec<(String, Password, Vec<AccessRight>)>;

pub struct SqliteAuthProvider {
    state_manager: StateManager
}

impl SqliteAuthProvider {
    pub fn new(base_folder: &Path, initial_users: UsersSpec) -> SqlResult<SqliteAuthProvider> {
        let provider = SqliteAuthProvider {
            state_manager: StateManager::new(base_folder)?
        };

        let mut session = provider.state_manager.pooled_session()?;
        if !provider.db_any_users(&mut session)? && !initial_users.is_empty() {
            info!("Setting up initial users...");
            for (username, password, access_rights) in initial_users {
                provider.db_add_user(&mut session, &username, &password, &access_rights)?;
            }
        }

        Ok(provider)
    }

    pub fn from_registry_config(registry_config: &RegistryConfig) -> SqlResult<SqliteAuthProvider> {
        SqliteAuthProvider::new(&registry_config.data_path, registry_config.initial_users.clone())
    }

    pub fn add_user(&self, username: String, password: Password, access_rights: Vec<AccessRight>, update: bool) -> bool {
        fn internal(provider: &SqliteAuthProvider,
                    username: String, password: Password, access_rights: Vec<AccessRight>,
                    update: bool) -> SqlResult<bool> {
            let mut session = provider.state_manager.pooled_session()?;

            if update {
                if provider.db_get_user(&mut session, &username)?.is_some() {
                    return Ok(provider.db_update_user(&mut session, &username, &password, &access_rights).is_ok());
                }
            }

            Ok(provider.db_add_user(&mut session, &username, &password, &access_rights).is_ok())
        }

        internal(self, username, password, access_rights, update).unwrap_or(false)
    }

    pub fn remove_user(&self, username: &str) -> bool {
        fn internal(provider: &SqliteAuthProvider, username: &str) -> SqlResult<bool> {
            let mut session = provider.state_manager.pooled_session()?;
            Ok(provider.db_remove_user(&mut session, &username).is_ok())
        }

        internal(self, username).unwrap_or(false)
    }

    fn db_any_users(&self, session: &mut StateSession) -> SqlResult<bool> {
        let count = session.connection.query_one(
            "SELECT COUNT(*) FROM registry_users",
            [],
            |row| row.get::<_, i64>(0)
        )?;

        Ok(count > 0)
    }

    fn db_add_user(&self,
                   session: &mut StateSession,
                   username: &str,
                   password: &Password,
                   access_rights: &Vec<AccessRight>) -> SqlResult<()> {
        session.connection.execute(
            "INSERT INTO registry_users (username, password, access_rights) VALUES (?1, ?2, ?3)",
            (username, password.to_string(), &serde_json::to_value(access_rights).unwrap())
        )?;
        Ok(())
    }

    fn db_update_user(&self,
                      session: &mut StateSession,
                      username: &str,
                      password: &Password,
                      access_rights: &Vec<AccessRight>) -> SqlResult<()> {
        session.connection.execute(
            "UPDATE registry_users SET password=?2, access_rights=?3 WHERE username = ?1",
            (username, password.to_string(), &serde_json::to_value(access_rights).unwrap())
        )?;
        Ok(())
    }

    fn db_remove_user(&self,
                      session: &mut StateSession,
                      username: &str) -> SqlResult<()> {
        session.connection.execute(
            "DELETE FROM registry_users WHERE username=?1",
            (username, )
        )?;
        Ok(())
    }

    fn db_get_user(&self, session: &mut StateSession, username: &str) -> SqlResult<Option<RegistryUser>> {
        session.connection.query_row(
            "SELECT username, password, access_rights FROM registry_users WHERE username=?1",
            (username, ),
            |row| {
                let access_rights: serde_json::Value = row.get(2)?;
                let access_rights: Vec<AccessRight> = serde_json::from_value(access_rights)
                    .map_err(|_| FromSqlError::InvalidType)?;

                Ok(
                    RegistryUser {
                        username: row.get(0)?,
                        password: Password::from_hash(&row.get::<_, String>(1)?),
                        access_rights,
                    }
                )
            }
        ).optional()
    }
}

impl AuthProvider for SqliteAuthProvider {
    fn has_access(&self, username: &str, password: &Password, requested_access_right: AccessRight) -> AppResult<bool> {
        fn internal(provider: &SqliteAuthProvider, username: &str, password: &Password, requested_access_right: AccessRight) -> SqlResult<AppResult<bool>> {
            let mut session = provider.state_manager.pooled_session()?;

            let entry = match provider.db_get_user(&mut session, username)? {
                Some(user) => user,
                None => {
                    info!("User '{}' does not exist.", username);
                    return Ok(Ok(false));
                }
            };

            if &entry.password != password {
                info!("Invalid password for user: {}.", username);
                return Ok(Ok(false));
            }

            if requested_access_right == AccessRight::Access {
                return Ok(Ok(true));
            }

            if entry.access_rights.contains(&requested_access_right) {
                Ok(Ok(true))
            } else {
                info!("User '{}' does not have {:?} access rights.", username, requested_access_right);
                Ok(Ok(false))
            }
        }

        internal(&self, username, password, requested_access_right)
            .unwrap_or_else(|err| {
                error!("SQL failure: {}", err);
                Ok(false)
            })
    }
}

struct RegistryUser {
    #[allow(dead_code)]
    username: String,
    password: Password,
    access_rights: Vec<AccessRight>
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Password(String);

impl Password {
    pub fn from_hash(hash: &str) -> Password {
        Password(hash.to_owned())
    }

    pub fn from_plain_text(password: &str) -> Password {
        Password(base16ct::lower::encode_string(&Sha256::digest(password.as_bytes())))
    }
}

impl Display for Password {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.clone())
    }
}

impl FromStr for Password {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        Ok(Password::from_hash(text))
    }
}

#[test]
fn test_access_rights1() {
    let tmp_folder = crate::test_helpers::TempFolder::new();

    let provider = SqliteAuthProvider::new(
        &tmp_folder,
        vec![
            (
                "guest".to_owned(),
                Password::from_plain_text("guest"),
                vec![AccessRight::List, AccessRight::Download]
            )
        ]
    ).unwrap();

    assert_eq!(Some(true), provider.has_access("guest", &Password::from_plain_text("guest"), AccessRight::Access).ok());
    assert_eq!(Some(true), provider.has_access("guest", &Password::from_plain_text("guest"), AccessRight::List).ok());
    assert_eq!(Some(false), provider.has_access("guest", &Password::from_plain_text("guest"), AccessRight::Delete).ok());
    assert_eq!(Some(false), provider.has_access("guest", &Password::from_plain_text("gueste"), AccessRight::List).ok());
    assert_eq!(Some(false), provider.has_access("gueste", &Password::from_plain_text("guest"), AccessRight::List).ok());
}

#[test]
fn test_access_rights2() {
    let tmp_folder = crate::test_helpers::TempFolder::new();

    let provider = SqliteAuthProvider::new(
        &tmp_folder,
        vec![
            (
                "guest".to_owned(),
                Password::from_plain_text("guest"),
                vec![]
            )
        ]
    ).unwrap();

    assert_eq!(Some(true), provider.has_access("guest", &Password::from_plain_text("guest"), AccessRight::Access).ok());
}