use std::collections::HashMap;
use std::sync::Arc;

use hmac::{Hmac, Mac};
use sha2::Sha256;

use tokio::sync::Mutex;

use crate::helpers::ResourcePool;
use crate::image::{Image, Layer, LayerOperation};
use crate::image_manager::{ImageId, ImageManager, ImageManagerResult, PooledStateSession};
use crate::registry::auth::{AuthProvider, AuthToken, SqliteAuthProvider};
use crate::registry::{helpers, ChangedUploadLayerFileOperation, RunRegistryError};
use crate::registry::config::RegistryConfig;
use crate::registry::external_storage::{BoxExternalStorage, InMemoryStorage, S3Storage};
use crate::registry::helpers::PooledImageManager;
use crate::registry::model::AppResult;

pub struct AppState {
    pub config: RegistryConfig,

    pub sign_key: Hmac<Sha256>,
    pub access_provider: Box<dyn AuthProvider + Send + Sync>,

    pub external_storage: Option<BoxExternalStorage>,

    image_manager_pool: Arc<ResourcePool<ImageManager>>,

    pub delayed_image_inserts: Mutex<HashMap<ImageId, Vec<Image>>>,
    layer_cache: Mutex<HashMap<ImageId, Arc<Layer>>>,
    pending_upload_layer_cache: Mutex<HashMap<String, Arc<Layer>>>,
    changed_pending_upload_layer_operations: Mutex<HashMap<String, Vec<ChangedUploadLayerFileOperation>>>
}

impl AppState {
    pub fn new(mut config: RegistryConfig, sign_key: &str) -> Result<Arc<AppState>, RunRegistryError> {
        let access_provider = SqliteAuthProvider::new(
            config.image_manager_config().base_folder(),
            std::mem::take(&mut config.initial_users)
        ).map_err(|err| RunRegistryError::AuthSetup { reason: err.to_string() })?;

        let external_storage = match (config.s3_storage.as_ref(), config.in_memory_storage.as_ref()) {
            (Some(s3_storage), _) => {
                let storage: BoxExternalStorage = Box::new(S3Storage::new(s3_storage));
                Some(storage)
            }
            (_, Some(in_memory_storage)) => {
                let storage: BoxExternalStorage = Box::new(InMemoryStorage::new(in_memory_storage));
                Some(storage)
            }
            _ => None
        };

        Ok(
            Arc::new(
                AppState {
                    config,

                    sign_key: Hmac::new_from_slice(
                        sign_key.as_bytes()
                    ).map_err(|err| RunRegistryError::AuthSetup { reason: err.to_string() })?,
                    access_provider: Box::new(access_provider),

                    external_storage,

                    image_manager_pool: Arc::new(ResourcePool::new(Vec::new())),
                    delayed_image_inserts: Mutex::new(HashMap::new()),
                    layer_cache: Mutex::new(HashMap::new()),
                    pending_upload_layer_cache: Mutex::new(HashMap::new()),
                    changed_pending_upload_layer_operations: Mutex::new(HashMap::new())
                }
            )
        )
    }

    pub fn pooled_image_manager(&self, token: &AuthToken) -> PooledImageManager {
        if let Some(image_manager) = self.image_manager_pool.get_resource() {
            PooledImageManager::new(self.image_manager_pool.clone(), image_manager)
        } else {
            PooledImageManager::new(self.image_manager_pool.clone(), helpers::create_image_manager(self, token))
        }
    }

    pub async fn get_layer(&self, image_manager: &ImageManager, hash: &ImageId) -> ImageManagerResult<Arc<Layer>> {
        let mut cache = self.layer_cache.lock().await;
        match cache.get(hash) {
            Some(layer) => Ok(layer.clone()),
            None => {
                let mut layer = image_manager.get_layer(&hash.clone().to_ref())?;
                layer.accelerate();

                let layer = Arc::new(layer);
                cache.insert(hash.clone(), layer.clone());
                Ok(layer)
            }
        }
    }

    pub async fn clear_layer_cache(&self) {
        self.layer_cache.lock().await.clear();
    }

    pub async fn get_pending_upload_layer_by_id(&self, state_session: PooledStateSession, upload_id: &str) -> AppResult<Arc<Layer>> {
        let mut cache = self.pending_upload_layer_cache.lock().await;
        match cache.get(upload_id) {
            Some(layer) => Ok(layer.clone()),
            None => {
                let mut layer = helpers::get_pending_upload_layer_by_id(&state_session, upload_id)?;
                layer.accelerate();

                let layer = Arc::new(layer);
                cache.insert(upload_id.to_owned(), layer.clone());
                Ok(layer)
            }
        }
    }

    pub async fn remove_pending_upload_layer_by_id(&self, upload_id: &str) {
        self.pending_upload_layer_cache.lock().await.remove(upload_id);
        self.changed_pending_upload_layer_operations.lock().await.remove(upload_id);
    }

    pub async fn changed_pending_upload_layer_operations(&self, upload_id: &str, file_index: usize, operation: LayerOperation) {
        self.changed_pending_upload_layer_operations
            .lock().await
            .entry(upload_id.to_owned())
            .or_insert_with(|| Vec::new())
            .push(ChangedUploadLayerFileOperation { file_index, operation })
    }

    pub async fn take_changed_pending_upload_layer_operations(&self, upload_id: &str) -> Option<Vec<ChangedUploadLayerFileOperation>> {
        self.changed_pending_upload_layer_operations.lock().await.remove(upload_id)
    }
}