use std::path::{Path, PathBuf};

use crate::content::{compute_content_hash, compute_content_hash_async};
use crate::helpers::{compress_file, decompress_file, DataSize};
use crate::image::{Layer, LayerOperation};
use crate::image_manager::{ImageManagerConfig, ImageManagerResult, PrinterRef, StateSession, StorageMode};
use crate::image_manager::layer::LayerManager;
use crate::reference::ImageTag;

pub struct CompressionManager {
    config: ImageManagerConfig,
    _printer: PrinterRef
}

impl CompressionManager {
    pub fn new(config: ImageManagerConfig, printer: PrinterRef) -> CompressionManager {
        CompressionManager {
            config,
            _printer: printer
        }
    }

    pub fn compress(&mut self,
                    session: &mut StateSession,
                    layer_manager: &LayerManager,
                    tag: &ImageTag) -> ImageManagerResult<()> {
        let mut stack = vec![layer_manager.fully_qualify_reference(&session, &tag.clone().to_ref())?];
        while let Some(layer_id) = stack.pop() {
            let mut layer = layer_manager.get_layer(session, &layer_id.to_ref())?;
            self.compress_layer(&mut layer, true)?;
            layer_manager.insert_or_replace_layer(session, &layer)?;
            layer.visit_image_ids(|id| stack.push(id.clone()));
        }

        Ok(())
    }

    pub fn compress_layer(&self, layer: &mut Layer, always: bool) -> ImageManagerResult<()> {
        let mut compressed_operations = Vec::new();
        for (operation_index, operation) in layer.operations.iter().enumerate() {
            match operation {
                LayerOperation::File { path, source_path, original_source_path, content_hash, link_type, writable } => {
                    let abs_source_path = self.config.base_folder().join(source_path);

                    if !always {
                        if DataSize::from_file(&abs_source_path) < DataSize(1024) {
                            return Ok(());
                        }
                    }

                    let temp_source_path = abs_source_path.to_str().unwrap().to_owned() + ".tmp";
                    let temp_source_path = Path::new(&temp_source_path).to_path_buf();
                    compress_file(&abs_source_path, &temp_source_path)?;

                    let compressed_content_hash = compute_content_hash(&temp_source_path)?;
                    compressed_operations.push((
                        operation_index,
                        temp_source_path,
                        abs_source_path,
                        LayerOperation::CompressedFile {
                            path: path.to_owned(),
                            source_path: source_path.to_owned(),
                            original_source_path: original_source_path.to_owned(),
                            content_hash: content_hash.to_owned(),
                            link_type: *link_type,
                            writable: *writable,
                            compressed_content_hash
                        }
                    ));
                }
                LayerOperation::Image { .. } => {}
                LayerOperation::ImageAlias { .. } => {}
                LayerOperation::Directory { .. } => {}
                LayerOperation::CompressedFile { .. } => {}
                LayerOperation::Label { .. } => {}
            }
        }

        for (operation_index, temp_source_path, abs_source_path, new_operation) in compressed_operations {
            std::fs::rename(temp_source_path, abs_source_path)?;
            layer.operations[operation_index] = new_operation;
        }

        Ok(())
    }

    pub fn decompress(&mut self,
                      session: &mut StateSession,
                      layer_manager: &LayerManager,
                      tag: &ImageTag) -> ImageManagerResult<()> {
        let mut stack = vec![layer_manager.fully_qualify_reference(&session, &tag.clone().to_ref())?];
        while let Some(layer_id) = stack.pop() {
            let mut layer = layer_manager.get_layer(session, &layer_id.clone().to_ref())?;
            self.decompress_layer(&mut layer)?;
            layer_manager.insert_or_replace_layer(session, &layer)?;
            layer.visit_image_ids(|id| stack.push(id.clone()));
        }

        Ok(())
    }

    pub fn decompress_layer(&self, layer: &mut Layer) -> ImageManagerResult<()> {
        let mut decompressed_operations = Vec::new();
        for (operation_index, operation) in layer.operations.iter().enumerate() {
            match operation {
                LayerOperation::CompressedFile { path, source_path, original_source_path, content_hash, link_type, writable, .. } => {
                    let abs_source_path = self.config.base_folder.join(&source_path);

                    let temp_source_path = abs_source_path.to_str().unwrap().to_owned() + ".tmp";
                    let temp_source_path = Path::new(&temp_source_path).to_path_buf();
                    decompress_file(&abs_source_path, &temp_source_path)?;

                    decompressed_operations.push((
                        operation_index,
                        temp_source_path,
                        abs_source_path,
                        LayerOperation::File {
                            path: path.to_owned(),
                            source_path: source_path.to_owned(),
                            original_source_path: original_source_path.to_owned(),
                            content_hash: content_hash.to_owned(),
                            link_type: *link_type,
                            writable: *writable
                        }
                    ));
                }
                LayerOperation::Image { .. } => {},
                LayerOperation::ImageAlias { .. } => {}
                LayerOperation::Directory { .. } => {},
                LayerOperation::File { .. } => {},
                LayerOperation::Label { .. } => {}
            }
        }

        for (operation_index, temp_source_path, abs_source_path, new_operation) in decompressed_operations {
            std::fs::rename(temp_source_path, abs_source_path)?;
            layer.operations[operation_index] = new_operation;
        }

        Ok(())
    }

    pub async fn handle_compression(&self,
                                    storage_mode: &StorageMode,
                                    operation: &LayerOperation,
                                    data_path: &Path) -> ImageManagerResult<Option<(PathBuf, LayerOperation)>> {
        Ok(
            match storage_mode {
                StorageMode::AlwaysCompressed | StorageMode::PreferCompressed => {
                    self.compress_operation(
                        operation,
                        data_path,
                        storage_mode == &StorageMode::AlwaysCompressed,
                    ).await?
                }
                StorageMode::AlwaysUncompressed => {
                    self.decompress_operation(
                        operation,
                        data_path
                    ).await?
                }
                StorageMode::PreferUncompressed => None
            }
        )
    }

    async fn compress_operation(&self,
                                operation: &LayerOperation,
                                data_path: &Path,
                                always: bool) -> ImageManagerResult<Option<(PathBuf, LayerOperation)>> {
        match operation {
            LayerOperation::File { path, source_path, original_source_path, content_hash, link_type, writable } => {
                if !always {
                    if DataSize::from_file(&data_path) < DataSize(1024) {
                        return Ok(None);
                    }
                }

                let temp_source_path = data_path.to_str().unwrap().to_owned() + ".tmp";
                let temp_source_path = Path::new(&temp_source_path).to_path_buf();

                let data_path_clone = data_path.to_path_buf();
                let temp_source_path_clone = temp_source_path.clone();
                tokio::task::spawn_blocking(move || compress_file(&data_path_clone, &temp_source_path_clone)).await.unwrap()?;

                let compressed_content_hash = compute_content_hash_async(&temp_source_path).await?;
                Ok(
                    Some((
                        temp_source_path,
                        LayerOperation::CompressedFile {
                            path: path.to_owned(),
                            source_path: source_path.to_owned(),
                            original_source_path: original_source_path.to_owned(),
                            content_hash: content_hash.to_owned(),
                            link_type: *link_type,
                            writable: *writable,
                            compressed_content_hash
                        }
                    ))
                )
            }
            LayerOperation::Image { .. } => Ok(None),
            LayerOperation::ImageAlias { .. } => Ok(None),
            LayerOperation::Directory { .. } => Ok(None),
            LayerOperation::CompressedFile { .. } => Ok(None),
            LayerOperation::Label { .. } => Ok(None)
        }
    }

    async fn decompress_operation(&self,
                                  operation: &LayerOperation,
                                  data_path: &Path) -> ImageManagerResult<Option<(PathBuf, LayerOperation)>> {
        match operation {
            LayerOperation::CompressedFile { path, source_path, original_source_path, content_hash, link_type, writable, .. } => {
                let temp_source_path = data_path.to_str().unwrap().to_owned() + ".tmp";
                let temp_source_path = Path::new(&temp_source_path).to_path_buf();

                let data_path_clone = data_path.to_path_buf();
                let temp_source_path_clone = temp_source_path.clone();
                tokio::task::spawn_blocking(move || decompress_file(&data_path_clone, &temp_source_path_clone)).await.unwrap()?;

                Ok(
                    Some((
                        temp_source_path,
                        LayerOperation::File {
                            path: path.to_owned(),
                            source_path: source_path.to_owned(),
                            original_source_path: original_source_path.to_owned(),
                            content_hash: content_hash.to_owned(),
                            link_type: *link_type,
                            writable: *writable
                        }
                    ))
                )
            }
            LayerOperation::Image { .. } => Ok(None),
            LayerOperation::ImageAlias { .. } => Ok(None),
            LayerOperation::Directory { .. } => Ok(None),
            LayerOperation::File { .. } => Ok(None),
            LayerOperation::Label { .. } => Ok(None)
        }
    }
}