use std::collections::{BTreeSet};
use std::sync::Arc;

use crate::helpers::DataSize;
use crate::image_manager::{ImageManagerConfig, ImageManagerError, ImageManagerResult};
use crate::image::{Layer, Image, LayerOperation};
use crate::image_manager::state::StateManager;
use crate::reference::{ImageId, ImageTag, Reference};

pub struct LayerManager {
    config: ImageManagerConfig,
    state_manager: Arc<StateManager>
}

impl LayerManager {
    pub fn new(config: ImageManagerConfig, state_manager: Arc<StateManager>) -> LayerManager {
        LayerManager {
            config,
            state_manager
        }
    }

    pub fn all_layers(&self) -> ImageManagerResult<Vec<Layer>> {
        let layers = self.state_manager.all_layers()?;
        Ok(layers)
    }

    pub fn get_layer(&self, reference: &Reference) -> ImageManagerResult<Layer> {
        self.get_layer_by_hash(&self.fully_qualify_reference(reference)?)
            .map_err(|_| ImageManagerError::ImageNotFound { reference: reference.clone() })
    }

    fn get_layer_by_hash(&self, hash: &ImageId) -> ImageManagerResult<Layer> {
        let layer = self.state_manager.get_layer(hash)?;
        layer.ok_or_else(|| ImageManagerError::LayerNotFound { image_id: hash.clone() })
    }

    pub fn layer_exist(&self, hash: &ImageId) -> ImageManagerResult<bool> {
        Ok(self.state_manager.get_layer(&hash)?.is_some())
    }

    pub fn insert_layer(&mut self, layer: Layer) -> ImageManagerResult<()> {
        self.state_manager.insert_layer(layer)?;
        Ok(())
    }

    pub fn remove_layer(&self, hash: &ImageId) -> ImageManagerResult<()> {
        let removed = self.state_manager.remove_layer(hash)?;
        if !removed {
            return Err(ImageManagerError::LayerNotFound { image_id: hash.clone() });
        }

        Ok(())
    }

    pub fn find_used_layers(&self, hash: &ImageId, used_layers: &mut BTreeSet<ImageId>) -> ImageManagerResult<()> {
        used_layers.insert(hash.clone());
        let layer = self.get_layer_by_hash(hash)?;

        for operation in &layer.operations {
            match operation {
                LayerOperation::Image { hash } => {
                    used_layers.insert(hash.clone());

                    if let Some(parent_hash) = self.get_layer_by_hash(hash)?.parent_hash.as_ref() {
                        self.find_used_layers(&parent_hash, used_layers)?;
                    }
                },
                _ => {}
            }
        }

        if let Some(parent_hash) = layer.parent_hash.as_ref() {
            self.find_used_layers(&parent_hash, used_layers)?;
        }

        Ok(())
    }

    pub fn fully_qualify_reference(&self, reference: &Reference) -> ImageManagerResult<ImageId> {
        match reference {
            Reference::ImageTag(tag) => {
                if let Some(image_hash) = self.get_image_hash(tag)? {
                    return Ok(image_hash);
                }

                Err(ImageManagerError::ImageNotFound { reference: reference.clone() })
            }
            Reference::ImageId(id) => {
                Ok(id.clone())
            }
        }
    }

    pub fn images_iter(&self) -> ImageManagerResult<impl Iterator<Item=Image>> {
        Ok(self.state_manager.all_images()?.into_iter())
    }

    pub fn get_image(&self, tag: &ImageTag) -> ImageManagerResult<Image> {
        let image = self.state_manager.get_image(tag)?;
        image
            .ok_or_else(|| ImageManagerError::ImageNotFound { reference: Reference::ImageTag(tag.clone()) })
    }

    pub fn get_image_hash(&self, tag: &ImageTag) -> ImageManagerResult<Option<ImageId>> {
        let image = self.state_manager.get_image(tag)?;
        Ok(image.map(|image| image.hash.clone()))
    }

    pub fn insert_or_replace_image(&mut self, image: Image) -> ImageManagerResult<()> {
        self.state_manager.begin_transaction()?;

        if self.state_manager.image_exists(&image.tag)? {
            self.state_manager.replace_image(image)?;
        } else {
            self.state_manager.insert_image(image)?;
        }

        self.state_manager.end_transaction()?;
        Ok(())
    }

    pub fn remove_image(&mut self, tag: &ImageTag) -> ImageManagerResult<Option<Image>> {
        self.state_manager.begin_transaction()?;

        let image = self.state_manager.get_image(tag)?;
        self.state_manager.remove_image(tag)?;

        self.state_manager.end_transaction()?;
        Ok(image)
    }

    pub fn size_of_reference(&self, reference: &Reference, recursive: bool) -> ImageManagerResult<DataSize> {
        let layer = self.get_layer(reference)?;
        let mut total_size = DataSize(0);

        if recursive {
            if let Some(parent_hash) = layer.parent_hash.as_ref() {
                total_size += self.size_of_reference(&parent_hash.clone().to_ref(), true)?;
            }
        }

        for operation in &layer.operations {
            match operation {
                LayerOperation::Image { hash } => {
                    total_size += self.size_of_reference(&hash.clone().to_ref(), true)?;
                },
                LayerOperation::File { source_path, .. } => {
                    let abs_source_path = self.config.base_folder.join(source_path);
                    total_size += DataSize(std::fs::metadata(abs_source_path).map(|metadata| metadata.len()).unwrap_or(0) as usize);
                },
                _ => {}
            }
        }

        Ok(total_size)
    }
}