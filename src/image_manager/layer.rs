use std::collections::{BTreeSet};

use crate::helpers::DataSize;
use crate::image_manager::{ImageManagerConfig, ImageManagerError, ImageManagerResult};
use crate::image::{Layer, Image, LayerOperation};
use crate::image_manager::state::{StateSession};
use crate::reference::{ImageId, ImageTag, Reference};

pub struct LayerManager {
    config: ImageManagerConfig
}

impl LayerManager {
    pub fn new(config: ImageManagerConfig) -> LayerManager {
        LayerManager {
            config
        }
    }

    pub fn all_layers(&self, session: &StateSession) -> ImageManagerResult<Vec<Layer>> {
        let layers = session.all_layers()?;
        Ok(layers)
    }

    pub fn get_layer(&self, session: &StateSession, reference: &Reference) -> ImageManagerResult<Layer> {
        self.get_layer_by_hash(session, &self.fully_qualify_reference(session, reference)?)
            .map_err(|_| ImageManagerError::ImageNotFound { reference: reference.clone() })
    }

    fn get_layer_by_hash(&self, session: &StateSession, hash: &ImageId) -> ImageManagerResult<Layer> {
        let layer = session.get_layer(hash)?;
        layer.ok_or_else(|| ImageManagerError::LayerNotFound { image_id: hash.clone() })
    }

    pub fn layer_exist(&self, session: &StateSession, hash: &ImageId) -> ImageManagerResult<bool> {
        Ok(session.layer_exists(&hash)?)
    }

    pub fn insert_layer(&self, session: &StateSession, layer: Layer) -> ImageManagerResult<()> {
        session.insert_layer(layer)?;
        Ok(())
    }

    pub fn remove_layer(&self, session: &StateSession, hash: &ImageId) -> ImageManagerResult<()> {
        let removed = session.remove_layer(hash)?;
        if !removed {
            return Err(ImageManagerError::LayerNotFound { image_id: hash.clone() });
        }

        Ok(())
    }

    pub fn find_used_layers(&self,
                            session: &StateSession,
                            hash: &ImageId, used_layers: &mut BTreeSet<ImageId>) -> ImageManagerResult<()> {
        used_layers.insert(hash.clone());
        let layer = self.get_layer_by_hash(session, hash)?;

        for operation in &layer.operations {
            match operation {
                LayerOperation::Image { hash } => {
                    used_layers.insert(hash.clone());

                    if let Some(parent_hash) = self.get_layer_by_hash(session, hash)?.parent_hash.as_ref() {
                        self.find_used_layers(session, &parent_hash, used_layers)?;
                    }
                },
                _ => {}
            }
        }

        if let Some(parent_hash) = layer.parent_hash.as_ref() {
            self.find_used_layers(session, &parent_hash, used_layers)?;
        }

        Ok(())
    }

    pub fn fully_qualify_reference(&self, session: &StateSession, reference: &Reference) -> ImageManagerResult<ImageId> {
        match reference {
            Reference::ImageTag(tag) => {
                if let Some(image_hash) = self.get_image_hash(session, tag)? {
                    return Ok(image_hash);
                }

                Err(ImageManagerError::ImageNotFound { reference: reference.clone() })
            }
            Reference::ImageId(id) => {
                Ok(id.clone())
            }
        }
    }

    pub fn images_iter(&self, session: &StateSession) -> ImageManagerResult<impl Iterator<Item=Image>> {
        Ok(session.all_images()?.into_iter())
    }

    pub fn get_image(&self, session: &StateSession, tag: &ImageTag) -> ImageManagerResult<Image> {
        let image = session.get_image(tag)?;
        image
            .ok_or_else(|| ImageManagerError::ImageNotFound { reference: Reference::ImageTag(tag.clone()) })
    }

    pub fn get_image_hash(&self, session: &StateSession, tag: &ImageTag) -> ImageManagerResult<Option<ImageId>> {
        let image = session.get_image(tag)?;
        Ok(image.map(|image| image.hash.clone()))
    }

    pub fn insert_or_replace_image(&self, session: &mut StateSession, image: Image) -> ImageManagerResult<()> {
        session.insert_or_replace_image(image)?;
        Ok(())
    }

    pub fn remove_image(&self, session: &mut StateSession, tag: &ImageTag) -> ImageManagerResult<Option<Image>> {
        let image = session.remove_image(tag)?;
        Ok(image)
    }

    pub fn size_of_reference(&self, session: &StateSession, reference: &Reference, recursive: bool) -> ImageManagerResult<DataSize> {
        let layer = self.get_layer(session, reference)?;
        let mut total_size = DataSize(0);

        if recursive {
            if let Some(parent_hash) = layer.parent_hash.as_ref() {
                total_size += self.size_of_reference(session, &parent_hash.clone().to_ref(), true)?;
            }
        }

        for operation in &layer.operations {
            match operation {
                LayerOperation::Image { hash } => {
                    total_size += self.size_of_reference(session, &hash.clone().to_ref(), true)?;
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