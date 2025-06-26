use std::collections::{HashMap, BTreeSet};

use crate::image_manager::{ImageManagerError, ImageManagerResult};
use crate::image::{Layer, Image, LayerOperation};
use crate::reference::{ImageId, ImageTag, Reference};

pub struct LayerManager {
    pub layers: HashMap<ImageId, Layer>,
    images: HashMap<ImageTag, Image>,
}

impl LayerManager {
    pub fn new() -> LayerManager {
        LayerManager {
            layers: HashMap::new(),
            images: HashMap::new()
        }
    }

    pub fn layers_iter(&self) -> impl Iterator<Item=&Layer> {
        self.layers.values()
    }

    pub fn fully_qualify_reference(&self, reference: &Reference) -> ImageManagerResult<ImageId> {
        match reference {
            Reference::ImageTag(tag) => {
                if let Some(image_hash) = self.get_image_hash(tag) {
                    return Ok(image_hash);
                }

                Err(ImageManagerError::ImageNotFound { reference: reference.clone() })
            }
            Reference::ImageId(id) => {
                Ok(id.clone())
            }
        }
    }

    pub fn get_layer(&self, reference: &Reference) -> ImageManagerResult<&Layer> {
        self.layers.get(&self.fully_qualify_reference(reference)?)
            .ok_or_else(|| ImageManagerError::ImageNotFound { reference: reference.clone() })
    }

    pub fn layer_exist(&self, hash: &ImageId) -> bool {
        self.layers.contains_key(hash)
    }

    pub fn add_layer(&mut self, layer: Layer) {
        self.layers.insert(layer.hash.clone(), layer);
    }

    pub fn find_used_layers(&self, hash: &ImageId, used_layers: &mut BTreeSet<ImageId>) {
        used_layers.insert(hash.clone());
        let layer = self.layers.get(hash).unwrap();

        for operation in &layer.operations {
            match operation {
                LayerOperation::Image { hash } => {
                    used_layers.insert(hash.clone());

                    if let Some(parent_hash) = self.layers.get(&hash).unwrap().parent_hash.as_ref() {
                        self.find_used_layers(&parent_hash, used_layers);
                    }
                },
                _ => {}
            }
        }

        if let Some(parent_hash) = layer.parent_hash.as_ref() {
            self.find_used_layers(&parent_hash, used_layers);
        }
    }

    pub fn images_iter(&self) -> impl Iterator<Item=&Image> {
        self.images.values()
    }

    pub fn get_image(&self, tag: &ImageTag) -> ImageManagerResult<&Image> {
        self.images.get(&tag)
            .ok_or_else(|| ImageManagerError::ImageNotFound { reference: Reference::ImageTag(tag.clone()) })
    }

    pub fn get_image_hash(&self, tag: &ImageTag) -> Option<ImageId> {
        self.images.get(&tag).map(|image| image.hash.clone())
    }

    pub fn insert_image(&mut self, image: Image) {
        self.images.insert(image.tag.clone(), image);
    }

    pub fn insert_or_replace_image(&mut self, image: &Image) {
        if let Some(existing_image) = self.images.get_mut(&image.tag) {
            *existing_image = image.clone();
        } else {
            self.images.insert(image.tag.clone(), image.clone());
        }
    }

    pub fn remove_image_tag(&mut self, tag: &ImageTag) -> Option<Image> {
        self.images.remove(&tag)
    }
}
