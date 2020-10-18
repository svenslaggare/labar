use std::collections::{HashMap, BTreeSet};

use crate::image_manager::{ImageManagerError, ImageManagerResult};
use crate::image::{Layer, Image, LayerOperation};

pub struct LayerManager {
    pub layers: HashMap<String, Layer>,
    pub images: HashMap<String, Image>,
}

impl LayerManager {
    pub fn new() -> LayerManager {
        LayerManager {
            layers: HashMap::new(),
            images: HashMap::new()
        }
    }

    pub fn fully_qualify_reference(&self, reference: &str) -> String {
        if let Some(image_hash) = self.get_image_hash(reference) {
            return image_hash;
        }

        return reference.to_owned();
    }

    pub fn get_layer(&self, reference: &str) -> ImageManagerResult<&Layer> {
        self.layers.get(&self.fully_qualify_reference(reference))
            .ok_or_else(|| ImageManagerError::ImageNotFound { reference: reference.to_owned() })
    }

    pub fn layer_exist(&self, hash: &str) -> bool {
        self.layers.contains_key(hash)
    }

    pub fn add_layer(&mut self, layer: Layer) {
        self.layers.insert(layer.hash.clone(), layer);
    }

    pub fn find_used_layers(&self, hash: &str, used_layers: &mut BTreeSet<String>) {
        used_layers.insert(hash.to_owned());
        let layer = self.layers.get(hash).unwrap();

        for operation in &layer.operations {
            match operation {
                LayerOperation::Image { hash } => {
                    used_layers.insert(hash.clone());

                    if let Some(parent_hash) = self.layers.get(hash).unwrap().parent_hash.as_ref() {
                        self.find_used_layers(parent_hash, used_layers);
                    }
                },
                _ => {}
            }
        }

        if let Some(parent_hash) = layer.parent_hash.as_ref() {
            self.find_used_layers(parent_hash, used_layers);
        }
    }

    pub fn images_iter(&self) -> impl Iterator<Item=&Image> {
        self.images.values()
    }

    pub fn get_image_hash(&self, tag: &str) -> Option<String> {
        self.images.get(tag).map(|image| image.hash.clone())
    }

    pub fn insert_or_replace_image(&mut self, image: &Image) {
        if let Some(existing_image) = self.images.get_mut(&image.tag) {
            *existing_image = image.clone();
        } else {
            self.images.insert(image.tag.clone(), image.clone());
        }
    }

    pub fn remove_image_tag(&mut self, tag: &str) -> Option<Image> {
        self.images.remove(tag)
    }
}
