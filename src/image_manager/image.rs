use std::collections::{HashMap, BTreeSet};
use std::path::{Path};

use crate::image::{Image, LayerOperation, Layer};
use crate::image_definition::{ImageDefinition};
use crate::image_manager::{ImageManagerError, ImageManagerConfig, ImageManagerResult};
use crate::image_manager::registry::{RegistryState, RegistryManager};
use crate::image_manager::layer::LayerManager;
use crate::image_manager::unpack::{UnpackManager, Unpacking};
use crate::image_manager::build::BuildManager;
use crate::helpers;
use rusoto_s3::S3Client;
use rusoto_core::Region;

pub struct ImageMetadata {
    pub image: Image,
    pub created: std::time::SystemTime,
    pub size: usize
}

pub struct ImageManager {
    config: ImageManagerConfig,
    layer_manager: LayerManager,
    build_manager: BuildManager,
    unpack_manager: UnpackManager,
    registry_manager: RegistryManager
}

impl ImageManager {
    pub fn new(registry_manager: RegistryManager) -> ImageManager {
        ImageManager::with_config(ImageManagerConfig::new(), registry_manager)
    }

    pub fn with_config(config: ImageManagerConfig, registry_manager: RegistryManager) -> ImageManager {
        ImageManager {
            config: config.clone(),
            layer_manager: LayerManager::new(),
            build_manager: BuildManager::new(config),
            unpack_manager: UnpackManager::new(),
            registry_manager
        }
    }

    pub fn from_state_file(registry_manager: RegistryManager) -> Result<ImageManager, String> {
        let mut image_manager = ImageManager::new(registry_manager);

        let state_content = std::fs::read_to_string(image_manager.config.base_dir().join("state.json"))
            .map_err(|err| format!("{}", err))?;

        let state: RegistryState = serde_json::from_str(&state_content)
            .map_err(|err| format!("{}", err))?;

        for layer_hash in state.layers {
            let layer_manifest_filename = image_manager.config.get_layer_folder(&layer_hash).join("manifest.json");
            let layer_content = std::fs::read_to_string(layer_manifest_filename)
                .map_err(|err| format!("{}", err))?;

            let layer: Layer = serde_json::from_str(&layer_content)
                .map_err(|err| format!("{}", err))?;

            image_manager.layer_manager.add_layer(layer);
        }

        for image in state.images {
            image_manager.layer_manager.images.insert(image.tag.clone(), image);
        }

        let unpackings_content = std::fs::read_to_string(image_manager.config.base_dir().join("unpackings.json"))
            .map_err(|err| format!("{}", err))?;

        image_manager.unpack_manager.unpackings = serde_json::from_str(&unpackings_content)
            .map_err(|err| format!("{}", err))?;

        Ok(image_manager)
    }

    pub fn save_state(&self) -> Result<(), String> {
        let layers = self.layer_manager.layers.keys().cloned().collect::<Vec<_>>();
        let images = self.layer_manager.images.values().cloned().collect::<Vec<_>>();

        std::fs::write(
            self.config.base_dir().join("state.json"),
            serde_json::to_string_pretty(&RegistryState {
                layers,
                images
            }).map_err(|err| format!("{}", err))?
        ).map_err(|err| format!("{}", err))?;

        std::fs::write(
            self.config.base_dir().join("unpackings.json"),
            serde_json::to_string_pretty(&self.unpack_manager.unpackings).map_err(|err| format!("{}", err))?
        ).map_err(|err| format!("{}", err))?;

        Ok(())
    }

    pub fn registry_uri(&self) -> &str {
        &self.registry_manager.registry_uri
    }

    pub fn build_image(&mut self, image_definition: ImageDefinition, tag: &str) -> ImageManagerResult<Image> {
        self.build_manager.build_image(&mut self.layer_manager, image_definition, tag)
    }

    pub fn tag_image(&mut self, reference: &str, tag: &str) -> ImageManagerResult<Image> {
        let layer = self.layer_manager.get_layer(reference)?;
        let image = Image::new(layer.hash.clone(), tag.to_owned());
        self.layer_manager.insert_or_replace_image(&image);
        Ok(image)
    }

    pub fn unpack(&mut self, unpack_dir: &Path, reference: &str, replace: bool) -> ImageManagerResult<()> {
        self.unpack_manager.unpack(&mut self.layer_manager, unpack_dir, reference, replace)
    }

    pub fn remove_unpacking(&mut self, unpack_dir: &Path, force: bool) -> ImageManagerResult<()> {
        self.unpack_manager.remove_unpacking(&mut self.layer_manager, unpack_dir, force)
    }

    pub fn remove_image(&mut self, tag: &str) -> ImageManagerResult<()> {
        if let Some(image) = self.layer_manager.remove_image_tag(tag) {
            println!("Removed image: {} ({})", tag, image.hash);
            self.garbage_collect()?;
            Ok(())
        } else {
            Err(ImageManagerError::ImageNotFound { reference: tag.to_owned() })
        }
    }

    fn delete_layer(&self, layer: &Layer) -> ImageManagerResult<()> {
        let mut reclaimed_size = 0;

        for operation in &layer.operations {
            match operation {
                LayerOperation::File { source_path, .. } => {
                    reclaimed_size += std::fs::metadata(source_path).map(|metadata| metadata.len()).unwrap_or(0);
                },
                _ => {}
            }
        }

        let layer_file = self.config.get_layer_folder(&layer.hash);
        std::fs::remove_dir_all(&layer_file)
            .map_err(|err|
                ImageManagerError::FileIOError {
                    message: format!("Failed to remove layer {} due to: {}", layer_file.to_str().unwrap(), err)
                }
            )?;

        println!("Deleted layer: {} (reclaimed {:.2} MB)", layer.hash, reclaimed_size as f64 / (1024.0 * 1024.0));
        Ok(())
    }

    pub fn garbage_collect(&mut self) -> ImageManagerResult<usize> {
        let mut deleted_layers = 0;

        let mut used_layers = BTreeSet::new();
        for hash in self.get_hard_references() {
            self.layer_manager.find_used_layers(hash, &mut used_layers);
        }

        let mut tmp_layers = HashMap::new();
        std::mem::swap(&mut tmp_layers, &mut self.layer_manager.layers);

        tmp_layers.retain(|hash, layer| {
            let keep = used_layers.contains(hash);

            if !keep {
                if let Err(err) = self.delete_layer(layer) {
                    println!("Failed to remove layer: {}", err);
                }

                deleted_layers += 1;
            }

            keep
        });

        std::mem::swap(&mut tmp_layers, &mut self.layer_manager.layers);

        Ok(deleted_layers)
    }

    fn get_hard_references(&self) -> Vec<&str> {
        let mut hard_references = Vec::new();
        for image in self.layer_manager.images_iter() {
            hard_references.push(image.hash.as_str());
        }

        for unpacking in &self.unpack_manager.unpackings {
            hard_references.push(unpacking.hash.as_str());
        }

        hard_references
    }

    pub fn list_images(&self) -> ImageManagerResult<Vec<ImageMetadata>> {
        let mut images = Vec::new();

        for image in self.layer_manager.images_iter() {
            images.push(ImageMetadata {
                image: image.clone(),
                created: self.layer_manager.get_layer(&image.hash)?.created,
                size: self.image_size(&image.hash)?
            })
        }

        Ok(images)
    }

    fn image_size(&self, reference: &str) -> ImageManagerResult<usize> {
        let mut stack = Vec::new();
        stack.push(reference.to_owned());

        let mut total_size = 0;
        while let Some(current) = stack.pop() {
            let layer = self.layer_manager.get_layer(&current)?;
            if let Some(parent_hash) = layer.parent_hash.as_ref() {
                stack.push(parent_hash.clone());
            }

            for operation in &layer.operations {
                match operation {
                    LayerOperation::Image { hash } => {
                        stack.push(hash.clone());
                    },
                    LayerOperation::File { source_path, .. } => {
                        total_size += std::fs::metadata(source_path).map(|metadata| metadata.len()).unwrap_or(0) as usize;
                    },
                    _ => {}
                }
            }
        }

        Ok(total_size)
    }

    pub fn list_unpackings(&self) -> &Vec<Unpacking> {
        &self.unpack_manager.unpackings
    }

    pub async fn push(&self, reference: &str, force: bool) -> ImageManagerResult<()> {
        let top_layer = self.layer_manager.get_layer(reference)?;

        let mut stack = Vec::new();
        stack.push(top_layer.hash.clone());
        let mut exported_layers = Vec::new();
        while let Some(current) = stack.pop() {
            println!("\t* Pushing layer: {}", current);

            let layer = self.layer_manager.get_layer(&current)?;
            let mut exported_layer = layer.clone();

            if let Some(parent_hash) = exported_layer.parent_hash.as_ref() {
                stack.push(parent_hash.clone());
            }

            for operation in &mut exported_layer.operations {
                match operation {
                    LayerOperation::Image { hash } => {
                        stack.push((*hash).clone());
                    },
                    _ => {}
                }
            }

            let uploaded = if force {
                self.registry_manager.force_upload_layer(&layer).await?;
                true
            } else {
                self.registry_manager.upload_layer(&layer).await?
            };

            if !uploaded {
                println!("\t\t* Layer already exist.")
            }

            exported_layers.push(layer.hash.clone());
        }

        let mut remote_state = self.registry_manager.download_state().await?;

        remote_state.add_layers(&exported_layers);
        remote_state.add_image(Image::new(top_layer.hash.clone(), reference.to_owned()));

        self.registry_manager.upload_state(&remote_state).await?;

        println!();

        Ok(())
    }

    pub async fn pull(&mut self, reference: &str) -> ImageManagerResult<Image> {
        let remote_state = self.registry_manager.download_state().await?;

        let top_level_hash = remote_state.get_hash(reference)
            .ok_or_else(|| ImageManagerError::ImageNotFound { reference: reference.to_owned() })?;

        let mut stack = Vec::new();
        stack.push(top_level_hash.clone());

        let visit_layer = |stack: &mut Vec<String>, layer: &Layer| {
            if let Some(parent_hash) = layer.parent_hash.as_ref() {
                stack.push(parent_hash.clone());
            }

            for operation in &layer.operations {
                match operation {
                    LayerOperation::Image { hash } => {
                        stack.push(hash.to_owned());
                    },
                    _ => {}
                }
            }
        };

        while let Some(current) = stack.pop() {
            if let Ok(layer) = self.layer_manager.get_layer(&current) {
                println!("\t* Layer already exist: {}", current);
                visit_layer(&mut stack, &layer);
            } else {
                println!("\t* Downloading layer: {}", current);
                let layer = self.registry_manager.download_layer(&self.config.images_base_dir(), &current).await?;

                visit_layer(&mut stack, &layer);
                self.layer_manager.add_layer(layer);
            };
        }

        let image = Image::new(top_level_hash, reference.to_owned());
        self.layer_manager.insert_or_replace_image(&image);

        Ok(image)
    }

    pub async fn list_images_remote(&self) -> ImageManagerResult<Vec<ImageMetadata>> {
        let remote_state = self.registry_manager.download_state().await?;

        let mut images = Vec::new();

        for image in &remote_state.images {
            let remote_layer = self.registry_manager.download_layer_manifest(&image.hash).await?;
            let layer_size = self.registry_manager.get_layer_size(&image.hash).await?;

            let mut remote_image = image.clone();
            remote_image.tag = format!("{}/{}", self.registry_uri(), remote_image.tag);

            images.push(ImageMetadata {
                image: remote_image,
                created: remote_layer.created,
                size: layer_size
            })
        }

        Ok(images)
    }
}

impl Drop for ImageManager {
    fn drop(&mut self) {
        self.save_state().unwrap();
    }
}

#[test]
fn test_remove_image1() {
    let tmp_dir = helpers::get_temp_folder();
    std::fs::create_dir_all(&tmp_dir).unwrap();

    let config = ImageManagerConfig::with_base_dir(tmp_dir.clone());

    let mut image_manager = ImageManager::with_config(
        config,
        RegistryManager::new(String::new(), S3Client::new(Region::EuCentral1))
    );

    let image_definition = ImageDefinition::from_str(&std::fs::read_to_string("testdata/definitions/simple1.dfdfile").unwrap());
    assert!(image_definition.is_ok());
    let image_definition = image_definition.unwrap();

    image_manager.build_image(image_definition, "test").unwrap();

    let result = image_manager.remove_image("test");
    assert!(result.is_ok());

    let images = image_manager.list_images();
    assert!(result.is_ok());
    let images = images.unwrap();

    assert_eq!(0, images.len());
}

#[test]
fn test_remove_image2() {
    let tmp_dir = helpers::get_temp_folder();
    std::fs::create_dir_all(&tmp_dir).unwrap();

    let config = ImageManagerConfig::with_base_dir(tmp_dir.clone());

    let mut image_manager = ImageManager::with_config(
        config,
        RegistryManager::new(String::new(), S3Client::new(Region::EuCentral1))
    );

    let image_definition = ImageDefinition::from_str(&std::fs::read_to_string("testdata/definitions/simple1.dfdfile").unwrap());
    assert!(image_definition.is_ok());
    image_manager.build_image(image_definition.unwrap(), "test").unwrap();

    let image_definition = ImageDefinition::from_str(&std::fs::read_to_string("testdata/definitions/simple1.dfdfile").unwrap());
    assert!(image_definition.is_ok());
    image_manager.build_image(image_definition.unwrap(), "test2").unwrap();

    let result = image_manager.remove_image("test");
    assert!(result.is_ok());

    let images = image_manager.list_images();
    assert!(result.is_ok());
    let images = images.unwrap();

    assert_eq!(1, images.len());
    assert_eq!("test2", images[0].image.tag);
}