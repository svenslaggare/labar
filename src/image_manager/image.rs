use std::collections::{HashMap, BTreeSet};
use std::path::{Path};
use std::time::SystemTime;

use serde::{Deserialize, Serialize};

use crate::image::{Image, LayerOperation, Layer};
use crate::image_definition::{ImageDefinition};
use crate::image_manager::{ImageManagerError, ImageManagerConfig, ImageManagerResult, State};
use crate::image_manager::layer::LayerManager;
use crate::image_manager::unpack::{UnpackManager, Unpacking};
use crate::image_manager::build::BuildManager;
use crate::helpers::DataSize;
use crate::image_manager::printing::BoxPrinter;
use crate::image_manager::registry::{RegistryManager};

#[derive(Debug, Serialize, Deserialize)]
pub struct ImageMetadata {
    pub image: Image,
    pub created: SystemTime,
    pub size: DataSize
}

pub struct ImageManager {
    config: ImageManagerConfig,
    printer: BoxPrinter,
    changed: bool,

    layer_manager: LayerManager,
    build_manager: BuildManager,
    unpack_manager: UnpackManager,
    registry_manager: RegistryManager
}

impl ImageManager {
    pub fn new(printer: BoxPrinter) -> ImageManager {
        ImageManager::with_config(ImageManagerConfig::new(), printer)
    }

    pub fn with_config(config: ImageManagerConfig,
                       printer: BoxPrinter) -> ImageManager {
        ImageManager {
            config: config.clone(),
            printer: printer.clone(),
            changed: false,

            layer_manager: LayerManager::new(),
            build_manager: BuildManager::new(config.clone(), printer.clone()),
            unpack_manager: UnpackManager::new(config.clone(), printer.clone()),
            registry_manager: RegistryManager::new(printer.clone()),
        }
    }

    pub fn from_state_file(printer: BoxPrinter) -> Result<ImageManager, String> {
        let mut image_manager = ImageManager::new(printer);
        image_manager.load_state()?;
        Ok(image_manager)
    }

    pub fn config(&self) -> &ImageManagerConfig {
        &self.config
    }

    pub fn save_state(&self) -> Result<(), String> {
        let layers = self.layer_manager.layers.keys().cloned().collect::<Vec<_>>();
        let images = self.layer_manager.images.values().cloned().collect::<Vec<_>>();

        std::fs::write(
            self.config.base_folder().join("state.json"),
            serde_json::to_string_pretty(&State {
                layers,
                images
            }).map_err(|err| format!("{}", err))?
        ).map_err(|err| format!("{}", err))?;

        std::fs::write(
            self.config.base_folder().join("unpackings.json"),
            serde_json::to_string_pretty(&self.unpack_manager.unpackings)
                .map_err(|err| format!("{}", err))?
        ).map_err(|err| format!("{}", err))?;

        Ok(())
    }

    pub fn load_state(&mut self) -> Result<(), String> {
        let state_content = std::fs::read_to_string(self.config.base_folder().join("state.json"))
            .map_err(|err| format!("{}", err))?;

        let state: State = serde_json::from_str(&state_content)
            .map_err(|err| format!("{}", err))?;

        for layer_hash in state.layers {
            let layer_manifest_filename = self.config.get_layer_folder(&layer_hash).join("manifest.json");
            let layer_content = std::fs::read_to_string(layer_manifest_filename)
                .map_err(|err| format!("{}", err))?;

            let layer: Layer = serde_json::from_str(&layer_content)
                .map_err(|err| format!("{}", err))?;

            self.layer_manager.add_layer(layer);
        }

        for image in state.images {
            self.layer_manager.images.insert(image.tag.clone(), image);
        }

        let unpackings_content = std::fs::read_to_string(self.config.base_folder().join("unpackings.json"))
            .map_err(|err| format!("{}", err))?;

        self.unpack_manager.unpackings = serde_json::from_str(&unpackings_content)
            .map_err(|err| format!("{}", err))?;

        Ok(())
    }

    pub fn state_exists(&self) -> bool {
        self.config.base_folder().join("state.json").exists()
    }

    pub fn build_image(&mut self, build_context: &Path, image_definition: ImageDefinition, tag: &str) -> ImageManagerResult<Image> {
        let image = self.build_manager.build_image(&mut self.layer_manager, build_context, image_definition, tag)?;
        self.changed = true;
        Ok(image)
    }

    pub fn tag_image(&mut self, reference: &str, tag: &str) -> ImageManagerResult<Image> {
        let layer = self.layer_manager.get_layer(reference)?;
        let image = Image::new(layer.hash.clone(), tag.to_owned());
        self.layer_manager.insert_or_replace_image(&image);
        self.changed = true;
        Ok(image)
    }

    pub fn unpack(&mut self, unpack_dir: &Path, reference: &str, replace: bool) -> ImageManagerResult<()> {
        self.unpack_manager.unpack(&mut self.layer_manager, unpack_dir, reference, replace)?;
        self.changed = true;
        Ok(())
    }

    pub fn remove_unpacking(&mut self, unpack_dir: &Path, force: bool) -> ImageManagerResult<()> {
        self.unpack_manager.remove_unpacking(&mut self.layer_manager, unpack_dir, force)?;
        self.changed = true;
        Ok(())
    }

    pub fn remove_image(&mut self, tag: &str) -> ImageManagerResult<()> {
        if let Some(image) = self.layer_manager.remove_image_tag(tag) {
            self.printer.println(&format!("Removed image: {} ({})", tag, image.hash));
            self.garbage_collect()?;
            self.changed = true;
            Ok(())
        } else {
            Err(ImageManagerError::ImageNotFound { reference: tag.to_owned() })
        }
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
                    self.printer.println(&format!("Failed to remove layer: {}", err));
                } else {
                    deleted_layers += 1;
                }
            }

            keep
        });

        std::mem::swap(&mut tmp_layers, &mut self.layer_manager.layers);

        self.changed = true;
        Ok(deleted_layers)
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

        let reclaimed_size = DataSize(reclaimed_size as usize);
        self.printer.println(&format!("Deleted layer: {} (reclaimed {:.2} MB)", layer.hash, reclaimed_size));
        Ok(())
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

    pub fn image_size(&self, reference: &str) -> ImageManagerResult<DataSize> {
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
                        let abs_source_path = self.config.base_folder.join(source_path);
                        total_size += std::fs::metadata(abs_source_path).map(|metadata| metadata.len()).unwrap_or(0) as usize;
                    },
                    _ => {}
                }
            }
        }

        Ok(DataSize(total_size))
    }

    pub fn list_content(&self, reference: &str) -> ImageManagerResult<Vec<String>> {
        let mut files = Vec::new();

        let mut stack = Vec::new();
        stack.push(reference);

        while let Some(current) = stack.pop() {
            let layer = self.layer_manager.get_layer(current)?;
            if let Some(parent_hash) = layer.parent_hash.as_ref() {
                stack.push(parent_hash.as_str());
            }

            let mut local_files = Vec::new();
            for operation in &layer.operations {
                match operation {
                    LayerOperation::Image { hash } => {
                        stack.push(hash.as_str());
                    }
                    LayerOperation::File { path, ..  } => {
                        local_files.push(path.clone());
                    }
                    LayerOperation::Directory { path } => {
                        local_files.push(path.clone());
                    }
                }
            }

            local_files.reverse();

            files.append(&mut local_files);
        }

        files.reverse();

        Ok(files)
    }

    pub fn list_unpackings(&self) -> &Vec<Unpacking> {
        &self.unpack_manager.unpackings
    }

    pub async fn list_images_registry(&self, registry: &str) -> ImageManagerResult<Vec<ImageMetadata>> {
        let images = self.registry_manager.list_images(registry).await?;
        Ok(images)
    }

    pub async fn pull(&mut self, registry: &str, reference: &str) -> ImageManagerResult<Image> {
        let mut stack = Vec::new();
        stack.push(reference.to_string());

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

        let mut top_level_hash = None;
        while let Some(current) = stack.pop() {
            if let Ok(layer) = self.get_layer(&current) {
                self.printer.println(&format!("\t* Layer already exist: {}", current));
                if top_level_hash.is_none() {
                    top_level_hash = Some(layer.hash.clone());
                }

                visit_layer(&mut stack, &layer);
            } else {
                self.printer.println(&format!("\t* Downloading layer: {}", current));
                let layer = self.registry_manager.download_layer(self.config(), registry, &current).await?;
                if top_level_hash.is_none() {
                    top_level_hash = Some(layer.hash.clone());
                }

                visit_layer(&mut stack, &layer);
                self.add_layer(layer);
            }
        }

        let image = Image::new(top_level_hash.unwrap(), reference.to_owned());
        self.insert_or_replace_image(image.clone());

        Ok(image)
    }

    pub async fn push(&self, registry: &str, reference: &str) -> ImageManagerResult<()> {
        let top_layer = self.get_layer(reference)?;

        let mut stack = Vec::new();
        stack.push(top_layer.hash.clone());
        while let Some(current) = stack.pop() {
            self.printer.println(&format!("\t* Pushing layer: {}", current));

            let layer = self.get_layer(&current)?;

            if let Some(parent_hash) = layer.parent_hash.as_ref() {
                stack.push(parent_hash.clone());
            }

            for operation in &layer.operations {
                match operation {
                    LayerOperation::Image { hash } => {
                        stack.push((*hash).clone());
                    },
                    _ => {}
                }
            }

            self.registry_manager.upload_layer(self.config(), registry, layer).await?;
        }

        self.registry_manager.upload_image(registry, &top_layer.hash, reference).await?;

        self.printer.println("");

        Ok(())
    }

    pub fn get_layer(&self, reference: &str) -> ImageManagerResult<&Layer> {
        self.layer_manager.get_layer(reference)
    }

    pub fn add_layer(&mut self, layer: Layer) {
        self.layer_manager.add_layer(layer);
        self.changed = true;
    }

    pub fn get_image(&self, reference: &str) -> ImageManagerResult<&Image> {
        self.layer_manager.get_image(reference)
    }

    pub fn insert_or_replace_image(&mut self, image: Image) {
        self.layer_manager.insert_or_replace_image(&image);
        self.changed = true;
    }
}

impl Drop for ImageManager {
    fn drop(&mut self) {
        if self.changed {
            self.save_state().unwrap();
        }
    }
}

#[test]
fn test_build() {
    use crate::helpers;
    use crate::image_manager::ConsolePrinter;

    let tmp_dir = helpers::get_temp_folder();

    {
        let config = ImageManagerConfig::with_base_dir(tmp_dir.clone());

        let mut image_manager = ImageManager::with_config(config, ConsolePrinter::new());

        let image_definition = ImageDefinition::from_str_without_context(&std::fs::read_to_string("testdata/definitions/simple1.labarfile").unwrap());
        assert!(image_definition.is_ok());
        let image_definition = image_definition.unwrap();

        let result = image_manager.build_image(Path::new(""), image_definition, "test");
        assert!(result.is_ok());
        let result = result.unwrap();

        assert_eq!("test", result.tag);

        let top_layer = image_manager.get_layer("test");
        assert!(top_layer.is_ok());
        let top_layer = top_layer.unwrap();
        assert_eq!(top_layer.hash, result.hash);

        let image = image_manager.get_image("test");
        assert!(image.is_ok());
        let image = image.unwrap();
        assert_eq!(image.hash, top_layer.hash);

        assert_eq!(Some(DataSize(974)), image_manager.image_size("test").ok());
    }

    #[allow(unused_must_use)] {
        std::fs::remove_dir_all(&tmp_dir);
    }
}

#[test]
fn test_remove_image1() {
    use crate::helpers;
    use crate::image_manager::ConsolePrinter;

    let tmp_dir = helpers::get_temp_folder();
    std::fs::create_dir_all(&tmp_dir).unwrap();

    let config = ImageManagerConfig::with_base_dir(tmp_dir.clone());
    let printer = ConsolePrinter::new();

    let mut image_manager = ImageManager::with_config(
        config,
        printer
    );

    let image_definition = ImageDefinition::from_str_without_context(&std::fs::read_to_string("testdata/definitions/simple1.labarfile").unwrap());
    assert!(image_definition.is_ok());
    let image_definition = image_definition.unwrap();

    image_manager.build_image(Path::new(""), image_definition, "test").unwrap();

    let result = image_manager.remove_image("test");
    assert!(result.is_ok());

    let images = image_manager.list_images();
    assert!(result.is_ok());
    let images = images.unwrap();

    assert_eq!(0, images.len());
}

#[test]
fn test_remove_image2() {
    use crate::helpers;
    use crate::image_manager::ConsolePrinter;

    let tmp_dir = helpers::get_temp_folder();
    std::fs::create_dir_all(&tmp_dir).unwrap();

    let config = ImageManagerConfig::with_base_dir(tmp_dir.clone());
    let printer = ConsolePrinter::new();

    let mut image_manager = ImageManager::with_config(
        config,
        printer
    );

    let image_definition = ImageDefinition::from_str_without_context(&std::fs::read_to_string("testdata/definitions/simple1.labarfile").unwrap());
    assert!(image_definition.is_ok());
    image_manager.build_image(Path::new(""), image_definition.unwrap(), "test").unwrap();

    let image_definition = ImageDefinition::from_str_without_context(&std::fs::read_to_string("testdata/definitions/simple1.labarfile").unwrap());
    assert!(image_definition.is_ok());
    image_manager.build_image(Path::new(""), image_definition.unwrap(), "test2").unwrap();

    let result = image_manager.remove_image("test");
    assert!(result.is_ok());

    let images = image_manager.list_images();
    assert!(result.is_ok());
    let images = images.unwrap();

    assert_eq!(1, images.len());
    assert_eq!("test2", images[0].image.tag);
}

#[tokio::test]
async fn test_push_pull() {
    use std::net::SocketAddr;

    use crate::helpers;
    use crate::image_manager::ConsolePrinter;
    use crate::registry::RegistryConfig;

    let tmp_dir = helpers::get_temp_folder();
    let tmp_registry_dir = helpers::get_temp_folder();

    let address: SocketAddr = "0.0.0.0:9567".parse().unwrap();
    tokio::spawn(crate::registry::run(RegistryConfig {
        data_path: tmp_registry_dir.clone(),
        address
    }));

    // Wait until registry starts
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    {
        let config = ImageManagerConfig::with_base_dir(tmp_dir.clone());
        let mut image_manager = ImageManager::with_config(config, ConsolePrinter::new());

        // Build
        let image_definition = ImageDefinition::from_str_without_context(&std::fs::read_to_string("testdata/definitions/simple1.labarfile").unwrap()).unwrap();
        let image = image_manager.build_image(Path::new(""), image_definition, "test").unwrap();

        // Push
        let push_result = image_manager.push(&address.to_string(), &image.tag).await;
        assert!(push_result.is_ok(), "{}", push_result.unwrap_err());

        // List remote
        let remote_images = image_manager.list_images_registry(&address.to_string()).await;
        assert!(remote_images.is_ok());
        let remote_images = remote_images.unwrap();
        assert_eq!(1, remote_images.len());
        assert_eq!("test", &remote_images[0].image.tag);

        // Remove in order to pull
        assert!(image_manager.remove_image(&image.tag).is_ok());

        // Pull
        let pull_result = image_manager.pull(&address.to_string(), &image.tag).await;
        assert!(pull_result.is_ok());
        let pull_image = pull_result.unwrap();
        assert_eq!(image, pull_image);

        // List images
        let images = image_manager.list_images();
        assert!(images.is_ok());
        let images = images.unwrap();
        assert_eq!(1, images.len());
        assert_eq!("test", &images[0].image.tag);
        assert_eq!(Some(DataSize(974)), image_manager.image_size(&image.tag).ok());
    }

    #[allow(unused_must_use)] {
        std::fs::remove_dir_all(&tmp_dir);
        std::fs::remove_dir_all(&tmp_registry_dir);
    }
}