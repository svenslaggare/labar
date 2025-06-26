use std::path::{Path};
use std::str::FromStr;

use sha2::{Sha256, Digest};

use crate::image_manager::layer::LayerManager;
use crate::image_definition::{ImageDefinition, LayerOperationDefinition, LayerDefinition};
use crate::image_manager::{ImageManagerResult, ImageManagerError, ImageManagerConfig};
use crate::image::{Image, Layer, LayerOperation};
use crate::image_manager::printing::BoxPrinter;
use crate::reference::{ImageId, ImageTag};

pub struct BuildManager {
    config: ImageManagerConfig,
    printer: BoxPrinter
}

impl BuildManager {
    pub fn new(config: ImageManagerConfig, printer: BoxPrinter) -> BuildManager {
        BuildManager {
            config,
            printer
        }
    }

    pub fn build_image(&self,
                       layer_manager: &mut LayerManager,
                       build_context: &Path,
                       image_definition: ImageDefinition,
                       tag: &ImageTag) -> ImageManagerResult<Image> {
        let mut parent_hash: Option<ImageId> = None;

        if let Some(base_image_reference) = image_definition.base_image {
            let hash = layer_manager.fully_qualify_reference(&base_image_reference)?;
            if !layer_manager.layer_exist(&hash) {
                return Err(ImageManagerError::ImageNotFound { reference: base_image_reference.clone() });
            }

            parent_hash = Some(hash);
        }

        let num_layers = image_definition.layers.len();
        for (layer_index, layer_definition) in image_definition.layers.into_iter().enumerate() {
            self.printer.println(&format!("Step {}/{} : {}", layer_index + 1, num_layers, layer_definition.input_line));

            let layer_definition = layer_definition.expand(build_context)?;
            let layer = self.create_layer(layer_manager, layer_definition, &parent_hash)?;
            let hash = layer.hash.clone();

            self.build_layer(layer_manager, layer)?;

            parent_hash = Some(hash);
        }

        let image = Image::new(parent_hash.unwrap(), tag.to_owned());
        layer_manager.insert_or_replace_image(&image);

        Ok(image)
    }

    fn build_layer(&self, layer_manager: &mut LayerManager, mut layer: Layer) -> ImageManagerResult<bool> {
        if layer_manager.layer_exist(&layer.hash) {
            self.printer.println(&format!("\t* Layer already built: {}", layer.hash));
            return Ok(false);
        }

        self.printer.println(&format!("\t* Building layer: {}", layer.hash));

        let destination_base_path = self.config.get_layer_folder(&layer.hash);

        #[allow(unused_must_use)] {
            std::fs::create_dir_all(&destination_base_path);
        }

        for operation in &mut layer.operations {
            self.printer.println(&format!("\t* {}", operation));

            match operation {
                LayerOperation::File { path, source_path, .. } => {
                    let destination_path = destination_base_path.join(Path::new(&create_hash(path)));
                    let relative_destination_path = destination_path.strip_prefix(&self.config.base_folder).unwrap();

                    std::fs::copy(&source_path, &destination_path)
                        .map_err(|err|
                            ImageManagerError::FileIOError {
                                message: format!(
                                    "Failed to copy file {} -> {} due to: {}",
                                    source_path,
                                    destination_path.to_str().unwrap(),
                                    err
                                )
                            }
                        )?;

                    *source_path = relative_destination_path.to_str().unwrap().to_owned();
                },
                _ => {}
            }
        }

        std::fs::write(
            destination_base_path.join("manifest.json"),
            serde_json::to_string_pretty(&layer)
                .map_err(|err|
                    ImageManagerError::OtherError {
                        message: format!("{}", err)
                    }
                )?
        )?;

        layer_manager.add_layer(layer);
        Ok(true)
    }

    fn create_layer(&self,
                    layer_manager: &LayerManager,
                    layer_definition: LayerDefinition,
                    parent_hash: &Option<ImageId>) -> ImageManagerResult<Layer> {
        let mut layer_operations = Vec::new();
        let mut hash_input = String::new();
        if let Some(parent_hash) = parent_hash.as_ref() {
            hash_input += &parent_hash.to_string();
        }

        for operation_definition in &layer_definition.operations {
            match operation_definition {
                LayerOperationDefinition::Image { reference } => {
                    let hash = layer_manager.fully_qualify_reference(reference)?;
                    if !layer_manager.layer_exist(&hash) {
                        return Err(ImageManagerError::ImageNotFound { reference: reference.clone() });
                    }

                    hash_input += &hash.to_string();
                    layer_operations.push(LayerOperation::Image { hash });
                }
                LayerOperationDefinition::File { path, source_path, link_type } => {
                    let source_path_entry = Path::new(&source_path);
                    if !source_path_entry.exists() {
                        return Err(
                            ImageManagerError::FileIOError {
                                message: format!("The file '{}' does not exist.", source_path)
                            }
                        );
                    }

                    layer_operations.push(LayerOperation::File {
                        path: path.clone(),
                        source_path: source_path.clone(),
                        link_type: *link_type
                    });

                    let created_time = source_path_entry.metadata()?.modified()?;
                    hash_input += &format!(
                        "{}{}{}{:?}",
                        path,
                        source_path,
                        created_time.duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos(),
                        link_type
                    );
                },
                LayerOperationDefinition::Directory { path } => {
                    layer_operations.push(LayerOperation::Directory { path: path.clone() });
                    hash_input += path;
                },
            }
        }

        Ok(
            Layer::new(
                parent_hash.clone(),
                ImageId::from_str(&create_hash(&hash_input)).unwrap(),
                layer_operations
            )
        )
    }
}

fn create_hash(input: &str) -> String {
    base16ct::lower::encode_string(&Sha256::digest(input.as_bytes()))
}

#[test]
fn test_build() {
    use crate::helpers;
    use crate::image_manager::ConsolePrinter;
    use crate::reference::Reference;

    let tmp_dir = helpers::get_temp_folder();
    let config = ImageManagerConfig::with_base_dir(tmp_dir.clone());

    let printer = ConsolePrinter::new();
    let mut layer_manager = LayerManager::new();
    let build_manager = BuildManager::new(config, printer);

    let image_definition = ImageDefinition::from_str_without_context(&std::fs::read_to_string("testdata/definitions/simple1.labarfile").unwrap());
    assert!(image_definition.is_ok());
    let image_definition = image_definition.unwrap();

    let result = build_manager.build_image(&mut layer_manager, Path::new(""), image_definition, &ImageTag::from_str("test").unwrap());
    assert!(result.is_ok());
    let result = result.unwrap();

    assert_eq!(ImageTag::from_str("test").unwrap(), result.tag);

    let image = layer_manager.get_layer(&Reference::from_str("test").unwrap());
    assert!(image.is_ok());
    let image = image.unwrap();
    assert_eq!(image.hash, result.hash);

    assert_eq!(layer_manager.get_image_hash(&ImageTag::from_str("test").unwrap()), Some(result.hash));

    #[allow(unused_must_use)] {
        std::fs::remove_dir_all(&tmp_dir);
    }
}

#[test]
fn test_build_with_cache() {
    use crate::helpers;
    use crate::image_manager::ConsolePrinter;
    use crate::reference::Reference;

    let tmp_dir = helpers::get_temp_folder();
    let config = ImageManagerConfig::with_base_dir(tmp_dir.clone());

    let printer = ConsolePrinter::new();
    let mut layer_manager = LayerManager::new();
    let build_manager = BuildManager::new(config, printer.clone());

    // Build first time
    let image_definition = ImageDefinition::from_str_without_context(&std::fs::read_to_string("testdata/definitions/simple1.labarfile").unwrap());
    assert!(image_definition.is_ok());
    let image_definition = image_definition.unwrap();

    let first_result = build_manager.build_image(&mut layer_manager, Path::new(""), image_definition, &ImageTag::from_str("test").unwrap());
    assert!(first_result.is_ok());
    let first_result = first_result.unwrap();

    // Build second time
    let image_definition = ImageDefinition::from_str_without_context(&std::fs::read_to_string("testdata/definitions/simple1.labarfile").unwrap());
    assert!(image_definition.is_ok());
    let image_definition = image_definition.unwrap();

    let second_result = build_manager.build_image(&mut layer_manager, Path::new(""), image_definition, &ImageTag::from_str("test").unwrap());
    assert!(second_result.is_ok());
    let second_result = second_result.unwrap();

    assert_eq!(ImageTag::from_str("test").unwrap(), second_result.tag);
    assert_eq!(first_result.hash, second_result.hash);

    let image = layer_manager.get_layer(&Reference::from_str("test").unwrap());
    assert!(image.is_ok());
    let image = image.unwrap();
    assert_eq!(image.hash, second_result.hash);

    assert_eq!(layer_manager.get_image_hash(&ImageTag::from_str("test").unwrap()), Some(second_result.hash));

    #[allow(unused_must_use)] {
        std::fs::remove_dir_all(&tmp_dir);
    }
}

#[test]
fn test_build_with_image_ref() {
    use crate::helpers;
    use crate::image_manager::ConsolePrinter;
    use crate::reference::Reference;

    let tmp_dir = helpers::get_temp_folder();
    let config = ImageManagerConfig::with_base_dir(tmp_dir.clone());

    let printer = ConsolePrinter::new();
    let mut layer_manager = LayerManager::new();
    let build_manager = BuildManager::new(config, printer);

    let image_definition = ImageDefinition::from_str_without_context(
        &std::fs::read_to_string("testdata/definitions/simple1.labarfile").unwrap()
    ).unwrap();
    build_manager.build_image(
        &mut layer_manager, Path::new(""), image_definition, &ImageTag::from_str("test").unwrap()
    ).unwrap();

    let image_definition = ImageDefinition::from_str_without_context(&std::fs::read_to_string("testdata/definitions/with_image_ref.labarfile").unwrap());
    assert!(image_definition.is_ok());
    let image_definition = image_definition.unwrap();

    let result = build_manager.build_image(&mut layer_manager, Path::new(""), image_definition, &ImageTag::from_str("that").unwrap());
    assert!(result.is_ok());
    let result = result.unwrap();

    assert_eq!(ImageTag::from_str("that").unwrap(), result.tag);

    let image = layer_manager.get_layer(&Reference::from_str("that").unwrap());
    assert!(image.is_ok());
    let image = image.unwrap();
    assert_eq!(image.hash, result.hash);

    assert_eq!(layer_manager.get_image_hash(&ImageTag::from_str("that").unwrap()), Some(result.hash));

    #[allow(unused_must_use)] {
        std::fs::remove_dir_all(&tmp_dir);
    }
}