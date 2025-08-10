use std::path::{Path, PathBuf};
use std::str::FromStr;

use sha2::{Sha256, Digest};

use crate::content::compute_content_hash;
use crate::image_manager::layer::{LayerManager};
use crate::image_definition::{ImageDefinition, LayerOperationDefinition, LayerDefinition};
use crate::image_manager::{ImageManagerResult, ImageManagerError, ImageManagerConfig};
use crate::image::{Image, Layer, LayerOperation};
use crate::image_manager::printing::BoxPrinter;
use crate::image_manager::state::{StateSession};
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
                       session: &mut StateSession,
                       layer_manager: &LayerManager,
                       request: BuildRequest) -> ImageManagerResult<BuildResult> {
        let mut parent_hash: Option<ImageId> = None;

        if let Some(base_image_reference) = request.image_definition.base_image {
            let hash = layer_manager.fully_qualify_reference(session, &base_image_reference)?;
            if !layer_manager.layer_exist(session, &hash)? {
                return Err(ImageManagerError::ImageNotFound { reference: base_image_reference.clone() });
            }

            parent_hash = Some(hash);
        }

        let num_layers = request.image_definition.layers.len();
        let mut built_layers = 0;

        for (layer_index, layer_definition) in request.image_definition.layers.into_iter().enumerate() {
            self.printer.println(&format!("Step {}/{} : {}", layer_index + 1, num_layers, layer_definition.input_line));

            let layer_definition = layer_definition.expand(&request.build_context)?;
            let layer = self.create_layer(session, layer_manager, &request.build_context, layer_definition, &parent_hash)?;
            let hash = layer.hash.clone();

            if self.build_layer(session, layer_manager, &request.build_context, layer, request.force)? {
                built_layers += 1;
            }

            parent_hash = Some(hash);
        }

        let image = Image::new(parent_hash.unwrap(), request.tag.to_owned());
        layer_manager.insert_or_replace_image(session, image.clone())?;

        if image.tag.tag() != "latest" {
            let mut latest_image = image.clone();
            latest_image.tag = latest_image.tag.set_tag("latest");
            layer_manager.insert_or_replace_image(session, latest_image)?;
        }

        Ok(
            BuildResult {
                image,
                built_layers,
            }
        )
    }

    fn build_layer(&self,
                   session: &StateSession,
                   layer_manager: &LayerManager,
                   build_context: &Path,
                   mut layer: Layer,
                   force: bool) -> ImageManagerResult<bool> {
        if !force && layer_manager.layer_exist(session, &layer.hash)? {
            self.printer.println(&format!("\t* Layer already built: {}", layer.hash));
            return Ok(false);
        }

        self.printer.println(&format!("\t* Building layer: {}", layer.hash));

        let destination_base_path = self.config.get_layer_folder(&layer.hash);
        std::fs::create_dir_all(&destination_base_path)?;

        for operation in &mut layer.operations {
            self.printer.println(&format!("\t* {}", operation));

            match operation {
                LayerOperation::File { path, source_path, .. } => {
                    let destination_path = destination_base_path.join(Path::new(&create_hash(path)));
                    let relative_destination_path = destination_path.strip_prefix(&self.config.base_folder).unwrap();

                    std::fs::copy(&build_context.join(&source_path), &destination_path)
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

        layer_manager.insert_layer(session, layer)?;
        Ok(true)
    }

    fn create_layer(&self,
                    session: &StateSession,
                    layer_manager: &LayerManager,
                    build_context: &Path,
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
                    let hash = layer_manager.fully_qualify_reference(session, reference)?;
                    if !layer_manager.layer_exist(session, &hash)? {
                        return Err(ImageManagerError::ImageNotFound { reference: reference.clone() });
                    }

                    hash_input += &hash.to_string();
                    layer_operations.push(LayerOperation::Image { hash });
                }
                LayerOperationDefinition::File { path, source_path, link_type, writable } => {
                    let source_path_entry = Path::new(&source_path);
                    if !source_path_entry.exists() {
                        return Err(
                            ImageManagerError::FileNotInBuildContext {
                                path: source_path.clone()
                            }
                        );
                    }

                    let modified_time = source_path_entry.metadata()?.modified()?;
                    let modified_time_ms = modified_time.duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;

                    let content_hash = match session.get_content_hash(source_path, modified_time_ms)? {
                        Some(content_hash) => content_hash,
                        None => {
                            let content_hash = compute_content_hash(source_path_entry)?;
                            session.add_content_hash(source_path, modified_time_ms, &content_hash)?;
                            content_hash
                        }
                    };

                    let relative_source_path = Path::new(source_path).strip_prefix(build_context)
                        .map_err(|_| ImageManagerError::FileNotInBuildContext { path: source_path.clone() })?;
                    let relative_source_path = relative_source_path.to_str().unwrap();

                    layer_operations.push(LayerOperation::File {
                        path: path.clone(),
                        source_path: relative_source_path.to_owned(),
                        content_hash: content_hash.clone(),
                        link_type: *link_type,
                        writable: *writable
                    });

                    let file_hash = format!(
                        "{}{}{}{}{}",
                        path,
                        relative_source_path,
                        content_hash,
                        link_type,
                        writable
                    );

                    hash_input += &file_hash;
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

#[derive(Debug)]
pub struct BuildRequest {
    pub build_context: PathBuf,
    pub image_definition: ImageDefinition,
    pub tag: ImageTag,
    pub force: bool
}

#[derive(Debug)]
pub struct BuildResult {
    pub image: Image,
    #[allow(dead_code)]
    pub built_layers: usize
}

fn create_hash(input: &str) -> String {
    base16ct::lower::encode_string(&Sha256::digest(input.as_bytes()))
}

#[test]
fn test_build() {
    use crate::helpers;
    use crate::image_manager::ConsolePrinter;
    use crate::reference::Reference;
    use crate::image_manager::state::StateManager;

    let tmp_dir = helpers::get_temp_folder();
    let config = ImageManagerConfig::with_base_folder(tmp_dir.clone());

    let printer = ConsolePrinter::new();
    let state_manager = StateManager::new(&config.base_folder()).unwrap();
    let mut layer_manager = LayerManager::new(config.clone());
    let build_manager = BuildManager::new(config, printer);
    let mut session = state_manager.session().unwrap();

    let result = build_test_image(
        &mut session,
        &mut layer_manager,
        &build_manager,
        Path::new("testdata/definitions/simple1.labarfile"),
        ImageTag::from_str("test").unwrap()
    );
    assert!(result.is_ok());
    let result = result.unwrap().image;

    assert_eq!(ImageTag::from_str("test").unwrap(), result.tag);

    let session = state_manager.session().unwrap();
    let image = layer_manager.get_layer(&session, &Reference::from_str("test").unwrap());
    assert!(image.is_ok());
    let image = image.unwrap();
    assert_eq!(image.hash, result.hash);

    assert_eq!(layer_manager.get_image_hash(&session, &ImageTag::from_str("test").unwrap()).unwrap(), Some(result.hash));

    #[allow(unused_must_use)] {
        std::fs::remove_dir_all(&tmp_dir);
    }
}

#[test]
fn test_build_with_cache1() {
    use crate::helpers;
    use crate::image_manager::ConsolePrinter;
    use crate::reference::Reference;
    use crate::image_manager::state::StateManager;

    let tmp_dir = helpers::get_temp_folder();
    let config = ImageManagerConfig::with_base_folder(tmp_dir.clone());

    let printer = ConsolePrinter::new();
    let state_manager = StateManager::new(&config.base_folder()).unwrap();
    let mut layer_manager = LayerManager::new(config.clone());
    let build_manager = BuildManager::new(config, printer.clone());
    let mut session = state_manager.session().unwrap();

    // Build first time
    let first_result = build_test_image(
        &mut session,
        &mut layer_manager,
        &build_manager,
        Path::new("testdata/definitions/simple1.labarfile"),
        ImageTag::from_str("test").unwrap()
    );
    assert!(first_result.is_ok());
    let first_result = first_result.unwrap();

    // Build second time
    let second_result = build_test_image(
        &mut session,
        &mut layer_manager,
        &build_manager,
        Path::new("testdata/definitions/simple1.labarfile"),
        ImageTag::from_str("test").unwrap()
    );
    assert!(second_result.is_ok());
    let second_result = second_result.unwrap();

    assert_eq!(ImageTag::from_str("test").unwrap(), second_result.image.tag);
    assert_eq!(first_result.image.hash, second_result.image.hash);
    assert_eq!(0, second_result.built_layers);

    let session = state_manager.session().unwrap();
    let image = layer_manager.get_layer(&session, &Reference::from_str("test").unwrap());
    assert!(image.is_ok());
    let image = image.unwrap();
    assert_eq!(image.hash, second_result.image.hash);

    assert_eq!(
        layer_manager.get_image_hash(&session, &ImageTag::from_str("test").unwrap()).unwrap(),
        Some(second_result.image.hash)
    );

    #[allow(unused_must_use)] {
        std::fs::remove_dir_all(&tmp_dir);
    }
}

#[test]
fn test_build_with_cache2() {
    use crate::helpers;
    use crate::image_manager::ConsolePrinter;
    use crate::reference::Reference;
    use crate::image_manager::state::StateManager;
    use crate::image::{LinkType};

    let tmp_dir = helpers::get_temp_folder();
    let config = ImageManagerConfig::with_base_folder(tmp_dir.clone());

    let printer = ConsolePrinter::new();
    let state_manager = StateManager::new(&config.base_folder()).unwrap();
    let mut layer_manager = LayerManager::new(config.clone());
    let build_manager = BuildManager::new(config, printer.clone());
    let mut session = state_manager.session().unwrap();

    let tmp_content_file = tmp_dir.join("test.txt");
    std::fs::write(&tmp_content_file, "Hello, World!").unwrap();

    // Build first time
    let image_definition = ImageDefinition {
        base_image: None,
        layers: vec![
            LayerDefinition::new(
                "".to_owned(),
                vec![
                    LayerOperationDefinition::File {
                        path: "test.txt".to_string(),
                        source_path: "test.txt".to_string(),
                        link_type: LinkType::Hard,
                        writable: false
                    }
                ]
            )
        ],
    };

    let first_result = build_manager.build_image(
        &mut session,
        &mut layer_manager,
        BuildRequest {
            build_context: tmp_dir.clone(),
            image_definition: image_definition.clone(),
            tag: ImageTag::from_str("test").unwrap(),
            force: false,
        }
    );
    assert!(first_result.is_ok(), "{}", first_result.unwrap_err());
    let first_result = first_result.unwrap();

    // Build second time
    std::fs::write(&tmp_content_file, "Hello, World!").unwrap();
    let second_result = build_manager.build_image(
        &mut session,
        &mut layer_manager,
        BuildRequest {
            build_context: tmp_dir.clone(),
            image_definition: image_definition.clone(),
            tag: ImageTag::from_str("test").unwrap(),
            force: false,
        }
    );
    assert!(second_result.is_ok());
    let second_result = second_result.unwrap();

    assert_eq!(ImageTag::from_str("test").unwrap(), second_result.image.tag);
    assert_eq!(first_result.image.hash, second_result.image.hash);
    assert_eq!(0, second_result.built_layers);

    let mut session = state_manager.session().unwrap();
    let image = layer_manager.get_layer(&session, &Reference::from_str("test").unwrap());
    assert!(image.is_ok());
    let image = image.unwrap();
    assert_eq!(image.hash, second_result.image.hash);

    assert_eq!(
        layer_manager.get_image_hash(&session, &ImageTag::from_str("test").unwrap()).unwrap(),
        Some(second_result.image.hash)
    );

    // Build third time (change)
    std::fs::write(&tmp_content_file, "Hello, World!!").unwrap();
    let third_result = build_manager.build_image(
        &mut session,
        &mut layer_manager,
        BuildRequest {
            build_context: tmp_dir.clone(),
            image_definition,
            tag: ImageTag::from_str("test").unwrap(),
            force: false,
        }
    );
    assert!(third_result.is_ok());
    let third_result = third_result.unwrap();

    assert_eq!(ImageTag::from_str("test").unwrap(), third_result.image.tag);
    assert_ne!(first_result.image.hash, third_result.image.hash);
    assert_eq!(1, third_result.built_layers);

    #[allow(unused_must_use)] {
        std::fs::remove_dir_all(&tmp_dir);
    }
}

#[test]
fn test_build_with_image_ref() {
    use crate::helpers;
    use crate::image_manager::ConsolePrinter;
    use crate::reference::Reference;
    use crate::image_manager::state::StateManager;

    let tmp_dir = helpers::get_temp_folder();
    let config = ImageManagerConfig::with_base_folder(tmp_dir.clone());

    let printer = ConsolePrinter::new();
    let state_manager = StateManager::new(&config.base_folder()).unwrap();
    let mut layer_manager = LayerManager::new(config.clone());
    let build_manager = BuildManager::new(config, printer);
    let mut session = state_manager.session().unwrap();

    build_test_image(
        &mut session,
        &mut layer_manager,
        &build_manager,
        Path::new("testdata/definitions/simple1.labarfile"),
        ImageTag::from_str("test").unwrap()
    ).unwrap();

    let result = build_test_image(
        &mut session,
        &mut layer_manager,
        &build_manager,
        Path::new("testdata/definitions/with_image_ref.labarfile"),
        ImageTag::from_str("that").unwrap()
    );
    assert!(result.is_ok());
    let result = result.unwrap().image;

    assert_eq!(ImageTag::from_str("that").unwrap(), result.tag);

    let session = state_manager.session().unwrap();
    let image = layer_manager.get_layer(&session, &Reference::from_str("that").unwrap());
    assert!(image.is_ok());
    let image = image.unwrap();
    assert_eq!(image.hash, result.hash);

    assert_eq!(layer_manager.get_image_hash(&session, &ImageTag::from_str("that").unwrap()).unwrap(), Some(result.hash));

    #[allow(unused_must_use)] {
        std::fs::remove_dir_all(&tmp_dir);
    }
}

#[cfg(test)]
fn build_test_image(session: &mut StateSession,
                    layer_manager: &mut LayerManager,
                    build_manager: &BuildManager,
                    path: &Path,
                    image_tag: ImageTag) -> Result<BuildResult, String> {
    use crate::image_definition::ImageDefinition;
    use crate::image_manager::BuildRequest;

    let image_definition = ImageDefinition::parse_file_without_context(path).map_err(|err| err.to_string())?;

    build_manager.build_image(
        session,
        layer_manager,
        BuildRequest {
            build_context: Path::new("").to_path_buf(),
            image_definition,
            tag: image_tag,
            force: false,
        }
    ).map_err(|err| err.to_string())
}
