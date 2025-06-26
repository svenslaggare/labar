use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::image_manager::layer::LayerManager;
use crate::image::{Layer, LayerOperation, LinkType};
use crate::image_manager::{ImageManagerConfig, ImageManagerError, ImageManagerResult};
use crate::image_manager::printing::{BoxPrinter};
use crate::reference::{ImageId, Reference};

#[derive(Debug, Serialize, Deserialize)]
pub struct Unpacking {
    pub hash: ImageId,
    pub destination: String,
    pub time: std::time::SystemTime
}

pub struct UnpackManager {
    config: ImageManagerConfig,
    printer: BoxPrinter,
    pub unpackings: Vec<Unpacking>
}

impl UnpackManager {
    pub fn new(config: ImageManagerConfig, printer: BoxPrinter) -> UnpackManager {
        UnpackManager {
            config,
            printer,
            unpackings: Vec::new()
        }
    }

    pub fn unpack(&mut self, layer_manager: &LayerManager, unpack_folder: &Path, reference: &Reference, replace: bool) -> ImageManagerResult<()> {
        if replace && unpack_folder.exists() {
            if let Err(err) = self.remove_unpacking(layer_manager, unpack_folder, true) {
                self.printer.println(&format!("Failed removing packing due to: {}", err));
            }
        }

        let top_layer = layer_manager.get_layer(reference)?;
        if !unpack_folder.exists() {
            std::fs::create_dir_all(unpack_folder)?;
        }

        let unpack_folder_str = unpack_folder.canonicalize()?.to_str().unwrap().to_owned();

        if self.unpackings.iter().any(|unpacking| unpacking.destination == unpack_folder_str) {
            return Err(ImageManagerError::UnpackingExist { path: unpack_folder_str.clone() });
        }

        if unpack_folder.exists() {
            if std::fs::read_dir(unpack_folder)?.count() > 0 {
                return Err(ImageManagerError::FolderNotEmpty { path: unpack_folder_str.clone() });
            }
        }

        self.printer.println(&format!("Unpacking {} ({}) to {}", reference, top_layer.hash, unpack_folder_str));
        self.unpack_layer(layer_manager, unpack_folder, &top_layer)?;

        self.unpackings.push(Unpacking {
            hash: top_layer.hash.clone(),
            destination: unpack_folder_str,
            time: std::time::SystemTime::now()
        });

        Ok(())
    }

    fn unpack_layer(&self, layer_manager: &LayerManager, unpack_dir: &Path, layer: &Layer) -> ImageManagerResult<()> {
        if let Some(parent_hash) = layer.parent_hash.as_ref() {
            let parent_hash = Reference::ImageId(parent_hash.clone());
            let parent_layer = layer_manager.get_layer(&parent_hash)?;
            self.unpack_layer(layer_manager, unpack_dir, parent_layer)?;
        }

        let mut has_files = false;
        for operation in &layer.operations {
            match operation {
                LayerOperation::Image { hash } => {
                    self.unpack_layer(
                        layer_manager,
                        unpack_dir,
                        layer_manager.get_layer(&Reference::ImageId(hash.clone()))?
                    )?;
                },
                LayerOperation::File { path, source_path, link_type } => {
                    let abs_source_path = self.config.base_folder.join(source_path);

                    has_files = true;
                    let destination_path = unpack_dir.join(path);
                    self.printer.println(&format!("\t* Unpacking file {} -> {}", path, destination_path.to_str().unwrap()));

                    #[allow(unused_must_use)] {
                        if let Some(parent_dir) = destination_path.parent() {
                            std::fs::create_dir_all(parent_dir);
                        }

                        std::fs::remove_file(&destination_path);
                    }

                    match link_type {
                        LinkType::Soft => {
                            std::os::unix::fs::symlink(abs_source_path, destination_path)
                                .map_err(|err|
                                    ImageManagerError::FileIOError {
                                        message: format!("Failed to unpack file {} due to: {}", path, err)
                                    }
                                )?;
                        },
                        LinkType::Hard => {
                            std::fs::hard_link(abs_source_path, destination_path)
                                .map_err(|err|
                                    ImageManagerError::FileIOError {
                                        message: format!("Failed to unpack file {} due to: {}", path, err)
                                    }
                                )?;
                        },
                    }
                },
                LayerOperation::Directory { path } => {
                    self.printer.println(&format!("\t* Creating directory {}", path));

                    #[allow(unused_must_use)] {
                        std::fs::create_dir_all(unpack_dir.join(path));
                    }
                },
            }
        }

        if has_files {
            self.printer.println("");
        }

        Ok(())
    }

    pub fn remove_unpacking(&mut self, layer_manager: &LayerManager, unpack_dir: &Path, force: bool) -> ImageManagerResult<()> {
        let unpack_dir_str = unpack_dir.canonicalize()?.to_str().unwrap().to_owned();

        let unpacking = self.unpackings.iter()
            .find(|unpacking| unpacking.destination == unpack_dir_str)
            .ok_or_else(|| ImageManagerError::UnpackingNotFound { path: unpack_dir_str.clone() })?;

        self.printer.println(&format!("Clearing unpacking of {} at {}", unpacking.hash, unpack_dir_str));
        let unpacking_hash = Reference::ImageId(unpacking.hash.clone());
        let top_layer = layer_manager.get_layer(&unpacking_hash)?;

        if !force {
            self.remove_unpacked_layer(layer_manager, unpack_dir, top_layer)?;
        } else {
            if let Err(err) = self.remove_unpacked_layer(layer_manager, unpack_dir, top_layer) {
                self.printer.println(&format!("Failed to clear unpacking due to: {}", err));
            }
        }

        self.unpackings.retain(|unpacking| {
            return unpacking.destination != unpack_dir_str;
        });

        Ok(())
    }

    fn remove_unpacked_layer(&self, layer_manager: &LayerManager, unpack_dir: &Path, layer: &Layer) -> ImageManagerResult<()> {
        for operation in layer.operations.iter().rev() {
            match operation {
                LayerOperation::Image { hash } => {
                    self.remove_unpacked_layer(
                        layer_manager,
                        unpack_dir,
                        layer_manager.get_layer(&Reference::ImageId(hash.clone()))?
                    )?;
                },
                LayerOperation::File { path, .. } => {
                    let destination_path = unpack_dir.join(path);
                    self.printer.println(&format!("\t* Deleting link of file {}", destination_path.to_str().unwrap()));
                    std::fs::remove_file(destination_path)?;
                },
                LayerOperation::Directory { path } => {
                    let path = unpack_dir.join(path);
                    self.printer.println(&format!("\t* Deleting directory {}", path.to_str().unwrap()));
                    std::fs::remove_dir(path)?
                },
            }
        }

        if let Some(parent_hash) = layer.parent_hash.as_ref() {
            let parent_hash = Reference::ImageId(parent_hash.clone());
            let parent_layer = layer_manager.get_layer(&parent_hash)?;
            self.remove_unpacked_layer(layer_manager, unpack_dir, parent_layer)?;
        }

        println!();
        Ok(())
    }
}

#[test]
fn test_unpack() {
    use std::str::FromStr;

    use crate::helpers;
    use crate::reference::ImageTag;
    use crate::image_definition::ImageDefinition;
    use crate::image_manager::build::BuildManager;
    use crate::image_manager::ImageManagerConfig;
    use crate::image_manager::printing::{ConsolePrinter};

    let tmp_dir = helpers::get_temp_folder();
    let config = ImageManagerConfig::with_base_dir(tmp_dir.clone());

    let printer = ConsolePrinter::new();
    let mut layer_manager = LayerManager::new();
    let build_manager = BuildManager::new(config.clone(), printer.clone());
    let mut unpack_manager = UnpackManager::new(config.clone(), printer.clone());

    let image_definition = ImageDefinition::from_str_without_context(&std::fs::read_to_string("testdata/definitions/simple1.labarfile").unwrap());
    assert!(image_definition.is_ok());
    let image_definition = image_definition.unwrap();

    assert!(build_manager.build_image(&mut layer_manager, Path::new(""), image_definition, &ImageTag::from_str("test").unwrap()).is_ok());

    let unpack_result = unpack_manager.unpack(
        &layer_manager,
        &tmp_dir.join("unpack"),
        &Reference::from_str("test").unwrap(),
        false
    );

    assert!(unpack_result.is_ok());
    assert!(tmp_dir.join("unpack").join("file1.txt").exists());

    #[allow(unused_must_use)] {
        std::fs::remove_dir_all(&tmp_dir);
    }
}

#[test]
fn test_unpack_exist() {
    use std::str::FromStr;

    use crate::helpers;
    use crate::reference::ImageTag;
    use crate::image_definition::ImageDefinition;
    use crate::image_manager::build::BuildManager;
    use crate::image_manager::ImageManagerConfig;
    use crate::image_manager::printing::{ConsolePrinter};

    let tmp_dir = helpers::get_temp_folder();
    let config = ImageManagerConfig::with_base_dir(tmp_dir.clone());

    let printer = ConsolePrinter::new();
    let mut layer_manager = LayerManager::new();
    let build_manager = BuildManager::new(config.clone(), printer.clone());
    let mut unpack_manager = UnpackManager::new(config.clone(), printer.clone());

    let image_definition = ImageDefinition::from_str_without_context(&std::fs::read_to_string("testdata/definitions/simple1.labarfile").unwrap());
    assert!(image_definition.is_ok());
    let image_definition = image_definition.unwrap();

    assert!(build_manager.build_image(&mut layer_manager, Path::new(""), image_definition, &ImageTag::from_str("test").unwrap()).is_ok());

    unpack_manager.unpack(
        &layer_manager,
        &tmp_dir.join("unpack"),
        &Reference::from_str("test").unwrap(),
        false
    ).unwrap();

    let unpack_result = unpack_manager.unpack(
        &layer_manager,
        &tmp_dir.join("unpack"),
        &Reference::from_str("test").unwrap(),
        false
    );

    assert!(unpack_result.is_err());
    assert!(tmp_dir.join("unpack").join("file1.txt").exists());

    #[allow(unused_must_use)] {
        std::fs::remove_dir_all(&tmp_dir);
    }
}

#[test]
fn test_remove_unpack() {
    use std::str::FromStr;

    use crate::helpers;
    use crate::reference::ImageTag;
    use crate::image_definition::ImageDefinition;
    use crate::image_manager::build::BuildManager;
    use crate::image_manager::ImageManagerConfig;
    use crate::image_manager::printing::{ConsolePrinter};

    let tmp_dir = helpers::get_temp_folder();
    let config = ImageManagerConfig::with_base_dir(tmp_dir.clone());

    let printer = ConsolePrinter::new();
    let mut layer_manager = LayerManager::new();
    let build_manager = BuildManager::new(config.clone(), printer.clone());
    let mut unpack_manager = UnpackManager::new(config.clone(), printer.clone());

    let image_definition = ImageDefinition::from_str_without_context(&std::fs::read_to_string("testdata/definitions/simple1.labarfile").unwrap());
    assert!(image_definition.is_ok());
    let image_definition = image_definition.unwrap();

    assert!(build_manager.build_image(&mut layer_manager, Path::new(""), image_definition, &ImageTag::from_str("test").unwrap()).is_ok());

    unpack_manager.unpack(
        &layer_manager,
        &tmp_dir.join("unpack"),
        &Reference::from_str("test").unwrap(),
        false
    ).unwrap();

    let result = unpack_manager.remove_unpacking(
        &layer_manager,
        &tmp_dir.join("unpack"),
        false
    );

    assert!(result.is_ok());
    assert!(!tmp_dir.join("unpack").join("file1.txt").exists());

    #[allow(unused_must_use)] {
        std::fs::remove_dir_all(&tmp_dir);
    }
}

#[test]
fn test_unpack_replace1() {
    use std::str::FromStr;

    use crate::helpers;
    use crate::reference::ImageTag;
    use crate::image_definition::ImageDefinition;
    use crate::image_manager::build::BuildManager;
    use crate::image_manager::ImageManagerConfig;
    use crate::image_manager::printing::{ConsolePrinter};

    let tmp_dir = helpers::get_temp_folder();
    let config = ImageManagerConfig::with_base_dir(tmp_dir.clone());

    let printer = ConsolePrinter::new();
    let mut layer_manager = LayerManager::new();
    let build_manager = BuildManager::new(config.clone(), printer.clone());
    let mut unpack_manager = UnpackManager::new(config.clone(), printer.clone());

    let image_definition = ImageDefinition::from_str_without_context(&std::fs::read_to_string("testdata/definitions/simple1.labarfile").unwrap());
    assert!(image_definition.is_ok());
    let image_definition = image_definition.unwrap();

    assert!(build_manager.build_image(&mut layer_manager, Path::new(""), image_definition, &ImageTag::from_str("test").unwrap()).is_ok());

    unpack_manager.unpack(
        &layer_manager,
        &tmp_dir.join("unpack"),
        &Reference::from_str("test").unwrap(),
        false
    ).unwrap();

    let unpack_result = unpack_manager.unpack(
        &layer_manager,
        &tmp_dir.join("unpack"),
        &Reference::from_str("test").unwrap(),
        true
    );

    assert!(unpack_result.is_ok());
    assert!(tmp_dir.join("unpack").join("file1.txt").exists());

    #[allow(unused_must_use)] {
        std::fs::remove_dir_all(&tmp_dir);
    }
}

#[test]
fn test_unpack_replace2() {
    use std::str::FromStr;

    use crate::helpers;
    use crate::reference::ImageTag;
    use crate::image_definition::ImageDefinition;
    use crate::image_manager::build::BuildManager;
    use crate::image_manager::ImageManagerConfig;
    use crate::image_manager::printing::{ConsolePrinter};

    let tmp_dir = helpers::get_temp_folder();
    let config = ImageManagerConfig::with_base_dir(tmp_dir.clone());

    let printer = ConsolePrinter::new();
    let mut layer_manager = LayerManager::new();
    let build_manager = BuildManager::new(config.clone(), printer.clone());
    let mut unpack_manager = UnpackManager::new(config.clone(), printer.clone());

    let image_definition = ImageDefinition::from_str_without_context(&std::fs::read_to_string("testdata/definitions/simple1.labarfile").unwrap());
    assert!(image_definition.is_ok());
    let image_definition = image_definition.unwrap();

    assert!(build_manager.build_image(&mut layer_manager, Path::new(""), image_definition, &ImageTag::from_str("test").unwrap()).is_ok());

    let unpack_result = unpack_manager.unpack(
        &layer_manager,
        &tmp_dir.join("unpack"),
        &Reference::from_str("test").unwrap(),
        true
    );

    assert!(unpack_result.is_ok());
    assert!(tmp_dir.join("unpack").join("file1.txt").exists());

    #[allow(unused_must_use)] {
        std::fs::remove_dir_all(&tmp_dir);
    }
}