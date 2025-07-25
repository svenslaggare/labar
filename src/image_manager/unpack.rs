use std::collections::HashSet;
use std::fs::{File};
use std::io::{BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::{DateTime, Local};

use rusqlite::Row;

use serde::{Deserialize, Serialize};

use zip::write::{SimpleFileOptions};
use zip::ZipWriter;

use crate::helpers::clean_path;
use crate::image_manager::layer::{LayerManager};
use crate::image::{Layer, LayerOperation, LinkType};
use crate::image_manager::{ImageManagerConfig, ImageManagerError, ImageManagerResult};
use crate::image_manager::printing::{BoxPrinter};
use crate::image_manager::state::StateManager;
use crate::reference::{ImageId, Reference};

#[derive(Debug, Serialize, Deserialize)]
pub struct Unpacking {
    pub destination: String,
    pub hash: ImageId,
    pub time: DateTime<Local>
}

impl Unpacking {
    pub fn from_row(row: &Row) -> rusqlite::Result<Unpacking> {
        Ok(
            Unpacking {
                destination: row.get(0)?,
                hash: row.get(1)?,
                time: row.get(2)?
            }
        )
    }
}

pub struct UnpackManager {
    config: ImageManagerConfig,
    printer: BoxPrinter,
    state_manager: Arc<StateManager>
}

impl UnpackManager {
    pub fn new(config: ImageManagerConfig, printer: BoxPrinter, state_manager: Arc<StateManager>) -> UnpackManager {
        UnpackManager {
            config,
            printer,
            state_manager
        }
    }

    pub fn unpackings(&self) -> ImageManagerResult<Vec<Unpacking>> {
        Ok(self.state_manager.all_unpackings()?)
    }

    pub fn unpack(&mut self,
                  layer_manager: &LayerManager,
                  reference: &Reference,
                  unpack_folder: &Path,
                  replace: bool) -> ImageManagerResult<()> {
        self.unpack_with(
            &StandardUnpacker,
            layer_manager,
            reference,
            unpack_folder,
            replace
        )
    }

    pub fn unpack_with(&mut self,
                       unpacker: &impl Unpacker,
                       layer_manager: &LayerManager,
                       reference: &Reference,
                       unpack_folder: &Path,
                       replace: bool) -> ImageManagerResult<()> {
        if replace && unpack_folder.exists() {
            if let Err(err) = self.remove_unpacking(layer_manager, &unpack_folder, true) {
                self.printer.println(&format!("Failed removing packing due to: {}", err));
            }
        }

        let top_layer = layer_manager.get_layer(reference)?;
        if !unpack_folder.exists() {
            unpacker.create_dir_all(unpack_folder)?;
        }

        let unpack_folder = unpacker.canonicalize(unpack_folder)?;
        let unpack_folder_str = unpack_folder.to_str().unwrap().to_owned();

        if self.state_manager.unpacking_exist_at(&unpack_folder_str)? {
            return Err(ImageManagerError::UnpackingExist { path: unpack_folder_str.clone() });
        }

        if unpack_folder.exists() {
            if std::fs::read_dir(&unpack_folder)?.count() > 0 {
                return Err(ImageManagerError::FolderNotEmpty { path: unpack_folder_str.clone() });
            }
        }

        self.printer.println(&format!("Unpacking {} ({}) to {}", reference, top_layer.hash, unpack_folder_str));
        self.unpack_layer(unpacker, layer_manager, &mut HashSet::new(), &top_layer, &unpack_folder)?;

        if unpacker.should_insert() {
            self.state_manager.insert_unpacking(
                Unpacking {
                    hash: top_layer.hash.clone(),
                    destination: unpack_folder_str,
                    time: Local::now()
                }
            )?;
        }

        Ok(())
    }

    fn unpack_layer(&self,
                    unpacker: &impl Unpacker,
                    layer_manager: &LayerManager,
                    already_unpacked: &mut HashSet<ImageId>,
                    layer: &Layer,
                    unpack_folder: &Path) -> ImageManagerResult<()> {
        if already_unpacked.contains(&layer.hash) {
            return Err(ImageManagerError::SelfReferential);
        }

        already_unpacked.insert(layer.hash.clone());

        if let Some(parent_hash) = layer.parent_hash.as_ref() {
            let parent_hash = parent_hash.clone().to_ref();
            let parent_layer = layer_manager.get_layer(&parent_hash)?;
            self.unpack_layer(
                unpacker,
                layer_manager,
                already_unpacked,
                &parent_layer,
                &unpack_folder
            )?;
        }

        let mut has_files = false;
        for operation in &layer.operations {
            match operation {
                LayerOperation::Image { hash } => {
                    self.unpack_layer(
                        unpacker,
                        layer_manager,
                        already_unpacked,
                        &layer_manager.get_layer(&Reference::ImageId(hash.clone()))?,
                        &unpack_folder
                    )?;
                },
                LayerOperation::File { path, source_path, link_type, writable, .. } => {
                    let abs_source_path = self.config.base_folder.canonicalize()?.join(source_path);
                    if abs_source_path != clean_path(&abs_source_path) {
                        return Err(ImageManagerError::InvalidUnpack);
                    }

                    has_files = true;
                    let destination_path = unpack_folder.join(path);
                    if destination_path != clean_path(&destination_path) {
                        return Err(ImageManagerError::InvalidUnpack);
                    }

                    self.printer.println(&format!("\t* Unpacking file {} -> {}", path, destination_path.to_str().unwrap()));

                    #[allow(unused_must_use)] {
                        if let Some(parent_dir) = destination_path.parent() {
                            unpacker.create_dir_all(parent_dir)?;
                        }

                        unpacker.remove_file(&destination_path);
                    }

                    match link_type {
                        LinkType::Soft => {
                            unpacker.create_soft_link(&abs_source_path, &destination_path)?;
                        },
                        LinkType::Hard => {
                            unpacker.create_hard_link(&abs_source_path, &destination_path)?;
                        },
                    }

                    if !writable {
                        unpacker.set_readonly(&destination_path)?;
                    }
                },
                LayerOperation::Directory { path } => {
                    self.printer.println(&format!("\t* Creating directory {}", path));

                    #[allow(unused_must_use)] {
                        unpacker.create_dir_all(&unpack_folder.join(path))?;
                    }
                },
            }
        }

        if has_files {
            self.printer.println("");
        }

        Ok(())
    }

    pub fn remove_unpacking(&mut self, layer_manager: &LayerManager, unpack_folder: &Path, force: bool) -> ImageManagerResult<()> {
        let unpack_folder_str = unpack_folder.canonicalize()?.to_str().unwrap().to_owned();

        let unpacking = self.state_manager.get_unpacking(&unpack_folder_str)?
            .ok_or_else(|| ImageManagerError::UnpackingNotFound { path: unpack_folder_str.clone() })?;

        self.printer.println(&format!("Clearing unpacking of {} at {}", unpacking.hash, unpack_folder_str));
        let unpacking_hash = Reference::ImageId(unpacking.hash.clone());
        let top_layer = layer_manager.get_layer(&unpacking_hash)?;

        if !force {
            self.remove_unpacked_layer(layer_manager, unpack_folder, &top_layer)?;
        } else {
            if let Err(err) = self.remove_unpacked_layer(layer_manager, unpack_folder, &top_layer) {
                self.printer.println(&format!("Failed to clear unpacking due to: {}", err));
            }
        }

        self.state_manager.remove_unpacking(&unpack_folder_str)?;

        Ok(())
    }

    fn remove_unpacked_layer(&self, layer_manager: &LayerManager, unpack_folder: &Path, layer: &Layer) -> ImageManagerResult<()> {
        for operation in layer.operations.iter().rev() {
            match operation {
                LayerOperation::Image { hash } => {
                    self.remove_unpacked_layer(
                        layer_manager,
                        unpack_folder,
                        &layer_manager.get_layer(&Reference::ImageId(hash.clone()))?
                    )?;
                },
                LayerOperation::File { path, .. } => {
                    let destination_path = unpack_folder.join(path);
                    self.printer.println(&format!("\t* Deleting link of file {}", destination_path.to_str().unwrap()));
                    std::fs::remove_file(destination_path)?;
                },
                LayerOperation::Directory { path } => {
                    let path = unpack_folder.join(path);
                    self.printer.println(&format!("\t* Deleting directory {}", path.to_str().unwrap()));
                    std::fs::remove_dir(path)?
                },
            }
        }

        if let Some(parent_hash) = layer.parent_hash.as_ref() {
            let parent_hash = Reference::ImageId(parent_hash.clone());
            let parent_layer = layer_manager.get_layer(&parent_hash)?;
            self.remove_unpacked_layer(layer_manager, unpack_folder, &parent_layer)?;
        }

        self.printer.println("");
        Ok(())
    }

    pub fn extract(&self, layer_manager: &LayerManager, reference: &Reference, archive_path: &Path) -> ImageManagerResult<()> {
        let file = File::create(archive_path)?;
        let mut writer = ZipWriter::new(file);

        fn inner(config: &ImageManagerConfig,
                 layer_manager: &LayerManager,
                 layer: &Layer,
                 writer: &mut ZipWriter<File>) -> ImageManagerResult<()> {
            if let Some(parent_hash) = layer.parent_hash.as_ref() {
                let parent_layer = layer_manager.get_layer(&parent_hash.clone().to_ref())?;
                inner(config, layer_manager, &parent_layer, writer)?;
            }

            for operation in &layer.operations {
                match operation {
                    LayerOperation::Image { hash } => {
                        let layer = layer_manager.get_layer(&hash.clone().to_ref())?;
                        inner(config, layer_manager, &layer, writer)?;
                    }
                    LayerOperation::File { path, source_path, .. } => {
                        let abs_source_path = config.base_folder.join(source_path);
                        let mut reader = BufReader::new(File::open(&abs_source_path)?);

                        writer.start_file_from_path(path, SimpleFileOptions::default())?;

                        let mut buffer = vec![0; 4096];
                        loop {
                            let count = reader.read(&mut buffer)?;
                            if count == 0 {
                                break;
                            }

                            writer.write_all(&buffer[..count])?;
                        }
                    }
                    LayerOperation::Directory { path } => {
                        writer.add_directory_from_path(path, SimpleFileOptions::default())?
                    }
                }
            }

            Ok(())
        }

        let top_layer = layer_manager.get_layer(reference)?;
        inner(&self.config, layer_manager, &top_layer, &mut writer)?;
        writer.finish()?;

        Ok(())
    }
}

pub trait Unpacker {
    fn should_insert(&self) -> bool;

    fn canonicalize(&self, path: &Path) -> ImageManagerResult<PathBuf>;
    fn create_dir_all(&self, path: &Path) -> ImageManagerResult<()>;
    fn remove_file(&self, path: &Path) -> ImageManagerResult<()>;
    fn create_soft_link(&self, source: &Path, target: &Path) -> ImageManagerResult<()>;
    fn create_hard_link(&self, source: &Path, target: &Path) -> ImageManagerResult<()>;
    fn set_readonly(&self, path: &Path) -> ImageManagerResult<()>;
}

pub struct StandardUnpacker;
impl Unpacker for StandardUnpacker {
    fn should_insert(&self) -> bool {
        true
    }

    fn canonicalize(&self, path: &Path) -> ImageManagerResult<PathBuf> {
        Ok(path.canonicalize()?)
    }

    fn create_dir_all(&self, path: &Path) -> ImageManagerResult<()> {
        std::fs::create_dir_all(path)?;
        Ok(())
    }

    fn remove_file(&self, path: &Path) -> ImageManagerResult<()> {
        std::fs::remove_file(path)?;
        Ok(())
    }

    fn create_soft_link(&self, source: &Path, target: &Path) -> ImageManagerResult<()> {
        std::os::unix::fs::symlink(&source, &target)
            .map_err(|err|
                ImageManagerError::FileIOError {
                    message: format!("Failed to unpack file {} due to: {}", target.display(), err)
                }
            )?;
        Ok(())
    }

    fn create_hard_link(&self, source: &Path, target: &Path) -> ImageManagerResult<()> {
        std::fs::hard_link(&source, &target)
            .map_err(|err|
                ImageManagerError::FileIOError {
                    message: format!("Failed to unpack file {} due to: {}", target.display(), err)
                }
            )?;
        Ok(())
    }

    fn set_readonly(&self, path: &Path) -> ImageManagerResult<()> {
        let file = File::open(path)?;
        let mut permissions = file.metadata()?.permissions();
        permissions.set_readonly(true);
        file.set_permissions(permissions)?;
        Ok(())
    }
}

pub struct DryRunUnpacker {
    printer: BoxPrinter
}

impl DryRunUnpacker {
    pub fn new(printer: BoxPrinter) -> DryRunUnpacker {
        DryRunUnpacker {
            printer
        }
    }
}

impl Unpacker for DryRunUnpacker {
    fn should_insert(&self) -> bool {
        false
    }

    fn canonicalize(&self, path: &Path) -> ImageManagerResult<PathBuf> {
        Ok(clean_path(path))
    }

    fn create_dir_all(&self, path: &Path) -> ImageManagerResult<()> {
        self.printer.println(&format!("\t\t* Create directory {}", path.display()));
        Ok(())
    }

    fn remove_file(&self, path: &Path) -> ImageManagerResult<()> {
        self.printer.println(&format!("\t\t* Remove file {}", path.display()));
        Ok(())
    }

    fn create_soft_link(&self, source: &Path, target: &Path) -> ImageManagerResult<()> {
        self.printer.println(&format!("\t\t* Creating soft link  {} -> {}", source.display(), target.display()));
        Ok(())
    }

    fn create_hard_link(&self, source: &Path, target: &Path) -> ImageManagerResult<()> {
        self.printer.println(&format!("\t\t* Creating hard link  {} -> {}", source.display(), target.display()));
        Ok(())
    }

    fn set_readonly(&self, path: &Path) -> ImageManagerResult<()> {
        self.printer.println(&format!("\t\t* Setting {} to read only", path.display()));
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
    use crate::image_manager::build::BuildRequest;

    let tmp_dir = helpers::get_temp_folder();
    let config = ImageManagerConfig::with_base_folder(tmp_dir.clone());

    let printer = ConsolePrinter::new();
    let state_manager = Arc::new(StateManager::new(&config.base_folder()).unwrap());
    let mut layer_manager = LayerManager::new(config.clone(), state_manager.clone());
    let build_manager = BuildManager::new(config.clone(), printer.clone(), state_manager.clone());
    let mut unpack_manager = UnpackManager::new(config.clone(), printer.clone(), state_manager.clone());

    let image_definition = ImageDefinition::parse_without_context(&std::fs::read_to_string("testdata/definitions/simple1.labarfile").unwrap());
    assert!(image_definition.is_ok());
    let image_definition = image_definition.unwrap();

    assert!(build_manager.build_image(
        &mut layer_manager,
        BuildRequest {
            build_context: Path::new("").to_path_buf(),
            image_definition,
            tag: ImageTag::from_str("test").unwrap(),
            force: false,
        }
    ).is_ok());

    let unpack_result = unpack_manager.unpack(
        &layer_manager,
        &Reference::from_str("test").unwrap(),
        &tmp_dir.join("unpack"),
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
    use crate::image_manager::build::BuildRequest;

    let tmp_dir = helpers::get_temp_folder();
    let config = ImageManagerConfig::with_base_folder(tmp_dir.clone());

    let printer = ConsolePrinter::new();
    let state_manager = Arc::new(StateManager::new(&config.base_folder()).unwrap());
    let mut layer_manager = LayerManager::new(config.clone(), state_manager.clone());
    let build_manager = BuildManager::new(config.clone(), printer.clone(), state_manager.clone());
    let mut unpack_manager = UnpackManager::new(config.clone(), printer.clone(), state_manager);

    let image_definition = ImageDefinition::parse_without_context(&std::fs::read_to_string("testdata/definitions/simple1.labarfile").unwrap());
    assert!(image_definition.is_ok());
    let image_definition = image_definition.unwrap();

    assert!(build_manager.build_image(
        &mut layer_manager,
        BuildRequest {
            build_context: Path::new("").to_path_buf(),
            image_definition,
            tag: ImageTag::from_str("test").unwrap(),
            force: false,
        }
    ).is_ok());

    unpack_manager.unpack(
        &layer_manager,
        &Reference::from_str("test").unwrap(),
        &tmp_dir.join("unpack"),
        false
    ).unwrap();

    let unpack_result = unpack_manager.unpack(
        &layer_manager,
        &Reference::from_str("test").unwrap(),
        &tmp_dir.join("unpack"),
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
    use crate::image_manager::build::BuildRequest;

    let tmp_dir = helpers::get_temp_folder();
    let config = ImageManagerConfig::with_base_folder(tmp_dir.clone());

    let printer = ConsolePrinter::new();
    let state_manager = Arc::new(StateManager::new(&config.base_folder()).unwrap());
    let mut layer_manager = LayerManager::new(config.clone(), state_manager.clone());
    let build_manager = BuildManager::new(config.clone(), printer.clone(), state_manager.clone());
    let mut unpack_manager = UnpackManager::new(config.clone(), printer.clone(), state_manager);

    let image_definition = ImageDefinition::parse_without_context(&std::fs::read_to_string("testdata/definitions/simple1.labarfile").unwrap());
    assert!(image_definition.is_ok());
    let image_definition = image_definition.unwrap();

    assert!(build_manager.build_image(
        &mut layer_manager,
        BuildRequest {
            build_context: Path::new("").to_path_buf(),
            image_definition,
            tag: ImageTag::from_str("test").unwrap(),
            force: false,
        }
    ).is_ok());

    unpack_manager.unpack(
        &layer_manager,
        &Reference::from_str("test").unwrap(),
        &tmp_dir.join("unpack"),
        false
    ).unwrap();

    let result = unpack_manager.remove_unpacking(
        &layer_manager,
        &tmp_dir.join("unpack"),
        false
    );

    assert!(result.is_ok(), "{}", result.unwrap_err());
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
    use crate::image_manager::build::BuildRequest;

    let tmp_dir = helpers::get_temp_folder();
    let config = ImageManagerConfig::with_base_folder(tmp_dir.clone());

    let printer = ConsolePrinter::new();
    let state_manager = Arc::new(StateManager::new(&config.base_folder()).unwrap());
    let mut layer_manager = LayerManager::new(config.clone(), state_manager.clone());
    let build_manager = BuildManager::new(config.clone(), printer.clone(), state_manager.clone());
    let mut unpack_manager = UnpackManager::new(config.clone(), printer.clone(), state_manager);

    let image_definition = ImageDefinition::parse_without_context(&std::fs::read_to_string("testdata/definitions/simple1.labarfile").unwrap());
    assert!(image_definition.is_ok());
    let image_definition = image_definition.unwrap();

    assert!(build_manager.build_image(
        &mut layer_manager,
        BuildRequest {
            build_context: Path::new("").to_path_buf(),
            image_definition,
            tag: ImageTag::from_str("test").unwrap(),
            force: false,
        }
    ).is_ok());

    unpack_manager.unpack(
        &layer_manager,
        &Reference::from_str("test").unwrap(),
        &tmp_dir.join("unpack"),
        false
    ).unwrap();

    let unpack_result = unpack_manager.unpack(
        &layer_manager,
        &Reference::from_str("test").unwrap(),
        &tmp_dir.join("unpack"),
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
    use crate::image_manager::build::BuildRequest;

    let tmp_dir = helpers::get_temp_folder();
    let config = ImageManagerConfig::with_base_folder(tmp_dir.clone());

    let printer = ConsolePrinter::new();
    let state_manager = Arc::new(StateManager::new(&config.base_folder()).unwrap());
    let mut layer_manager = LayerManager::new(config.clone(), state_manager.clone());
    let build_manager = BuildManager::new(config.clone(), printer.clone(), state_manager.clone());
    let mut unpack_manager = UnpackManager::new(config.clone(), printer.clone(), state_manager);

    let image_definition = ImageDefinition::parse_without_context(&std::fs::read_to_string("testdata/definitions/simple1.labarfile").unwrap());
    assert!(image_definition.is_ok());
    let image_definition = image_definition.unwrap();

    assert!(build_manager.build_image(
        &mut layer_manager,
        BuildRequest {
            build_context: Path::new("").to_path_buf(),
            image_definition,
            tag: ImageTag::from_str("test").unwrap(),
            force: false,
        }
    ).is_ok());

    let unpack_result = unpack_manager.unpack(
        &layer_manager,
        &Reference::from_str("test").unwrap(),
        &tmp_dir.join("unpack"),
        true
    );

    assert!(unpack_result.is_ok());
    assert!(tmp_dir.join("unpack").join("file1.txt").exists());

    #[allow(unused_must_use)] {
        std::fs::remove_dir_all(&tmp_dir);
    }
}

#[test]
fn test_unpack_self_reference() {
    use std::str::FromStr;

    use crate::helpers;
    use crate::image_manager::ImageManagerConfig;
    use crate::image_manager::printing::{ConsolePrinter};

    let tmp_dir = helpers::get_temp_folder();
    let config = ImageManagerConfig::with_base_folder(tmp_dir.clone());

    let printer = ConsolePrinter::new();
    let state_manager = Arc::new(StateManager::new(&config.base_folder()).unwrap());
    let mut layer_manager = LayerManager::new(config.clone(), state_manager.clone());
    let mut unpack_manager = UnpackManager::new(config.clone(), printer.clone(), state_manager);

    let hash = ImageId::from_str("3d197ee59b46d114379522e6f68340371f2f1bc1525cb4456caaf5b8430acea3").unwrap();
    let layer = Layer {
        parent_hash: None,
        hash: hash.clone(),
        operations: vec![LayerOperation::Image { hash: hash.clone() }],
        created: Local::now(),
    };
    layer_manager.insert_layer(layer).unwrap();

    let unpack_result = unpack_manager.unpack(
        &layer_manager,
        &hash.clone().to_ref(),
        &tmp_dir.join("unpack"),
        false
    );

    assert!(unpack_result.is_err());

    #[allow(unused_must_use)] {
        std::fs::remove_dir_all(&tmp_dir);
    }
}

#[test]
fn test_extract() {
    use std::str::FromStr;
    use zip::ZipArchive;

    use crate::helpers;
    use crate::reference::ImageTag;
    use crate::image_definition::ImageDefinition;
    use crate::image_manager::build::BuildManager;
    use crate::image_manager::ImageManagerConfig;
    use crate::image_manager::printing::{ConsolePrinter};
    use crate::image_manager::build::BuildRequest;

    let tmp_dir = helpers::get_temp_folder();
    let config = ImageManagerConfig::with_base_folder(tmp_dir.clone());

    let printer = ConsolePrinter::new();
    let state_manager = Arc::new(StateManager::new(&config.base_folder()).unwrap());
    let mut layer_manager = LayerManager::new(config.clone(), state_manager.clone());
    let build_manager = BuildManager::new(config.clone(), printer.clone(), state_manager.clone());
    let unpack_manager = UnpackManager::new(config.clone(), printer.clone(), state_manager.clone());

    let image_definition = ImageDefinition::parse_without_context(&std::fs::read_to_string("testdata/definitions/simple3.labarfile").unwrap());
    assert!(image_definition.is_ok());
    let image_definition = image_definition.unwrap();

    assert!(build_manager.build_image(
        &mut layer_manager,
        BuildRequest {
            build_context: Path::new("").to_path_buf(),
            image_definition,
            tag: ImageTag::from_str("test").unwrap(),
            force: false,
        }
    ).is_ok());

    let archive_file = tmp_dir.join("extract.zip");
    let extract_result = unpack_manager.extract(
        &layer_manager,
        &Reference::from_str("test").unwrap(),
        &archive_file
    );

    assert!(extract_result.is_ok());
    assert!(archive_file.exists());

    let zip_archive = ZipArchive::new(File::open(archive_file).unwrap()).unwrap();
    assert_eq!(vec!["test/file1.txt", "test2/"], zip_archive.file_names().collect::<Vec<_>>());
    assert_eq!(974, zip_archive.decompressed_size().unwrap() as u64);

    #[allow(unused_must_use)] {
        std::fs::remove_dir_all(&tmp_dir);
    }
}
