use std::collections::HashSet;
use std::fmt::{Display, Formatter};
use std::fs::{File};
use std::io::{BufReader};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use chrono::{DateTime, Local};
use itertools::izip;
use rusqlite::Row;

use serde::{Deserialize, Serialize};

use zip::write::{SimpleFileOptions};
use zip::ZipWriter;

use crate::helpers::clean_path;
use crate::image_manager::layer::{LayerManager};
use crate::image::{Layer, LayerOperation, LinkType};
use crate::image_manager::{ImageManagerConfig, ImageManagerError, ImageManagerResult};
use crate::image_manager::printing::{PrinterRef};
use crate::image_manager::state::StateSession;
use crate::reference::{ImageId, Reference};

#[derive(Debug, Serialize, Deserialize)]
pub struct Unpacking {
    pub destination: String,
    pub hash: ImageId,
    pub time: DateTime<Local>
}

impl Unpacking {
    pub fn new(layer: &Layer, destination: &str) -> Unpacking {
        Unpacking {
            hash: layer.hash.clone(),
            destination: destination.to_owned(),
            time: Local::now()
        }
    }

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
    printer: PrinterRef
}

impl UnpackManager {
    pub fn new(config: ImageManagerConfig, printer: PrinterRef) -> UnpackManager {
        UnpackManager {
            config,
            printer
        }
    }

    pub fn unpackings(&self, session: &StateSession) -> ImageManagerResult<Vec<Unpacking>> {
        Ok(session.all_unpackings()?)
    }

    pub fn unpack(&self,
                  session: &StateSession,
                  layer_manager: &LayerManager,
                  request: UnpackRequest) -> ImageManagerResult<()> {
        if !request.dry_run {
            self.unpack_with(
                &session,
                &StandardUnpacker,
                layer_manager,
                request
            )
        } else {
            self.unpack_with(
                &session,
                &DryRunUnpacker::new(self.printer.clone()),
                layer_manager,
                request
            )
        }
    }

    pub fn unpack_file(&self,
                       session: &StateSession,
                       layer_manager: &LayerManager,
                       unpack_file: UnpackFile) -> ImageManagerResult<()> {
        let dry_run = unpack_file.requests.get(0).map(|request| request.dry_run).unwrap_or(false);
        if !dry_run {
            self.unpack_requests_with(
                &session,
                &StandardUnpacker,
                layer_manager,
                &unpack_file.requests
            )
        } else {
            self.unpack_requests_with(
                &session,
                &DryRunUnpacker::new(self.printer.clone()),
                layer_manager,
                &unpack_file.requests
            )
        }
    }

    fn unpack_with(&self,
                   session: &StateSession,
                   unpacker: &impl Unpacker,
                   layer_manager: &LayerManager,
                   request: UnpackRequest) -> ImageManagerResult<()> {
        self.unpack_requests_with(
            session,
            unpacker,
            layer_manager,
            &[request]
        )
    }

    fn unpack_requests_with(&self,
                            session: &StateSession,
                            unpacker: &impl Unpacker,
                            layer_manager: &LayerManager,
                            requests: &[UnpackRequest]) -> ImageManagerResult<()> {
        let mut top_layers = Vec::new();
        for request in requests.iter() {
            top_layers.push(layer_manager.get_layer(&session, &request.reference)?);
        }

        for request in requests.iter() {
            if request.replace && request.unpack_folder.exists() {
                if let Err(err) = self.remove_unpacking(&session, layer_manager, &request.unpack_folder, true) {
                    self.printer.println(&format!("Failed removing packing due to: {}", err));
                }
            }
        }

        let mut unpack_folders = Vec::new();
        for request in requests.iter() {
            if !request.unpack_folder.exists() {
                unpacker.create_dir_all(&request.unpack_folder)?;
            }

            let unpack_folder = unpacker.canonicalize(&request.unpack_folder)?;

            self.check_exists(session, &unpack_folder)?;
            self.check_empty(&unpack_folder)?;

            unpack_folders.push(unpack_folder);
        }

        for (request, top_layer, unpack_folder) in izip!(requests.iter(), top_layers.iter(), unpack_folders.iter()) {
            let unpack_folder_str = unpack_folder.to_str().unwrap().to_owned();
            self.printer.println(&format!("Unpacking {} ({}) to {}", &request.reference, top_layer.hash, unpack_folder_str));
            self.unpack_layer(&session, unpacker, layer_manager, &mut HashSet::new(), &top_layer, &unpack_folder)?;

            if unpacker.should_insert() {
                session.insert_unpacking(Unpacking::new(&top_layer, &unpack_folder_str))?;
            }
        }

        Ok(())
    }

    fn check_empty(&self, unpack_folder: &Path) -> ImageManagerResult<()> {
        if unpack_folder.exists() {
            if std::fs::read_dir(&unpack_folder)?.count() > 0 {
                return Err(
                    ImageManagerError::FolderNotEmpty { path: unpack_folder.to_str().unwrap().to_owned() }
                );
            }
        }

        Ok(())
    }

    fn check_exists(&self, session: &StateSession, unpack_folder: &Path) -> ImageManagerResult<()> {
        let unpack_folder_str = unpack_folder.to_str().unwrap().to_owned();

        if session.unpacking_exist_at(&unpack_folder_str)? {
            return Err(ImageManagerError::UnpackingExist { path: unpack_folder_str.to_owned() });
        }

        Ok(())
    }

    fn unpack_layer(&self,
                    session: &StateSession,
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
            let parent_layer = layer_manager.get_layer(session, &parent_hash)?;
            self.unpack_layer(
                session,
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
                        session,
                        unpacker,
                        layer_manager,
                        already_unpacked,
                        &layer_manager.get_layer(session, &Reference::ImageId(hash.clone()))?,
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

                    if let Some(parent_dir) = destination_path.parent() {
                        unpacker.create_dir_all(parent_dir)?;
                    }

                    #[allow(unused_must_use)] {
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
                    unpacker.create_dir_all(&unpack_folder.join(path))?;
                },
            }
        }

        if has_files {
            self.printer.println("");
        }

        Ok(())
    }

    pub fn remove_unpacking(&self,
                            session: &StateSession,
                            layer_manager: &LayerManager,
                            unpack_folder: &Path, force: bool) -> ImageManagerResult<()> {
        let unpack_folder_str = unpack_folder.canonicalize()?.to_str().unwrap().to_owned();

        let unpacking = session.get_unpacking(&unpack_folder_str)?
            .ok_or_else(|| ImageManagerError::UnpackingNotFound { path: unpack_folder_str.clone() })?;

        self.printer.println(&format!("Clearing unpacking of {} at {}", unpacking.hash, unpack_folder_str));
        let unpacking_hash = Reference::ImageId(unpacking.hash.clone());
        let top_layer = layer_manager.get_layer(session, &unpacking_hash)?;

        if !force {
            self.remove_unpacked_layer(session, layer_manager, unpack_folder, &top_layer)?;
        } else {
            if let Err(err) = self.remove_unpacked_layer(session, layer_manager, unpack_folder, &top_layer) {
                self.printer.println(&format!("Failed to clear unpacking due to: {}", err));
            }
        }

        session.remove_unpacking(&unpack_folder_str)?;

        Ok(())
    }

    fn remove_unpacked_layer(&self,
                             session: &StateSession,
                             layer_manager: &LayerManager,
                             unpack_folder: &Path, layer: &Layer) -> ImageManagerResult<()> {
        for operation in layer.operations.iter().rev() {
            match operation {
                LayerOperation::Image { hash } => {
                    self.remove_unpacked_layer(
                        session,
                        layer_manager,
                        unpack_folder,
                        &layer_manager.get_layer(session, &Reference::ImageId(hash.clone()))?
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
            let parent_layer = layer_manager.get_layer(session, &parent_hash)?;
            self.remove_unpacked_layer(session, layer_manager, unpack_folder, &parent_layer)?;
        }

        self.printer.println("");
        Ok(())
    }

    pub fn extract(&self,
                   session: &StateSession,
                   layer_manager: &LayerManager,
                   reference: &Reference, archive_path: &Path) -> ImageManagerResult<()> {
        let file = File::create(archive_path)?;
        let mut writer = ZipWriter::new(file);

        fn inner(config: &ImageManagerConfig,
                 session: &StateSession,
                 layer_manager: &LayerManager,
                 layer: &Layer,
                 writer: &mut ZipWriter<File>) -> ImageManagerResult<()> {
            if let Some(parent_hash) = layer.parent_hash.as_ref() {
                let parent_layer = layer_manager.get_layer(&session, &parent_hash.clone().to_ref())?;
                inner(config, session, layer_manager, &parent_layer, writer)?;
            }

            for operation in &layer.operations {
                match operation {
                    LayerOperation::Image { hash } => {
                        let layer = layer_manager.get_layer(&session, &hash.clone().to_ref())?;
                        inner(config, session, layer_manager, &layer, writer)?;
                    }
                    LayerOperation::File { path, source_path, .. } => {
                        let abs_source_path = config.base_folder.join(source_path);
                        let mut reader = BufReader::new(File::open(&abs_source_path)?);

                        writer.start_file_from_path(path, SimpleFileOptions::default())?;
                        std::io::copy(&mut reader, writer)?;
                    }
                    LayerOperation::Directory { path } => {
                        writer.add_directory_from_path(path, SimpleFileOptions::default())?
                    }
                }
            }

            Ok(())
        }

        let top_layer = layer_manager.get_layer(&session, reference)?;
        inner(&self.config, &session, layer_manager, &top_layer, &mut writer)?;
        writer.finish()?;

        Ok(())
    }
}

pub struct UnpackRequest {
    pub reference: Reference,
    pub unpack_folder: PathBuf,
    pub replace: bool,
    pub dry_run: bool
}

pub struct UnpackFile {
    pub requests: Vec<UnpackRequest>
}

impl UnpackFile {
    pub fn parse(text: &str, dry_run: bool) -> Result<UnpackFile, UnpackFileParseError> {
        let mut requests = Vec::new();

        for line in text.lines() {
            let parts = line.split_whitespace().map(|x| x.to_owned()).collect::<Vec<_>>();
            if parts.len() >= 2 {
                let reference = Reference::from_str(&parts[0]).map_err(|error| UnpackFileParseError::InvalidReference { error })?;
                let unpack_folder = Path::new(&parts[1]).to_owned();

                let mut replace = false;
                if parts.len() == 3 {
                    if parts[2] == "--replace" {
                        replace = true;
                    }
                }

                requests.push(
                    UnpackRequest {
                        reference,
                        unpack_folder,
                        replace,
                        dry_run
                    }
                );
            } else {
                return Err(UnpackFileParseError::TooFewArguments);
            }
        }

        Ok(
            UnpackFile {
                requests
            }
        )
    }
}

#[derive(Debug)]
pub enum UnpackFileParseError {
    TooFewArguments,
    InvalidReference { error: String }
}

impl Display for UnpackFileParseError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            UnpackFileParseError::TooFewArguments => write!(f, "Too few arguments, expected at least 2"),
            UnpackFileParseError::InvalidReference { error } => write!(f, "Failed to parse image reference due to: {}", error)
        }
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
    printer: PrinterRef
}

impl DryRunUnpacker {
    pub fn new(printer: PrinterRef) -> DryRunUnpacker {
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

    use crate::reference::ImageTag;
    use crate::image_manager::build::BuildManager;
    use crate::image_manager::ImageManagerConfig;
    use crate::image_manager::printing::{ConsolePrinter};
    use crate::image_manager::state::StateManager;

    let tmp_folder = crate::test_helpers::TempFolder::new();
    let config = ImageManagerConfig::with_base_folder(tmp_folder.owned());

    let printer = ConsolePrinter::new();
    let state_manager = StateManager::new(&config.base_folder()).unwrap();
    let layer_manager = LayerManager::new(config.clone());
    let build_manager = BuildManager::new(config.clone(), printer.clone());
    let unpack_manager = UnpackManager::new(config.clone(), printer.clone());
    let mut session = state_manager.session().unwrap();

    super::test_helpers::build_image2(
        &mut session,
        &layer_manager,
        &build_manager,
        Path::new("testdata/definitions/simple1.labarfile"),
        ImageTag::from_str("test").unwrap()
    ).unwrap();

    let unpack_result = unpack_manager.unpack(
        &session,
        &layer_manager,
        UnpackRequest {
            reference: Reference::from_str("test").unwrap(),
            unpack_folder: tmp_folder.owned().join("unpack"),
            replace: false,
            dry_run: false,
        }
    );

    assert!(unpack_result.is_ok());
    assert!(tmp_folder.owned().join("unpack").join("file1.txt").exists());
}

#[test]
fn test_unpack_file() {
    use std::str::FromStr;

    use crate::reference::ImageTag;
    use crate::image_manager::build::BuildManager;
    use crate::image_manager::ImageManagerConfig;
    use crate::image_manager::printing::{ConsolePrinter};
    use crate::image_manager::state::StateManager;

    let tmp_folder = crate::test_helpers::TempFolder::new();
    let config = ImageManagerConfig::with_base_folder(tmp_folder.owned());

    let printer = ConsolePrinter::new();
    let state_manager = StateManager::new(&config.base_folder()).unwrap();
    let layer_manager = LayerManager::new(config.clone());
    let build_manager = BuildManager::new(config.clone(), printer.clone());
    let unpack_manager = UnpackManager::new(config.clone(), printer.clone());
    let mut session = state_manager.session().unwrap();

    super::test_helpers::build_image2(
        &mut session,
        &layer_manager,
        &build_manager,
        Path::new("testdata/definitions/simple1.labarfile"),
        ImageTag::from_str("test").unwrap()
    ).unwrap();

    super::test_helpers::build_image2(
        &mut session,
        &layer_manager,
        &build_manager,
        Path::new("testdata/definitions/simple5.labarfile"),
        ImageTag::from_str("test2").unwrap()
    ).unwrap();

    let unpack_result = unpack_manager.unpack_file(
        &session,
        &layer_manager,
        UnpackFile {
            requests: vec![
                UnpackRequest {
                    reference: Reference::from_str("test").unwrap(),
                    unpack_folder: tmp_folder.owned().join("unpack"),
                    replace: false,
                    dry_run: false,
                },
                UnpackRequest {
                    reference: Reference::from_str("test2").unwrap(),
                    unpack_folder: tmp_folder.owned().join("unpack2"),
                    replace: false,
                    dry_run: false,
                }
            ],
        }
    );

    assert!(unpack_result.is_ok());
    assert!(tmp_folder.owned().join("unpack").join("file1.txt").exists());
    assert!(tmp_folder.owned().join("unpack2").join("test/file1.txt").exists());
    assert!(tmp_folder.owned().join("unpack2").join("test/file2.txt").exists());
}

#[test]
fn test_unpack_exist() {
    use std::str::FromStr;

    use crate::reference::ImageTag;
    use crate::image_manager::build::BuildManager;
    use crate::image_manager::ImageManagerConfig;
    use crate::image_manager::printing::{ConsolePrinter};
    use crate::image_manager::state::StateManager;

    let tmp_folder = crate::test_helpers::TempFolder::new();
    let config = ImageManagerConfig::with_base_folder(tmp_folder.owned());

    let printer = ConsolePrinter::new();
    let state_manager = StateManager::new(&config.base_folder()).unwrap();
    let layer_manager = LayerManager::new(config.clone());
    let build_manager = BuildManager::new(config.clone(), printer.clone());
    let unpack_manager = UnpackManager::new(config.clone(), printer.clone());
    let mut session = state_manager.session().unwrap();

    super::test_helpers::build_image2(
        &mut session,
        &layer_manager,
        &build_manager,
        Path::new("testdata/definitions/simple1.labarfile"),
        ImageTag::from_str("test").unwrap()
    ).unwrap();

    unpack_manager.unpack(
        &session,
        &layer_manager,
        UnpackRequest {
            reference: Reference::from_str("test").unwrap(),
            unpack_folder: tmp_folder.owned().join("unpack"),
            replace: false,
            dry_run: false,
        }
    ).unwrap();

    let unpack_result = unpack_manager.unpack(
        &session,
        &layer_manager,
        UnpackRequest {
            reference: Reference::from_str("test").unwrap(),
            unpack_folder: tmp_folder.owned().join("unpack"),
            replace: false,
            dry_run: false,
        }
    );

    assert!(unpack_result.is_err());
    assert!(tmp_folder.owned().join("unpack").join("file1.txt").exists());
}

#[test]
fn test_remove_unpack() {
    use std::str::FromStr;

    use crate::reference::ImageTag;
    use crate::image_manager::build::BuildManager;
    use crate::image_manager::ImageManagerConfig;
    use crate::image_manager::printing::{ConsolePrinter};
    use crate::image_manager::state::StateManager;

    let tmp_folder = crate::test_helpers::TempFolder::new();
    let config = ImageManagerConfig::with_base_folder(tmp_folder.owned());

    let printer = ConsolePrinter::new();
    let state_manager = StateManager::new(&config.base_folder()).unwrap();
    let layer_manager = LayerManager::new(config.clone());
    let build_manager = BuildManager::new(config.clone(), printer.clone());
    let unpack_manager = UnpackManager::new(config.clone(), printer.clone());
    let mut session = state_manager.session().unwrap();

    super::test_helpers::build_image2(
        &mut session,
        &layer_manager,
        &build_manager,
        Path::new("testdata/definitions/simple1.labarfile"),
        ImageTag::from_str("test").unwrap()
    ).unwrap();

    unpack_manager.unpack(
        &session,
        &layer_manager,
        UnpackRequest {
            reference: Reference::from_str("test").unwrap(),
            unpack_folder: tmp_folder.owned().join("unpack"),
            replace: false,
            dry_run: false,
        }
    ).unwrap();

    let session = state_manager.session().unwrap();
    let result = unpack_manager.remove_unpacking(
        &session,
        &layer_manager,
        &tmp_folder.owned().join("unpack"),
        false
    );

    assert!(result.is_ok(), "{}", result.unwrap_err());
    assert!(!tmp_folder.owned().join("unpack").join("file1.txt").exists());
}

#[test]
fn test_unpack_replace1() {
    use std::str::FromStr;

    use crate::reference::ImageTag;
    use crate::image_manager::build::BuildManager;
    use crate::image_manager::ImageManagerConfig;
    use crate::image_manager::printing::{ConsolePrinter};
    use crate::image_manager::state::StateManager;

    let tmp_folder = crate::test_helpers::TempFolder::new();
    let config = ImageManagerConfig::with_base_folder(tmp_folder.owned());

    let printer = ConsolePrinter::new();
    let state_manager = StateManager::new(&config.base_folder()).unwrap();
    let layer_manager = LayerManager::new(config.clone());
    let build_manager = BuildManager::new(config.clone(), printer.clone());
    let unpack_manager = UnpackManager::new(config.clone(), printer.clone());
    let mut session = state_manager.session().unwrap();

    super::test_helpers::build_image2(
        &mut session,
        &layer_manager,
        &build_manager,
        Path::new("testdata/definitions/simple1.labarfile"),
        ImageTag::from_str("test").unwrap()
    ).unwrap();

    unpack_manager.unpack(
        &session,
        &layer_manager,
        UnpackRequest {
            reference: Reference::from_str("test").unwrap(),
            unpack_folder: tmp_folder.owned().join("unpack"),
            replace: false,
            dry_run: false,
        }
    ).unwrap();

    let unpack_result = unpack_manager.unpack(
        &session,
        &layer_manager,
        UnpackRequest {
            reference: Reference::from_str("test").unwrap(),
            unpack_folder: tmp_folder.owned().join("unpack"),
            replace: true,
            dry_run: false,
        }
    );

    assert!(unpack_result.is_ok());
    assert!(tmp_folder.owned().join("unpack").join("file1.txt").exists());
}

#[test]
fn test_unpack_replace2() {
    use std::str::FromStr;

    use crate::reference::ImageTag;
    use crate::image_manager::build::BuildManager;
    use crate::image_manager::ImageManagerConfig;
    use crate::image_manager::printing::{ConsolePrinter};
    use crate::image_manager::state::StateManager;

    let tmp_folder = crate::test_helpers::TempFolder::new();
    let config = ImageManagerConfig::with_base_folder(tmp_folder.owned().clone());

    let printer = ConsolePrinter::new();
    let state_manager = StateManager::new(&config.base_folder()).unwrap();
    let layer_manager = LayerManager::new(config.clone());
    let build_manager = BuildManager::new(config.clone(), printer.clone());
    let unpack_manager = UnpackManager::new(config.clone(), printer.clone());
    let mut session = state_manager.session().unwrap();

    super::test_helpers::build_image2(
        &mut session,
        &layer_manager,
        &build_manager,
        Path::new("testdata/definitions/simple1.labarfile"),
        ImageTag::from_str("test").unwrap()
    ).unwrap();

    let unpack_result = unpack_manager.unpack(
        &session,
        &layer_manager,
        UnpackRequest {
            reference: Reference::from_str("test").unwrap(),
            unpack_folder: tmp_folder.owned().join("unpack"),
            replace: true,
            dry_run: false,
        }
    );

    assert!(unpack_result.is_ok());
    assert!(tmp_folder.owned().join("unpack").join("file1.txt").exists());
}

#[test]
fn test_unpack_self_reference() {
    use std::str::FromStr;

    use crate::image_manager::ImageManagerConfig;
    use crate::image_manager::printing::{ConsolePrinter};
    use crate::image_manager::state::StateManager;

    let tmp_folder = crate::test_helpers::TempFolder::new();
    let config = ImageManagerConfig::with_base_folder(tmp_folder.owned().clone());

    let printer = ConsolePrinter::new();
    let state_manager = StateManager::new(&config.base_folder()).unwrap();
    let layer_manager = LayerManager::new(config.clone());
    let unpack_manager = UnpackManager::new(config.clone(), printer.clone());
    let session = state_manager.session().unwrap();

    let hash = ImageId::from_str("3d197ee59b46d114379522e6f68340371f2f1bc1525cb4456caaf5b8430acea3").unwrap();
    let layer = Layer {
        parent_hash: None,
        hash: hash.clone(),
        operations: vec![LayerOperation::Image { hash: hash.clone() }],
        created: Local::now(),
    };
    layer_manager.insert_layer(&session, layer).unwrap();

    let unpack_result = unpack_manager.unpack(
        &session,
        &layer_manager,
        UnpackRequest {
            reference: hash.clone().to_ref(),
            unpack_folder: tmp_folder.owned().join("unpack"),
            replace: false,
            dry_run: false,
        }
    );

    assert!(unpack_result.is_err());
}

#[test]
fn test_extract() {
    use std::str::FromStr;
    use zip::ZipArchive;

    use crate::reference::ImageTag;
    use crate::image_manager::build::BuildManager;
    use crate::image_manager::ImageManagerConfig;
    use crate::image_manager::printing::{ConsolePrinter};
    use crate::image_manager::state::StateManager;

    let tmp_folder = crate::test_helpers::TempFolder::new();
    let config = ImageManagerConfig::with_base_folder(tmp_folder.owned().clone());

    let printer = ConsolePrinter::new();
    let state_manager = StateManager::new(&config.base_folder()).unwrap();
    let mut layer_manager = LayerManager::new(config.clone());
    let build_manager = BuildManager::new(config.clone(), printer.clone());
    let unpack_manager = UnpackManager::new(config.clone(), printer.clone());
    let mut session = state_manager.session().unwrap();

    super::test_helpers::build_image2(
        &mut session,
        &mut layer_manager,
        &build_manager,
        Path::new("testdata/definitions/simple3.labarfile"),
        ImageTag::from_str("test").unwrap()
    ).unwrap();

    let archive_file = tmp_folder.owned().join("extract.zip");
    let extract_result = unpack_manager.extract(
        &session,
        &layer_manager,
        &Reference::from_str("test").unwrap(),
        &archive_file
    );

    assert!(extract_result.is_ok());
    assert!(archive_file.exists());

    let zip_archive = ZipArchive::new(File::open(archive_file).unwrap()).unwrap();
    assert_eq!(vec!["test/file1.txt", "test2/"], zip_archive.file_names().collect::<Vec<_>>());
    assert_eq!(973, zip_archive.decompressed_size().unwrap() as u64);
}

#[test]
fn test_parse_unpack_file1() {
    let content = std::fs::read_to_string("testdata/unpack_file/test1.unpackfile").unwrap();
    let unpack_file = UnpackFile::parse(&content, false).unwrap();

    assert_eq!(2, unpack_file.requests.len());

    assert_eq!(Reference::from_str("test:latest").unwrap(), unpack_file.requests[0].reference);
    assert_eq!(Path::new("/home/labar/test").to_owned(), unpack_file.requests[0].unpack_folder);
    assert_eq!(false, unpack_file.requests[0].replace);

    assert_eq!(Reference::from_str("test2:latest").unwrap(), unpack_file.requests[1].reference);
    assert_eq!(Path::new("/home/labar/test2").to_owned(), unpack_file.requests[1].unpack_folder);
    assert_eq!(true, unpack_file.requests[1].replace);
}