use std::path::{Path, PathBuf};
use zip::result::ZipError;

mod layer;
mod unpack;
mod build;
mod printing;
mod registry;
mod state;
mod image;

#[derive(Debug)]
pub enum ImageManagerError {
    ImageParser { error: ImageParseError },
    LayerNotFound { image_id: ImageId },
    ImageNotFound { reference: Reference },
    FileIOError { message: String },
    FileNotInBuildContext { path: String },
    ZIPError(ZipError),
    UnpackingExist { path: String },
    UnpackingNotFound { path: String },
    FolderNotEmpty { path: String },
    RegistryError { error: RegistryError },
    NoRegistryDefined,
    SelfReferential,
    InvalidUnpack,
    Sql(rusqlite::Error),
    OtherError { message: String }
}

impl From<ImageParseError> for ImageManagerError {
    fn from(error: ImageParseError) -> Self {
        ImageManagerError::ImageParser { error }
    }
}

impl From<std::io::Error> for ImageManagerError {
    fn from(error: std::io::Error) -> Self {
        ImageManagerError::FileIOError { message: format!("{}", error) }
    }
}

impl From<ZipError> for ImageManagerError {
    fn from(error: ZipError) -> Self {
        ImageManagerError::ZIPError(error)
    }
}

impl From<RegistryError> for ImageManagerError {
    fn from(error: RegistryError) -> Self {
        ImageManagerError::RegistryError { error }
    }
}

impl From<rusqlite::Error> for ImageManagerError {
    fn from(value: rusqlite::Error) -> Self {
        ImageManagerError::Sql(value)
    }
}

impl std::fmt::Display for ImageManagerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ImageManagerError::ImageParser { error } => {
                write!(f, "Image parser: {}", error)
            }
            ImageManagerError::LayerNotFound { image_id } => {
                write!(f, "Could not find the layer: {}.", image_id)
            }
            ImageManagerError::ImageNotFound { reference } => {
                write!(f, "Could not find the image: {}.", reference)
            }
            ImageManagerError::FileIOError { message } => {
                write!(f, "{}", message)
            },
            ImageManagerError::FileNotInBuildContext { path } => {
                write!(f, "The file '{}' does not exist in the build content", path)
            }
            ImageManagerError::ZIPError(error) => {
                write!(f, "{}", error)
            },
            ImageManagerError::UnpackingExist { path } => {
                write!(f, "An unpacking already exist at {}", path)
            },
            ImageManagerError::UnpackingNotFound { path } => {
                write!(f, "Could not find the unpacking at {}", path)
            },
            ImageManagerError::FolderNotEmpty { path } => {
                write!(f, "The folder {} is not empty", path)
            },
            ImageManagerError::RegistryError { error } => {
                write!(f, "{}", error)
            }
            ImageManagerError::NoRegistryDefined => {
                write!(f, "No registry defined")
            }
            ImageManagerError::SelfReferential => {
                write!(f, "This layer refers to itself")
            }
            ImageManagerError::InvalidUnpack => {
                write!(f, "Invalid unpack")
            }
            ImageManagerError::Sql(err) => {
                write!(f, "SQL: {}", err)
            }
            ImageManagerError::OtherError { message } => {
                write!(f, "{}", message)
            },
        }
    }
}

pub type ImageManagerResult<T> = Result<T, ImageManagerError>;

#[derive(Clone)]
pub struct ImageManagerConfig {
    base_folder: PathBuf,
    pub accept_self_signed: bool
}

impl ImageManagerConfig {
    pub fn new() -> ImageManagerConfig {
        ImageManagerConfig {
            base_folder: dirs::home_dir().unwrap().join(".labar"),
            accept_self_signed: true
        }
    }

    pub fn with_base_folder(base_folder: PathBuf) -> ImageManagerConfig {
        let mut config = Self::new();
        config.base_folder = base_folder;
        config
    }

    pub fn base_folder(&self) -> &Path {
        &self.base_folder
    }

    pub fn layers_base_folder(&self) -> PathBuf {
        self.base_folder().join("layers")
    }

    pub fn get_layer_folder(&self, hash: &ImageId) -> PathBuf {
        self.layers_base_folder().join(&Path::new(&hash.to_string()))
    }
}

pub use image::ImageManager;
pub use printing::{BoxPrinter, ConsolePrinter, EmptyPrinter, Printer};
pub use crate::image_definition::ImageParseError;
pub use crate::image_manager::registry::RegistryError;
pub use crate::reference::{ImageId, Reference};
pub use crate::image_manager::build::BuildRequest;
pub use crate::image_manager::unpack::UnpackRequest;
pub use crate::image_manager::state::StateSession;
