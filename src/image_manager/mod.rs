use std::path::{Path, PathBuf};

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
    UnpackingExist { path: String },
    UnpackingNotFound { path: String },
    RegistryError { error: RegistryError },
    FolderNotEmpty { path: String },
    NoRegistryDefined,
    SelfReferential,
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
            ImageManagerError::UnpackingExist { path } => {
                write!(f, "An unpacking already exist at {}", path)
            },
            ImageManagerError::UnpackingNotFound { path } => {
                write!(f, "Could not find the unpacking at {}", path)
            },
            ImageManagerError::RegistryError { error } => {
                write!(f, "Registry error: {}", error)
            }
            ImageManagerError::FolderNotEmpty { path } => {
                write!(f, "The folder {} is not empty", path)
            },
            ImageManagerError::NoRegistryDefined => {
                write!(f, "No registry defined")
            }
            ImageManagerError::SelfReferential => {
                write!(f, "This layer refers to itself")
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
    pub registry_username: String,
    pub registry_password: String,
    pub registry_use_ssl: bool,
}

impl ImageManagerConfig {
    pub fn new() -> ImageManagerConfig {
        ImageManagerConfig {
            base_folder: dirs::home_dir().unwrap().join(".labar"),
            registry_username: "guest".to_owned(),
            registry_password: "guest".to_owned(),
            registry_use_ssl: false
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
use crate::image_definition::ImageParseError;
use crate::image_manager::registry::RegistryError;
use crate::reference::{ImageId, Reference};
