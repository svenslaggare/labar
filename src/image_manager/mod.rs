use std::path::{PathBuf, Path};

use crate::image_manager::registry::RegistryError;

mod layer;
mod unpack;
mod build;
mod image;
mod registry;

#[derive(Debug)]
pub enum ImageManagerError {
    ImageNotFound { reference: String },
    RegistryError { error: RegistryError },
    FileIOError { message: String },
    UnpackingExist { path: String },
    UnpackingNotFound { path: String },
    FolderNotEmpty { path: String },
    OtherError { message: String }
}

impl From<std::io::Error> for ImageManagerError {
    fn from(error: std::io::Error) -> Self {
        ImageManagerError::FileIOError { message: format!("{}", error) }
    }
}

impl std::fmt::Display for ImageManagerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ImageManagerError::ImageNotFound { reference } => {
                write!(f, "Could not find the image: {}.", reference)
            }
            ImageManagerError::RegistryError { error } => {
                write!(f, "Repository error: {}", error)
            },
            ImageManagerError::FileIOError { message } => {
                write!(f, "{}", message)
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
            ImageManagerError::OtherError { message } => {
                write!(f, "{}", message)
            },
        }
    }
}

pub type ImageManagerResult<T> = Result<T, ImageManagerError>;

#[derive(Clone)]
pub struct ImageManagerConfig {
    base_dir: PathBuf
}

impl ImageManagerConfig {
    pub fn new() -> ImageManagerConfig {
        ImageManagerConfig {
            base_dir: dirs::home_dir().unwrap().join(".dfd")
        }
    }

    pub fn with_base_dir(base_dir: PathBuf) -> ImageManagerConfig {
        ImageManagerConfig {
            base_dir
        }
    }

    pub fn base_dir(&self) -> PathBuf {
        self.base_dir.clone()
    }

    pub fn images_base_dir(&self) -> PathBuf {
        self.base_dir().join("images")
    }

    pub fn get_layer_folder(&self, hash: &str) -> PathBuf {
        self.images_base_dir().join(&Path::new(&hash))
    }
}

pub use image::ImageManager;
pub use image::ImageMetadata;
pub use registry::RegistryManager;