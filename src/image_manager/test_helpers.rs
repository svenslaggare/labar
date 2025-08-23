use std::ops::Deref;
use std::path::{Path, PathBuf};

use crate::image::Image;
use crate::image_definition::ImageDefinition;
use crate::image_manager::{BuildRequest, ImageManager, StateSession};
use crate::image_manager::build::{BuildManager, BuildResult};
use crate::image_manager::layer::LayerManager;
use crate::reference::ImageTag;

pub fn build_image(image_manager: &mut ImageManager,
                   path: &Path,
                   image_tag: ImageTag) -> Result<Image, String> {
    use crate::image_definition::ImageDefinition;

    let image_definition = ImageDefinition::parse_file_without_context(
        Path::new(path)
    ).map_err(|err| err.to_string())?;

    image_manager.build_image(BuildRequest {
        build_context: Path::new("").to_path_buf(),
        image_definition,
        tag: image_tag,
        force: false,
    }).map_err(|err| err.to_string())
}

pub fn build_image2(session: &mut StateSession,
                    layer_manager: &LayerManager,
                    build_manager: &BuildManager,
                    path: &Path,
                    image_tag: ImageTag) -> Result<BuildResult, String> {
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


pub struct TempFolder {
    path: PathBuf
}

impl TempFolder {
    pub fn new() -> TempFolder {
        let named_temp_folder = tempfile::Builder::new()
            .suffix(".labar")
            .tempfile().unwrap();

        TempFolder {
            path: named_temp_folder.path().to_owned()
        }
    }

    pub fn owned(&self) -> PathBuf {
        self.path.clone()
    }

    pub fn create(&self) -> std::io::Result<()> {
        std::fs::create_dir_all(&self.path)
    }
}

impl Deref for TempFolder {
    type Target = Path;

    fn deref(&self) -> &Self::Target {
        self.path.as_path()
    }
}

impl Drop for TempFolder {
    fn drop(&mut self) {
        #[allow(unused_must_use)] {
            std::fs::remove_dir_all(&self.path);
        }
    }
}