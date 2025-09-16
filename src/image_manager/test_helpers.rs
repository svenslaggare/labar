use std::path::{Path};

use crate::image_definition::ImageDefinition;
use crate::image_manager::{BuildRequest, ImageManager, StateSession};
use crate::image_manager::build::{BuildManager, BuildResult};
use crate::image_manager::layer::LayerManager;
use crate::reference::ImageTag;

pub fn build_image(image_manager: &mut ImageManager,
                   path: &Path,
                   image_tag: ImageTag) -> Result<BuildResult, String> {
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