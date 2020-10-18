use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::image_manager::layer::LayerManager;
use crate::image::{Layer, LayerOperation, LinkType};
use crate::image_manager::{ImageManagerError, ImageManagerResult};

#[derive(Debug, Serialize, Deserialize)]
pub struct Unpacking {
    pub hash: String,
    pub destination: String,
    pub time: std::time::SystemTime
}

pub struct UnpackManager {
    pub unpackings: Vec<Unpacking>
}

impl UnpackManager {
    pub fn new() -> UnpackManager {
        UnpackManager {
            unpackings: Vec::new()
        }
    }

    fn unpack_layer(&self, layer_manager: &LayerManager, unpack_dir: &Path, layer: &Layer) -> ImageManagerResult<()> {
        if let Some(parent_hash) = layer.parent_hash.as_ref() {
            let parent_layer = layer_manager.get_layer(parent_hash)?;
            self.unpack_layer(layer_manager, unpack_dir, parent_layer)?;
        }

        let mut has_files = false;
        for operation in &layer.operations {
            match operation {
                LayerOperation::Image { hash } => {
                    self.unpack_layer(layer_manager, unpack_dir, layer_manager.get_layer(hash)?)?;
                },
                LayerOperation::File { path, source_path, link_type } => {
                    has_files = true;
                    let destination_path = unpack_dir.join(path);
                    println!("\t* Unpacking file {} -> {}", path, destination_path.to_str().unwrap());

                    #[allow(unused_must_use)] {
                        if let Some(parent_dir) = destination_path.parent() {
                            std::fs::create_dir_all(parent_dir);
                        }

                        std::fs::remove_file(&destination_path);
                    }

                    match link_type {
                        LinkType::Soft => {
                            std::os::unix::fs::symlink(source_path, destination_path)
                                .map_err(|err|
                                    ImageManagerError::FileIOError {
                                        message: format!("Failed to unpack file {} due to: {}", path, err)
                                    }
                                )?;
                        },
                        LinkType::Hard => {
                            std::fs::hard_link(source_path, destination_path)
                                .map_err(|err|
                                    ImageManagerError::FileIOError {
                                        message: format!("Failed to unpack file {} due to: {}", path, err)
                                    }
                                )?;
                        },
                    }
                },
                LayerOperation::Directory { path } => {
                    println!("\t* Creating directory {}", path);

                    #[allow(unused_must_use)] {
                        std::fs::create_dir_all(unpack_dir.join(path));
                    }
                },
            }
        }

        if has_files {
            println!();
        }

        Ok(())
    }

    pub fn unpack(&mut self, layer_manager: &LayerManager, unpack_dir: &Path, reference: &str, replace: bool) -> ImageManagerResult<()> {
        if replace && unpack_dir.exists() {
            if let Err(err) = self.remove_unpacking(layer_manager, unpack_dir, true) {
                println!("Failed removing packing due to: {}", err);
            }
        }

        let top_layer = layer_manager.get_layer(reference)?;
        if !unpack_dir.exists() {
            std::fs::create_dir_all(unpack_dir)?;
        }

        let unpack_dir_str = unpack_dir.canonicalize()?.to_str().unwrap().to_owned();

        if self.unpackings.iter().any(|unpacking| unpacking.destination == unpack_dir_str) {
            return Err(ImageManagerError::UnpackingExist { path: unpack_dir_str.clone() });
        }

        if unpack_dir.exists() {
            if std::fs::read_dir(unpack_dir)?.count() > 0 {
                return Err(ImageManagerError::FolderNotEmpty { path: unpack_dir_str.clone() });
            }
        }

        println!("Unpacking {} ({}) to {}", reference, top_layer.hash, unpack_dir_str);
        self.unpack_layer(layer_manager, unpack_dir, &top_layer)?;

        let hash = top_layer.hash.clone();

        self.unpackings.push(Unpacking {
            hash,
            destination: unpack_dir_str,
            time: std::time::SystemTime::now()
        });

        Ok(())
    }

    fn remove_unpacked_layer(&self, layer_manager: &LayerManager, unpack_dir: &Path, layer: &Layer) -> ImageManagerResult<()> {
        for operation in layer.operations.iter().rev() {
            match operation {
                LayerOperation::Image { hash } => {
                    self.remove_unpacked_layer(layer_manager, unpack_dir, layer_manager.get_layer(hash)?)?;
                },
                LayerOperation::File { path, .. } => {
                    let destination_path = unpack_dir.join(path);
                    println!("\t* Deleting link of file {}", destination_path.to_str().unwrap());
                    std::fs::remove_file(destination_path)?;
                },
                LayerOperation::Directory { path } => {
                    let path = unpack_dir.join(path);
                    println!("\t* Deleting directory {}", path.to_str().unwrap());
                    std::fs::remove_dir(path)?
                },
            }
        }

        if let Some(parent_hash) = layer.parent_hash.as_ref() {
            let parent_layer = layer_manager.get_layer(parent_hash)?;
            self.remove_unpacked_layer(layer_manager, unpack_dir, parent_layer)?;
        }

        println!();
        Ok(())
    }

    pub fn remove_unpacking(&mut self, layer_manager: &LayerManager, unpack_dir: &Path, force: bool) -> ImageManagerResult<()> {
        let unpack_dir_str = unpack_dir.canonicalize()?.to_str().unwrap().to_owned();

        let unpacking = self.unpackings.iter()
            .find(|unpacking| unpacking.destination == unpack_dir_str)
            .ok_or_else(|| ImageManagerError::UnpackingNotFound { path: unpack_dir_str.clone() })?;

        println!("Clearing unpacking of {} at {}", unpacking.hash, unpack_dir_str);
        let top_layer = layer_manager.get_layer(&unpacking.hash)?;

        if !force {
            self.remove_unpacked_layer(layer_manager, unpack_dir, top_layer)?;
        } else {
            if let Err(err) = self.remove_unpacked_layer(layer_manager, unpack_dir, top_layer) {
                println!("Failed to clear unpacking due to: {}", err);
            }
        }

        self.unpackings.retain(|unpacking| {
            return unpacking.destination != unpack_dir_str;
        });

        Ok(())
    }
}