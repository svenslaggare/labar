use std::collections::{BTreeSet};
use std::ops::Deref;
use std::path::Path;
use std::sync::Arc;

use crate::image::{Image, ImageMetadata, Layer, LayerOperation};
use crate::image_definition::ImageDefinition;
use crate::image_manager::{ImageManagerConfig, ImageManagerError, ImageManagerResult};
use crate::image_manager::layer::{LayerManager};
use crate::image_manager::unpack::{UnpackManager, Unpacking};
use crate::image_manager::build::BuildManager;
use crate::helpers::DataSize;
use crate::image_manager::printing::BoxPrinter;
use crate::image_manager::registry::RegistryManager;
use crate::image_manager::state::StateManager;
use crate::reference::{ImageId, ImageTag, Reference};

pub struct ImageManager {
    config: ImageManagerConfig,
    printer: BoxPrinter,

    state_manager: Arc<StateManager>,
    layer_manager: LayerManager,
    build_manager: BuildManager,
    unpack_manager: UnpackManager,
    registry_manager: RegistryManager
}

impl ImageManager {
    pub fn new(printer: BoxPrinter) -> ImageManagerResult<ImageManager> {
        ImageManager::with_config(ImageManagerConfig::new(), printer)
    }

    pub fn with_config(config: ImageManagerConfig, printer: BoxPrinter) -> ImageManagerResult<ImageManager> {
        let state_manager = Arc::new(StateManager::new(&config.base_folder)?);

        Ok(
            ImageManager {
                config: config.clone(),
                printer: printer.clone(),

                state_manager: state_manager.clone(),
                layer_manager: LayerManager::new(config.clone(), state_manager.clone()),
                build_manager: BuildManager::new(config.clone(), printer.clone(), state_manager.clone()),
                unpack_manager: UnpackManager::new(config.clone(), printer.clone(), state_manager.clone()),
                registry_manager: RegistryManager::new(config.clone(), printer.clone(), state_manager.clone()),
            }
        )
    }

    pub fn config(&self) -> &ImageManagerConfig {
        &self.config
    }

    pub fn build_image(&mut self, build_context: &Path, image_definition: ImageDefinition, tag: &ImageTag, force: bool) -> ImageManagerResult<Image> {
        let image = self.build_manager.build_image(&mut self.layer_manager, build_context, image_definition, tag, force)?;
        Ok(image)
    }

    pub fn tag_image(&mut self, reference: &Reference, tag: &ImageTag) -> ImageManagerResult<Image> {
        let layer = self.layer_manager.get_layer(reference)?;
        let image = Image::new(layer.hash.clone(), tag.clone());
        self.layer_manager.insert_or_replace_image(image.clone())?;
        Ok(image)
    }

    pub fn unpack(&mut self, reference: &Reference, unpack_folder: &Path, replace: bool) -> ImageManagerResult<()> {
        self.unpack_manager.unpack(&mut self.layer_manager, reference, unpack_folder, replace)?;
        Ok(())
    }

    pub fn remove_unpacking(&mut self, unpack_folder: &Path, force: bool) -> ImageManagerResult<()> {
        self.unpack_manager.remove_unpacking(&mut self.layer_manager, unpack_folder, force)?;
        Ok(())
    }

    pub fn extract(&self, reference: &Reference, archive_path: &Path) -> ImageManagerResult<()> {
        self.unpack_manager.extract(&self.layer_manager, reference, archive_path)?;
        Ok(())
    }

    pub fn remove_image(&mut self, tag: &ImageTag) -> ImageManagerResult<()> {
        if let Some(image) = self.layer_manager.remove_image(tag)? {
            self.printer.println(&format!("Removed image: {} ({})", tag, image.hash));
            self.garbage_collect()?;
            Ok(())
        } else {
            Err(ImageManagerError::ImageNotFound { reference: Reference::ImageTag(tag.clone()) })
        }
    }

    pub fn garbage_collect(&mut self) -> ImageManagerResult<usize> {
        let mut removed_layers = 0;

        let mut used_layers = BTreeSet::new();
        for hash in self.get_hard_references()? {
            self.layer_manager.find_used_layers(&hash, &mut used_layers)?;
        }

        for layer in self.layer_manager.all_layers()? {
            if !used_layers.contains(&layer.hash) {
                if let Err(err) = self.remove_layer(&layer) {
                    self.printer.println(&format!("Failed to remove layer: {}", err));
                } else {
                    removed_layers += 1;
                }
            }
        }

        Ok(removed_layers)
    }

    fn remove_layer(&self, layer: &Layer) -> ImageManagerResult<()> {
        self.layer_manager.remove_layer(&layer.hash)?;

        let mut reclaimed_size = DataSize(0);
        for operation in &layer.operations {
            match operation {
                LayerOperation::File { source_path, .. } => {
                    reclaimed_size += DataSize(std::fs::metadata(source_path).map(|metadata| metadata.len()).unwrap_or(0) as usize);
                },
                _ => {}
            }
        }

        let layer_path = self.config.get_layer_folder(&layer.hash);
        std::fs::remove_dir_all(&layer_path)
            .map_err(|err|
                ImageManagerError::FileIOError {
                    message: format!("Failed to remove layer {} due to: {}", layer_path.to_str().unwrap(), err)
                }
            )?;

        self.printer.println(&format!("Removed layer: {} (reclaimed {})", layer.hash, reclaimed_size));
        Ok(())
    }

    fn get_hard_references(&self) -> ImageManagerResult<Vec<ImageId>> {
        let mut hard_references = Vec::new();
        for image in self.layer_manager.images_iter()? {
            hard_references.push(image.hash.clone());
        }

        for unpacking in self.unpack_manager.unpackings()? {
            hard_references.push(unpacking.hash.clone());
        }

        Ok(hard_references)
    }

    pub fn list_images(&self) -> ImageManagerResult<Vec<ImageMetadata>> {
        let mut images = Vec::new();

        for image in self.layer_manager.images_iter()? {
            images.push(self.get_image_metadata(&image, &image.hash.clone().to_ref())?);
        }

        Ok(images)
    }

    pub fn resolve_image(&self, tag: &ImageTag) -> ImageManagerResult<ImageMetadata> {
        let image = self.get_image(tag)?;
        self.get_image_metadata(&image, &Reference::ImageTag(tag.clone()))
    }

    fn get_image_metadata(&self, image: &Image, reference: &Reference) -> ImageManagerResult<ImageMetadata> {
        Ok(
            ImageMetadata {
                image: image.clone(),
                created: self.layer_manager.get_layer(&reference)?.created,
                size: self.image_size(&reference)?
            }
        )
    }

    pub fn inspect(&self, reference: &Reference) -> ImageManagerResult<InspectResult> {
        let top_layer = self.get_layer(&reference)?;
        let mut layers = Vec::new();
        for layer in self.get_layers(&reference)? {
            let size = self.layer_size(&layer.hash.clone().to_ref())?;
            layers.push(InspectLayerResult {
                layer,
                size
            });
        }

        Ok(
            InspectResult {
                top_layer,
                image_tags: self.get_image_tags(reference)?,
                size: self.image_size(reference)?,
                layers
            }
        )
    }

    pub fn image_size(&self, reference: &Reference) -> ImageManagerResult<DataSize> {
        self.layer_manager.size_of_reference(reference, true)
    }

    pub fn layer_size(&self, reference: &Reference) -> ImageManagerResult<DataSize> {
        self.layer_manager.size_of_reference(reference, false)
    }

    pub fn list_content(&self, reference: &Reference) -> ImageManagerResult<Vec<String>> {
        let mut files = Vec::new();

        let mut stack = Vec::new();
        stack.push(reference.clone());

        while let Some(current) = stack.pop() {
            let layer = self.layer_manager.get_layer(&current)?;
            if let Some(parent_hash) = layer.parent_hash.as_ref() {
                stack.push(parent_hash.clone().to_ref());
            }

            let mut local_files = Vec::new();
            for operation in &layer.operations {
                match operation {
                    LayerOperation::Image { hash } => {
                        stack.push(hash.clone().to_ref());
                    }
                    LayerOperation::File { path, ..  } => {
                        local_files.push(path.clone());
                    }
                    LayerOperation::Directory { path } => {
                        local_files.push(path.clone());
                    }
                }
            }

            local_files.reverse();

            files.append(&mut local_files);
        }

        files.reverse();

        Ok(files)
    }

    pub fn list_unpackings(&self) -> ImageManagerResult<Vec<Unpacking>> {
        self.unpack_manager.unpackings()
    }

    pub async fn login(&mut self, registry: &str, username: &str, password: &str) -> ImageManagerResult<()> {
        self.registry_manager.verify_login(registry, username, password).await?;
        self.state_manager.add_login(registry, username, password)?;
        Ok(())
    }

    pub async fn list_images_registry(&self, registry: &str) -> ImageManagerResult<Vec<ImageMetadata>> {
        let images = self.registry_manager.list_images(registry).await?;
        Ok(images)
    }

    pub async fn pull(&mut self, tag: &ImageTag, default_registry: Option<&str>) -> ImageManagerResult<Image> {
        let mut tag = tag.clone();
        if tag.registry().is_none() {
            if let Some(default_registry) = default_registry {
                tag = tag.set_registry(default_registry);
            }
        }

        let registry = tag.registry().ok_or_else(|| ImageManagerError::NoRegistryDefined)?;

        self.printer.println(&format!("Pulling image {}", tag));

        let image_metadata = self.registry_manager.resolve_image(registry, &tag).await?;

        let mut stack = Vec::new();
        stack.push(image_metadata.image.hash.clone().to_ref());

        let visit_layer = |stack: &mut Vec<Reference>, layer: &Layer| {
            if let Some(parent_hash) = layer.parent_hash.as_ref() {
                stack.push(parent_hash.clone().to_ref());
            }

            for operation in &layer.operations {
                match operation {
                    LayerOperation::Image { hash } => {
                        stack.push(hash.clone().to_ref());
                    },
                    _ => {}
                }
            }
        };

        let mut top_level_hash = None;
        while let Some(current) = stack.pop() {
            if let Ok(layer) = self.get_layer(&current) {
                self.printer.println(&format!("\t* Layer already exist: {}", current));
                if top_level_hash.is_none() {
                    top_level_hash = Some(layer.hash.clone());
                }

                visit_layer(&mut stack, &layer);
            } else {
                self.printer.println(&format!("\t* Downloading layer: {}", current));
                let layer = self.registry_manager.download_layer(self.config(), registry, &current).await?;
                self.insert_layer(layer.clone())?;

                if top_level_hash.is_none() {
                    top_level_hash = Some(layer.hash.clone());
                }

                visit_layer(&mut stack, &layer);
            }
        }

        let image = Image::new(top_level_hash.unwrap(), tag.clone());
        self.insert_or_replace_image(image.clone())?;

        Ok(image)
    }

    pub async fn push(&self, tag: &ImageTag, default_registry: Option<&str>) -> ImageManagerResult<()> {
        let top_layer = self.get_layer(&Reference::ImageTag(tag.clone()))?;

        let mut tag = tag.clone();
        if tag.registry().is_none() {
            if let Some(default_registry) = default_registry {
                tag = tag.set_registry(default_registry);
            }
        }

        let registry = tag.registry().ok_or_else(|| ImageManagerError::NoRegistryDefined)?;

        self.printer.println(&format!("Pushing image {}", tag));

        let mut stack = Vec::new();
        stack.push(top_layer.hash.clone().to_ref());

        let visit_layer = |stack: &mut Vec<Reference>, layer: &Layer| {
            if let Some(parent_hash) = layer.parent_hash.as_ref() {
                stack.push(parent_hash.clone().to_ref());
            }

            for operation in &layer.operations {
                match operation {
                    LayerOperation::Image { hash } => {
                        stack.push(hash.clone().to_ref());
                    },
                    _ => {}
                }
            }
        };

        while let Some(current) = stack.pop() {
            self.printer.println(&format!("\t* Pushing layer: {}", current));
            let layer = self.get_layer(&current)?;
            visit_layer(&mut stack, &layer);
            self.registry_manager.upload_layer(self.config(), registry, &layer).await?;
        }

        self.registry_manager.upload_image(registry, &top_layer.hash, &tag).await?;

        self.printer.println("");

        Ok(())
    }

    pub fn get_layer(&self, reference: &Reference) -> ImageManagerResult<Layer> {
        self.layer_manager.get_layer(reference)
    }

    pub fn get_layers(&self, reference: &Reference) -> ImageManagerResult<Vec<Layer>> {
        let mut stack = vec![self.layer_manager.fully_qualify_reference(reference)?.to_ref()];

        let mut layers = Vec::new();
        while let Some(current) = stack.pop() {
            let layer = self.get_layer(&current)?;
            layers.push(layer.clone());

            if let Some(parent) = layer.parent_hash.as_ref() {
                stack.push(parent.clone().to_ref());
            }
        }

        Ok(layers)
    }

    pub fn insert_layer(&mut self, layer: Layer) -> ImageManagerResult<()> {
        self.layer_manager.insert_layer(layer)?;
        Ok(())
    }

    pub fn get_image(&self, tag: &ImageTag) -> ImageManagerResult<Image> {
        self.layer_manager.get_image(tag)
    }

    pub fn get_image_tags(&self, reference: &Reference) -> ImageManagerResult<Vec<ImageTag>> {
        let image_id = self.layer_manager.fully_qualify_reference(reference)?;

        let mut tags = Vec::new();
        for image in self.layer_manager.images_iter()? {
            if image.hash == image_id {
                tags.push(image.tag.clone())
            }
        }

        Ok(tags)
    }

    pub fn insert_or_replace_image(&mut self, image: Image) -> ImageManagerResult<()> {
        self.layer_manager.insert_or_replace_image(image)?;
        Ok(())
    }
}

pub struct InspectResult {
    pub top_layer: Layer,
    pub image_tags: Vec<ImageTag>,
    pub size: DataSize,
    pub layers: Vec<InspectLayerResult>
}

pub struct InspectLayerResult {
    pub layer: Layer,
    pub size: DataSize
}

impl Deref for InspectLayerResult {
    type Target = Layer;

    fn deref(&self) -> &Self::Target {
        &self.layer
    }
}

#[test]
fn test_build() {
    use std::str::FromStr;

    use crate::helpers;
    use crate::image_manager::ConsolePrinter;

    let tmp_folder = helpers::get_temp_folder();

    {
        let config = ImageManagerConfig::with_base_folder(tmp_folder.clone());

        let mut image_manager = ImageManager::with_config(config, ConsolePrinter::new()).unwrap();

        let image_definition = ImageDefinition::parse_without_context(&std::fs::read_to_string("testdata/definitions/simple1.labarfile").unwrap());
        assert!(image_definition.is_ok());
        let image_definition = image_definition.unwrap();

        let result = image_manager.build_image(Path::new(""), image_definition, &ImageTag::from_str("test").unwrap(), false);
        assert!(result.is_ok());
        let result = result.unwrap();

        assert_eq!(ImageTag::from_str("test").unwrap(), result.tag);

        let top_layer = image_manager.get_layer(&Reference::from_str("test").unwrap());
        assert!(top_layer.is_ok());
        let top_layer = top_layer.unwrap();
        assert_eq!(top_layer.hash, result.hash);

        let image = image_manager.get_image(&ImageTag::from_str("test").unwrap());
        assert!(image.is_ok());
        let image = image.unwrap();
        assert_eq!(image.hash, top_layer.hash);

        assert_eq!(Some(DataSize(974)), image_manager.image_size(&Reference::from_str("test").unwrap()).ok());
    }

    #[allow(unused_must_use)] {
        std::fs::remove_dir_all(&tmp_folder);
    }
}

#[test]
fn test_remove_image1() {
    use std::str::FromStr;

    use crate::helpers;
    use crate::image_manager::ConsolePrinter;

    let tmp_folder = helpers::get_temp_folder();
    std::fs::create_dir_all(&tmp_folder).unwrap();

    let config = ImageManagerConfig::with_base_folder(tmp_folder.clone());
    let printer = ConsolePrinter::new();

    let mut image_manager = ImageManager::with_config(
        config,
        printer
    ).unwrap();

    let image_definition = ImageDefinition::parse_without_context(&std::fs::read_to_string("testdata/definitions/simple1.labarfile").unwrap());
    assert!(image_definition.is_ok());
    let image_definition = image_definition.unwrap();

    image_manager.build_image(Path::new(""), image_definition, &ImageTag::from_str("test").unwrap(), false).unwrap();

    let result = image_manager.remove_image(&ImageTag::from_str("test").unwrap());
    assert!(result.is_ok());

    let images = image_manager.list_images();
    assert!(result.is_ok());
    let images = images.unwrap();

    assert_eq!(0, images.len());

    assert_eq!(0, image_manager.layer_manager.all_layers().unwrap().len());
}

#[test]
fn test_remove_image2() {
    use std::str::FromStr;

    use crate::helpers;
    use crate::image_manager::ConsolePrinter;

    let tmp_folder = helpers::get_temp_folder();
    std::fs::create_dir_all(&tmp_folder).unwrap();

    let config = ImageManagerConfig::with_base_folder(tmp_folder.clone());
    let printer = ConsolePrinter::new();

    let mut image_manager = ImageManager::with_config(
        config,
        printer
    ).unwrap();

    let image_definition = ImageDefinition::parse_without_context(&std::fs::read_to_string("testdata/definitions/simple1.labarfile").unwrap());
    assert!(image_definition.is_ok());
    image_manager.build_image(Path::new(""), image_definition.unwrap(), &ImageTag::from_str("test").unwrap(), false).unwrap();

    let image_definition = ImageDefinition::parse_without_context(&std::fs::read_to_string("testdata/definitions/simple1.labarfile").unwrap());
    assert!(image_definition.is_ok());
    image_manager.build_image(Path::new(""), image_definition.unwrap(), &ImageTag::from_str("test2").unwrap(), false).unwrap();

    let result = image_manager.remove_image(&ImageTag::from_str("test").unwrap());
    assert!(result.is_ok());

    let images = image_manager.list_images();
    assert!(result.is_ok());
    let images = images.unwrap();

    assert_eq!(1, images.len());
    assert_eq!(ImageTag::from_str("test2").unwrap(), images[0].image.tag);
}

#[test]
fn test_list_content() {
    use std::str::FromStr;

    use crate::helpers;
    use crate::image_manager::ConsolePrinter;

    let tmp_folder = helpers::get_temp_folder();

    {
        let config = ImageManagerConfig::with_base_folder(tmp_folder.clone());

        let mut image_manager = ImageManager::with_config(config, ConsolePrinter::new()).unwrap();

        let image_definition = ImageDefinition::parse_without_context(&std::fs::read_to_string("testdata/definitions/simple1.labarfile").unwrap()).unwrap();
        image_manager.build_image(Path::new(""), image_definition, &ImageTag::from_str("test").unwrap(), false).unwrap();

        let image_definition = ImageDefinition::parse_without_context(&std::fs::read_to_string("testdata/definitions/with_image_ref.labarfile").unwrap()).unwrap();
        image_manager.build_image(Path::new(""), image_definition, &ImageTag::from_str("that").unwrap(), false).unwrap();

        let files = image_manager.list_content(&Reference::from_str("that").unwrap());
        assert!(files.is_ok());
        let files = files.unwrap();

        assert_eq!(2, files.len());
        assert_eq!("file1.txt", files[0]);
        assert_eq!("file2.txt", files[1]);
    }

    #[allow(unused_must_use)] {
        std::fs::remove_dir_all(&tmp_folder);
    }
}

#[cfg(test)]
fn create_registry_config(address: std::net::SocketAddr, tmp_registry_folder: &Path) -> crate::registry::RegistryConfig {
    use crate::registry::RegistryConfig;
    use crate::registry::auth::AccessRight;

    RegistryConfig {
        data_path: tmp_registry_folder.to_path_buf(),
        address,
        pending_upload_expiration: 30.0,
        ssl_cert_path: None,
        ssl_key_path: None,
        users: vec![
            (
                "guest".to_owned(),
                crate::registry::auth::create_password_hash("guest"),
                vec![AccessRight::List, AccessRight::Download, AccessRight::Upload, AccessRight::Delete]
            )
        ]
    }
}

#[tokio::test]
async fn test_push_pull() {
    use std::net::SocketAddr;

    use crate::helpers;
    use crate::image_manager::ConsolePrinter;

    let tmp_folder = helpers::get_temp_folder();
    let tmp_registry_folder = helpers::get_temp_folder();

    let address: SocketAddr = "0.0.0.0:9567".parse().unwrap();
    tokio::spawn(crate::registry::run(create_registry_config(address, &tmp_registry_folder)));

    // Wait until registry starts
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    {
        let config = ImageManagerConfig::with_base_folder(tmp_folder.clone());
        let mut image_manager = ImageManager::with_config(config, ConsolePrinter::new()).unwrap();

        // Login
        let login_result = image_manager.login(&address.to_string(), "guest", "guest").await;
        assert!(login_result.is_ok(), "{}", login_result.unwrap_err());

        let image_tag = ImageTag::with_registry(&address.to_string(), "test", "latest");

        // Build
        let image_definition = ImageDefinition::parse_without_context(&std::fs::read_to_string("testdata/definitions/simple1.labarfile").unwrap()).unwrap();
        let image = image_manager.build_image(Path::new(""), image_definition, &image_tag, false).unwrap();
        assert_eq!(Some(DataSize(974)), image_manager.image_size(&image.hash.clone().to_ref()).ok());

        // Push
        let push_result = image_manager.push(&image.tag, None).await;
        assert!(push_result.is_ok(), "{}", push_result.unwrap_err());

        // List remote
        let remote_images = image_manager.list_images_registry(&address.to_string()).await;
        assert!(remote_images.is_ok());
        let remote_images = remote_images.unwrap();
        assert_eq!(1, remote_images.len());
        assert_eq!(&image_tag, &remote_images[0].image.tag);

        // Remove in order to pull
        assert!(image_manager.remove_image(&image.tag).is_ok());

        // Pull
        let pull_result = image_manager.pull(&image.tag, None).await;
        assert!(pull_result.is_ok(), "{}", pull_result.unwrap_err());
        let pull_image = pull_result.unwrap();
        assert_eq!(image, pull_image);

        // List images
        let images = image_manager.list_images();
        assert!(images.is_ok());
        let images = images.unwrap();
        assert_eq!(1, images.len());
        assert_eq!(&image_tag, &images[0].image.tag);

        // Check content
        let reference = Reference::ImageTag(image.tag.clone());
        assert_eq!(Some(DataSize(974)), image_manager.image_size(&reference).ok());
        let files = image_manager.list_content(&reference).unwrap();
        assert_eq!(vec!["file1.txt".to_owned()], files);
    }

    #[allow(unused_must_use)] {
        std::fs::remove_dir_all(&tmp_folder);
        std::fs::remove_dir_all(&tmp_registry_folder);
    }
}

#[tokio::test]
async fn test_push_pull_with_ref() {
    use std::net::SocketAddr;
    use std::str::FromStr;

    use crate::helpers;
    use crate::image_manager::ConsolePrinter;

    let tmp_folder = helpers::get_temp_folder();
    let tmp_registry_folder = helpers::get_temp_folder();

    let address: SocketAddr = "0.0.0.0:9568".parse().unwrap();
    tokio::spawn(crate::registry::run(create_registry_config(address, &tmp_registry_folder)));

    // Wait until registry starts
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    {
        let config = ImageManagerConfig::with_base_folder(tmp_folder.clone());
        let mut image_manager = ImageManager::with_config(config, ConsolePrinter::new()).unwrap();

        // Login
        let login_result = image_manager.login(&address.to_string(), "guest", "guest").await;
        assert!(login_result.is_ok(), "{}", login_result.unwrap_err());

        let image_tag = ImageTag::with_registry(&address.to_string(), "remote_image", "latest");

        // Build
        let image_definition = ImageDefinition::parse_without_context(&std::fs::read_to_string("testdata/definitions/simple1.labarfile").unwrap()).unwrap();
        let image_referred = image_manager.build_image(Path::new(""), image_definition, &ImageTag::from_str("test").unwrap(), false).unwrap();

        let image_definition = ImageDefinition::parse_without_context(&std::fs::read_to_string("testdata/definitions/with_image_ref.labarfile").unwrap()).unwrap();
        let image = image_manager.build_image(Path::new(""), image_definition, &image_tag, false).unwrap();
        assert_eq!(Some(DataSize(3003)), image_manager.image_size(&image.hash.clone().to_ref()).ok());

        // Push
        let push_result = image_manager.push(&image.tag, None).await;
        assert!(push_result.is_ok(), "{}", push_result.unwrap_err());

        // List remote
        let remote_images = image_manager.list_images_registry(&address.to_string()).await;
        assert!(remote_images.is_ok());
        let remote_images = remote_images.unwrap();
        assert_eq!(1, remote_images.len());
        assert_eq!(&image_tag, &remote_images[0].image.tag);

        // Remove in order to pull
        assert!(image_manager.remove_image(&image.tag).is_ok());
        assert!(image_manager.remove_image(&image_referred.tag).is_ok());

        // Pull
        let pull_result = image_manager.pull(&image.tag, None).await;
        assert!(pull_result.is_ok(), "{}", pull_result.unwrap_err());
        let pull_image = pull_result.unwrap();
        assert_eq!(image, pull_image);

        // List images
        let images = image_manager.list_images();
        assert!(images.is_ok());
        let images = images.unwrap();
        assert_eq!(1, images.len());
        assert_eq!(&image_tag, &images[0].image.tag);

        // Check content
        let reference = Reference::ImageTag(image.tag.clone());
        assert_eq!(Some(DataSize(3003)), image_manager.image_size(&reference).ok());
        let files = image_manager.list_content(&reference).unwrap();
        assert_eq!(vec!["file1.txt".to_owned(), "file2.txt".to_owned()], files);
    }

    #[allow(unused_must_use)] {
        std::fs::remove_dir_all(&tmp_folder);
        std::fs::remove_dir_all(&tmp_registry_folder);
    }
}