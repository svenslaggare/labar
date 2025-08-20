use std::collections::{BTreeSet};
use std::ops::Deref;
use std::path::Path;

use crate::image::{Image, ImageMetadata, Layer, LayerOperation};
use crate::image_manager::{ImageManagerConfig, ImageManagerError, ImageManagerResult, RegistryError};
use crate::image_manager::layer::{LayerManager};
use crate::image_manager::unpack::{UnpackManager, UnpackRequest, Unpacking};
use crate::image_manager::build::{BuildManager, BuildRequest};
use crate::helpers::DataSize;
use crate::image_manager::printing::BoxPrinter;
use crate::image_manager::registry::{RegistryManager, RegistrySession};
use crate::image_manager::state::{PooledStateSession, StateManager, StateSession};
use crate::reference::{ImageId, ImageTag, Reference};

pub struct ImageManager {
    config: ImageManagerConfig,
    printer: BoxPrinter,

    state_manager: StateManager,
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
        let state_manager = StateManager::new(&config.base_folder)?;

        Ok(
            ImageManager {
                config: config.clone(),
                printer: printer.clone(),

                state_manager,
                layer_manager: LayerManager::new(config.clone()),
                build_manager: BuildManager::new(config.clone(), printer.clone()),
                unpack_manager: UnpackManager::new(config.clone(), printer.clone()),
                registry_manager: RegistryManager::new(config.clone(), printer.clone()),
            }
        )
    }

    pub fn config(&self) -> &ImageManagerConfig {
        &self.config
    }

    pub fn pooled_state_session(&self) -> ImageManagerResult<PooledStateSession> {
        Ok(self.state_manager.pooled_session()?)
    }

    pub fn build_image(&mut self, request: BuildRequest) -> ImageManagerResult<Image> {
        let mut session = self.state_manager.pooled_session()?;

        let image = self.build_manager.build_image(&mut session, &mut self.layer_manager, request)?.image;
        Ok(image)
    }

    pub fn tag_image(&mut self, reference: &Reference, tag: &ImageTag) -> ImageManagerResult<Image> {
        let mut session = self.state_manager.pooled_session()?;

        let layer = self.layer_manager.get_layer(&session, reference)?;
        let image = Image::new(layer.hash.clone(), tag.clone());
        self.layer_manager.insert_or_replace_image(&mut session, image.clone())?;
        Ok(image)
    }

    pub fn unpack(&mut self, request: UnpackRequest) -> ImageManagerResult<()> {
        let session = self.state_manager.pooled_session()?;
        self.unpack_manager.unpack(&session, &mut self.layer_manager, request)?;
        Ok(())
    }

    pub fn remove_unpacking(&mut self, unpack_folder: &Path, force: bool) -> ImageManagerResult<()> {
        let session = self.state_manager.pooled_session()?;
        self.unpack_manager.remove_unpacking(&session, &mut self.layer_manager, unpack_folder, force)?;
        Ok(())
    }

    pub fn extract(&self, reference: &Reference, archive_path: &Path) -> ImageManagerResult<()> {
        let session = self.state_manager.pooled_session()?;
        self.unpack_manager.extract(&session, &self.layer_manager, reference, archive_path)?;
        Ok(())
    }

    pub fn remove_image(&mut self, tag: &ImageTag) -> ImageManagerResult<()> {
        let mut session = self.state_manager.pooled_session()?;

        if let Some(image) = self.layer_manager.remove_image(&mut session, tag)? {
            self.printer.println(&format!("Removed image: {} ({})", tag, image.hash));
            self.garbage_collect()?;
            Ok(())
        } else {
            Err(ImageManagerError::ReferenceNotFound { reference: Reference::ImageTag(tag.clone()) })
        }
    }

    pub fn garbage_collect(&mut self) -> ImageManagerResult<usize> {
        let session = self.state_manager.pooled_session()?;

        let mut removed_layers = 0;

        let mut used_layers = BTreeSet::new();
        for hash in self.get_hard_references(&session)? {
            self.layer_manager.find_used_layers(&session, &hash, &mut used_layers)?;
        }

        for layer in self.layer_manager.all_layers(&session)? {
            if !used_layers.contains(&layer.hash) {
                if let Err(err) = self.remove_layer(&session, &layer) {
                    self.printer.println(&format!("Failed to remove layer: {}", err));
                } else {
                    removed_layers += 1;
                }
            }
        }

        Ok(removed_layers)
    }

    fn remove_layer(&self, session: &StateSession, layer: &Layer) -> ImageManagerResult<()> {
        self.layer_manager.remove_layer(session, &layer.hash)?;

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

    fn get_hard_references(&self, session: &StateSession) -> ImageManagerResult<Vec<ImageId>> {
        let mut hard_references = Vec::new();
        for image in self.layer_manager.images_iter(&session)? {
            hard_references.push(image.hash.clone());
        }

        for unpacking in self.unpack_manager.unpackings(session)? {
            hard_references.push(unpacking.hash.clone());
        }

        Ok(hard_references)
    }

    pub fn list_images(&self) -> ImageManagerResult<Vec<ImageMetadata>> {
        let session = self.state_manager.pooled_session()?;

        let mut images = Vec::new();

        for image in self.layer_manager.images_iter(&session)? {
            images.push(self.get_image_metadata(&image)?);
        }

        Ok(images)
    }

    pub fn resolve_image(&self, tag: &ImageTag) -> ImageManagerResult<ImageMetadata> {
        let image = self.get_image(tag)?;
        self.get_image_metadata(&image)
    }

    fn get_image_metadata(&self, image: &Image) -> ImageManagerResult<ImageMetadata> {
        let session = self.state_manager.pooled_session()?;
        let reference = image.tag.clone().to_ref();

        Ok(
            ImageMetadata {
                image: image.clone(),
                created: self.layer_manager.get_layer(&session, &reference)?.created,
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
        let session = self.state_manager.pooled_session()?;
        self.layer_manager.size_of_reference(&session, reference, true)
    }

    pub fn layer_size(&self, reference: &Reference) -> ImageManagerResult<DataSize> {
        let session = self.state_manager.pooled_session()?;
        self.layer_manager.size_of_reference(&session, reference, false)
    }

    pub fn list_content(&self, reference: &Reference) -> ImageManagerResult<Vec<String>> {
        let session = self.state_manager.pooled_session()?;

        let mut files = Vec::new();

        let mut stack = Vec::new();
        stack.push(reference.clone());

        while let Some(current) = stack.pop() {
            let layer = self.layer_manager.get_layer(&session, &current)?;
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
        let session = self.state_manager.pooled_session()?;
        self.unpack_manager.unpackings(&session)
    }

    pub async fn login(&mut self, registry: &str, username: &str, password: &str) -> ImageManagerResult<()> {
        let mut session = self.state_manager.pooled_session()?;

        self.registry_manager.verify_login(registry, username, password).await?;
        session.add_login(registry, username, password)?;
        Ok(())
    }

    pub async fn list_images_in_registry(&self, registry: &str) -> ImageManagerResult<Vec<ImageMetadata>> {
        let session = self.state_manager.pooled_session()?;
        let registry_session = RegistrySession::new(&session, registry)?;

        let images = self.registry_manager.list_images(&registry_session).await?;
        Ok(images)
    }

    pub async fn resolve_image_in_registry(&self, registry: &str, tag: &ImageTag) -> ImageManagerResult<ImageMetadata> {
        let session = self.state_manager.pooled_session()?;
        let registry_session = RegistrySession::new(&session, registry)?;

        let image_metadata = self.resolve_image_in_registry_internal(&registry_session, &tag, false).await?;
        Ok(image_metadata)
    }

    pub async fn get_layer_from_registry(&self, registry: &str, hash: &ImageId) -> ImageManagerResult<Layer> {
        let session = self.state_manager.pooled_session()?;
        let registry_session = RegistrySession::new(&session, registry)?;

        let layer = self.registry_manager.get_layer_definition(&registry_session, hash).await?;
        Ok(layer)
    }

    pub async fn pull(&mut self, tag: &ImageTag, default_registry: Option<&str>) -> ImageManagerResult<Image> {
        let session = self.state_manager.pooled_session()?;

        let mut tag = tag.clone();
        if tag.registry().is_none() {
            tag = tag.set_registry_opt(default_registry);
        }

        let registry = tag.registry().ok_or_else(|| ImageManagerError::NoRegistryDefined)?;
        let registry_session = RegistrySession::new(&session, registry)?;

        self.printer.println(&format!("Pulling image {}", tag));

        let image_metadata = self.resolve_image_in_registry_internal(&registry_session, &tag, true).await?;

        let mut stack = Vec::new();
        stack.push(image_metadata.image.hash.clone());

        let visit_layer = |stack: &mut Vec<ImageId>, layer: &Layer| {
            layer.visit_image_ids(|image_id| stack.push(image_id.clone()));
        };

        let mut top_level_hash = None;
        while let Some(current) = stack.pop() {
            if let Ok(layer) = self.get_layer(&current.clone().to_ref()) {
                self.printer.println(&format!("\t* Layer already exist: {}", current));
                if top_level_hash.is_none() {
                    top_level_hash = Some(layer.hash.clone());
                }

                visit_layer(&mut stack, &layer);
            } else {
                self.printer.println(&format!("\t* Downloading layer: {}", current));
                let layer = self.registry_manager.download_layer(&registry_session, &current).await?;
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

    pub async fn push(&self, tag: &ImageTag, default_registry: Option<&str>) -> ImageManagerResult<usize> {
        let session = self.state_manager.pooled_session()?;

        let top_layer = self.get_layer(&tag.clone().to_ref())?;

        let mut tag = tag.clone();
        if tag.registry().is_none() {
            tag = tag.set_registry_opt(default_registry);
        }

        let registry = tag.registry().ok_or_else(|| ImageManagerError::NoRegistryDefined)?;
        let registry_session = RegistrySession::new(&session, registry)?;

        self.printer.println(&format!("Pushing image {}", tag));

        let mut stack = Vec::new();
        stack.push(top_layer.hash.clone());

        let visit_layer = |stack: &mut Vec<ImageId>, layer: &Layer| {
            layer.visit_image_ids(|image_id| stack.push(image_id.clone()));
        };

        let mut layers_uploaded = 0;
        while let Some(current) = stack.pop() {
            self.printer.println(&format!("\t* Pushing layer: {}", current));
            let layer = self.get_layer(&current.clone().to_ref())?;
            visit_layer(&mut stack, &layer);
            if self.registry_manager.upload_layer(&registry_session, &layer).await? {
                layers_uploaded += 1;
            }
        }

        self.registry_manager.upload_image(&registry_session, &top_layer.hash, &tag).await?;

        self.printer.println("");

        Ok(layers_uploaded)
    }

    pub async fn pull_layer(&mut self, registry: &str, hash: &ImageId) -> ImageManagerResult<Layer> {
        let session = self.state_manager.pooled_session()?;
        let registry_session = RegistrySession::new(&session, registry)?;

        let layer = self.registry_manager.download_layer(&registry_session, &hash).await?;
        Ok(layer)
    }

    pub async fn sync<
        BeforeLayerPull: Fn(&mut StateSession, &Layer) -> bool,
        CommitLayer: Fn(&mut StateSession, Layer) -> bool
    >(
        &mut self,
        registry: &str,
        local_registry: Option<&str>,
        before_layer_pull: BeforeLayerPull,
        commit_layer: CommitLayer
    ) -> ImageManagerResult<DownloadResult> {
        let mut session = self.state_manager.pooled_session()?;
        let registry_session = RegistrySession::new(&session, registry)?;

        let mut download_result = DownloadResult::new();

        let images = self.registry_manager.list_images(&registry_session).await?;
        'next_image:
        for image in images {
            for layer_to_download in self.get_non_downloaded_layers(&registry_session, &image.image.hash).await? {
                if !before_layer_pull(&mut session, &layer_to_download) {
                    // Somebody else is pulling this layer
                    continue;
                }

                let layer = self.registry_manager.download_layer(&registry_session, &layer_to_download.hash).await?;
                let layer_hash = layer.hash.clone();
                if !commit_layer(&mut session, layer) {
                    // We failed to commit, skip this image
                    continue 'next_image;
                }

                download_result.downloaded_layers += 1;
                if &layer_hash == &image.image.hash {
                    download_result.downloaded_images += 1;
                }
            }

            if self.layer_manager.layer_exist(&session, &image.image.hash)? {
                let new_tag = image.image.tag.clone().set_registry_opt(local_registry);
                let image = Image::new(image.image.hash, new_tag);
                self.insert_or_replace_image(image.clone())?;
            }
        }

        Ok(download_result)
    }

    async fn resolve_image_in_registry_internal(&self,
                                                registry_session: &RegistrySession,
                                                tag: &ImageTag,
                                                can_pull_through: bool) -> ImageManagerResult<ImageMetadata> {
        match self.registry_manager.resolve_image(&registry_session, &tag, can_pull_through).await {
            Ok(image) => Ok(image),
            Err(RegistryError::ReferenceNotFound) => {
                Err(ImageManagerError::ReferenceNotFound { reference: tag.clone().to_ref() })
            }
            Err(err) => Err(err.into())
        }
    }

    async fn get_non_downloaded_layers(&mut self,
                                       registry: &RegistrySession,
                                       hash: &ImageId) -> ImageManagerResult<Vec<Layer>> {
        let mut stack = Vec::new();
        stack.push(hash.clone());

        let visit_layer = |stack: &mut Vec<ImageId>, layer: &Layer| {
            layer.visit_image_ids(|image_id| stack.push(image_id.clone()));
        };

        let mut layers = Vec::new();
        while let Some(current) = stack.pop() {
            if let Ok(layer) = self.get_layer(&current.clone().to_ref()) {
                visit_layer(&mut stack, &layer);
            } else {
                let layer = self.registry_manager.get_layer_definition(&registry, &current).await?;
                layers.push(layer.clone());
                visit_layer(&mut stack, &layer);
            }
        }

        Ok(layers)
    }

    pub fn get_layer(&self, reference: &Reference) -> ImageManagerResult<Layer> {
        let session = self.state_manager.pooled_session()?;
        self.layer_manager.get_layer(&session, reference)
    }

    pub fn get_layers(&self, reference: &Reference) -> ImageManagerResult<Vec<Layer>> {
        let session = self.state_manager.pooled_session()?;

        let mut stack = vec![self.layer_manager.fully_qualify_reference(&session, reference)?.to_ref()];

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
        let session = self.state_manager.pooled_session()?;
        self.layer_manager.insert_layer(&session, layer)?;
        Ok(())
    }

    pub fn get_image(&self, tag: &ImageTag) -> ImageManagerResult<Image> {
        let session = self.state_manager.pooled_session()?;
        self.layer_manager.get_image(&session, tag)
    }

    pub fn get_image_tags(&self, reference: &Reference) -> ImageManagerResult<Vec<ImageTag>> {
        let session = self.state_manager.pooled_session()?;

        let image_id = self.layer_manager.fully_qualify_reference(&session, reference)?;

        let mut tags = Vec::new();
        for image in self.layer_manager.images_iter(&session)? {
            if image.hash == image_id {
                tags.push(image.tag.clone())
            }
        }

        Ok(tags)
    }

    pub fn insert_or_replace_image(&mut self, image: Image) -> ImageManagerResult<()> {
        let mut session = self.state_manager.pooled_session()?;
        self.layer_manager.insert_or_replace_image(&mut session, image)?;
        Ok(())
    }
}

pub struct DownloadResult {
    pub downloaded_images: usize,
    pub downloaded_layers: usize
}

impl DownloadResult {
    pub fn new() -> DownloadResult {
        DownloadResult {
            downloaded_images: 0,
            downloaded_layers: 0,
        }
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

        let result = build_test_image(
            &mut image_manager,
            Path::new("testdata/definitions/simple1.labarfile"),
            ImageTag::from_str("test").unwrap()
        );
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

    build_test_image(
        &mut image_manager,
        Path::new("testdata/definitions/simple1.labarfile"),
        ImageTag::from_str("test").unwrap()
    ).unwrap();

    let result = image_manager.remove_image(&ImageTag::from_str("test").unwrap());
    assert!(result.is_ok());

    let images = image_manager.list_images();
    assert!(result.is_ok());
    let images = images.unwrap();

    assert_eq!(0, images.len());

    let session = image_manager.state_manager.session().unwrap();
    assert_eq!(0, image_manager.layer_manager.all_layers(&session).unwrap().len());
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

    build_test_image(
        &mut image_manager,
        Path::new("testdata/definitions/simple1.labarfile"),
        ImageTag::from_str("test").unwrap()
    ).unwrap();

    build_test_image(
        &mut image_manager,
        Path::new("testdata/definitions/simple1.labarfile"),
        ImageTag::from_str("test2").unwrap()
    ).unwrap();

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

        build_test_image(
            &mut image_manager,
            Path::new("testdata/definitions/simple1.labarfile"),
            ImageTag::from_str("test").unwrap()
        ).unwrap();

        build_test_image(
            &mut image_manager,
            Path::new("testdata/definitions/with_image_ref.labarfile"),
            ImageTag::from_str("that").unwrap()
        ).unwrap();

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
fn build_test_image(image_manager: &mut ImageManager,
                    path: &Path, image_tag: ImageTag) -> Result<Image, String> {
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