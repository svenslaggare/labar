use std::collections::{HashSet};
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::ops::Deref;
use std::path::Path;
use std::time::{Duration, Instant};

use chrono::Local;
use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use crate::content::compute_content_hash;
use crate::image::{Image, ImageMetadata, Layer, LayerOperation};
use crate::image_manager::{ImageManagerConfig, ImageManagerError, ImageManagerResult, RegistryError, StorageMode, UnpackFile};
use crate::image_manager::layer::{LayerManager};
use crate::image_manager::unpack::{UnpackManager, UnpackRequest, Unpacking};
use crate::image_manager::build::{BuildManager, BuildRequest, BuildResult};
use crate::helpers::DataSize;
use crate::image_definition::{ImageDefinition};
use crate::image_manager::printing::PrinterRef;
use crate::image_manager::registry::{RegistryManager, RegistrySession};
use crate::image_manager::state::{PooledStateSession, StateManager, StateSession, STATE_FILENAME};
use crate::image_manager::transfer::TransferManager;
use crate::reference::{ImageId, ImageTag, Reference};

pub struct ImageManager {
    config: ImageManagerConfig,
    printer: PrinterRef,

    state_manager: StateManager,
    layer_manager: LayerManager,
    build_manager: BuildManager,
    unpack_manager: UnpackManager,
    transfer_manager: TransferManager,
    registry_manager: RegistryManager
}

impl ImageManager {
    pub fn new(config: ImageManagerConfig, printer: PrinterRef) -> ImageManagerResult<ImageManager> {
        let state_manager = StateManager::new(&config.base_folder)?;

        Ok(
            ImageManager {
                config: config.clone(),
                printer: printer.clone(),

                state_manager,
                layer_manager: LayerManager::new(config.clone()),
                build_manager: BuildManager::new(config.clone(), printer.clone()),
                unpack_manager: UnpackManager::new(config.clone(), printer.clone()),
                transfer_manager: TransferManager::new(config.clone(), printer.clone()),
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

    pub fn system_usage(&self) -> ImageManagerResult<SystemUsage> {
        let session = self.state_manager.pooled_session()?;

        let mut file_storage_size = DataSize(0);

        let mut stack = vec![self.config.layers_base_folder()];

        while let Some(current) = stack.pop() {
            let mut read_dir = std::fs::read_dir(&current)?;
            while let Some(entry) = read_dir.next() {
                let entry = entry?;

                if entry.path().is_file() {
                    file_storage_size += DataSize::from_file(&entry.path());
                } else {
                    stack.push(entry.path());
                }
            }
        }

        Ok(
            SystemUsage {
                layers: session.number_of_layers()?,
                images: session.number_of_images()?,
                state_storage_size: DataSize::from_file(&self.config.base_folder.join(STATE_FILENAME)),
                file_storage_size,
            }
        )
    }

    pub fn build_image(&mut self, request: BuildRequest) -> ImageManagerResult<BuildResult> {
        let mut session = self.state_manager.pooled_session()?;

        let image = self.build_manager.build_image(&mut session, &mut self.layer_manager, request)?;
        Ok(image)
    }

    pub fn build_image_from_directory(&mut self, directory: &Path, tag: ImageTag, force: bool, verbose_output: bool) -> ImageManagerResult<BuildResult> {
        self.build_image(
            BuildRequest {
                build_context: Default::default(),
                image_definition: ImageDefinition::create_from_directory(directory)?,
                tag,
                force,
                verbose_output
            }
        )
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

    pub fn unpack_file(&mut self, unpack_file: UnpackFile) -> ImageManagerResult<()> {
        let session = self.state_manager.pooled_session()?;
        self.unpack_manager.unpack_file(&session, &mut self.layer_manager, unpack_file)?;
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

    pub fn export_image(&self, tag: &ImageTag, path: &Path) -> ImageManagerResult<()> {
        let session = self.state_manager.pooled_session()?;
        self.transfer_manager.export_image(&session, &self.layer_manager, tag, path)?;
        Ok(())
    }

    pub fn import_image(&self, path: &Path) -> ImageManagerResult<()> {
        let mut session = self.state_manager.pooled_session()?;
        self.transfer_manager.import_image(&mut session, &self.layer_manager, path)?;
        Ok(())
    }

    pub fn remove_image(&mut self, tag: &ImageTag) -> ImageManagerResult<usize> {
        self.remove_image_internal(tag, true)
    }

    fn remove_image_internal(&mut self, tag: &ImageTag, gc: bool) -> ImageManagerResult<usize> {
        let mut session = self.state_manager.pooled_session()?;

        if let Some(image) = self.layer_manager.remove_image(&mut session, tag)? {
            self.printer.println(&format!("Removed image: {} ({})", tag, image.hash));

            if gc {
                let removed_layers = self.garbage_collect()?;
                Ok(removed_layers)
            } else {
                Ok(0)
            }
        } else {
            Err(ImageManagerError::ReferenceNotFound { reference: Reference::ImageTag(tag.clone()) })
        }
    }

    pub fn clean_old_images(&mut self, duration: Duration) -> ImageManagerResult<()> {
        let session = self.state_manager.pooled_session()?;

        let now = Local::now();
        for image in self.layer_manager.images_iter(&session)? {
            let layer = self.layer_manager.get_layer(&session, &image.hash.clone().to_ref())?;
            if (now - layer.created).to_std().unwrap() > duration {
                self.remove_image_internal(&image.tag, false)?;
            }
        }

        Ok(())
    }

    pub fn garbage_collect(&mut self) -> ImageManagerResult<usize> {
        let session = self.state_manager.pooled_session()?;

        let mut removed_layers = 0;

        let mut used_layers = HashSet::new();
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
                LayerOperation::Image { .. } => {}
                LayerOperation::Directory { .. } => {}
                LayerOperation::File { source_path, .. } => {
                    let source_path = self.config.base_folder.join(source_path);
                    reclaimed_size += DataSize::from_file(&source_path);
                }
                LayerOperation::CompressedFile { source_path, .. } => {
                    let source_path = self.config.base_folder.join(source_path);
                    reclaimed_size += DataSize::from_file(&source_path);
                }
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
                    LayerOperation::Directory { path } => {
                        local_files.push(path.clone());
                    }
                    LayerOperation::File { path, ..  } => {
                        local_files.push(path.clone());
                    }
                    LayerOperation::CompressedFile { path, ..  } => {
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

    pub fn compress(&mut self, tag: &ImageTag) -> ImageManagerResult<()> {
        let mut session = self.state_manager.pooled_session()?;

        let mut stack = vec![self.layer_manager.fully_qualify_reference(&session, &tag.clone().to_ref())?];
        while let Some(layer_id) = stack.pop() {
            let mut layer = self.get_layer(&layer_id.to_ref())?;
            self.compress_layer(&mut layer, true)?;
            self.layer_manager.insert_or_replace_layer(&mut session, &layer)?;
            layer.visit_image_ids(|id| stack.push(id.clone()));
        }

        Ok(())
    }

    pub fn compress_layer(&self, layer: &mut Layer, always: bool) -> ImageManagerResult<()> {
        let mut compressed_operations = Vec::new();
        for (operation_index, operation) in layer.operations.iter().enumerate() {
            match operation {
                LayerOperation::Image { .. } => {}
                LayerOperation::Directory { .. } => {}
                LayerOperation::File { path, source_path, original_source_path, content_hash, link_type, writable } => {
                    let abs_source_path = self.config.base_folder.join(&source_path);
                    if !always {
                        if DataSize::from_file(&abs_source_path) < DataSize(1024) {
                            continue;
                        }
                    }

                    let temp_source_path = abs_source_path.to_str().unwrap().to_owned() + ".tmp";
                    let temp_source_path = Path::new(&temp_source_path).to_path_buf();

                    {
                        let mut reader = BufReader::new(File::open(&abs_source_path)?);
                        let mut writer = BufWriter::new(GzEncoder::new(File::create(&temp_source_path)?, Compression::default()));
                        std::io::copy(&mut reader, &mut writer)?;
                    }

                    let compressed_content_hash = compute_content_hash(&temp_source_path)?;
                    compressed_operations.push((
                        operation_index,
                        temp_source_path,
                        abs_source_path,
                        LayerOperation::CompressedFile {
                            path: path.clone(),
                            source_path: source_path.clone(),
                            original_source_path: original_source_path.clone(),
                            content_hash: content_hash.clone(),
                            link_type: *link_type,
                            writable: *writable,
                            compressed_content_hash
                        }
                    ));
                }
                LayerOperation::CompressedFile { .. } => {}
            }
        }

        for (operation_index, temp_source_path, abs_source_path, new_operation) in compressed_operations {
            std::fs::rename(temp_source_path, abs_source_path)?;
            layer.operations[operation_index] = new_operation;
        }

        Ok(())
    }

    pub fn decompress(&mut self, tag: &ImageTag) -> ImageManagerResult<()> {
        let mut session = self.state_manager.pooled_session()?;

        let mut stack = vec![self.layer_manager.fully_qualify_reference(&session, &tag.clone().to_ref())?];
        while let Some(layer_id) = stack.pop() {
            let mut layer = self.get_layer(&layer_id.clone().to_ref())?;
            self.decompress_layer(&mut layer)?;
            self.layer_manager.insert_or_replace_layer(&mut session, &layer)?;
            layer.visit_image_ids(|id| stack.push(id.clone()));
        }

        Ok(())
    }

    pub fn decompress_layer(&self, layer: &mut Layer) -> ImageManagerResult<()> {
        let mut decompressed_operations = Vec::new();
        for (operation_index, operation) in layer.operations.iter().enumerate() {
            match operation {
                LayerOperation::Image { .. } => {}
                LayerOperation::Directory { .. } => {}
                LayerOperation::File { .. } => {}
                LayerOperation::CompressedFile { path, source_path, original_source_path, content_hash, link_type, writable, .. } => {
                    let abs_source_path = self.config.base_folder.join(&source_path);

                    let temp_source_path = abs_source_path.to_str().unwrap().to_owned() + ".tmp";
                    let temp_source_path = Path::new(&temp_source_path).to_path_buf();

                    {
                        let mut reader = GzDecoder::new(BufReader::new(File::open(&abs_source_path)?));
                        let mut writer = BufWriter::new(File::create(&temp_source_path)?);
                        std::io::copy(&mut reader, &mut writer)?;
                    }

                    decompressed_operations.push((
                        operation_index,
                        temp_source_path,
                        abs_source_path,
                        LayerOperation::File {
                            path: path.clone(),
                            source_path: source_path.clone(),
                            original_source_path: original_source_path.clone(),
                            content_hash: content_hash.clone(),
                            link_type: *link_type,
                            writable: *writable
                        }
                    ));
                }
            }
        }

        for (operation_index, temp_source_path, abs_source_path, new_operation) in decompressed_operations {
            std::fs::rename(temp_source_path, abs_source_path)?;
            layer.operations[operation_index] = new_operation;
        }

        Ok(())
    }

    fn handle_compression(&self, layer: &mut Layer) -> ImageManagerResult<()> {
        match self.config.storage_mode {
            StorageMode::AlwaysUncompressed => {
                self.printer.println("\t\t* Decompressing...");
                self.decompress_layer(layer)?;
                self.printer.println("\t\t* Decompressed.");
            }
            StorageMode::AlwaysCompressed => {
                self.printer.println("\t\t* Compressing...");
                self.compress_layer(layer, true)?;
                self.printer.println("\t\t* Compressed.");
            }
            StorageMode::PreferCompressed => {}
            StorageMode::PreferUncompressed => {}
        }

        Ok(())
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

    pub async fn pull(&mut self, request: PullRequest<'_>) -> ImageManagerResult<Image> {
        let pull_tag = request.tag.clone().set_registry_opt_if_empty(request.default_registry);

        let session = self.state_manager.pooled_session()?;

        let registry = pull_tag.registry().ok_or_else(|| ImageManagerError::NoRegistryDefined)?;
        let registry_session = RegistrySession::new(&session, registry)?;

        let t0 = Instant::now();
        self.printer.println(&format!("Pulling image: {}", pull_tag));

        let image_metadata = self.resolve_image_in_registry_internal(&registry_session, &pull_tag, true).await?;

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
                self.printer.println(&format!("\t* Pulling layer: {}...", current));
                let mut retries = request.retry.unwrap_or(0);
                let layer = loop {
                    self.printer.println("\t\t* Downloading...");
                    let layer = self.registry_manager.download_layer(&registry_session, &current, request.verbose_output)
                        .await
                        .map_err(|error| ImageManagerError::PullFailed { error });

                    self.printer.println("\t\t* Downloaded.");

                    match layer {
                        Ok(mut layer) => {
                            self.handle_compression(&mut layer)?;
                            break layer;
                        },
                        Err(err) => {
                            if retries == 0 {
                                return Err(err);
                            } else {
                                retries -= 1;

                                let retry_time = 2.0;
                                tokio::time::sleep(Duration::from_secs_f64(retry_time)).await;
                                self.printer.println(&format!(
                                    "{} - will retry in {} seconds...",
                                    err.to_string(),
                                    retry_time
                                ));
                            }
                        }
                    }
                };

                self.insert_layer(layer.clone())?;

                if top_level_hash.is_none() {
                    top_level_hash = Some(layer.hash.clone());
                }

                visit_layer(&mut stack, &layer);
            }
        }

        let image_tag = request.new_tag.unwrap_or_else(|| request.tag.clone());
        let image = Image::new(top_level_hash.unwrap(), image_tag.clone());
        self.insert_or_replace_image(image.clone())?;

        self.printer.println(&format!("Pull complete in {:.1} seconds.", t0.elapsed().as_secs_f64()));

        Ok(image)
    }

    pub async fn push(&self, tag: &ImageTag, default_registry: Option<&str>) -> ImageManagerResult<usize> {
        let top_layer = self.get_layer(&tag.clone().to_ref())?;

        let tag = tag.clone().set_registry_opt_if_empty(default_registry);

        let session = self.state_manager.pooled_session()?;

        let registry = tag.registry().ok_or_else(|| ImageManagerError::NoRegistryDefined)?;
        let registry_session = RegistrySession::new(&session, registry)?;

        let t0 = Instant::now();
        self.printer.println(&format!("Pushing image: {}", tag));

        let mut stack = Vec::new();
        stack.push(top_layer.hash.clone());

        let visit_layer = |stack: &mut Vec<ImageId>, layer: &Layer| {
            layer.visit_image_ids(|image_id| stack.push(image_id.clone()));
        };

        let mut layers_uploaded = 0;
        while let Some(current) = stack.pop() {
            self.printer.println(&format!("\t* Pushing layer: {}...", current));
            let layer = self.get_layer(&current.clone().to_ref())?;
            visit_layer(&mut stack, &layer);
            if self.registry_manager.upload_layer(&registry_session, &layer).await? {
                layers_uploaded += 1;
            }
        }

        self.registry_manager.upload_image(&registry_session, &top_layer.hash, &tag).await?;

        self.printer.println(&format!("Push complete in {:.1} seconds.", t0.elapsed().as_secs_f64()));
        self.printer.println("");
        Ok(layers_uploaded)
    }

    pub async fn pull_layer(&mut self, registry: &str, hash: &ImageId) -> ImageManagerResult<Layer> {
        let session = self.state_manager.pooled_session()?;
        let registry_session = RegistrySession::new(&session, registry)?;

        let layer = self.download_layer(&registry_session, hash).await?;
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

                let layer = self.download_layer(&registry_session, &layer_to_download.hash).await?;
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
                self.insert_or_replace_image(Image::new(image.image.hash, new_tag))?;
            }
        }

        Ok(download_result)
    }

    async fn download_layer(&self, registry_session: &RegistrySession, hash: &ImageId) -> ImageManagerResult<Layer> {
        let mut layer = self.registry_manager.download_layer(&registry_session, &hash, false).await?;
        self.handle_compression(&mut layer)?;
        Ok(layer)
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
        self.layer_manager.insert_layer(&session, &layer)?;
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

pub struct SystemUsage {
    pub layers: usize,
    pub images: usize,
    pub state_storage_size: DataSize,
    pub file_storage_size: DataSize
}

pub struct PullRequest<'a> {
    pub tag: ImageTag,
    pub default_registry: Option<&'a str>,
    pub new_tag: Option<ImageTag>,
    pub retry: Option<usize>,
    pub verbose_output: bool
}

impl<'a> PullRequest<'a> {
    pub fn from_tag(tag: &ImageTag) -> PullRequest<'a> {
        PullRequest {
            tag: tag.clone(),
            default_registry: None,
            new_tag: None,
            retry: None,
            verbose_output: false,
        }
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

    use crate::image_manager::ConsolePrinter;

    let tmp_folder = crate::test_helpers::TempFolder::new();

    {
        let config = ImageManagerConfig::with_base_folder(tmp_folder.owned());

        let mut image_manager = ImageManager::new(config, ConsolePrinter::new()).unwrap();

        let result = super::test_helpers::build_image(
            &mut image_manager,
            Path::new("testdata/definitions/simple1.labarfile"),
            ImageTag::from_str("test").unwrap()
        );
        assert!(result.is_ok());
        let result = result.unwrap().image;

        assert_eq!(ImageTag::from_str("test").unwrap(), result.tag);

        let top_layer = image_manager.get_layer(&Reference::from_str("test").unwrap());
        assert!(top_layer.is_ok());
        let top_layer = top_layer.unwrap();
        assert_eq!(top_layer.hash, result.hash);

        let image = image_manager.get_image(&ImageTag::from_str("test").unwrap());
        assert!(image.is_ok());
        let image = image.unwrap();
        assert_eq!(image.hash, top_layer.hash);

        assert_eq!(Some(DataSize(973)), image_manager.image_size(&Reference::from_str("test").unwrap()).ok());
    }
}

#[test]
fn test_build_from_directory() {
    use std::str::FromStr;

    use crate::image_manager::ConsolePrinter;

    let tmp_folder = crate::test_helpers::TempFolder::new();

    {
        let config = ImageManagerConfig::with_base_folder(tmp_folder.owned());

        let mut image_manager = ImageManager::new(config, ConsolePrinter::new()).unwrap();

        let result = image_manager.build_image_from_directory(
            Path::new("testdata/rawdata2"),
            ImageTag::from_str("test").unwrap(),
            false,
            false
        );
        assert!(result.is_ok());
        let result = result.unwrap().image;

        assert_eq!(ImageTag::from_str("test").unwrap(), result.tag);

        let top_layer = image_manager.get_layer(&Reference::from_str("test").unwrap());
        assert!(top_layer.is_ok());
        let top_layer = top_layer.unwrap();
        assert_eq!(top_layer.hash, result.hash);

        let image = image_manager.get_image(&ImageTag::from_str("test").unwrap());
        assert!(image.is_ok());
        let image = image.unwrap();
        assert_eq!(image.hash, top_layer.hash);

        assert_eq!(Some(DataSize(4257)), image_manager.image_size(&Reference::from_str("test").unwrap()).ok());
    }
}

#[test]
fn test_remove_image1() {
    use std::str::FromStr;

    use crate::image_manager::ConsolePrinter;

    let tmp_folder = crate::test_helpers::TempFolder::new();

    let config = ImageManagerConfig::with_base_folder(tmp_folder.owned());
    let printer = ConsolePrinter::new();

    let mut image_manager = ImageManager::new(
        config,
        printer
    ).unwrap();

    super::test_helpers::build_image(
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

    use crate::image_manager::ConsolePrinter;

    let tmp_folder = crate::test_helpers::TempFolder::new();

    let config = ImageManagerConfig::with_base_folder(tmp_folder.owned());
    let printer = ConsolePrinter::new();

    let mut image_manager = ImageManager::new(
        config,
        printer
    ).unwrap();

    super::test_helpers::build_image(
        &mut image_manager,
        Path::new("testdata/definitions/simple1.labarfile"),
        ImageTag::from_str("test").unwrap()
    ).unwrap();

    super::test_helpers::build_image(
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

    use crate::image_manager::ConsolePrinter;

    let tmp_folder = crate::test_helpers::TempFolder::new();

    {
        let config = ImageManagerConfig::with_base_folder(tmp_folder.owned());

        let mut image_manager = ImageManager::new(config, ConsolePrinter::new()).unwrap();

        super::test_helpers::build_image(
            &mut image_manager,
            Path::new("testdata/definitions/simple1.labarfile"),
            ImageTag::from_str("test").unwrap()
        ).unwrap();

        super::test_helpers::build_image(
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
}

#[test]
fn test_compress() {
    use std::str::FromStr;

    use crate::image_manager::ConsolePrinter;

    let tmp_folder = crate::test_helpers::TempFolder::new();

    {
        let config = ImageManagerConfig::with_base_folder(tmp_folder.owned());

        let mut image_manager = ImageManager::new(config, ConsolePrinter::new()).unwrap();
        let image_tag = ImageTag::from_str("test").unwrap();

        let result = super::test_helpers::build_image(
            &mut image_manager,
            Path::new("testdata/definitions/simple1.labarfile"),
            image_tag.clone()
        );
        assert!(result.is_ok());

        let result = image_manager.compress(&image_tag);
        assert!(result.is_ok(), "{}", result.unwrap_err());

        let unpack_folder = tmp_folder.join("unpack");
        let result = image_manager.unpack(UnpackRequest {
            reference: image_tag.clone().to_ref(),
            unpack_folder: unpack_folder.clone(),
            replace: false,
            dry_run: false,
        });
        assert!(result.is_ok(), "{}", result.unwrap_err());
        crate::assert_file_content_eq!(Path::new("testdata/rawdata/file1.txt"), unpack_folder.join("file1.txt"));
    }
}

#[test]
fn test_decompress() {
    use std::str::FromStr;

    use crate::image_manager::ConsolePrinter;

    let tmp_folder = crate::test_helpers::TempFolder::new();

    {
        let config = ImageManagerConfig::with_base_folder(tmp_folder.owned());

        let mut image_manager = ImageManager::new(config, ConsolePrinter::new()).unwrap();
        let image_tag = ImageTag::from_str("test").unwrap();

        let result = super::test_helpers::build_image(
            &mut image_manager,
            Path::new("testdata/definitions/simple1.labarfile"),
            image_tag.clone()
        );
        assert!(result.is_ok());

        image_manager.compress(&image_tag).unwrap();

        let result = image_manager.decompress(&image_tag);
        assert!(result.is_ok(), "{}", result.unwrap_err());

        let unpack_folder = tmp_folder.join("unpack");
        let result = image_manager.unpack(UnpackRequest {
            reference: image_tag.clone().to_ref(),
            unpack_folder: unpack_folder.clone(),
            replace: false,
            dry_run: false,
        });
        assert!(result.is_ok(), "{}", result.unwrap_err());
        crate::assert_file_content_eq!(Path::new("testdata/rawdata/file1.txt"), unpack_folder.join("file1.txt"));
    }
}