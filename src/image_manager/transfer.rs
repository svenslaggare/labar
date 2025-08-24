use std::fs::File;
use std::io::{BufReader, Read, Write};
use std::path::Path;
use std::str::FromStr;

use zip::write::SimpleFileOptions;
use zip::{ZipArchive, ZipWriter};

use crate::image::{Image, Layer, LayerOperation};
use crate::image_manager::{ImageManagerConfig, ImageManagerError, ImageManagerResult, PrinterRef, StateSession};
use crate::image_manager::layer::LayerManager;
use crate::reference::{ImageId, ImageTag};

pub struct TransferManager {
    config: ImageManagerConfig,
    printer: PrinterRef
}

impl TransferManager {
    pub fn new(config: ImageManagerConfig, printer: PrinterRef) -> TransferManager {
        TransferManager{
            config,
            printer
        }
    }

    pub fn export_image(&self,
                        session: &StateSession,
                        layer_manager: &LayerManager,
                        tag: &ImageTag, path: &Path) -> ImageManagerResult<()> {
        let file = File::create(path)
            .map_err(|err| ImageManagerError::FileIOError { message: format!("Failed to create archive file due to: {}", err) })?;

        let mut writer = ZipWriter::new(file);

        let top_layer_hash = layer_manager.fully_qualify_reference(session, &tag.clone().to_ref())?;
        let mut stack = vec![top_layer_hash.clone()];
        while let Some(hash) = stack.pop() {
            let layer = layer_manager.get_layer(&session, &hash.clone().to_ref())?;
            writer.start_file(&format!("layers/{}/manifest.json", layer.hash), SimpleFileOptions::default())?;
            writer.write_all(serde_json::to_string(&layer)?.as_bytes())?;

            for operation in &layer.operations {
                match operation {
                    LayerOperation::Image { hash } => {
                        stack.push(hash.clone());
                    }
                    LayerOperation::File { source_path, .. } => {
                        let abs_source_path = self.config.base_folder.join(source_path);
                        let mut reader = BufReader::new(File::open(&abs_source_path)?);

                        writer.start_file_from_path(source_path, SimpleFileOptions::default())?;
                        std::io::copy(&mut reader, &mut writer)?;
                    }
                    LayerOperation::Directory { .. } => {}
                }
            }

            if let Some(parent_hash) = layer.parent_hash.as_ref() {
                stack.push(parent_hash.clone());
            }
        }

        writer.start_file("images.json", SimpleFileOptions::default())?;
        writer.write_all(serde_json::to_string(&vec![Image::new(top_layer_hash, tag.clone())])?.as_bytes())?;

        writer.finish()?;

        Ok(())
    }

    pub fn import_image(&self,
                        session: &mut StateSession,
                        layer_manager: &LayerManager,
                        path: &Path) -> ImageManagerResult<ImportResult> {
        let file = File::open(path)
            .map_err(|err| ImageManagerError::FileIOError { message: format!("Failed to open archive file due to: {}", err) })?;
        let mut archive = ZipArchive::new(BufReader::new(file))?;

        let mut import_result = ImportResult {
            layers: Vec::new(),
            images: Vec::new()
        };

        for file_name in archive.file_names().map(|x| x.to_owned()).collect::<Vec<String>>() {
            let parts = file_name.split("/").collect::<Vec<_>>();
            if parts.last() == Some(&"manifest.json") {
                let hash = ImageId::from_str(parts[1])
                    .map_err(|err| ImageManagerError::InvalidImageId { error: err.to_string() })?;

                if layer_manager.layer_exist(session, &hash)? {
                    self.printer.println(&format!("Layer {} already exists, skipping import.", hash));
                    continue;
                }

                self.printer.println(&format!("Importing layer {}...", hash));

                std::fs::create_dir_all(self.config.get_layer_folder(&hash))?;

                let mut buffer = String::new();
                archive.by_name(&file_name)?.read_to_string(&mut buffer)?;
                let layer: Layer = serde_json::from_str(&buffer)?;

                for operation in &layer.operations {
                    match operation {
                        LayerOperation::File { source_path, .. } => {
                            let mut archive_file = archive.by_name(&source_path)?;

                            let abs_source_path = self.config.base_folder.join(&source_path);
                            let mut file = File::create(&abs_source_path)?;
                            std::io::copy(&mut archive_file, &mut file)?;
                        }
                        LayerOperation::Image { .. } => {}
                        LayerOperation::Directory { .. } => {}
                    }
                }

                if !layer.verify_valid_paths(&self.config.base_folder) {
                    return Err(ImageManagerError::InvalidImageImport);
                }

                layer_manager.insert_layer(session, layer)?;
                import_result.layers.push(hash.clone());
            }
        }

        let mut buffer = String::new();
        archive.by_name("images.json")?.read_to_string(&mut buffer)?;
        let images: Vec<Image> = serde_json::from_str(&buffer)?;
        for image in images {
            layer_manager.insert_or_replace_image(session, image.clone())?;
            self.printer.println(&format!("Imported image {} ({}).", image.tag, image.hash));
            import_result.images.push(image);
        }

        Ok(import_result)
    }
}

#[derive(Debug, Clone)]
pub struct ImportResult {
    pub layers: Vec<ImageId>,
    pub images: Vec<Image>
}

#[test]
fn test_export_import() {
    use std::str::FromStr;

    use crate::reference::ImageTag;
    use crate::image_manager::build::BuildManager;
    use crate::image_manager::ImageManagerConfig;
    use crate::image_manager::printing::{ConsolePrinter};
    use crate::image_manager::state::StateManager;

    let tmp_folder = super::test_helpers::TempFolder::new();
    tmp_folder.create().unwrap();
    let archive_file = tmp_folder.owned().join("image.zip");

    // Export
    let expected_layers;
    {
        let tmp_env_folder = super::test_helpers::TempFolder::new();
        let config = ImageManagerConfig::with_base_folder(tmp_env_folder.owned().clone());

        let printer = ConsolePrinter::new();
        let state_manager = StateManager::new(&config.base_folder()).unwrap();
        let layer_manager = LayerManager::new(config.clone());
        let build_manager = BuildManager::new(config.clone(), printer.clone());
        let transfer_manager = TransferManager::new(config.clone(), printer.clone());
        let mut session = state_manager.session().unwrap();

        let build_result = super::test_helpers::build_image2(
            &mut session,
            &layer_manager,
            &build_manager,
            Path::new("testdata/definitions/simple5.labarfile"),
            ImageTag::from_str("test").unwrap()
        ).unwrap();
        expected_layers = build_result.layers;

        let export_result = transfer_manager.export_image(
            &session,
            &layer_manager,
            &ImageTag::from_str("test").unwrap(),
            &archive_file
        );

        assert!(export_result.is_ok(), "{}", export_result.unwrap_err());
        assert!(archive_file.exists());
        let archive_size = std::fs::metadata(&archive_file).unwrap().len();
        assert!(archive_size >= 3800 && archive_size <= 3830, "Archive size: {}", archive_size);
    }

    // Import
    {
        let tmp_env_folder = super::test_helpers::TempFolder::new();
        let config = ImageManagerConfig::with_base_folder(tmp_env_folder.owned().clone());

        let printer = ConsolePrinter::new();
        let state_manager = StateManager::new(&config.base_folder()).unwrap();
        let layer_manager = LayerManager::new(config.clone());
        let transfer_manager = TransferManager::new(config.clone(), printer.clone());
        let mut session = state_manager.session().unwrap();

        let import_result = transfer_manager.import_image(
            &mut session,
            &layer_manager,
            &archive_file
        );

        assert!(import_result.is_ok(), "{}", import_result.unwrap_err());
        let import_result = import_result.unwrap();
        assert_eq!(1, import_result.images.len());
        assert_eq!(3, import_result.layers.len());

        for layer in expected_layers {
            assert!(layer_manager.layer_exist(&session, &layer).unwrap(), "Layer {} does not exist", layer);
        }

        assert!(layer_manager.get_image(&session, &ImageTag::from_str("test").unwrap()).is_ok());
    }
}