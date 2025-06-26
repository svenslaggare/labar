use std::path::{Path, PathBuf};

use chrono::{DateTime, Local};

use serde::{Deserialize};

use structopt::StructOpt;

pub mod helpers;
pub mod lock;
pub mod image_definition;
pub mod image;
pub mod image_manager;
pub mod registry;

use crate::helpers::TablePrinter;
use crate::image_definition::{ImageDefinition, ImageDefinitionContext};
use crate::lock::FileLock;
use crate::image_manager::{ImageManager, ImageMetadata, ImageManagerConfig, BoxPrinter, ConsolePrinter};
use crate::registry::RegistryConfig;

#[derive(Deserialize)]
pub struct FileConfig {

}

impl FileConfig {
    pub fn load(filename: &Path) -> Result<FileConfig, String> {
        let content = std::fs::read_to_string(filename).map_err(|err| format!("{}", err))?;
        serde_yaml::from_str(&content).map_err(|err| format!("{}", err))
    }
}

impl Default for FileConfig {
    fn default() -> Self {
        FileConfig {

        }
    }
}

#[derive(Debug, StructOpt)]
#[structopt(name="labar", about="Layer Based Archive")]
enum CommandLineInput {
    #[structopt(about="Builds an image")]
    Build {
        #[structopt(name="file", help="The file with the build definition")]
        file: String,
        #[structopt(name="tag", help="The tag of the image")]
        tag: String,
        #[structopt(long, help="The build context")]
        build_context: Option<PathBuf>,
        #[structopt(long, help="The build arguments on format key=value")]
        build_arguments: Vec<String>
    },
    #[structopt(about="Removes an image")]
    RemoveImage {
        #[structopt(name="tag", help="The tag to remove")]
        tag: String
    },
    #[structopt(about="Tags an image")]
    #[structopt(name="tag")]
    TagImage {
        #[structopt(name="reference", help="The source image")]
        reference: String,
        #[structopt(name="tag", help="The new tag for the image")]
        tag: String
    },
    #[structopt(about="Lists the available images")]
    ListImages {

    },
    #[structopt(about="Lists the content of an image")]
    ListContent {
        #[structopt(name="tag", help="The tag to list for")]
        tag: String
    },
    #[structopt(about="Lists the unpackings that has been made")]
    ListUnpackings {

    },
    #[structopt(about="Unpacks an image to a directory")]
    Unpack {
        #[structopt(name="tag", help="The image to unpack")]
        tag: String,
        #[structopt(name="destination", help="The directory to unpack to. Must be empty.")]
        destination: String,
        #[structopt(long, help="Replaces the existing unpacking")]
        replace: bool,
    },
    #[structopt(about="Removes an unpacking")]
    RemoveUnpacking {
        #[structopt(name="path", name="The unpacking to remove")]
        path: String,
        #[structopt(long, help="Force removes an unpacking, not guaranteeing that all files are removed, but entry removed")]
        force: bool,
    },
    #[structopt(about="Removes images not used")]
    Purge {

    },
    #[structopt(about="Pulls an image from a remote registry")]
    Pull {
        #[structopt(name="registry", help="The registry to pull from")]
        registry: String,
        #[structopt(name="tag", help="The image to pull")]
        tag: String
    },
    #[structopt(about="Pushes a local image to a remote registry")]
    Push {
        #[structopt(name="registry", help="The registry to push to")]
        registry: String,
        #[structopt(name="tag", help="The image to push")]
        tag: String
    },
    #[structopt(about="Lists the images in a remote registry")]
    ListImagesRegistry {
        #[structopt(name="registry", help="The registry to list for")]
        registry: String
    },
    #[structopt(about="Runs a labar registry")]
    RunRegistry {
        #[structopt(name="config_file", help="The YAML configuration file of the registry")]
        config_file: PathBuf
    },
}

fn create_image_manager(_file_config: &FileConfig, printer: BoxPrinter) -> ImageManager {
    ImageManager::from_state_file(printer.clone()).unwrap_or_else(|_| ImageManager::new(printer.clone()))
}

fn create_write_lock() -> FileLock {
    FileLock::new(ImageManagerConfig::new().base_folder().join("write_lock"))
}

fn create_unpack_lock() -> FileLock {
    FileLock::new(ImageManagerConfig::new().base_folder().join("unpack_lock"))
}

fn print_images(images: &Vec<ImageMetadata>) {
    let mut table_printer = TablePrinter::new(
        vec![
            "TAG".to_owned(),
            "IMAGE ID".to_owned(),
            "CREATED".to_owned(),
            "SIZE".to_owned()
        ]
    );

    for metadata in images {
        let created: DateTime<Local> = metadata.created.into();

        table_printer.add_row(vec![
            metadata.image.tag.clone(),
            metadata.image.hash.clone(),
            created.format("%Y-%m-%d %T").to_string(),
            metadata.size.to_string()
        ]);
    }

    table_printer.print();
}

async fn main_run(file_config: FileConfig, command_line_input: CommandLineInput) -> Result<(), String> {
    let printer = ConsolePrinter::new();

    match command_line_input {
        CommandLineInput::Build { file, tag, build_context, build_arguments } => {
            let _write_lock = create_write_lock();
            let mut image_manager = create_image_manager(&file_config, printer.clone());

            let mut image_definition_context = ImageDefinitionContext::new();
            for argument in build_arguments {
                let parts = argument.split("=").collect::<Vec<_>>();
                if parts.len() == 2 {
                    image_definition_context.add_variable(parts[0], parts[1]);
                }
            }

            println!("Building image: {}", tag);
            let image_definition_content = std::fs::read_to_string(file).map_err(|err| format!("Build definition file not found: {}", err))?;
            let image_definition = ImageDefinition::from_str(&image_definition_content, &image_definition_context).map_err(|err| format!("Failed parsing build definition: {}", err))?;
            let build_context = build_context.unwrap_or_else(|| Path::new("").to_owned());
            let image = image_manager.build_image(&build_context, image_definition, &tag).map_err(|err| format!("{}", err))?;
            let image_size = image_manager.image_size(&image.tag).map_err(|err| format!("{}", err))?;
            println!("Built image {} ({}) of size {:.2}", image.tag, image.hash, image_size);
        },
        CommandLineInput::RemoveImage { tag } => {
            let _write_lock = create_write_lock();
            let _unpack_lock = create_unpack_lock();
            let mut image_manager = create_image_manager(&file_config, printer.clone());

            image_manager.remove_image(&tag).map_err(|err| format!("{}", err))?;
        },
        CommandLineInput::TagImage { reference, tag } => {
            let _write_lock = create_write_lock();
            let mut image_manager = create_image_manager(&file_config, printer.clone());

            let image = image_manager.tag_image(&reference, &tag).map_err(|err| format!("{}", err))?;
            println!("Tagged {} ({}) as {}", reference, image.hash, image.tag);
        },
        CommandLineInput::ListImages { } => {
            let image_manager = create_image_manager(&file_config, printer.clone());

            let images = image_manager.list_images().map_err(|err| format!("{}", err))?;
            print_images(&images);
        }
        CommandLineInput::ListContent { tag } => {
            let image_manager = create_image_manager(&file_config, printer.clone());

            let content = image_manager.list_content(&tag).map_err(|err| format!("{}", err))?;
            for path in content {
                println!("{}", path);
            }
        }
        CommandLineInput::ListUnpackings { } => {
            let image_manager = create_image_manager(&file_config, printer.clone());

            let unpackings = image_manager.list_unpackings();
            let mut table_printer = TablePrinter::new(
                vec![
                    "PATH".to_owned(),
                    "IMAGE ID".to_owned(),
                    "CREATED".to_owned()
                ]
            );

            for unpacking in unpackings {
                let datetime: DateTime<Local> = unpacking.time.into();

                table_printer.add_row(vec![
                    unpacking.destination.clone(),
                    unpacking.hash.clone(),
                    datetime.format("%Y-%m-%d %T").to_string()
                ]);
            }

            table_printer.print();
        }
        CommandLineInput::Unpack { tag, destination, replace } => {
            let _unpack_lock = create_unpack_lock();
            let mut image_manager = create_image_manager(&file_config, printer.clone());

            image_manager.unpack(&Path::new(&destination), &tag, replace).map_err(|err| format!("{}", err))?;
        },
        CommandLineInput::RemoveUnpacking { path, force } => {
            let _unpack_lock = create_unpack_lock();
            let mut image_manager = create_image_manager(&file_config, printer.clone());

            image_manager.remove_unpacking(&Path::new(&path), force).map_err(|err| format!("{}", err))?;
        }
        CommandLineInput::Purge { } => {
            let _write_lock = create_write_lock();
            let mut image_manager = create_image_manager(&file_config, printer.clone());

            image_manager.garbage_collect().map_err(|err| format!("{}", err))?;
        },
        CommandLineInput::Push { tag, registry } => {
            let _write_lock = create_write_lock();
            let image_manager = create_image_manager(&file_config, printer.clone());

            println!("Pushing image {} to {}", tag, registry);
            image_manager.push(&registry, &tag).await.map_err(|err| format!("{}", err))?;
        },
        CommandLineInput::Pull { tag, registry } => {
            let _write_lock = create_write_lock();
            let mut image_manager = create_image_manager(&file_config, printer.clone());

            println!("Pulling image {} from {}", tag, registry);
            image_manager.pull(&registry, &tag).await.map_err(|err| format!("{}", err))?;
        },
        CommandLineInput::ListImagesRegistry { registry } => {
            let image_manager = create_image_manager(&file_config, printer.clone());

            let images = image_manager.list_images_registry(&registry).await.map_err(|err| format!("{}", err))?;
            print_images(&images);
        }
        CommandLineInput::RunRegistry { config_file } => {
            let registry_config = RegistryConfig::load(&config_file)?;
            registry::run(registry_config).await;
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), String> {
    let file_config = FileConfig::load(&ImageManagerConfig::new().base_folder().join("config.yaml")).unwrap_or(FileConfig::default());
    let command_line_input = CommandLineInput::from_args();

    if let Err(err) = main_run(file_config, command_line_input).await {
        println!("{}", err);
    }

    Ok(())
}