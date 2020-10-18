use std::path::{Path};
use std::str::FromStr;

use chrono::{DateTime, Local};

use serde::{Deserialize};

use structopt::StructOpt;

use rusoto_s3::S3Client;
use rusoto_core::Region;

pub mod helpers;
pub mod lock;
pub mod image_definition;
pub mod image;
pub mod image_manager;

use crate::helpers::TablePrinter;
use crate::image_definition::{ImageDefinition};
use crate::lock::FileLock;
use crate::image_manager::{ImageManager, RegistryManager, ImageMetadata, ImageManagerConfig};

#[derive(Deserialize)]
pub struct FileConfig {
    pub registry_uri: String,
    pub registry_region: String
}

impl FileConfig {
    pub fn default() -> FileConfig {
        FileConfig {
            registry_uri: "s3://undefined".to_owned(),
            registry_region: "undefined".to_owned()
        }
    }
}

pub fn load_file_config(filename: &Path) -> std::result::Result<FileConfig, String> {
    let content = std::fs::read_to_string(filename).map_err(|err| format!("{}", err))?;
    serde_yaml::from_str(&content).map_err(|err| format!("{}", err))
}

#[derive(Debug, StructOpt)]
#[structopt(name="docker-for-data", about="Docker-for-data")]
enum CommandLineInput {
    #[structopt(about="Builds an image")]
    Build {
        #[structopt(name="file", help="The file with the build definition")]
        file: String,
        #[structopt(short, long, help="The tag of the image")]
        tag: String
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
    #[structopt(about="Pulls an image from a remote repository")]
    Pull {
        #[structopt(name="tag", help="The image to pull")]
        tag: String,
        #[structopt(long, help="Pull from this registry instead of the default")]
        registry: Option<String>
    },
    #[structopt(about="Pushes a local image to a remote repository")]
    Push {
        #[structopt(name="tag", help="The image to push")]
        tag: String,
        #[structopt(long, help="Push to this registry instead of the default")]
        registry: Option<String>,
        #[structopt(long, help="Pushes all the layer to the remote, even if they exists on remote")]
        force: bool,
    },
    #[structopt(about="Lists the images in a remote registry")]
    ListImagesRegistry {
        #[structopt(long, help="Use this registry instead of the default one")]
        registry: Option<String>,
    },
}

fn create_registry_manager(file_config: &FileConfig, registry_uri: Option<String>) -> RegistryManager {
    RegistryManager::new(
        registry_uri.unwrap_or_else(|| file_config.registry_uri.clone()),
        S3Client::new(Region::from_str(&file_config.registry_region).unwrap())
    )
}

fn create_image_manager(file_config: &FileConfig, registry_uri: Option<String>) -> ImageManager {
    ImageManager::from_state_file(create_registry_manager(file_config, registry_uri.clone()))
        .unwrap_or_else(|_| ImageManager::new(create_registry_manager(file_config, registry_uri)))
}

fn create_write_lock() -> FileLock {
    FileLock::new(ImageManagerConfig::new().base_dir().join("write_lock"))
}

fn create_unpack_lock() -> FileLock {
    FileLock::new(ImageManagerConfig::new().base_dir().join("unpack_lock"))
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
            format!("{:.2} MB", metadata.size as f64 / 1024.0 / 1024.0)
        ]);
    }

    table_printer.print();
}

async fn main_run(file_config: FileConfig, command_line_input: CommandLineInput) -> Result<(), String> {
    match command_line_input {
        CommandLineInput::Build { file, tag } => {
            let _write_lock = create_write_lock();
            let mut image_manager = create_image_manager(&file_config, None);

            println!("Building image: {}", tag);
            let image_definition_content = std::fs::read_to_string(file).map_err(|err| format!("Build definition file not found: {}", err))?;
            let image_definition = ImageDefinition::from_str(&image_definition_content).map_err(|err| format!("Failed parsing build definition: {}", err))?;
            let image = image_manager.build_image(image_definition, &tag).map_err(|err| format!("{}", err))?;
            println!("Built image {} ({})", image.tag, image.hash);
        },
        CommandLineInput::RemoveImage { tag } => {
            let _write_lock = create_write_lock();
            let _unpack_lock = create_unpack_lock();
            let mut image_manager = create_image_manager(&file_config,None);

            image_manager.remove_image(&tag).map_err(|err| format!("{}", err))?;
        },
        CommandLineInput::TagImage { reference, tag } => {
            let _write_lock = create_write_lock();
            let mut image_manager = create_image_manager(&file_config,None);

            let image = image_manager.tag_image(&reference, &tag).map_err(|err| format!("{}", err))?;
            println!("Tagged {} ({}) as {}", reference, image.hash, image.tag);
        },
        CommandLineInput::ListImages { } => {
            let image_manager = create_image_manager(&file_config,None);

            let images = image_manager.list_images().map_err(|err| format!("{}", err))?;
            print_images(&images);
        }
        CommandLineInput::ListContent { tag } => {
            let image_manager = create_image_manager(&file_config,None);

            let content = image_manager.list_content(&tag).map_err(|err| format!("{}", err))?;

            for path in content {
                println!("{}", path);
            }
        }
        CommandLineInput::ListUnpackings { } => {
            let image_manager = create_image_manager(&file_config,None);

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
            let mut image_manager = create_image_manager(&file_config, None);

            image_manager.unpack(&Path::new(&destination), &tag, replace).map_err(|err| format!("{}", err))?;
        },
        CommandLineInput::RemoveUnpacking { path, force } => {
            let _unpack_lock = create_unpack_lock();
            let mut image_manager = create_image_manager(&file_config, None);

            image_manager.remove_unpacking(&Path::new(&path), force).map_err(|err| format!("{}", err))?;
        }
        CommandLineInput::Purge { } => {
            let _write_lock = create_write_lock();
            let mut image_manager = create_image_manager(&file_config, None);

            image_manager.garbage_collect().map_err(|err| format!("{}", err))?;
        },
        CommandLineInput::Push { tag, registry, force } => {
            let _write_lock = create_write_lock();
            let image_manager = create_image_manager(&file_config, registry);

            println!("Pushing image {} to {}", tag, image_manager.registry_uri());
            image_manager.push(&tag, force).await.map_err(|err| format!("{}", err))?;
        },
        CommandLineInput::Pull { tag, registry } => {
            let _write_lock = create_write_lock();
            let mut image_manager = create_image_manager(&file_config, registry);

            println!("Pulling image {} from {}", tag, image_manager.registry_uri());
            image_manager.pull(&tag).await.map_err(|err| format!("{}", err))?;
        },
        CommandLineInput::ListImagesRegistry { registry } => {
            let image_manager = create_image_manager(&file_config, registry);

            let images = image_manager.list_images_remote().await.map_err(|err| format!("{}", err))?;
            print_images(&images);
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), String> {
    let file_config = load_file_config(&ImageManagerConfig::new().base_dir().join("config.yaml")).unwrap_or(FileConfig::default());
    let command_line_input = CommandLineInput::from_args();

    if let Err(err) = main_run(file_config, command_line_input).await {
        println!("{}", err);
    }

    Ok(())
}