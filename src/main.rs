use std::path::{Path, PathBuf};

use chrono::{DateTime, Local};

use serde::{Deserialize, Serialize};
use structopt::clap::Shell;
use structopt::StructOpt;
use image::ImageMetadata;

pub mod helpers;
pub mod lock;
pub mod image_definition;
pub mod image;
pub mod image_manager;
pub mod registry;
pub mod reference;

use crate::helpers::TablePrinter;
use crate::image_definition::{ImageDefinition, ImageDefinitionContext};
use crate::lock::FileLock;
use crate::image_manager::{BoxPrinter, ConsolePrinter, ImageManager, ImageManagerConfig};
use crate::reference::{ImageTag, Reference};
use crate::registry::RegistryConfig;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileConfig {
    default_registry: Option<String>,
    registry_username: Option<String>,
    registry_password: Option<String>,
    registry_use_ssl: bool
}

impl FileConfig {
    pub fn default_registry(&self) -> Option<&str> {
        self.default_registry.as_ref().map(|x| x.as_str())
    }
}

impl FileConfig {
    pub fn from_file(path: &Path) -> Result<FileConfig, String> {
        let content = std::fs::read_to_string(path).map_err(|err| format!("{}", err))?;
        toml::from_str(&content).map_err(|err| format!("{}", err))
    }

    pub fn save_to_file(&self, path: &Path) -> Result<(), String> {
        let content = toml::to_string(self).map_err(|err| format!("{}", err))?;
        std::fs::write(path, content).map_err(|err| format!("{}", err))?;
        Ok(())
    }
}

impl Default for FileConfig {
    fn default() -> Self {
        FileConfig {
            default_registry: None,
            registry_username: None,
            registry_password: None,
            registry_use_ssl: false
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
        tag: ImageTag,
        #[structopt(long, help="The build context")]
        context: Option<PathBuf>,
        #[structopt(long, help="The build arguments on format key=value")]
        arguments: Vec<String>,
        #[structopt(long, help="Forces a build, ignoring previously cached layers")]
        force: bool,
    },
    #[structopt(about="Removes an image")]
    RemoveImage {
        #[structopt(name="tag", help="The tag to remove")]
        tag: ImageTag
    },
    #[structopt(about="Tags an image")]
    #[structopt(name="tag")]
    TagImage {
        #[structopt(name="reference", help="The source image")]
        reference: Reference,
        #[structopt(name="tag", help="The new tag for the image")]
        tag: ImageTag
    },
    #[structopt(about="Lists the available images")]
    ListImages {

    },
    #[structopt(about="Lists the content of an image")]
    ListContent {
        #[structopt(name="reference", help="The reference to list for")]
        reference: Reference
    },
    #[structopt(about="Inspects an image")]
    Inspect {
        #[structopt(name="tag", help="The image to inspect")]
        reference: Reference
    },
    #[structopt(about="Lists the unpackings that has been made")]
    ListUnpackings {

    },
    #[structopt(about="Unpacks an image to a directory")]
    Unpack {
        #[structopt(name="reference", help="The image to unpack")]
        reference: Reference,
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
    #[structopt(about="Removes layers not used")]
    Purge {

    },
    #[structopt(about="Pulls an image from a remote registry")]
    Pull {
        #[structopt(name="tag", help="The image to pull")]
        tag: ImageTag
    },
    #[structopt(about="Pushes a local image to a remote registry")]
    Push {
        #[structopt(name="tag", help="The image to push")]
        tag: ImageTag
    },
    #[structopt(about="Lists the images in a remote registry")]
    ListImagesRegistry {
        #[structopt(name="registry", help="The registry to list for")]
        registry: String
    },
    #[structopt(about="Runs a labar registry")]
    RunRegistry {
        #[structopt(name="config_file", help="The toml configuration file of the registry")]
        config_file: PathBuf
    },
    #[structopt(about="Manages the configuration")]
    Config {
        #[structopt(long, help="Sets a configuration value (key=value)")]
        edit: Option<String>
    }
}

fn create_image_manager(file_config: &FileConfig, printer: BoxPrinter) -> ImageManager {
    let mut config = ImageManagerConfig::new();
    if let Some(registry_username) = file_config.registry_username.as_ref() {
        config.registry_username = registry_username.clone();
    }

    if let Some(registry_password) = file_config.registry_password.as_ref() {
        config.registry_password = registry_password.clone();
    }

    config.registry_use_ssl = file_config.registry_use_ssl;

    ImageManager::from_state_file(config, printer.clone()).unwrap_or_else(|_| ImageManager::new(printer.clone()))
}

fn create_write_lock(_file_config: &FileConfig) -> FileLock {
    FileLock::new(ImageManagerConfig::new().base_folder().join("write_lock"))
}

fn create_unpack_lock(_file_config: &FileConfig) -> FileLock {
    FileLock::new(ImageManagerConfig::new().base_folder().join("unpack_lock"))
}

fn print_images(images: &Vec<ImageMetadata>) {
    let mut table_printer = TablePrinter::new(
        vec![
            "REPOSITORY".to_owned(),
            "TAG".to_owned(),
            "IMAGE ID".to_owned(),
            "CREATED".to_owned(),
            "SIZE".to_owned()
        ]
    );

    for metadata in images {
        let created: DateTime<Local> = metadata.created.into();

        table_printer.add_row(vec![
            metadata.image.tag.full_repository(),
            metadata.image.tag.tag().to_owned(),
            metadata.image.hash.to_string(),
            created.format(DATE_FORMAT).to_string(),
            metadata.size.to_string()
        ]);
    }

    table_printer.print();
}

async fn main_run(file_config: FileConfig, command_line_input: CommandLineInput) -> Result<(), String> {
    let printer = ConsolePrinter::new();

    match command_line_input {
        CommandLineInput::Build { file, tag, context, arguments, force } => {
            let _write_lock = create_write_lock(&file_config);
            let mut image_manager = create_image_manager(&file_config, printer.clone());

            let mut image_definition_context = ImageDefinitionContext::new();
            for argument in arguments {
                let parts = argument.split("=").collect::<Vec<_>>();
                if parts.len() == 2 {
                    image_definition_context.add_variable(parts[0], parts[1]);
                }
            }

            println!("Building image: {}", tag);
            let image_definition_content = std::fs::read_to_string(file).map_err(|err| format!("Build definition not found: {}", err))?;
            let image_definition = ImageDefinition::parse(&image_definition_content, &image_definition_context).map_err(|err| format!("Failed parsing build definition: {}", err))?;
            let context = context.unwrap_or_else(|| std::env::current_dir().unwrap());
            let image = image_manager.build_image(&context, image_definition, &tag, force).map_err(|err| format!("{}", err))?;
            let image_size = image_manager.image_size(&Reference::ImageTag(image.tag.clone())).map_err(|err| format!("{}", err))?;
            println!("Built image {} ({}) of size {:.2}", image.tag, image.hash, image_size);
        }
        CommandLineInput::RemoveImage { tag } => {
            let _write_lock = create_write_lock(&file_config);
            let _unpack_lock = create_unpack_lock(&file_config);
            let mut image_manager = create_image_manager(&file_config, printer.clone());

            image_manager.remove_image(&tag).map_err(|err| format!("{}", err))?;
        },
        CommandLineInput::TagImage { reference, tag } => {
            let _write_lock = create_write_lock(&file_config);
            let mut image_manager = create_image_manager(&file_config, printer.clone());

            let image = image_manager.tag_image(&reference, &tag).map_err(|err| format!("{}", err))?;
            println!("Tagged {} ({}) as {}", reference, image.hash, image.tag);
        },
        CommandLineInput::ListImages {} => {
            let image_manager = create_image_manager(&file_config, printer.clone());

            let images = image_manager.list_images().map_err(|err| format!("{}", err))?;
            print_images(&images);
        }
        CommandLineInput::ListContent { reference } => {
            let image_manager = create_image_manager(&file_config, printer.clone());

            let content = image_manager.list_content(&reference).map_err(|err| format!("{}", err))?;
            for path in content {
                println!("{}", path);
            }
        }
        CommandLineInput::Inspect { reference } => {
            let image_manager = create_image_manager(&file_config, printer.clone());

            let inspect_result = image_manager.inspect(&reference).map_err(|err| format!("{}", err))?;

            println!("Image id: {}", inspect_result.top_layer.hash);
            println!("Tags: {}", inspect_result.image_tags.iter().map(|tag| tag.to_string()).collect::<Vec<_>>().join(", "));
            println!("Created: {}", inspect_result.top_layer.created_datetime().format(DATE_FORMAT));
            println!("Size: {}", inspect_result.size);
            println!();
            println!("Layers:");

            let mut table_printer = TablePrinter::new(
                vec![
                    "IMAGE ID".to_owned(),
                    "CREATED".to_owned(),
                    "SIZE".to_owned()
                ]
            );

            for layer in inspect_result.layers {
                table_printer.add_row(vec![
                    layer.hash.to_string(),
                    layer.created_datetime().format(DATE_FORMAT).to_string(),
                    layer.size.to_string(),
                ]);
            }

            table_printer.print();
        }
        CommandLineInput::ListUnpackings {} => {
            let image_manager = create_image_manager(&file_config, printer.clone());

            let unpackings = image_manager.list_unpackings();
            let mut table_printer = TablePrinter::new(
                vec![
                    "PATH".to_owned(),
                    "IMAGE TAG".to_owned(),
                    "IMAGE ID".to_owned(),
                    "CREATED".to_owned()
                ]
            );

            let images = image_manager.list_images().map_err(|err| format!("{}", err))?;

            for unpacking in unpackings {
                let datetime: DateTime<Local> = unpacking.time.into();
                let image_tag = images
                    .iter()
                    .find(|image| image.image.hash == unpacking.hash)
                    .map(|image| &image.image.tag);

                table_printer.add_row(vec![
                    unpacking.destination.clone(),
                    image_tag.map(|tag| tag.to_string()).unwrap_or_else(|| "N/A".to_owned()),
                    unpacking.hash.to_string(),
                    datetime.format(DATE_FORMAT).to_string()
                ]);
            }

            table_printer.print();
        }
        CommandLineInput::Unpack { reference, destination, replace } => {
            let _unpack_lock = create_unpack_lock(&file_config);
            let mut image_manager = create_image_manager(&file_config, printer.clone());

            image_manager.unpack(&Path::new(&destination), &reference, replace).map_err(|err| format!("{}", err))?;
        },
        CommandLineInput::RemoveUnpacking { path, force } => {
            let _unpack_lock = create_unpack_lock(&file_config);
            let mut image_manager = create_image_manager(&file_config, printer.clone());

            image_manager.remove_unpacking(&Path::new(&path), force).map_err(|err| format!("{}", err))?;
        }
        CommandLineInput::Purge {} => {
            let _write_lock = create_write_lock(&file_config);
            let mut image_manager = create_image_manager(&file_config, printer.clone());

            image_manager.garbage_collect().map_err(|err| format!("{}", err))?;
        },
        CommandLineInput::Push { tag } => {
            let _write_lock = create_write_lock(&file_config);
            let image_manager = create_image_manager(&file_config, printer.clone());
            image_manager.push(&tag, file_config.default_registry()).await.map_err(|err| format!("{}", err))?;
        },
        CommandLineInput::Pull { tag } => {
            let _write_lock = create_write_lock(&file_config);
            let mut image_manager = create_image_manager(&file_config, printer.clone());
            image_manager.pull(&tag, file_config.default_registry()).await.map_err(|err| format!("{}", err))?;
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
        CommandLineInput::Config { edit } => {
            fn print_config(file_config: &FileConfig) {
                println!("default_registry: {}", file_config.default_registry.as_ref().map(|x| x.as_str()).unwrap_or("N/A"));
                println!("registry_username: {}", file_config.registry_username.as_ref().map(|x| x.as_str()).unwrap_or("N/A"));
                println!("registry_password: {}", file_config.registry_password.as_ref().map(|x| "*".repeat(x.len())).unwrap_or("N/A".to_owned()));
                println!("registry_use_ssl: {}", file_config.registry_use_ssl);
            }

            if let Some(edit) = edit {
                let parts = edit.split('=').collect::<Vec<&str>>();

                let mut new_file_config = file_config.clone();
                if parts.len() == 2 {
                    let key = parts[0];
                    let value = parts[1];
                    let value_opt = if value.is_empty() {
                        None
                    } else {
                        Some(value.to_owned())
                    };

                    match key {
                        "default_registry" => {
                            new_file_config.default_registry = value_opt;
                        }
                        "registry_username" => {
                            new_file_config.registry_username = value_opt;
                        }
                        "registry_password" => {
                            new_file_config.registry_password = value_opt;
                        }
                        "registry_use_ssl" => {
                            new_file_config.registry_use_ssl = value == "yes" || value == "true";
                        }
                        _ => {
                            return Err(format!("Invalid key '{}'", key));
                        }
                    }
                } else {
                    return Err("Expected key=value".to_owned());
                }

                new_file_config.save_to_file(&get_config_file())?;

                println!("New config:");
                print_config(&new_file_config);
            } else {
                println!("Config file: {}", get_config_file().display());
                print_config(&file_config);
            }
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), String> {
    if generate_completions() {
        return Ok(());
    }

    let file_config = FileConfig::from_file(&get_config_file()).unwrap_or(FileConfig::default());
    let command_line_input = CommandLineInput::from_args();

    if let Err(err) = main_run(file_config, command_line_input).await {
        println!("{}", err);
    }

    Ok(())
}

fn get_config_file() -> PathBuf {
    dirs::home_dir().unwrap().join(".labar").join("config.toml")
}

const DATE_FORMAT: &str = "%Y-%m-%d %T";

fn generate_completions() -> bool {
    if std::env::args().skip(1).next() == Some("generate-completions".to_owned()) {
        let output_dir = "completions";
        std::fs::create_dir_all(output_dir).unwrap();
        CommandLineInput::clap().gen_completions("labar", Shell::Bash, output_dir);
        true
    } else {
        false
    }
}