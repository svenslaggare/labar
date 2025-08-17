use std::path::{Path, PathBuf};
use std::time::Instant;
use chrono::{DateTime, Local};
use serde::{Deserialize, Serialize};
use structopt::clap::Shell;
use structopt::StructOpt;

pub mod helpers;
pub mod lock;
pub mod image_definition;
pub mod image;
pub mod image_manager;
pub mod registry;
pub mod reference;
pub mod content;

use crate::helpers::{edit_key_value, TablePrinter};
use crate::image::ImageMetadata;
use crate::image_definition::{ImageDefinition, ImageDefinitionContext};
use crate::lock::FileLock;
use crate::image_manager::{BoxPrinter, BuildRequest, ConsolePrinter, ImageManager, ImageManagerConfig, ImageManagerError, ImageManagerResult, RegistryError, UnpackRequest};
use crate::reference::{ImageTag, Reference};
use crate::registry::auth::{AccessRight, Password};
use crate::registry::config::{config_file_add_user, config_file_remove_user, RegistryConfig};

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
        #[structopt(name="tag", help="The tag of the image to remove")]
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
        #[structopt(long, help="Simulates what an unpacking would do")]
        dry_run: bool
    },
    #[structopt(about="Removes an unpacking")]
    RemoveUnpacking {
        #[structopt(name="path", name="The unpacking to remove")]
        path: String,
        #[structopt(long, help="Force removes an unpacking, not guaranteeing that all files are removed, but entry removed")]
        force: bool,
    },
    #[structopt(about="Extracts an image to an archive file")]
    Extract {
        #[structopt(name="reference", help="The image to extract")]
        reference: Reference,
        #[structopt(name="archive", help="The archive file to extract into")]
        archive: String,
    },
    #[structopt(about="Removes layers not used")]
    Purge {

    },
    #[structopt(about="Login into a remote registry")]
    Login {
        #[structopt(name="registry", help="The registry to login for")]
        registry: String,
        #[structopt(name="username", help="The username")]
        username: String,
        #[structopt(name="password", help="The password")]
        password: String,
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
    #[structopt(about="Manages a labar registry")]
    Registry {
        #[structopt(subcommand)]
        command: RegistryCommandLineInput
    },
    #[structopt(about="Manages the configuration")]
    Config {
        #[structopt(long, help="Sets a configuration value (key=value)")]
        edit: Option<String>
    }
}

#[derive(Debug, StructOpt)]
enum RegistryCommandLineInput {
    #[structopt(about="Runs a labar registry")]
    Run {
        #[structopt(name="config_file", help="The toml configuration file of the registry")]
        config_file: PathBuf
    },
    #[structopt(about="Removes an image from the registry")]
    RemoveImage {
        #[structopt(name="config_file", help="The toml configuration file of the registry")]
        config_file: PathBuf,
        #[structopt(name="tag", help="The tag of the image to remove")]
        tag: ImageTag
    },
    #[structopt(about="Adds a new user to the registry")]
    AddUser {
        #[structopt(name="config_file", help="The toml configuration file of the registry")]
        config_file: PathBuf,
        #[structopt(help="The username")]
        username: String,
        #[structopt(help="The password")]
        password: Password,
        #[structopt(help="The list of access rights")]
        access_rights: Vec<AccessRight>
    },
    #[structopt(about="Removes a new user to the registry")]
    RemoveUser {
        #[structopt(name="config_file", help="The toml configuration file of the registry")]
        config_file: PathBuf,
        #[structopt(help="The username")]
        username: String
    }
}

fn create_image_manager(file_config: &FileConfig, printer: BoxPrinter) -> ImageManager {
    let mut config = ImageManagerConfig::new();
    config.accept_self_signed = file_config.accept_self_signed;
    ImageManager::with_config(config, printer.clone()).unwrap_or_else(|_| ImageManager::new(printer.clone()).unwrap())
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
            let start_time = Instant::now();
            let image_definition_content = std::fs::read_to_string(file).map_err(|err| format!("Build definition not found: {}", err))?;
            let image_definition = ImageDefinition::parse(
                &image_definition_content,
                &image_definition_context
            ).map_err(|err| format!("Failed parsing build definition: {}", err))?;

            let request = BuildRequest {
                build_context: context.unwrap_or_else(|| std::env::current_dir().unwrap()),
                image_definition,
                tag,
                force,
            };

            let image = image_manager.build_image(request).map_err(|err| format!("{}", err))?;
            let image_size = image_manager.image_size(&Reference::ImageTag(image.tag.clone())).map_err(|err| format!("{}", err))?;
            println!("Built image {} ({}) of size {:.2} in {:.2} seconds", image.tag, image.hash, image_size, start_time.elapsed().as_secs_f64());
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

            let unpackings = image_manager.list_unpackings().map_err(|err| format!("{}", err))?;
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
        CommandLineInput::Unpack { reference, destination, replace, dry_run } => {
            let _unpack_lock = create_unpack_lock(&file_config);
            let mut image_manager = create_image_manager(&file_config, printer.clone());

            let request = UnpackRequest {
                reference,
                unpack_folder: Path::new(&destination).to_path_buf(),
                replace,
                dry_run,
            };

            image_manager.unpack(request).map_err(|err| format!("{}", err))?;
        },
        CommandLineInput::RemoveUnpacking { path, force } => {
            let _unpack_lock = create_unpack_lock(&file_config);
            let mut image_manager = create_image_manager(&file_config, printer.clone());

            image_manager.remove_unpacking(&Path::new(&path), force).map_err(|err| format!("{}", err))?;
        }
        CommandLineInput::Extract { reference, archive } => {
            let image_manager = create_image_manager(&file_config, printer.clone());
            image_manager.extract(&reference, Path::new(&archive)).map_err(|err| format!("{}", err))?;
        }
        CommandLineInput::Purge {} => {
            let _write_lock = create_write_lock(&file_config);
            let mut image_manager = create_image_manager(&file_config, printer.clone());

            image_manager.garbage_collect().map_err(|err| format!("{}", err))?;
        },
        CommandLineInput::Login { registry, username, password } => {
            let mut image_manager = create_image_manager(&file_config, printer.clone());
            image_manager.login(&registry, &username, &password).await.map_err(|err| format!("{}", err))?;
            println!("Logged into registry {}.", registry);
        }
        CommandLineInput::Push { tag } => {
            let _write_lock = create_write_lock(&file_config);
            let image_manager = create_image_manager(&file_config, printer.clone());
            transform_registry_result(image_manager.push(&tag, file_config.default_registry()).await)?;
        },
        CommandLineInput::Pull { tag } => {
            let _write_lock = create_write_lock(&file_config);
            let mut image_manager = create_image_manager(&file_config, printer.clone());
            transform_registry_result(image_manager.pull(&tag, file_config.default_registry()).await)?;
        },
        CommandLineInput::ListImagesRegistry { registry } => {
            let image_manager = create_image_manager(&file_config, printer.clone());

            let images = transform_registry_result(image_manager.list_images_in_registry(&registry).await)?;
            print_images(&images);
        }
        CommandLineInput::Registry { command } => {
            match command {
                RegistryCommandLineInput::Run { config_file } => {
                    let registry_config = RegistryConfig::load_from_file(&config_file)?;
                    registry::run(registry_config).await;
                }
                RegistryCommandLineInput::RemoveImage { config_file, tag } => {
                    let registry_config = RegistryConfig::load_from_file(&config_file)?;

                    let mut image_manager = ImageManager::with_config(registry_config.image_manager_config(), printer.clone()).unwrap();
                    image_manager.remove_image(&tag).map_err(|err| format!("{}", err))?;
                }
                RegistryCommandLineInput::AddUser { config_file, username, password, access_rights } => {
                    config_file_add_user(&config_file, username, password, access_rights)?;
                }
                RegistryCommandLineInput::RemoveUser { config_file, username } => {
                    config_file_remove_user(&config_file, username)?;
                }
            }
        }
        CommandLineInput::Config { edit } => {
            fn print_config(file_config: &FileConfig) {
                println!("default_registry: {}", file_config.default_registry.as_ref().map(|x| x.as_str()).unwrap_or("N/A"));
                println!("accept_self_signed: {}", file_config.accept_self_signed);
            }

            if let Some(edit) = edit {
                let mut new_file_config = file_config.clone();
                let (key, value) = edit_key_value(&edit)?;
                let value_str = value.unwrap_or("");
                match key {
                    "default_registry" => {
                        new_file_config.default_registry = value.map(|x| x.to_owned());
                    }
                    "accept_self_signed" => {
                        new_file_config.accept_self_signed = value_str == "yes" || value_str == "true";
                    }
                    _ => {
                        return Err(format!("Invalid key '{}'", key));
                    }
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
        std::process::exit(1);
    }

    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileConfig {
    default_registry: Option<String>,
    #[serde(default="default_accept_self_signed")]
    accept_self_signed: bool
}

fn default_accept_self_signed() -> bool {
    true
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
            accept_self_signed: true
        }
    }
}

fn get_config_file() -> PathBuf {
    dirs::home_dir().unwrap().join(".labar").join("config.toml")
}

fn transform_registry_result<T>(result: ImageManagerResult<T>) -> Result<T, String> {
    match result {
        Ok(value) => {
            Ok(value)
        }
        Err(ImageManagerError::RegistryError { error: RegistryError::InvalidAuthentication }) => {
            Err("Not signed into registry. Please run the login command.".to_owned())
        }
        Err(err) => {
            Err(err.to_string())
        }
    }
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