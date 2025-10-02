use std::path::{Path, StripPrefixError};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::str::FromStr;

use regex::Regex;
use crate::helpers::split_parts;
use crate::image::LinkType;
use crate::reference::Reference;

pub type ImageParseResult<T> = Result<T, ImageParseError>;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ImageDefinition {
    pub base_image: Option<Reference>,
    pub layers: Vec<LayerDefinition>
}

impl ImageDefinition {
    pub fn new(base_image: Option<Reference>, layers: Vec<LayerDefinition>) -> ImageDefinition {
        ImageDefinition {
            base_image,
            layers
        }
    }

    pub fn expand(self, build_context: &Path) -> ImageParseResult<ImageDefinition> {
        let mut expanded_layers = Vec::new();
        for layer in self.layers {
            expanded_layers.push(layer.expand(build_context)?);
        }

        Ok(
            ImageDefinition {
                base_image: self.base_image,
                layers: expanded_layers
            }
        )
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct LayerDefinition {
    pub input_line: String,
    pub operations: Vec<LayerOperationDefinition>
}

impl LayerDefinition {
    pub fn new(input_line: String, operations: Vec<LayerOperationDefinition>) -> LayerDefinition {
        LayerDefinition {
            input_line,
            operations
        }
    }

    pub fn expand(self, build_context: &Path) -> ImageParseResult<LayerDefinition> {
        Ok(
            LayerDefinition {
                input_line: self.input_line,
                operations: expand_operations(build_context, self.operations)?
            }
        )
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum LayerOperationDefinition {
    Image { reference: Reference },
    Directory { path: String },
    File { path: String, source_path: String, link_type: LinkType, writable: bool },
}

#[derive(Debug)]
pub enum ImageParseError {
    DefinitionFileNotFound(std::io::Error),
    InvalidLine(String),
    UndefinedCommand(String),
    FromOnlyOnFirst,
    ExpectedArguments { expected: usize, actual: usize },
    ExpectedSubcomand,
    NotWithinSubLayer,
    AlreadyWithinSubLayer,
    SubLayerNotEnded,
    InvalidSubcommand(String),
    VariableNotFound(String),
    InvalidImageReference(String),
    IsAbsolutePath(String),
    IO(std::io::Error),
    StripPrefix(StripPrefixError),
    Other(String),
}

impl From<std::io::Error> for ImageParseError {
    fn from(error: std::io::Error) -> Self {
        ImageParseError::IO(error)
    }
}

impl From<StripPrefixError> for ImageParseError {
    fn from(error: StripPrefixError) -> Self {
        ImageParseError::StripPrefix(error)
    }
}

impl Display for ImageParseError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ImageParseError::DefinitionFileNotFound(error) => write!(f, "Definition file not found: {}", error),
            ImageParseError::InvalidLine(line) => write!(f, "Invalid line: {}", line),
            ImageParseError::UndefinedCommand(command) => write!(f, "'{}' is not a defined command", command),
            ImageParseError::FromOnlyOnFirst => write!(f, "FROM statement only allowed on the first line"),
            ImageParseError::ExpectedArguments { expected, actual } => write!(f, "Expected {} arguments but got {}", expected, actual),
            ImageParseError::ExpectedSubcomand => write!(f, "Expected subcommand"),
            ImageParseError::InvalidSubcommand(subcommand) => write!(f, "'{}' is not a valid subcommand", subcommand),
            ImageParseError::NotWithinSubLayer => write!(f, "Not within a sublayer"),
            ImageParseError::AlreadyWithinSubLayer => write!(f, "Already within a sublayer"),
            ImageParseError::SubLayerNotEnded => write!(f, "Sublayer not terminated with END"),
            ImageParseError::VariableNotFound(name) => write!(f, "Variable '{}' not found", name),
            ImageParseError::InvalidImageReference(error) => write!(f, "Invalid image reference: {}", error),
            ImageParseError::IsAbsolutePath(path) => write!(f, "The path '{}' is absolute", path),
            ImageParseError::StripPrefix(error) => write!(f, "Failed to strip prefix due to: {}", error),
            ImageParseError::IO(error) => write!(f, "IO error: {}", error),
            ImageParseError::Other(error) => write!(f, "{}", error),
        }
    }
}

impl ImageDefinition {
    pub fn parse(content: &str, context: &ImageDefinitionContext) -> ImageParseResult<ImageDefinition> {
        let mut image_definition = ImageDefinition::new(None, Vec::new());
        let variable_regex = [
            Regex::new("\\$([A-Za-z0-9_]+)").unwrap(),
            Regex::new("\\$\\{([A-Za-z0-9_]+)}").unwrap()
        ];

        let mut is_first_line = true;

        struct ActiveLayer {
            layer: Option<LayerDefinition>,
            within_layer: bool
        }

        impl ActiveLayer {
            fn new_layer(&mut self, line: &str) -> ImageParseResult<()> {
                if self.within_layer {
                    return Err(ImageParseError::AlreadyWithinSubLayer);
                }

                self.layer = Some(LayerDefinition::new(line.to_owned(), Vec::new()));
                self.within_layer = true;

                Ok(())
            }

            fn end_layer(&mut self, image_definition: &mut ImageDefinition) -> ImageParseResult<()> {
                if self.within_layer {
                    if let Some(layer) = self.layer.take() {
                        image_definition.layers.push(layer);
                    }

                    self.within_layer = false;
                    Ok(())
                } else {
                    Err(ImageParseError::NotWithinSubLayer)
                }
            }

            fn add_operation(&mut self,
                             image_definition: &mut ImageDefinition,
                             line: &str,
                             operation: LayerOperationDefinition) {
                if !self.within_layer {
                    self.layer = Some(LayerDefinition::new(line.to_owned(), Vec::new()));
                }

                if let Some(layer) = self.layer.as_mut() {
                    layer.operations.push(operation);
                }

                if !self.within_layer {
                    if let Some(layer) = self.layer.take() {
                        image_definition.layers.push(layer);
                    }
                }
            }

            fn finish(&self) -> ImageParseResult<()> {
                if self.within_layer {
                    Err(ImageParseError::SubLayerNotEnded)
                } else {
                    Ok(())
                }
            }
        }

        let mut active_layer = ActiveLayer {
            layer: None,
            within_layer: false,
        };

        for line in content.lines() {
            if line.trim_start().starts_with("#") {
                continue;
            }

            let mut parts = split_parts(&line);
            if parts.len() >= 1 {
                for part in parts.iter_mut().skip(1) {
                    for regex in &variable_regex {
                        while let Some(regex_capture) = regex.captures(part) {
                            let group = regex_capture.get(1).unwrap();
                            let variable = group.as_str();
                            let variable_value = context.get_variable(variable).ok_or_else(|| ImageParseError::VariableNotFound(variable.to_owned()))?;
                            part.replace_range(regex_capture.get(0).unwrap().range(), variable_value);
                        }
                    }
                }

                let command = parts[0].as_str();
                let num_arguments = parts.len() - 1;

                match command {
                    "FROM" => {
                        if num_arguments != 1 {
                            return Err(ImageParseError::ExpectedArguments { expected: 1, actual: num_arguments });
                        }

                        if is_first_line {
                            let base_image = Reference::from_str(&parts[1].to_owned()).map_err(|err| ImageParseError::InvalidImageReference(err))?;
                            image_definition.base_image = Some(base_image);
                        } else {
                            return Err(ImageParseError::FromOnlyOnFirst);
                        }
                    },
                    "COPY" => {
                        if num_arguments < 2 {
                            return Err(ImageParseError::ExpectedArguments { expected: 2, actual: num_arguments });
                        }

                        let arguments = extract_arguments(&mut parts);

                        let source = parts[1].to_owned();
                        let destination = parts[2].to_owned();
                        let link_type = match arguments.get("link").map(|x| x.as_str()) {
                            Some("soft") => LinkType::Soft,
                            Some("hard") => LinkType::Hard,
                            _ => LinkType::Hard
                        };

                        let writable = match arguments.get("writable").map(|x| x.as_str()) {
                            Some("yes" | "true") => true,
                            Some("no" | "false") => false,
                            _ => false
                        };

                        active_layer.add_operation(
                            &mut image_definition,
                            line,
                            LayerOperationDefinition::File {
                                path: destination,
                                source_path: source,
                                link_type,
                                writable
                            }
                        );
                    },
                    "MKDIR" => {
                        if num_arguments != 1 {
                            return Err(ImageParseError::ExpectedArguments { expected: 1, actual: num_arguments });
                        }

                        let path = parts[1].to_owned();
                        active_layer.add_operation(
                            &mut image_definition,
                            line,
                            LayerOperationDefinition::Directory { path }
                        );
                    },
                    "IMAGE" => {
                        let reference = Reference::from_str(&parts[1].to_owned()).map_err(|err| ImageParseError::InvalidImageReference(err))?;

                        if num_arguments != 1 {
                            return Err(ImageParseError::ExpectedArguments { expected: 1, actual: num_arguments });
                        }

                        active_layer.add_operation(
                            &mut image_definition,
                            line,
                            LayerOperationDefinition::Image { reference }
                        );
                    }
                    "BEGIN" => {
                        if num_arguments != 1 {
                            return Err(ImageParseError::ExpectedSubcomand)
                        }

                        let subcommand = &parts[1];
                        match subcommand.as_ref() {
                            "LAYER" => {
                                active_layer.new_layer(line)?;
                            }
                            _ => {
                                return Err(ImageParseError::InvalidSubcommand(subcommand.clone()));
                            }
                        }
                    }
                    "END" => {
                        active_layer.end_layer(&mut image_definition)?;
                    }
                    _ => { return Err(ImageParseError::UndefinedCommand(command.to_owned())); }
                }
            } else if !line.trim().is_empty() {
                return Err(ImageParseError::InvalidLine(line.to_owned()));
            }

            is_first_line = false;
        }

        active_layer.finish()?;

        Ok(image_definition)
    }

    pub fn parse_without_context(content: &str) -> ImageParseResult<ImageDefinition> {
        ImageDefinition::parse(content, &ImageDefinitionContext::new())
    }

    pub fn parse_file(path: &Path, context: &ImageDefinitionContext) -> ImageParseResult<ImageDefinition> {
        let content = std::fs::read_to_string(path).map_err(|err| ImageParseError::DefinitionFileNotFound(err))?;
        ImageDefinition::parse(&content, context)
    }

    pub fn parse_file_without_context(path: &Path) -> ImageParseResult<ImageDefinition> {
        ImageDefinition::parse_file(path, &ImageDefinitionContext::new())
    }

    pub fn create_from_directory(directory: &Path) -> ImageParseResult<ImageDefinition> {
        let mut read_dir = std::fs::read_dir(directory)?;
        let mut root_files = Vec::new();
        let mut directories = Vec::new();
        while let Some(entry) = read_dir.next() {
            let entry = entry?;

            if entry.path().is_file() {
                root_files.push(entry.path());
            } else {
                directories.push(entry.path());
            }
        }

        root_files.sort();
        directories.sort();

        let mut layers = Vec::new();
        for current_directory in directories {
            let current_directory_relative = current_directory.strip_prefix(&directory)?;

            layers.push(
                LayerDefinition {
                    input_line: String::new(),
                    operations: vec![
                        LayerOperationDefinition::File {
                            path: current_directory_relative.to_str().unwrap().to_owned(),
                            source_path: current_directory.to_str().unwrap().to_owned(),
                            link_type: LinkType::Hard,
                            writable: false,
                        }
                    ]
                }
            );
        }

        for file in root_files {
            let file_relative = file.strip_prefix(&directory)?;

            layers.push(
                LayerDefinition {
                    input_line: String::new(),
                    operations: vec![
                        LayerOperationDefinition::File {
                            path: file_relative.to_str().unwrap().to_owned(),
                            source_path: file.to_str().unwrap().to_owned(),
                            link_type: LinkType::Hard,
                            writable: false
                        }
                    ]
                }
            );
        }

        Ok(
            ImageDefinition {
                base_image: None,
                layers
            }
        )
    }
}

fn expand_operations(build_context: &Path, operations: Vec<LayerOperationDefinition>) -> ImageParseResult<Vec<LayerOperationDefinition>> {
    let mut expanded_operations = Vec::new();

    for operation_definition in operations{
        match operation_definition {
            LayerOperationDefinition::Image { reference } => {
                expanded_operations.push(LayerOperationDefinition::Image { reference });
            }
            LayerOperationDefinition::File { path, source_path, link_type, writable } => {
                let source_path_obj = Path::new(&source_path);
                if source_path_obj.is_absolute() {
                    return Err(ImageParseError::IsAbsolutePath(source_path.clone()));
                }
                let source_path_obj = build_context.join(source_path_obj);

                let destination_path = Path::new(&path);

                if source_path_obj.is_file() {
                    if path.chars().last() == Some('/') {
                        expanded_operations.push(
                            LayerOperationDefinition::File {
                                path: destination_path.join(source_path_obj.file_name().unwrap()).to_str().unwrap().to_owned(),
                                source_path: source_path_obj.to_str().unwrap().to_owned(),
                                link_type,
                                writable
                            }
                        );
                    } else if path == "." {
                        expanded_operations.push(
                            LayerOperationDefinition::File {
                                path: source_path_obj.file_name().unwrap().to_str().unwrap().to_owned(),
                                source_path: source_path_obj.to_str().unwrap().to_owned(),
                                link_type,
                                writable
                            }
                        );
                    } else {
                        expanded_operations.push(
                            LayerOperationDefinition::File {
                                path,
                                source_path: source_path_obj.to_str().unwrap().to_owned(),
                                link_type,
                                writable
                            }
                        );
                    }
                } else {
                    expanded_operations.append(&mut recursive_copy_operations(
                        &source_path_obj,
                        destination_path,
                        link_type,
                        writable
                    )?);
                }
            },
            LayerOperationDefinition::Directory { path } => {
                expanded_operations.push(LayerOperationDefinition::Directory { path });
            },
        }
    }

    Ok(expanded_operations)
}

fn recursive_copy_operations(source_path: &Path,
                             base_destination_path: &Path,
                             link_type: LinkType,
                             writable: bool) -> ImageParseResult<Vec<LayerOperationDefinition>> {
    let mut stack = Vec::new();
    stack.push(source_path.to_owned());

    let mut results = Vec::new();

    while let Some(current) = stack.pop() {
        let mut read_dir = std::fs::read_dir(current)?;
        while let Some(entry) = read_dir.next() {
            let entry = entry?;

            let entry_path = entry.path();
            let relative_entry_path = entry_path.strip_prefix(source_path).map_err(|err| ImageParseError::Other(err.to_string()))?;

            let relative_entry_path = if base_destination_path != Path::new(".") {
                base_destination_path.join(relative_entry_path)
            } else {
                relative_entry_path.to_owned()
            };

            if entry_path.is_dir() {
                results.push(LayerOperationDefinition::Directory {
                    path: relative_entry_path.to_str().unwrap().to_owned()
                });

                stack.push(entry_path);
            } else if entry_path.is_file() {
                results.push(LayerOperationDefinition::File {
                    path: relative_entry_path.to_str().unwrap().to_owned(),
                    source_path: entry_path.to_str().unwrap().to_owned(),
                    link_type,
                    writable
                });
            }
        }
    }

    results.sort();

    Ok(results)
}

pub struct ImageDefinitionContext {
    variables: HashMap<String, String>,
}

impl ImageDefinitionContext {
    pub fn new() -> ImageDefinitionContext {
        ImageDefinitionContext {
            variables: HashMap::new()
        }
    }

    pub fn get_variable(&self, key: &str) -> Option<&str> {
        self.variables.get(key).map(|x| x.as_str())
    }

    pub fn add_variable(&mut self, key: &str, value: &str) -> &mut Self {
        self.variables.insert(key.to_owned(), value.to_owned());
        self
    }
}

fn extract_arguments(parts: &mut Vec<String>) -> HashMap<String, String> {
    let mut arguments = HashMap::new();
    let argument_regex = Regex::new(r"--(.+)=(.+)").unwrap();

    parts.retain(|part| {
        if let Some(capture_result) = argument_regex.captures(part) {
            arguments.insert(
                capture_result.get(1).unwrap().as_str().to_owned(),
                capture_result.get(2).unwrap().as_str().to_owned(),
            );

            false
        } else {
            true
        }
    });

    arguments
}

#[test]
fn test_extract_arguments1() {
    let mut parts = vec![
        "test/this/stuff".to_owned()
    ];

    let arguments = extract_arguments(&mut parts);

    assert_eq!(
        parts,
        vec![
            "test/this/stuff"
        ],
    );

    assert_eq!(arguments.len(), 0);
}

#[test]
fn test_extract_arguments2() {
    let mut parts = vec![
        "test/this/stuff".to_owned(),
        "--test1=troll".to_owned()
    ];

    let arguments = extract_arguments(&mut parts);

    assert_eq!(
        parts,
        vec![
            "test/this/stuff"
        ],
    );

    assert_eq!(arguments.len(), 1);
    assert_eq!(arguments.get("test1"), Some("troll".to_owned()).as_ref());
}

#[test]
fn test_extract_arguments3() {
    let mut parts = vec![
        "--test1=troll".to_owned(),
        "test/this/stuff".to_owned()
    ];

    let arguments = extract_arguments(&mut parts);

    assert_eq!(
        parts,
        vec![
            "test/this/stuff"
        ],
    );

    assert_eq!(arguments.len(), 1);
    assert_eq!(arguments.get("test1"), Some("troll".to_owned()).as_ref());
}

#[test]
fn test_extract_arguments4() {
    let mut parts = vec![
        "--test1=troll".to_owned(),
        "test/this/stuff".to_owned(),
        "test/this--troll/stuff".to_owned()
    ];

    let arguments = extract_arguments(&mut parts);

    assert_eq!(
        parts,
        vec![
            "test/this/stuff",
            "test/this--troll/stuff",
        ],
    );

    assert_eq!(arguments.len(), 1);
    assert_eq!(arguments.get("test1"), Some("troll".to_owned()).as_ref());
}

#[test]
fn test_extract_arguments5() {
    let mut parts = vec![
        "--test1=troll".to_owned(),
        "--test2=haha".to_owned(),
        "test/this/stuff".to_owned(),
        "test/this--troll/stuff".to_owned(),
    ];

    let arguments = extract_arguments(&mut parts);

    assert_eq!(
        parts,
        vec![
            "test/this/stuff",
            "test/this--troll/stuff",
        ],
    );

    assert_eq!(arguments.len(), 2);
    assert_eq!(arguments.get("test1"), Some("troll".to_owned()).as_ref());
    assert_eq!(arguments.get("test2"), Some("haha".to_owned()).as_ref());
}

#[cfg(test)]
fn image_definition_from_file(path: &str, context: &ImageDefinitionContext) -> ImageParseResult<ImageDefinition> {
    ImageDefinition::parse(&std::fs::read_to_string(path)?, context)
}

#[cfg(test)]
fn image_definition_from_file2(path: &str) -> ImageParseResult<ImageDefinition> {
    image_definition_from_file(path, &ImageDefinitionContext::new())
}

#[test]
fn test_parse_copy1() {
    let result = image_definition_from_file2("testdata/parsing/success/copy1.labarfile");
    assert!(result.is_ok(), "{}", result.unwrap_err());
    let result = result.unwrap();

    assert_eq!(1, result.layers.len());
    assert_eq!(
        result.layers[0].operations,
        vec![
            LayerOperationDefinition::File {
                path: "file1.txt".to_owned(), source_path: "testdata/rawdata/file1.txt".to_owned(),
                link_type: LinkType::Hard, writable: false
            }
        ],
    );
}

#[test]
fn test_parse_copy2() {
    let result = image_definition_from_file2("testdata/parsing/success/copy2.labarfile");
    assert!(result.is_ok(), "{}", result.unwrap_err());
    let result = result.unwrap();

    assert_eq!(1, result.layers.len());
    assert_eq!(
        result.layers[0].operations,
        vec![
            LayerOperationDefinition::File {
                path: "sub/file1.txt".to_owned(),source_path: "testdata/rawdata/file1.txt".to_owned(),
                link_type: LinkType::Hard, writable: false
            }
        ],
    );
}

#[test]
fn test_parse_copy3() {
    let result = image_definition_from_file2("testdata/parsing/success/copy3.labarfile");
    assert!(result.is_ok(), "{}", result.err().unwrap());
    let result = result.unwrap();

    let result = result.expand(Path::new(""));
    assert!(result.is_ok(), "{}", result.err().unwrap());
    let result = result.unwrap();

    assert_eq!(result.layers.len(), 1);
    assert_eq!(
        result.layers[0].operations,
        vec![
            LayerOperationDefinition::Directory { path: "dir2".to_owned() },
            LayerOperationDefinition::File {
                path: "dir2/file1.txt".to_owned(), source_path: "testdata/dir1/dir2/file1.txt".to_owned(), link_type: LinkType::Hard , writable: false
            },
            LayerOperationDefinition::File {
                path: "dir2/file2.txt".to_owned(), source_path: "testdata/dir1/dir2/file2.txt".to_owned(), link_type: LinkType::Hard, writable: false
            },
            LayerOperationDefinition::File {
                path: "file1.txt".to_owned(), source_path: "testdata/dir1/file1.txt".to_owned(), link_type: LinkType::Hard, writable: false
            },
        ],
    );
}

#[test]
fn test_parse_copy4() {
    let result = image_definition_from_file2("testdata/parsing/success/copy4.labarfile");
    assert!(result.is_ok(), "{}", result.err().unwrap());
    let result = result.unwrap();

    let result = result.expand(Path::new(""));
    assert!(result.is_ok(), "{}", result.err().unwrap());
    let result = result.unwrap();

    assert_eq!(result.layers.len(), 1);
    assert_eq!(
        result.layers[0].operations,
        vec![
            LayerOperationDefinition::Directory { path: "test/dir2".to_owned() },
            LayerOperationDefinition::File { path: "test/dir2/file1.txt".to_owned(), source_path: "testdata/dir1/dir2/file1.txt".to_owned(), link_type: LinkType::Hard, writable: false },
            LayerOperationDefinition::File { path: "test/dir2/file2.txt".to_owned(), source_path: "testdata/dir1/dir2/file2.txt".to_owned(), link_type: LinkType::Hard, writable: false },
            LayerOperationDefinition::File { path: "test/file1.txt".to_owned(), source_path: "testdata/dir1/file1.txt".to_owned(), link_type: LinkType::Hard, writable: false },
        ],
    );
}

#[test]
fn test_parse_copy5() {
    let result = image_definition_from_file2("testdata/parsing/success/copy5.labarfile");
    assert!(result.is_ok(), "{}", result.unwrap_err());
    let result = result.unwrap();

    let result = result.expand(Path::new(""));
    assert!(result.is_ok(), "{}", result.err().unwrap());
    let result = result.unwrap();

    assert_eq!(1, result.layers.len());
    assert_eq!(
        result.layers[0].operations,
        vec![
            LayerOperationDefinition::File {
                path: "sub/file1.txt".to_owned(), source_path: "testdata/rawdata/file1.txt".to_owned(),
                link_type: LinkType::Hard, writable: false
            }
        ]
    );
}


#[test]
fn test_parse_copy6() {
    let result = image_definition_from_file2("testdata/parsing/success/copy6.labarfile");
    assert!(result.is_ok(), "{}", result.unwrap_err());
    let result = result.unwrap();

    let result = result.expand(Path::new(""));
    assert!(result.is_ok(), "{}", result.err().unwrap());
    let result = result.unwrap();

    assert_eq!(1, result.layers.len());
    assert_eq!(
        result.layers[0].operations,
        vec![
            LayerOperationDefinition::File {
                path: "file1.txt".to_owned(), source_path: "testdata/rawdata/file1.txt".to_owned(),
                link_type: LinkType::Hard, writable: false
            }
        ]
    );
}

#[test]
fn test_parse_copy7() {
    let result = image_definition_from_file2("testdata/parsing/success/copy7.labarfile");
    assert!(result.is_ok(), "{}", result.unwrap_err());
    let result = result.unwrap();

    assert_eq!(1, result.layers.len());
    assert_eq!(
        result.layers[0].operations,
        vec![
            LayerOperationDefinition::File {
                path: "file1.txt".to_owned(), source_path: "testdata/rawdata/file1.txt".to_owned(),
                link_type: LinkType::Hard, writable: false
            }
        ],
    );
}

#[test]
fn test_parse_copy8() {
    let result = image_definition_from_file2("testdata/parsing/success/copy8.labarfile");
    assert!(result.is_ok(), "{}", result.unwrap_err());
    let result = result.unwrap();

    assert_eq!(1, result.layers.len());
    assert_eq!(
        result.layers[0].operations,
        vec![
            LayerOperationDefinition::File {
                path: "file1.txt".to_owned(), source_path: "testdata/rawdata/file1.txt".to_owned(),
                link_type: LinkType::Soft, writable: false
            }
        ],
    );
}

#[test]
fn test_parse_copy9() {
    let result = image_definition_from_file2("testdata/parsing/success/copy9.labarfile");
    assert!(result.is_ok(), "{}", result.unwrap_err());
    let result = result.unwrap();

    assert_eq!(1, result.layers.len());
    assert_eq!(
        result.layers[0].operations,
        vec![
            LayerOperationDefinition::File {
                path: "file1.txt".to_owned(), source_path: "testdata/rawdata/file1.txt".to_owned(),
                link_type: LinkType::Hard, writable: true
            }
        ],
    );
}

#[test]
fn test_parse_copy10() {
    let result = image_definition_from_file2("testdata/parsing/success/copy10.labarfile");
    assert!(result.is_ok(), "{}", result.unwrap_err());
    let result = result.unwrap();

    assert_eq!(1, result.layers.len());
    assert_eq!(
        result.layers[0].operations,
        vec![
            LayerOperationDefinition::File {
                path: "file 1.txt".to_owned(), source_path: "testdata/rawdata/file1.txt".to_owned(),
                link_type: LinkType::Hard, writable: false
            }
        ],
    );
}

#[test]
fn test_parse_mkdir1() {
    let result = image_definition_from_file2("testdata/parsing/success/mkdir1.labarfile");
    assert!(result.is_ok(), "{}", result.unwrap_err());
    let result = result.unwrap();

    assert_eq!(result.layers.len(), 1);
    assert_eq!(
        result.layers[0].operations,
        vec![LayerOperationDefinition::Directory { path: "sub1".to_owned() }],
    );
}

#[test]
fn test_parse_mkdir2() {
    let result = image_definition_from_file2("testdata/parsing/success/mkdir2.labarfile");
    assert!(result.is_ok(), "{}", result.unwrap_err());
    let result = result.unwrap();

    assert_eq!(result.layers.len(), 1);
    assert_eq!(
        result.layers[0].operations,
        vec![LayerOperationDefinition::Directory { path: "sub1/sub2".to_owned() }],
    );
}

#[test]
fn test_parse_image1() {
    let result = image_definition_from_file2("testdata/parsing/success/image1.labarfile");
    assert!(result.is_ok(), "{}", result.unwrap_err());
    let result = result.unwrap();

    assert_eq!(result.layers.len(), 1);
    assert_eq!(
        result.layers[0].operations,
        vec![LayerOperationDefinition::Image { reference: Reference::from_str("test:this").unwrap() }],
    );
}

#[test]
fn test_parse_from1() {
    let result = image_definition_from_file2("testdata/parsing/success/from1.labarfile");
    assert!(result.is_ok(), "{}", result.unwrap_err());
    let result = result.unwrap();

    assert_eq!(result.layers.len(), 0);
    assert_eq!(result.base_image, Some(Reference::from_str("test:this").unwrap()));
}

#[test]
fn test_parse_multi1() {
    let result = image_definition_from_file2("testdata/parsing/success/multi1.labarfile");
    assert!(result.is_ok(), "{}", result.unwrap_err());
    let result = result.unwrap();

    assert_eq!(result.layers.len(), 3);
    assert_eq!(
        result.layers[0].operations,
        vec![LayerOperationDefinition::Directory { path: "sub1/sub2".to_owned() }]
    );

    assert_eq!(
        result.layers[1].operations,
        vec![
            LayerOperationDefinition::File {
                path: "file1.txt".to_owned(), source_path: "testdata/rawdata/file1.txt".to_owned(),
                link_type: LinkType::Hard, writable: false
            }
        ]
    );

    assert_eq!(
        result.layers[2].operations,
        vec![LayerOperationDefinition::Image { reference: Reference::from_str("test:this").unwrap() }]
    );
}

#[test]
fn test_parse_multi2() {
    let result = image_definition_from_file2("testdata/parsing/success/multi2.labarfile");
    assert!(result.is_ok(), "{}", result.unwrap_err());
    let result = result.unwrap();

    assert_eq!(result.base_image, Some(Reference::from_str("test:this").unwrap()));

    assert_eq!(result.layers.len(), 3);
    assert_eq!(
        result.layers[0].operations,
        vec![LayerOperationDefinition::Directory { path: "sub1/sub2".to_owned() }]
    );

    assert_eq!(
        result.layers[1].operations,
        vec![
            LayerOperationDefinition::File {
                path: "file1.txt".to_owned(), source_path: "testdata/rawdata/file1.txt".to_owned(),
                link_type: LinkType::Hard, writable: false
            }
        ]
    );

    assert_eq!(
        result.layers[2].operations,
        vec![LayerOperationDefinition::Image { reference: Reference::from_str("test:that").unwrap() }]
    );
}

#[test]
fn test_parse_variables1() {
    let result = image_definition_from_file(
        "testdata/parsing/success/variables1.labarfile",
        ImageDefinitionContext::new().add_variable("INPUT_FOLDER", "testdata/rawdata")
    );
    assert!(result.is_ok(), "{}", result.unwrap_err());
    let result = result.unwrap();

    assert_eq!(1, result.layers.len());
    assert_eq!(
        result.layers[0].operations,
        vec![
            LayerOperationDefinition::File {
                path: "file1.txt".to_owned(), source_path: "testdata/rawdata/file1.txt".to_owned(),
                link_type: LinkType::Hard, writable: false
            }
        ],
    );
}

#[test]
fn test_parse_variables2() {
    let result = image_definition_from_file(
        "testdata/parsing/success/variables2.labarfile",
        ImageDefinitionContext::new().add_variable("INPUT_FILE", "testdata/rawdata/file1.txt")
    );
    assert!(result.is_ok(), "{}", result.unwrap_err());
    let result = result.unwrap();

    assert_eq!(1, result.layers.len());
    assert_eq!(
        result.layers[0].operations,
        vec![
            LayerOperationDefinition::File {
                path: "file1.txt".to_owned(), source_path: "testdata/rawdata/file1.txt".to_owned(),
                link_type: LinkType::Hard, writable: false
            }
        ],
    );
}

#[test]
fn test_parse_variables3() {
    let result = image_definition_from_file(
        "testdata/parsing/success/variables3.labarfile",
        ImageDefinitionContext::new().add_variable("INPUT_FOLDER", "testdata/rawdata/")
    );
    assert!(result.is_ok(), "{}", result.unwrap_err());
    let result = result.unwrap();

    assert_eq!(1, result.layers.len());
    assert_eq!(
        result.layers[0].operations,
        vec![
            LayerOperationDefinition::File {
                path: "file1.txt".to_owned(), source_path: "testdata/rawdata/file1.txt".to_owned(),
                link_type: LinkType::Hard, writable: false
            }
        ],
    );
}

#[test]
fn test_parse_variables4() {
    let result = image_definition_from_file(
        "testdata/parsing/success/variables4.labarfile",
        ImageDefinitionContext::new()
            .add_variable("INPUT_FILE", "testdata/rawdata/file1.txt")
            .add_variable("OUTPUT_FILE", "file_1.txt")
    );
    assert!(result.is_ok(), "{}", result.unwrap_err());
    let result = result.unwrap();

    assert_eq!(1, result.layers.len());
    assert_eq!(
        result.layers[0].operations,
        vec![
            LayerOperationDefinition::File {
                path: "file_1.txt".to_owned(), source_path: "testdata/rawdata/file1.txt".to_owned(),
                link_type: LinkType::Hard, writable: false
            }
        ],
    );
}

#[test]
fn test_parse_image5() {
    let result = image_definition_from_file(
        "testdata/parsing/success/variables5.labarfile",
        ImageDefinitionContext::new()
            .add_variable("IMAGE", "test:this")
    );
    assert!(result.is_ok(), "{}", result.unwrap_err());
    let result = result.unwrap();

    assert_eq!(result.layers.len(), 1);
    assert_eq!(
        result.layers[0].operations,
        vec![LayerOperationDefinition::Image { reference: Reference::from_str("test:this").unwrap() }],
    );
}

#[test]
fn test_parse_sublayer1() {
    let result = image_definition_from_file2("testdata/parsing/success/sublayer1.labarfile");
    assert!(result.is_ok(), "{}", result.unwrap_err());
    let result = result.unwrap();

    assert_eq!(1, result.layers.len());
    assert_eq!(
        result.layers[0].operations,
        vec![
            LayerOperationDefinition::File {
                path: "file1.txt".to_owned(), source_path: "testdata/rawdata/file1.txt".to_owned(),
                link_type: LinkType::Hard, writable: false
            },
            LayerOperationDefinition::File {
                path: "file2.txt".to_owned(), source_path: "testdata/rawdata/file2.txt".to_owned(),
                link_type: LinkType::Hard, writable: false
            }
        ],
    );
}

#[test]
fn test_parse_sublayer2() {
    let result = image_definition_from_file2("testdata/parsing/success/sublayer2.labarfile");
    assert!(result.is_ok(), "{}", result.unwrap_err());
    let result = result.unwrap();

    assert_eq!(2, result.layers.len());
    assert_eq!(
        result.layers[0].operations,
        vec![
            LayerOperationDefinition::File {
                path: "file1.txt".to_owned(), source_path: "testdata/rawdata/file1.txt".to_owned(),
                link_type: LinkType::Hard, writable: false
            },
            LayerOperationDefinition::File {
                path: "file2.txt".to_owned(), source_path: "testdata/rawdata/file2.txt".to_owned(),
                link_type: LinkType::Hard, writable: false
            }
        ],
    );

    assert_eq!(
        result.layers[1].operations,
        vec![
            LayerOperationDefinition::Directory {
                path: "test".to_string()
            }
        ],
    );
}

#[test]
fn test_failed_parse_mkdir1() {
    let result = image_definition_from_file2("testdata/parsing/failed/mkdir1.labarfile");
    assert!(result.is_err());
}

#[test]
fn test_failed_parse_from1() {
    let result = image_definition_from_file2("testdata/parsing/failed/from1.labarfile");
    assert!(result.is_err());
}

#[test]
fn test_failed_parse_variables1() {
    let result = image_definition_from_file2("testdata/parsing/failed/variables1.labarfile");
    assert!(result.is_err());
}

#[test]
fn test_failed_parse_sublayer1() {
    let result = image_definition_from_file2("testdata/parsing/failed/sublayer1.labarfile");
    assert!(result.is_err());
}

#[test]
fn test_failed_parse_sublayer2() {
    let result = image_definition_from_file2("testdata/parsing/failed/sublayer2.labarfile");
    assert!(result.is_err());
}
