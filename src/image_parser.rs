use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::path::StripPrefixError;
use std::str::FromStr;

use regex::Regex;

use crate::helpers::split_parts;
use crate::image::LinkType;
use crate::image_definition::{ImageDefinition, ImageParseResult, LayerDefinition, LayerOperationDefinition};
use crate::image_manager::Reference;

#[derive(Debug)]
pub enum ImageParseError {
    DefinitionFileNotFound(std::io::Error),
    InvalidLine(String),
    UndefinedCommand(String),
    FromOnlyOnFirst,
    ExpectedArguments { expected: usize, actual: usize },
    ExpectedSubcommand,
    NotWithinSubLayer,
    AlreadyWithinSubLayer,
    SubLayerNotEnded,
    InvalidSubcommand(String),
    VariableNotFound(String),
    InvalidImageReference(String),
    IsAbsolutePath(String),
    IO(std::io::Error),
    StripPrefix(StripPrefixError),
    ExpectedKeyValue(String),
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
            ImageParseError::ExpectedSubcommand => write!(f, "Expected subcommand"),
            ImageParseError::InvalidSubcommand(subcommand) => write!(f, "'{}' is not a valid subcommand", subcommand),
            ImageParseError::NotWithinSubLayer => write!(f, "Not within a sublayer"),
            ImageParseError::AlreadyWithinSubLayer => write!(f, "Already within a sublayer"),
            ImageParseError::SubLayerNotEnded => write!(f, "Sublayer not terminated with END"),
            ImageParseError::VariableNotFound(name) => write!(f, "Variable '{}' not found", name),
            ImageParseError::InvalidImageReference(error) => write!(f, "Invalid image reference: {}", error),
            ImageParseError::IsAbsolutePath(path) => write!(f, "The path '{}' is absolute", path),
            ImageParseError::StripPrefix(error) => write!(f, "Failed to strip prefix due to: {}", error),
            ImageParseError::ExpectedKeyValue(argument) => write!(f, "Expected key=value but got: {}", argument),
            ImageParseError::IO(error) => write!(f, "IO error: {}", error),
            ImageParseError::Other(error) => write!(f, "{}", error),
        }
    }
}

pub struct ImageParser<'a> {
    context: &'a ImageParserContext,

    image_definition: ImageDefinition,

    variable_regex: Vec<Regex>,
    label_regex: Regex,

    is_first_line: bool,
    active_layer: Option<LayerDefinition>,
    within_layer: bool
}

impl<'a> ImageParser<'a> {
    pub fn new(context: &'a ImageParserContext) -> ImageParser<'a> {
        ImageParser {
            context,

            image_definition: ImageDefinition::new(None, Vec::new()),

            variable_regex: vec![
                Regex::new("\\$([A-Za-z0-9_]+)").unwrap(),
                Regex::new("\\$\\{([A-Za-z0-9_]+)}").unwrap()
            ],
            label_regex: Regex::new("(.*)\\s*=\\s*(.*)\\s?").unwrap(),

            is_first_line: true,
            active_layer: None,
            within_layer: false
        }
    }

    pub fn parse(mut self, content: &str) -> ImageParseResult<ImageDefinition> {
        for line in content.lines() {
            self.parse_line(line)?;
        }
        self.finish()?;

        Ok(self.image_definition)
    }

    fn parse_line(&mut self, line: &str) -> ImageParseResult<()> {
        if line.trim_start().starts_with("#") {
            return Ok(());
        }

        let mut parts = split_parts(&line);
        if parts.len() >= 1 {
            self.evaluate_variables(&mut parts)?;

            let command = parts[0].as_str();
            let num_arguments = parts.len() - 1;

            match command {
                "FROM" => {
                    self.parse_from(&mut parts, num_arguments)?;
                },
                "COPY" => {
                    self.parse_copy(line, &mut parts, num_arguments)?;
                },
                "MKDIR" => {
                    self.parse_mkdir(line, &mut parts, num_arguments)?;
                },
                "IMAGE" => {
                    self.parse_image_ref(line, &mut parts, num_arguments)?;
                }
                "BEGIN" => {
                    self.parse_begin_layer(line, &mut parts, num_arguments)?;
                }
                "END" => {
                    self.end_layer()?;
                }
                "LABEL" => {
                    self.parse_label(line, &mut parts)?;
                }
                _ => {
                    return Err(ImageParseError::UndefinedCommand(command.to_owned()));
                }
            }
        } else if !line.trim().is_empty() {
            return Err(ImageParseError::InvalidLine(line.to_owned()));
        }

        self.is_first_line = false;

        Ok(())
    }

    fn evaluate_variables(&self, parts: &mut Vec<String>) -> ImageParseResult<()> {
        for part in parts.iter_mut().skip(1) {
            for regex in &self.variable_regex {
                while let Some(regex_capture) = regex.captures(part) {
                    let group = regex_capture.get(1).unwrap();
                    let variable = group.as_str();
                    let variable_value = self.context.get_variable(variable).ok_or_else(|| ImageParseError::VariableNotFound(variable.to_owned()))?;
                    part.replace_range(regex_capture.get(0).unwrap().range(), variable_value);
                }
            }
        }

        Ok(())
    }

    fn parse_label(&mut self, line: &str, parts: &mut Vec<String>) -> ImageParseResult<()> {
        let mut key_values = Vec::new();
        for argument in parts.iter().skip(1) {
            for capture in self.label_regex.captures_iter(argument) {
                let key = capture.get(1).unwrap().as_str();
                let value = capture.get(2).unwrap().as_str();
                key_values.push((key.to_owned(), value.to_owned()));
            }
        }

        if !key_values.is_empty() {
            self.add_operation(
                line,
                LayerOperationDefinition::Label { key_values }
            );

            Ok(())
        } else {
            Err(ImageParseError::ExpectedKeyValue(line.to_owned()))
        }
    }

    fn parse_begin_layer(&mut self, line: &str, parts: &mut Vec<String>, num_arguments: usize) -> ImageParseResult<()> {
        if num_arguments != 1 {
            return Err(ImageParseError::ExpectedSubcommand)
        }

        let subcommand = &parts[1];
        match subcommand.as_ref() {
            "LAYER" => {
                self.new_layer(line)
            }
            _ => {
                Err(ImageParseError::InvalidSubcommand(subcommand.clone()))
            }
        }
    }

    fn parse_image_ref(&mut self, line: &str, parts: &mut Vec<String>, num_arguments: usize) -> ImageParseResult<()>  {
        let reference = Reference::from_str(&parts[1].to_owned()).map_err(|err| ImageParseError::InvalidImageReference(err))?;

        if num_arguments != 1 {
            return Err(ImageParseError::ExpectedArguments { expected: 1, actual: num_arguments });
        }

        self.add_operation(
            line,
            LayerOperationDefinition::Image { reference }
        );

        Ok(())
    }

    fn parse_mkdir(&mut self, line: &str, parts: &mut Vec<String>, num_arguments: usize) -> ImageParseResult<()> {
        if num_arguments != 1 {
            return Err(ImageParseError::ExpectedArguments { expected: 1, actual: num_arguments });
        }

        let path = parts[1].to_owned();
        self.add_operation(
            line,
            LayerOperationDefinition::Directory { path }
        );

        Ok(())
    }

    fn parse_copy(&mut self, line: &str, mut parts: &mut Vec<String>, num_arguments: usize) -> ImageParseResult<()> {
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

        self.add_operation(
            line,
            LayerOperationDefinition::File {
                path: destination,
                source_path: source,
                link_type,
                writable
            }
        );

        Ok(())
    }

    fn parse_from(&mut self, parts: &mut Vec<String>, num_arguments: usize) -> ImageParseResult<()> {
        if num_arguments != 1 {
            return Err(ImageParseError::ExpectedArguments { expected: 1, actual: num_arguments });
        }

        if self.is_first_line {
            let base_image = Reference::from_str(&parts[1].to_owned()).map_err(|err| ImageParseError::InvalidImageReference(err))?;
            self.image_definition.base_image = Some(base_image);
            Ok(())
        } else {
            Err(ImageParseError::FromOnlyOnFirst)
        }
    }

    fn new_layer(&mut self, line: &str) -> ImageParseResult<()> {
        if self.within_layer {
            return Err(ImageParseError::AlreadyWithinSubLayer);
        }

        self.active_layer = Some(LayerDefinition::new(line.to_owned(), Vec::new()));
        self.within_layer = true;

        Ok(())
    }

    fn end_layer(&mut self) -> ImageParseResult<()> {
        if self.within_layer {
            if let Some(layer) = self.active_layer.take() {
                self.image_definition.layers.push(layer);
            }

            self.within_layer = false;
            Ok(())
        } else {
            Err(ImageParseError::NotWithinSubLayer)
        }
    }

    fn add_operation(&mut self,
                     line: &str,
                     operation: LayerOperationDefinition) {
        if !self.within_layer {
            self.active_layer = Some(LayerDefinition::new(line.to_owned(), Vec::new()));
        }

        if let Some(layer) = self.active_layer.as_mut() {
            layer.operations.push(operation);
        }

        if !self.within_layer {
            if let Some(layer) = self.active_layer.take() {
                self.image_definition.layers.push(layer);
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

pub struct ImageParserContext {
    variables: HashMap<String, String>,
}

impl ImageParserContext {
    pub fn new() -> ImageParserContext {
        ImageParserContext {
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