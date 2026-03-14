use std::path::{Path};

use crate::image::LinkType;
use crate::image_parser::{ImageParserContext, ImageParseError, ImageParser};
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
    Label { key_values: Vec<(String, String)> }
}

impl ImageDefinition {
    pub fn parse(content: &str, context: &ImageParserContext) -> ImageParseResult<ImageDefinition> {
        ImageParser::new(context).parse(content)
    }

    pub fn parse_without_context(content: &str) -> ImageParseResult<ImageDefinition> {
        ImageDefinition::parse(content, &ImageParserContext::new())
    }

    pub fn parse_file(path: &Path, context: &ImageParserContext) -> ImageParseResult<ImageDefinition> {
        let content = std::fs::read_to_string(path).map_err(|err| ImageParseError::DefinitionFileNotFound(err))?;
        ImageDefinition::parse(&content, context)
    }

    pub fn parse_file_without_context(path: &Path) -> ImageParseResult<ImageDefinition> {
        ImageDefinition::parse_file(path, &ImageParserContext::new())
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
                    input_line: format!("directory: {}", current_directory.display()),
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
                    input_line: format!("root file: {}", file_relative.display()),
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
            LayerOperationDefinition::Label { key_values } => {
                expanded_operations.push(LayerOperationDefinition::Label { key_values })
            }
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

#[cfg(test)]
fn image_definition_from_file(path: &str, context: &ImageParserContext) -> ImageParseResult<ImageDefinition> {
    ImageDefinition::parse(&std::fs::read_to_string(path)?, context)
}

#[cfg(test)]
fn image_definition_from_file2(path: &str) -> ImageParseResult<ImageDefinition> {
    image_definition_from_file(path, &ImageParserContext::new())
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
    use std::str::FromStr;

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
    use std::str::FromStr;

    let result = image_definition_from_file2("testdata/parsing/success/from1.labarfile");
    assert!(result.is_ok(), "{}", result.unwrap_err());
    let result = result.unwrap();

    assert_eq!(result.layers.len(), 0);
    assert_eq!(result.base_image, Some(Reference::from_str("test:this").unwrap()));
}

#[test]
fn test_parse_multi1() {
    use std::str::FromStr;

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
    use std::str::FromStr;

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
        ImageParserContext::new().add_variable("INPUT_FOLDER", "testdata/rawdata")
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
        ImageParserContext::new().add_variable("INPUT_FILE", "testdata/rawdata/file1.txt")
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
        ImageParserContext::new().add_variable("INPUT_FOLDER", "testdata/rawdata/")
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
        ImageParserContext::new()
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
    use std::str::FromStr;

    let result = image_definition_from_file(
        "testdata/parsing/success/variables5.labarfile",
        ImageParserContext::new()
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
fn test_parse_label1() {
    let result = image_definition_from_file2("testdata/parsing/success/label1.labarfile");
    assert!(result.is_ok(), "{}", result.unwrap_err());
    let result = result.unwrap();

    assert_eq!(2, result.layers.len());
    assert_eq!(
        result.layers[0].operations,
        vec![
            LayerOperationDefinition::File {
                path: "file1.txt".to_owned(), source_path: "testdata/rawdata/file1.txt".to_owned(),
                link_type: LinkType::Hard, writable: false
            }
        ],
    );

    assert_eq!(
        result.layers[1].operations,
        vec![
            LayerOperationDefinition::Label {
                key_values: vec![
                    ("version".to_owned(), "1.2.3".to_owned())
                ]
            }
        ],
    );
}

#[test]
fn test_parse_label2() {
    let result = image_definition_from_file2("testdata/parsing/success/label2.labarfile");
    assert!(result.is_ok(), "{}", result.unwrap_err());
    let result = result.unwrap();

    assert_eq!(2, result.layers.len());
    assert_eq!(
        result.layers[0].operations,
        vec![
            LayerOperationDefinition::File {
                path: "file1.txt".to_owned(), source_path: "testdata/rawdata/file1.txt".to_owned(),
                link_type: LinkType::Hard, writable: false
            }
        ],
    );

    assert_eq!(
        result.layers[1].operations,
        vec![
            LayerOperationDefinition::Label {
                key_values: vec![
                    ("owner".to_owned(), "me".to_owned()),
                    ("version".to_owned(), "1.2.3".to_owned()),
                ]
            }
        ],
    );
}

#[test]
fn test_parse_label3() {
    let result = image_definition_from_file(
        "testdata/parsing/success/label3.labarfile",
        ImageParserContext::new()
            .add_variable("VERSION", "4.5.6")
    );
    assert!(result.is_ok(), "{}", result.unwrap_err());
    let result = result.unwrap();

    assert_eq!(2, result.layers.len());
    assert_eq!(
        result.layers[0].operations,
        vec![
            LayerOperationDefinition::File {
                path: "file1.txt".to_owned(), source_path: "testdata/rawdata/file1.txt".to_owned(),
                link_type: LinkType::Hard, writable: false
            }
        ],
    );

    assert_eq!(
        result.layers[1].operations,
        vec![
            LayerOperationDefinition::Label {
                key_values: vec![
                    ("version".to_owned(), "4.5.6".to_owned())
                ]
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
