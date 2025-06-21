use std::path::Path;
use std::collections::HashMap;

use regex::Regex;

use crate::image::LinkType;

pub type ImageParseResult<T> = Result<T, String>;

#[derive(Debug, Eq, PartialEq)]
pub struct ImageDefinition {
    pub base_image: Option<String>,
    pub layers: Vec<LayerDefinition>
}

impl ImageDefinition {
    pub fn new(base_image: Option<String>, layers: Vec<LayerDefinition>) -> ImageDefinition {
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

#[derive(Debug, Eq, PartialEq)]
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

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum LayerOperationDefinition {
    Image { reference: String },
    Directory { path: String },
    File { path: String, source_path: String, link_type: LinkType },
}

fn recursive_copy_operations(source_path: &Path, base_destination_path: &Path, link_type: LinkType) -> ImageParseResult<Vec<LayerOperationDefinition>> {
    let mut stack = Vec::new();
    stack.push(source_path.to_owned());

    let mut results = Vec::new();

    while let Some(current) = stack.pop() {
        let mut read_dir = std::fs::read_dir(current).map_err(|err| format!("{}", err))?;
        while let Some(entry) = read_dir.next() {
            let entry = entry.map_err(|err| format!("{}", err))?;

            let entry_path = entry.path();
            let relative_entry_path = entry_path.strip_prefix(source_path).map_err(|err| format!("{}", err))?;

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
                    link_type
                });
            }
        }
    }

    results.sort();

    Ok(results)
}

fn expand_operations(build_context: &Path, operations: Vec<LayerOperationDefinition>) -> ImageParseResult<Vec<LayerOperationDefinition>> {
    let mut expanded_operations = Vec::new();

    for operation_definition in operations{
        match operation_definition {
            LayerOperationDefinition::Image { reference } => {
                expanded_operations.push(LayerOperationDefinition::Image { reference });
            }
            LayerOperationDefinition::File { path, source_path, link_type } => {
                let source_path_obj = build_context.join(Path::new(&source_path));
                let destination_path = Path::new(&path);

                if source_path_obj.is_file() {
                    if path.chars().last() == Some('/') {
                        expanded_operations.push(
                            LayerOperationDefinition::File {
                                path: destination_path.join(source_path_obj.file_name().unwrap()).to_str().unwrap().to_owned(),
                                source_path: source_path_obj.to_str().unwrap().to_owned(),
                                link_type
                            }
                        );
                    } else if path == "." {
                        expanded_operations.push(
                            LayerOperationDefinition::File {
                                path: source_path_obj.file_name().unwrap().to_str().unwrap().to_owned(),
                                source_path: source_path_obj.to_str().unwrap().to_owned(),
                                link_type
                            }
                        );
                    } else {
                        expanded_operations.push(
                            LayerOperationDefinition::File {
                                path,
                                source_path: source_path_obj.to_str().unwrap().to_owned(),
                                link_type
                            }
                        );
                    }
                } else {
                    expanded_operations.append(&mut recursive_copy_operations(
                        &source_path_obj,
                        destination_path,
                        link_type
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

fn extract_arguments(parts: &mut Vec<&str>) -> HashMap<String, String> {
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
        "test/this/stuff"
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
        "test/this/stuff",
        "--test1=troll"
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
        "--test1=troll",
        "test/this/stuff",
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
        "--test1=troll",
        "test/this/stuff",
        "test/this--troll/stuff",
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
        "--test1=troll",
        "--test2=haha",
        "test/this/stuff",
        "test/this--troll/stuff",
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

impl ImageDefinition {
    pub fn from_str(content: &str) -> ImageParseResult<ImageDefinition> {
        let mut image_definition = ImageDefinition::new(None, Vec::new());

        let mut is_first_line = true;
        for line in content.lines() {
            if line.trim_start().starts_with("#") {
                continue;
            }

            let mut parts = line.split_whitespace().collect::<Vec<_>>();
            if parts.len() >= 1 {
                let command = parts[0];
                match command {
                    "FROM" => {
                        if is_first_line {
                            image_definition.base_image = Some(parts[1].to_owned());
                        } else {
                            return Err("FROM statement only allowed on the first line.".to_owned());
                        }
                    },
                    "COPY" => {
                        let arguments = extract_arguments(&mut parts);

                        let source = parts[1].to_owned();
                        let destination = parts[2].to_owned();
                        let link_type = match arguments.get("link").map(|x| x.as_str()) {
                            Some("soft") => LinkType::Soft,
                            Some("hard") => LinkType::Hard,
                            _ => LinkType::Soft
                        };

                        image_definition.layers.push(LayerDefinition::new(
                            line.to_owned(),
                            vec![
                                LayerOperationDefinition::File {
                                    path: destination,
                                    source_path: source,
                                    link_type
                                }
                            ]
                        ));
                    },
                    "MKDIR" => {
                        let path = parts[1].to_owned();
                        image_definition.layers.push(LayerDefinition::new(
                            line.to_owned(),
                            vec![LayerOperationDefinition::Directory { path }]
                        ));
                    },
                    "IMAGE" => {
                        let reference = parts[1].to_owned();
                        image_definition.layers.push(LayerDefinition::new(
                            line.to_owned(),
                            vec![LayerOperationDefinition::Image { reference }]
                        ));
                    }
                    _ => { return Err(format!("'{}' is not a defined command", command)); }
                }
            } else {
                return Err(format!("Invalid line: {}", line));
            }

            is_first_line = false;
        }

        Ok(image_definition)
    }
}

#[test]
fn test_parse_copy1() {
    let result = ImageDefinition::from_str(&std::fs::read_to_string("testdata/parsing/copy1.labarfile").unwrap());
    assert!(result.is_ok());
    let result = result.unwrap();

    assert_eq!(1, result.layers.len());
    assert_eq!(
        result.layers[0].operations,
        vec![LayerOperationDefinition::File { path: "file1.txt".to_owned(), source_path: "testdata/rawdata/file1.txt".to_owned(), link_type: LinkType::Soft }],
    );
}

#[test]
fn test_parse_copy2() {
    let result = ImageDefinition::from_str(&std::fs::read_to_string("testdata/parsing/copy2.labarfile").unwrap());
    assert!(result.is_ok());
    let result = result.unwrap();

    assert_eq!(1, result.layers.len());
    assert_eq!(
        result.layers[0].operations,
        vec![LayerOperationDefinition::File { path: "sub/file1.txt".to_owned(), source_path: "testdata/rawdata/file1.txt".to_owned(), link_type: LinkType::Soft  }],
    );
}

#[test]
fn test_parse_copy3() {
    let result = ImageDefinition::from_str(&std::fs::read_to_string("testdata/parsing/copy3.labarfile").unwrap());
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
            LayerOperationDefinition::File { path: "dir2/file1.txt".to_owned(), source_path: "testdata/dir1/dir2/file1.txt".to_owned(), link_type: LinkType::Soft },
            LayerOperationDefinition::File { path: "dir2/file2.txt".to_owned(), source_path: "testdata/dir1/dir2/file2.txt".to_owned(), link_type: LinkType::Soft },
            LayerOperationDefinition::File { path: "file1.txt".to_owned(), source_path: "testdata/dir1/file1.txt".to_owned(), link_type: LinkType::Soft },
        ],
    );
}

#[test]
fn test_parse_copy4() {
    let result = ImageDefinition::from_str(&std::fs::read_to_string("testdata/parsing/copy4.labarfile").unwrap());
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
            LayerOperationDefinition::File { path: "test/dir2/file1.txt".to_owned(), source_path: "testdata/dir1/dir2/file1.txt".to_owned(), link_type: LinkType::Soft },
            LayerOperationDefinition::File { path: "test/dir2/file2.txt".to_owned(), source_path: "testdata/dir1/dir2/file2.txt".to_owned(), link_type: LinkType::Soft },
            LayerOperationDefinition::File { path: "test/file1.txt".to_owned(), source_path: "testdata/dir1/file1.txt".to_owned(), link_type: LinkType::Soft },
        ],
    );
}

#[test]
fn test_parse_copy5() {
    let result = ImageDefinition::from_str(&std::fs::read_to_string("testdata/parsing/copy5.labarfile").unwrap());
    assert!(result.is_ok());
    let result = result.unwrap();

    let result = result.expand(Path::new(""));
    assert!(result.is_ok(), "{}", result.err().unwrap());
    let result = result.unwrap();

    assert_eq!(1, result.layers.len());
    assert_eq!(
        result.layers[0].operations,
        vec![LayerOperationDefinition::File { path: "sub/file1.txt".to_owned(), source_path: "testdata/rawdata/file1.txt".to_owned(), link_type: LinkType::Soft  }]
    );
}


#[test]
fn test_parse_copy6() {
    let result = ImageDefinition::from_str(&std::fs::read_to_string("testdata/parsing/copy6.labarfile").unwrap());
    assert!(result.is_ok());
    let result = result.unwrap();

    let result = result.expand(Path::new(""));
    assert!(result.is_ok(), "{}", result.err().unwrap());
    let result = result.unwrap();

    assert_eq!(1, result.layers.len());
    assert_eq!(
        result.layers[0].operations,
        vec![LayerOperationDefinition::File { path: "file1.txt".to_owned(), source_path: "testdata/rawdata/file1.txt".to_owned(), link_type: LinkType::Soft }]
    );
}

#[test]
fn test_parse_copy7() {
    let result = ImageDefinition::from_str(&std::fs::read_to_string("testdata/parsing/copy7.labarfile").unwrap());
    assert!(result.is_ok());
    let result = result.unwrap();

    assert_eq!(1, result.layers.len());
    assert_eq!(
        result.layers[0].operations,
        vec![LayerOperationDefinition::File { path: "file1.txt".to_owned(), source_path: "testdata/rawdata/file1.txt".to_owned(), link_type: LinkType::Hard }],
    );
}

#[test]
fn test_parse_copy8() {
    let result = ImageDefinition::from_str(&std::fs::read_to_string("testdata/parsing/copy8.labarfile").unwrap());
    assert!(result.is_ok());
    let result = result.unwrap();

    assert_eq!(1, result.layers.len());
    assert_eq!(
        result.layers[0].operations,
        vec![LayerOperationDefinition::File { path: "file1.txt".to_owned(), source_path: "testdata/rawdata/file1.txt".to_owned(), link_type: LinkType::Soft }],
    );
}

#[test]
fn test_parse_mkdir1() {
    let result = ImageDefinition::from_str(&std::fs::read_to_string("testdata/parsing/mkdir1.labarfile").unwrap());
    assert!(result.is_ok());
    let result = result.unwrap();

    assert_eq!(result.layers.len(), 1);
    assert_eq!(
        result.layers[0].operations,
        vec![LayerOperationDefinition::Directory { path: "sub1".to_owned() }],
    );
}

#[test]
fn test_parse_mkdir2() {
    let result = ImageDefinition::from_str(&std::fs::read_to_string("testdata/parsing/mkdir2.labarfile").unwrap());
    assert!(result.is_ok());
    let result = result.unwrap();

    assert_eq!(result.layers.len(), 1);
    assert_eq!(
        result.layers[0].operations,
        vec![LayerOperationDefinition::Directory { path: "sub1/sub2".to_owned() }],
    );
}

#[test]
fn test_parse_image1() {
    let result = ImageDefinition::from_str(&std::fs::read_to_string("testdata/parsing/image1.labarfile").unwrap());
    assert!(result.is_ok());
    let result = result.unwrap();

    assert_eq!(result.layers.len(), 1);
    assert_eq!(
        result.layers[0].operations,
        vec![LayerOperationDefinition::Image { reference: "test:this".to_owned() }],
    );
}

#[test]
fn test_parse_from1() {
    let result = ImageDefinition::from_str(&std::fs::read_to_string("testdata/parsing/from1.labarfile").unwrap());
    assert!(result.is_ok());
    let result = result.unwrap();

    assert_eq!(result.layers.len(), 0);
    assert_eq!(result.base_image, Some("test:this".to_owned()));
}

#[test]
fn test_parse_from2() {
    let result = ImageDefinition::from_str(&std::fs::read_to_string("testdata/parsing/from2.labarfile").unwrap());
    assert!(result.is_err());
}

#[test]
fn test_parse_multi1() {
    let result = ImageDefinition::from_str(&std::fs::read_to_string("testdata/parsing/multi1.labarfile").unwrap());
    assert!(result.is_ok());
    let result = result.unwrap();

    assert_eq!(result.layers.len(), 3);
    assert_eq!(
        result.layers[0].operations,
        vec![LayerOperationDefinition::Directory { path: "sub1/sub2".to_owned() }]
    );

    assert_eq!(
        result.layers[1].operations,
        vec![LayerOperationDefinition::File { path: "file1.txt".to_owned(), source_path: "testdata/rawdata/file1.txt".to_owned(), link_type: LinkType::Soft  }]
    );

    assert_eq!(
        result.layers[2].operations,
        vec![LayerOperationDefinition::Image { reference: "test:this".to_owned() }]
    );
}

#[test]
fn test_parse_multi2() {
    let result = ImageDefinition::from_str(&std::fs::read_to_string("testdata/parsing/multi2.labarfile").unwrap());
    assert!(result.is_ok());
    let result = result.unwrap();

    assert_eq!(result.base_image, Some("test:this".to_owned()));

    assert_eq!(result.layers.len(), 3);
    assert_eq!(
        result.layers[0].operations,
        vec![LayerOperationDefinition::Directory { path: "sub1/sub2".to_owned() }]
    );

    assert_eq!(
        result.layers[1].operations,
        vec![LayerOperationDefinition::File { path: "file1.txt".to_owned(), source_path: "testdata/rawdata/file1.txt".to_owned(), link_type: LinkType::Soft  }]
    );

    assert_eq!(
        result.layers[2].operations,
        vec![LayerOperationDefinition::Image { reference: "test:that".to_owned() }]
    );
}