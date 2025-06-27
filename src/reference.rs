use std::fmt::{Display, Formatter};
use std::str::FromStr;
use regex::Regex;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde::de::{Error, Visitor};

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Reference {
    ImageTag(ImageTag),
    ImageId(ImageId)
}

impl FromStr for Reference {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        if let Ok(image_id) = ImageId::from_str(text) {
            return Ok(Reference::ImageId(image_id));
        }

        if let Ok(image_tag) = ImageTag::from_str(text) {
            return Ok(Reference::ImageTag(image_tag));
        }

        Err("Expected tag or image id".to_owned())
    }
}

impl Display for Reference {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Reference::ImageTag(tag) => write!(f, "{}", tag),
            Reference::ImageId(id) => write!(f, "{}", id)
        }
    }
}

impl Reference {
    pub fn image_tag(&self) -> Option<&ImageTag> {
        if let Reference::ImageTag(tag) = self {
            Some(tag)
        } else {
            None
        }
    }

    pub fn image_id(&self) -> Option<&ImageId> {
        if let Reference::ImageId(id) = self {
            Some(id)
        } else {
            None
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Deserialize, Serialize)]
pub struct ImageId(String);

impl FromStr for ImageId {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let regex = Regex::new("^[a-z0-9]+$").unwrap();
        if text.len() == 64 && regex.is_match(text) {
            Ok(ImageId(text.to_owned()))
        } else {
            Err("Expected image id".to_owned())
        }
    }
}

impl Display for ImageId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ImageTag {
    registry: Option<String>,
    repository: String,
    tag: String
}

impl ImageTag {
    pub fn new(repository: &str, tag: &str) -> ImageTag {
        ImageTag {
            registry: None,
            repository: repository.to_owned(),
            tag: tag.to_owned()
        }
    }

    pub fn with_registry(registry: &str, repository: &str, tag: &str) -> ImageTag {
        ImageTag {
            registry: Some(registry.to_owned()),
            repository: repository.to_owned(),
            tag: tag.to_owned()
        }
    }

    pub fn registry(&self) -> Option<&str> {
        self.registry.as_deref()
    }

    pub fn repository(&self) -> &str {
        &self.repository
    }

    pub fn full_repository(&self) -> String {
        if let Some(registry) = self.registry.as_ref() {
            format!("{}/{}", registry, self.repository)
        } else {
            self.repository.clone()
        }
    }

    pub fn tag(&self) -> &str {
        &self.tag
    }

    pub fn set_registry(mut self, registry: &str) -> Self {
        self.registry = Some(registry.to_owned());
        self
    }
}

impl FromStr for ImageTag {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let regex = Regex::new("((.+)/)?([A-Za-z0-9_\\-\\.]+)(:([A-Za-z0-9_\\-\\.]+))?").unwrap();
        let capture = regex.captures(text).ok_or_else(|| "Expected image tag")?;

        let registry = capture.get(2).map(|x| x.as_str().to_string());
        let repository = capture.get(3).map(|x| x.as_str().to_string()).ok_or_else(|| "Expected image tag")?;
        let tag = capture.get(5).map(|x| x.as_str().to_string()).unwrap_or_else(|| "latest".to_owned());

        Ok(
            ImageTag {
                registry,
                repository,
                tag
            }
        )
    }
}

impl Display for ImageTag {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.registry {
            Some(registry) => write!(f, "{}/{}:{}", registry, self.repository, self.tag),
            None => write!(f, "{}:{}", self.repository, self.tag)
        }
    }
}

impl Serialize for ImageTag {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer {
        serializer.serialize_str(&self.to_string())
    }
}

struct ImageTagVisitor;

impl<'de> Visitor<'de> for ImageTagVisitor {
    type Value = ImageTag;

    fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
        formatter.write_str("a string on the format registry/repository:tag")
    }

    fn visit_string<E>(self, v: String) -> Result<Self::Value, E> where E: Error {
        ImageTag::from_str(&v).map_err(|err| E::custom(err.to_string()))
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E> where E: Error {
        ImageTag::from_str(v).map_err(|err| E::custom(err.to_string()))
    }
}

impl<'de> Deserialize<'de> for ImageTag {
    fn deserialize<D>(deserializer: D) -> Result<ImageTag, D::Error> where D: Deserializer<'de> {
        deserializer.deserialize_string(ImageTagVisitor)
    }
}

#[test]
fn test_reference1() {
    assert_eq!(
        Some(Reference::ImageTag(ImageTag::new("labar", "test"))),
        Reference::from_str("labar:test").ok()
    );
}

#[test]
fn test_reference2() {
    assert_eq!(
        Some(Reference::ImageId(ImageId("679447d45a6c8ed2dce1d106fd2ffbc61b96c3633ec3ae4ee20034055d7e0216".to_owned()))),
        Reference::from_str("679447d45a6c8ed2dce1d106fd2ffbc61b96c3633ec3ae4ee20034055d7e0216").ok()
    );
}

#[test]
fn test_image_id_parse1() {
    assert_eq!(
        Some(ImageId("679447d45a6c8ed2dce1d106fd2ffbc61b96c3633ec3ae4ee20034055d7e0216".to_owned())),
        ImageId::from_str("679447d45a6c8ed2dce1d106fd2ffbc61b96c3633ec3ae4ee20034055d7e0216").ok()
    )
}

#[test]
fn test_image_id_parse2() {
    assert_eq!(
        None,
        ImageId::from_str("679447d45a6c8ed2dce1d106bc61b96c3633ec3ae4ee20034055d7e0216").ok()
    )
}

#[test]
fn test_image_id_parse3() {
    assert_eq!(
        None,
        ImageId::from_str("labar:test").ok()
    )
}

#[test]
fn test_image_id_serialize1() {
    use serde_json;

    let image_id = ImageId("679447d45a6c8ed2dce1d106fd2ffbc61b96c3633ec3ae4ee20034055d7e0216".to_owned());
    assert_eq!("\"679447d45a6c8ed2dce1d106fd2ffbc61b96c3633ec3ae4ee20034055d7e0216\"", &serde_json::to_string(&image_id).unwrap());
}
#[test]
fn test_image_id_deserialize1() {
    use serde_json;

    let image_id = ImageId("679447d45a6c8ed2dce1d106fd2ffbc61b96c3633ec3ae4ee20034055d7e0216".to_owned());
    let content = serde_json::to_string(&image_id).unwrap();
    let deserialized: ImageId = serde_json::from_str(&content).unwrap();

    assert_eq!(image_id, deserialized);
}

#[test]
fn test_image_tag_access1() {
    let image_tag = ImageTag::new("labar", "test");
    assert_eq!("labar", image_tag.full_repository());
}

#[test]
fn test_image_tag_access2() {
    let image_tag = ImageTag::with_registry("localhost:3000", "labar", "test");
    assert_eq!("localhost:3000/labar", image_tag.full_repository());
}

#[test]
fn test_image_tag_to_string1() {
    assert_eq!("labar:test", &ImageTag::new("labar", "test").to_string());
}

#[test]
fn test_image_tag_to_string2() {
    assert_eq!("localhost:3000/labar:test", &ImageTag::with_registry("localhost:3000", "labar", "test").to_string());
}

#[test]
fn test_image_tag_parse1() {
    assert_eq!(Some(ImageTag::new("labar", "test")), ImageTag::from_str("labar:test").ok())
}

#[test]
fn test_image_tag_parse2() {
    assert_eq!(Some(ImageTag::with_registry("localhost:3000", "labar", "test")), ImageTag::from_str("localhost:3000/labar:test").ok())
}

#[test]
fn test_image_tag_parse3() {
    assert_eq!(Some(ImageTag::new("labar", "latest")), ImageTag::from_str("labar").ok())
}

#[test]
fn test_image_tag_parse4() {
    assert_eq!(Some(ImageTag::with_registry("localhost:3000", "labar", "latest")), ImageTag::from_str("localhost:3000/labar").ok())
}

#[test]
fn test_image_tag_serialize1() {
    use serde_json;

    let image_tag = ImageTag::new("labar", "test");
    assert_eq!("\"labar:test\"", &serde_json::to_string(&image_tag).unwrap());
}

#[test]
fn test_image_tag_serialize2() {
    use serde_json;

    let image_tag = ImageTag::with_registry("localhost:3000", "labar", "test");
    assert_eq!("\"localhost:3000/labar:test\"", &serde_json::to_string(&image_tag).unwrap());
}

#[test]
fn test_image_tag_deserialize1() {
    use serde_json;

    let image_tag = ImageTag::new("labar", "test");
    let content = serde_json::to_string(&image_tag).unwrap();
    let deserialized: ImageTag = serde_json::from_str(&content).unwrap();

    assert_eq!(image_tag, deserialized);
}

#[test]
fn test_image_tag_deserialize2() {
    use serde_json;

    let image_tag = ImageTag::with_registry("localhost:3000", "labar", "test");
    let content = serde_json::to_string(&image_tag).unwrap();
    let deserialized: ImageTag = serde_json::from_str(&content).unwrap();

    assert_eq!(image_tag, deserialized);
}