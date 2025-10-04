use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;

use sha2::{Digest, Sha256};

pub struct ContentHash {
    hasher: Sha256
}

impl ContentHash {
    pub fn new() -> ContentHash {
        ContentHash {
            hasher: Sha256::new()
        }
    }

    pub fn add(&mut self, data: &[u8]) {
        self.hasher.update(data);
    }

    pub fn finalize(self) -> String {
        base16ct::lower::encode_string(&self.hasher.finalize())
    }
}

pub fn compute_content_hash(path: &Path) -> std::io::Result<String> {
    let mut reader = BufReader::new(File::open(path)?);

    let mut buffer = [0; 4096];
    let mut hasher = ContentHash::new();
    loop {
        let count = reader.read(&mut buffer)?;
        if count == 0 {
            break;
        }

        hasher.add(&buffer[..count]);
    }

    Ok(hasher.finalize())
}

#[test]
fn test_compute_file() {
    assert_eq!(
        Some("22fab83a2b47a54531588ea2b025e8c48f5e03b40df4283f0ec1c70b0faba38e".to_owned()),
        compute_content_hash(Path::new("testdata/rawdata/file1.txt")).ok()
    );
}