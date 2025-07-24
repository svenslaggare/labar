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

    let mut buffer = vec![0; 4096];
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
        Some("a6d3bede153575dad6806556e10c79c48329201e9cb3ccee9d55ee5f1b1ed3e8".to_owned()),
        compute_content_hash(Path::new("testdata/rawdata/file1.txt")).ok()
    );
}