use std::ops::Deref;
use std::path::{Path, PathBuf};

pub struct TempFolder {
    path: PathBuf
}

impl TempFolder {
    pub fn new() -> TempFolder {
        let named_temp_folder = tempfile::Builder::new()
            .suffix(".labar")
            .tempfile().unwrap();

        TempFolder {
            path: named_temp_folder.path().to_owned()
        }
    }

    pub fn owned(&self) -> PathBuf {
        self.path.clone()
    }

    pub fn create(&self) -> std::io::Result<()> {
        std::fs::create_dir_all(&self.path)
    }
}

impl Deref for TempFolder {
    type Target = Path;

    fn deref(&self) -> &Self::Target {
        self.path.as_path()
    }
}

impl Drop for TempFolder {
    fn drop(&mut self) {
        #[allow(unused_must_use)] {
            std::fs::remove_dir_all(&self.path);
        }
    }
}