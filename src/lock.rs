use std::path::PathBuf;
use std::io::{Write, Read};
use std::str::FromStr;

use psutil::process::Process;

pub struct FileLock {
    path: PathBuf,
    has_lock: bool
}

impl FileLock {
    pub fn new(path: PathBuf) -> FileLock {
        let mut file_lock = FileLock {
            path,
            has_lock: false
        };

        file_lock.lock();
        file_lock
    }

    fn lock(&mut self) {
        let mut first_time = true;
        loop {
            if self.path.exists() {
                if first_time {
                    println!("Waiting for lock...");
                    first_time = false;
                }

                if let Ok(mut lock_file) = std::fs::File::open(&self.path) {
                    // We can remove the lock if the process holding the lock is dead
                    let mut content = String::new();
                    if let Ok(_) = lock_file.read_to_string(&mut content) {
                        if let Ok(pid) = u32::from_str(&content) {
                            let dead = if let Ok(process) = Process::new(pid) {
                                !process.is_running()
                            } else {
                                true
                            };

                            if dead {
                                #[allow(unused_must_use)] {
                                    std::fs::remove_file(&self.path);
                                }
                            }
                        }
                    }
                }

                std::thread::sleep(std::time::Duration::from_millis(50));
            } else {
                #[allow(unused_must_use)] {
                    std::fs::create_dir_all(self.path.parent().unwrap());
                }

                let lock_file = std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&self.path);

                if let Ok(mut lock_file) = lock_file {
                    // We use the PID to indicate if the process handling the lock is alive
                    let result = lock_file.write_all(std::process::id().to_string().as_bytes());

                    if result.is_ok() {
                        self.has_lock = true;
                        break;
                    } else {
                        // Remove the lock file as we failed to write our PID.
                        #[allow(unused_must_use)] {
                            std::fs::remove_file(&self.path);
                        }
                    }
                }
            }
        }
    }
}

impl Drop for FileLock {
    fn drop(&mut self) {
        if self.has_lock {
            #[allow(unused_must_use)] {
                std::fs::remove_file(&self.path);
            }
        }
    }
}