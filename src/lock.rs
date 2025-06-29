use std::path::PathBuf;
use std::io::{Write, Read};
use std::str::FromStr;

use sysinfo::{Pid, ProcessStatus, System};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub struct FileLock {
    path: PathBuf,
    has_lock: bool,
    system: System
}

impl FileLock {
    pub fn new(path: PathBuf) -> FileLock {
        let mut file_lock = FileLock {
            path,
            has_lock: false,
            system: System::new_all()
        };

        file_lock.lock();
        file_lock
    }

    pub async fn new_async(path: PathBuf) -> FileLock {
        let mut file_lock = FileLock {
            path,
            has_lock: false,
            system: System::new_all()
        };

        file_lock.lock_async().await;
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
                            let dead = if let Some(process) = self.system.process(Pid::from_u32(pid)) {
                                process.status() == ProcessStatus::Run
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

    async fn lock_async(&mut self) {
        let mut first_time = true;
        loop {
            if self.path.exists() {
                if first_time {
                    println!("Waiting for lock...");
                    first_time = false;
                }

                if let Ok(mut lock_file) = tokio::fs::File::open(&self.path).await {
                    // We can remove the lock if the process holding the lock is dead
                    let mut content = String::new();
                    if let Ok(_) = lock_file.read_to_string(&mut content).await {
                        if let Ok(pid) = u32::from_str(&content) {
                            let dead = if let Some(process) = self.system.process(Pid::from_u32(pid)) {
                                process.status() == ProcessStatus::Run
                            } else {
                                true
                            };

                            if dead {
                                #[allow(unused_must_use)] {
                                    tokio::fs::remove_file(&self.path).await;
                                }
                            }
                        }
                    }
                }

                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            } else {
                #[allow(unused_must_use)] {
                    tokio::fs::create_dir_all(self.path.parent().unwrap()).await;
                }

                let lock_file = tokio::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&self.path)
                    .await;

                if let Ok(mut lock_file) = lock_file {
                    // We use the PID to indicate if the process handling the lock is alive
                    let result = lock_file.write_all(std::process::id().to_string().as_bytes()).await;

                    if result.is_ok() {
                        self.has_lock = true;
                        break;
                    } else {
                        // Remove the lock file as we failed to write our PID.
                        #[allow(unused_must_use)] {
                            tokio::fs::remove_file(&self.path).await;
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