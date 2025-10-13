use std::fmt::{Display, Formatter};
use std::ops::{Add, AddAssign, Deref, DerefMut};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

pub fn split_parts(line: &str) -> Vec<String> {
    let mut parts = Vec::new();

    let mut current = Vec::new();
    let mut escaped = false;
    for char in line.chars() {
        if char.is_whitespace() && !escaped {
            if !current.is_empty() {
                parts.push(String::from_iter(current));
            }

            current = Vec::new();
        } else if char == '\\' {
            escaped = true;
        } else {
            current.push(char);
            escaped = false;
        }
    }

    if !current.is_empty() {
        parts.push(String::from_iter(current));
    }

    parts
}

#[test]
fn test_split_parts1() {
    let parts = split_parts("test this");
    assert_eq!(vec!["test", "this"], parts);
}

#[test]
fn test_split_parts2() {
    let parts = split_parts("test\\ that this");
    assert_eq!(vec!["test that", "this"], parts);
}

pub struct TablePrinter {
    rows: Vec<Vec<String>>,
    column_lengths: Vec<usize>
}

impl TablePrinter {
    pub fn new(columns: Vec<String>) -> TablePrinter {
        let column_lengths = columns.iter().map(|x| x.chars().count()).collect::<Vec<_>>();

        TablePrinter {
            rows: vec![columns],
            column_lengths
        }
    }

    pub fn add_row(&mut self, columns: Vec<String>) {
        for (i, column) in columns.iter().enumerate() {
            self.column_lengths[i] = std::cmp::max(self.column_lengths[i], column.chars().count());
        }

        self.rows.push(columns);
    }

    pub fn print(&self) {
        for row in &self.rows {
            for (i, column) in row.iter().enumerate() {
                print!("{}", column);

                if i + 1 < self.column_lengths.len() {
                    for _ in 0..(self.column_lengths[i] - column.chars().count() + 6) {
                        print!(" ");
                    }
                }
            }

            println!();
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DataSize(pub usize);

impl DataSize {
    pub fn from_file(path: &Path) -> DataSize {
        DataSize(std::fs::metadata(path).map(|metadata| metadata.len()).unwrap_or(0) as usize)
    }
}

impl Display for DataSize {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let megabytes = self.0 as f64 / 1024.0 / 1024.0;

        if megabytes > 0.1 {
            write!(f, "{:.2} MB", megabytes)
        } else {
            write!(f, "{:.2} KB", megabytes * 1024.0)
        }
    }
}

impl Add for DataSize {
    type Output = DataSize;

    fn add(self, rhs: Self) -> Self::Output {
        DataSize(self.0 + rhs.0)
    }
}

impl AddAssign for DataSize {
    fn add_assign(&mut self, rhs: Self) {
        self.0 += rhs.0;
    }
}

pub fn clean_path<P>(path: P) -> PathBuf where P: AsRef<Path> {
    // From: https://github.com/danreeves/path-clean/
    let mut out = Vec::new();

    for comp in path.as_ref().components() {
        match comp {
            Component::CurDir => (),
            Component::ParentDir => match out.last() {
                Some(Component::RootDir) => (),
                Some(Component::Normal(_)) => {
                    out.pop();
                }
                None
                | Some(Component::CurDir)
                | Some(Component::ParentDir)
                | Some(Component::Prefix(_)) => out.push(comp),
            },
            comp => out.push(comp),
        }
    }

    if !out.is_empty() {
        out.iter().collect()
    } else {
        PathBuf::from(".")
    }
}

pub struct DeferredFileDelete {
    path: PathBuf,
    skip: bool
}

impl DeferredFileDelete {
    pub fn new(path: PathBuf) -> DeferredFileDelete {
        DeferredFileDelete {
            path,
            skip: false,
        }
    }

    pub fn skip(&mut self) {
        self.skip = true;
    }
}

impl Drop for DeferredFileDelete {
    fn drop(&mut self) {
        if !self.skip {
            if let Err(err) = std::fs::remove_file(&self.path) {
                println!("Failed to delete file due to: {}", err);
            }
        }
    }
}

pub fn edit_key_value(input: &str) -> Result<(&str, Option<&str>), String> {
    let parts = input.split('=').collect::<Vec<&str>>();

    if parts.len() == 2 {
        let key = parts[0];
        let value = parts[1];
        let value_opt = if value.is_empty() {
            None
        } else {
            Some(value)
        };

        Ok((key, value_opt))
    } else {
        Err("Expected key=value".to_owned())
    }
}

pub struct ResourcePool<T> {
    resources: Mutex<Vec<T>>
}

impl<T> ResourcePool<T> {
    pub fn new(initial: Vec<T>) -> ResourcePool<T> {
        ResourcePool {
            resources: Mutex::new(initial)
        }
    }

    pub fn return_resource(&self, resource: T) {
        self.resources.lock().unwrap().push(resource);
    }

    pub fn get_resource(&self) -> Option<T> {
        self.resources.lock().unwrap().pop()
    }
}

pub struct PooledResource<T> {
    pool: Arc<ResourcePool<T>>,
    resource: Option<T>
}

impl<T> PooledResource<T> {
    pub fn new(pool: Arc<ResourcePool<T>>, resource: T) -> PooledResource<T> {
        PooledResource {
            pool,
            resource: Some(resource)
        }
    }
}

impl<T> Drop for PooledResource<T> {
    fn drop(&mut self) {
        if let Some(resource) = self.resource.take() {
            self.pool.return_resource(resource);
        }
    }
}

impl<T> Deref for PooledResource<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.resource.as_ref().unwrap()
    }
}

impl<T> DerefMut for PooledResource<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.resource.as_mut().unwrap()
    }
}