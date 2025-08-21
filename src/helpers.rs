use std::fmt::{Display, Formatter};
use std::ops::{Add, AddAssign};
use std::path::{Component, Path, PathBuf};

use serde::{Deserialize, Serialize};

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

impl Display for DataSize {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:.2} MB", self.0 as f64 / 1024.0 / 1024.0)
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