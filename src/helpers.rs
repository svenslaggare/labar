use std::fmt::{Display, Formatter};
use std::path::PathBuf;
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

#[allow(dead_code)]
pub fn get_temp_folder() -> PathBuf {
    let named_temp_folder = tempfile::Builder::new()
        .suffix(".labar")
        .tempfile().unwrap();

    named_temp_folder.path().to_owned()
}