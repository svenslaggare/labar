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