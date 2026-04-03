use std::io::stdout;
use std::sync::Arc;

use crossterm::{cursor, terminal, ExecutableCommand};
use crossterm::terminal::ClearType;

pub trait Printer {
    fn println(&self, line: &str);
    fn refresh_latest_line(&self, line: &str);
}

pub type PrinterRef = Arc<dyn Printer + Send + Sync>;

pub struct ConsolePrinter {

}

impl ConsolePrinter {
    pub fn new() -> Arc<ConsolePrinter> {
        Arc::new(ConsolePrinter { })
    }
}

impl Printer for ConsolePrinter {
    fn println(&self, line: &str) {
        println!("{}", line);
    }

    fn refresh_latest_line(&self, line: &str) {
        let mut stdout = stdout();
        stdout.execute(cursor::MoveUp(1)).unwrap();
        stdout.execute(terminal::Clear(ClearType::CurrentLine)).unwrap();
        self.println(line);
    }
}

pub struct EmptyPrinter {

}

impl EmptyPrinter {
    pub fn new() -> Arc<EmptyPrinter> {
        Arc::new(EmptyPrinter { })
    }
}

impl Printer for EmptyPrinter {
    fn println(&self, _line: &str) {

    }

    fn refresh_latest_line(&self, _line: &str) {

    }
}