use std::sync::Arc;

pub trait Printer {
    fn println(&self, line: &str);
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
}