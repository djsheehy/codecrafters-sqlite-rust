mod internals;

use internals::*;

use anyhow::{bail, Result};
use std::fs::File;

fn main() -> Result<()> {
    // Parse arguments
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    // Parse command and act accordingly
    let command = &args[2];
    match command.as_str() {
        ".dbinfo" => {
            let mut file = SqliteFile::new(File::open(&args[1])?)?;
            let page_size = file.page_size();
            println!("database page size: {}", page_size);
            let page = file.get_page(0)?;
            let header = page.get_header()?;
            println!("number of tables: {}", header.cell_count);
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
