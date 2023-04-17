mod sqlite;

use sqlite::*;

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
            let page = file.get_page(1)?;
            let header = page.get_header()?;
            println!("number of tables: {}", header.cell_count);
        }
        ".tables" => {
            let mut file = SqliteFile::new(File::open(&args[1])?)?;
            let page = file.get_page(1)?;
            let header = page.get_header()?;
            let input = &page.data[108..];
            let (_, pointers) = cell_pointers(input, header.cell_count as usize)
                .expect("tried to read cell pointers");
            let cells = pointers.iter().map(|ptr| {
                let (_, cell) = header
                    .parse_cell(&page.data[*ptr as usize..])
                    .expect("parse cell");
                cell
            });
            for c in cells {
                match c {
                    cells::Cell::TableLeaf { payload, .. } => {
                        let (_, records) = payload.parse().expect("parse records");
                        println!("{}", records[1]);
                    }
                    _ => unimplemented!(),
                }
            }
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
