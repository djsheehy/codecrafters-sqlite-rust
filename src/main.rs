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
            let file = SqliteFile::new(File::open(&args[1])?)?;
            let schema = file.get_page(1)?;
            let page_size = file.page_size();
            println!("database page size: {}", page_size);
            println!("number of tables: {}", schema.header.cell_count);
        }
        ".tables" => {
            let file = SqliteFile::new(File::open(&args[1])?)?;
            let schema = file.get_page(1)?;
            let input = &schema[108..];
            let (_, pointers) = cell_pointers(input, schema.header.cell_count as usize)
                .expect("tried to read cell pointers");
            let cells = pointers.iter().map(|ptr| {
                let (_, cell) = schema
                    .header
                    .parse_cell(&schema.data[*ptr as usize..])
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
        query => {
            let file = SqliteFile::new(File::open(&args[1])?)?;
            let schema = file.get_page(1)?;
            let name = query.split(' ').last().unwrap();
            let mut rows = schema.cells().map(|c| {
                c.get_payload()
                    .expect("get_payload")
                    .parse()
                    .expect("parse_payload")
                    .1
            });
            let table = rows
                .find(|row| row[1].to_string() == name)
                .expect("table not found");
            let rootpage: Option<u64> = Option::from(table[3].clone());
            let page = file.get_page(rootpage.expect("value is not an integer"))?;
            println!("{}", page.header.cell_count);
        }
    }

    Ok(())
}
