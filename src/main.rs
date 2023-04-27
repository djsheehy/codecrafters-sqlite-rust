mod sqlite;
use sqlite::*;

use anyhow::{bail, Result};
use sqlite::record::Value;
use std::fs::File;
use std::num::NonZeroU64;

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
            let schema = file.get_page(NonZeroU64::new(1).unwrap())?;
            let page_size = file.page_size();
            println!("database page size: {}", page_size);
            println!("number of tables: {}", schema.header.cell_count);
        }
        ".tables" => {
            let file = SqliteFile::new(File::open(&args[1])?)?;
            let schema = file.get_page(NonZeroU64::new(1).unwrap())?;
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
            let schema = file.get_schema();
            let stmt: Select = query.parse()?;
            let table = schema
                .iter()
                .find(|sch| sch.name == stmt.name)
                .ok_or_else(|| anyhow::anyhow!("table not found"))?;
            let create: CreateTable = table.try_into()?;
            let selected = create.select(&stmt);
            let pgno = NonZeroU64::new(table.rootpage).unwrap();
            let page = file.get_page(pgno)?;
            for cell in page.cells() {
                let row: Vec<Value> = cell.try_into()?;
                let mut result = vec![];
                for s in selected.iter() {
                    result.push(row[*s].to_string());
                }
                println!("{}", result.join("|"));
            }
        }
    }

    Ok(())
}
