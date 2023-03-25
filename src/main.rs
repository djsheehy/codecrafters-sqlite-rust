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
            // let mut file = File::open(&args[1])?;
            // let mut header = [0; 100];
            // file.read_exact(&mut header)?;
            let file = SqliteFile::new(File::open(&args[1])?)?;

            // The page size is stored at the 16th byte offset, using 2 bytes in big-endian order
            #[allow(unused_variables)]
            let page_size = file.page_size();

            // Uncomment this block to pass the first stage
            println!("database page size: {}", page_size);
        }
        ".pageinfo" => {
            let mut file = SqliteFile::new(File::open(&args[1])?)?;
            let pg = file.get_page(0)?;
            let header = pg.get_header()?;
            println!("{:?}", header);
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
