use anyhow::{anyhow, bail, Error, Result};
use nom::{
    combinator::map_res,
    multi::count,
    number::complete::{be_u16, be_u32, u8},
    sequence::tuple,
};
use nom::{Finish, IResult};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

pub(crate) mod cells;
pub(crate) mod record;
pub(crate) mod varint;

/// An SQLite database file. Top level thingy that gets everything else.
pub struct SqliteFile {
    file: File,
    page_size: u16,
}

impl SqliteFile {
    /// Create an SQLite file from a regular [File][std::fs::File].
    pub fn new(mut file: File) -> Result<Self> {
        file.seek(SeekFrom::Start(16))?;
        let page_size = {
            let mut buf = [0u8; 2];
            file.read_exact(&mut buf[..])?;
            u16::from_be_bytes(buf)
        };
        file.seek(SeekFrom::Start(0))?;
        Ok(Self { file, page_size })
    }

    /// Get the page size.
    pub fn page_size(&self) -> u16 {
        self.page_size
    }

    /// Get a page. `page_id` starts at 1.
    pub fn get_page(&mut self, page_id: u64) -> Result<Page> {
        let mut data = vec![0u8; self.page_size as usize];
        self.file.seek(SeekFrom::Current(
            (page_id - 1) as i64 * self.page_size as i64,
        ))?;
        self.file.read_exact(&mut data[..])?;
        Ok(Page { page_id, data })
    }
}

pub struct Page {
    pub page_id: u64,
    pub data: Vec<u8>,
}

impl Page {
    pub fn get_header(&self) -> Result<BtreeHeader> {
        let data = if self.page_id == 1 {
            &self.data[100..]
        } else {
            &self.data[..]
        };
        let (_, header) = parse_btree_header(data)
            .finish()
            .map_err(|e| anyhow!("get_header error: {:?}", e))?;
        Ok(header)
    }
}

/// B-Tree page type
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PageKind {
    IndexInterior,
    TableInterior,
    IndexLeaf,
    TableLeaf,
}

impl PageKind {
    const fn is_interior(self) -> bool {
        match self {
            Self::IndexInterior | Self::TableInterior => true,
            _ => false,
        }
    }
}

impl TryFrom<u8> for PageKind {
    type Error = Error;

    fn try_from(value: u8) -> std::result::Result<Self, Self::Error> {
        use PageKind::*;
        match value {
            2 => Ok(IndexInterior),
            5 => Ok(TableInterior),
            10 => Ok(IndexLeaf),
            13 => Ok(TableLeaf),
            _ => bail!("invalid b-tree page type: {}", value),
        }
    }
}

pub struct BtreeHeader {
    pub kind: PageKind,
    pub first_freeblock: u16,
    pub cell_count: u16,
    pub cell_contents: u16,
    pub fragmented_free_bytes: u8,
    pub rightmost_pointer: Option<u32>,
}

fn parse_btree_header(input: &[u8]) -> IResult<&[u8], BtreeHeader> {
    let (
        input,
        (
            kind,
            first_freeblock,
            cell_count,
            cell_contents,
            fragmented_free_bytes,
            rightmost_pointer,
        ),
    ) = tuple((
        map_res(u8, |n| PageKind::try_from(n)),
        be_u16,
        be_u16,
        be_u16,
        u8,
        be_u32,
    ))(input)?;
    let rightmost_pointer = kind.is_interior().then_some(rightmost_pointer);
    Ok((
        input,
        BtreeHeader {
            kind,
            first_freeblock,
            cell_contents,
            cell_count,
            fragmented_free_bytes,
            rightmost_pointer,
        },
    ))
}

pub fn cell_pointers(input: &[u8], n: usize) -> IResult<&[u8], Vec<u16>> {
    count(be_u16, n)(input)
}
