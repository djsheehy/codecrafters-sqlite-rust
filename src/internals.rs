#![allow(dead_code)]
use anyhow::{anyhow, bail, Error, Result};
use nom::combinator::map_res;
use nom::number::complete::{be_u16, be_u32, u8 as byte};
use nom::{Finish, IResult};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::marker::PhantomData;

pub struct SqliteFile {
    file: File,
    page_size: u16,
}

impl SqliteFile {
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

    pub fn page_size(&self) -> u16 {
        self.page_size
    }

    pub fn get_page(&mut self, page_id: u64) -> Result<Page> {
        let mut data = vec![0u8; self.page_size as usize];
        self.file
            .seek(SeekFrom::Current(page_id as i64 * self.page_size as i64))?;
        self.file.read_exact(&mut data[..])?;
        Ok(Page {
            _phantom: PhantomData,
            page_id,
            data,
        })
    }
}

pub struct Page<'db> {
    _phantom: std::marker::PhantomData<&'db SqliteFile>,
    page_id: u64,
    data: Vec<u8>,
}

impl<'db> Page<'db> {
    pub fn get_header(&self) -> Result<BtreeHeader> {
        let data = if self.page_id == 0 {
            &self.data[100..]
        } else {
            &self.data[..]
        };
        let (_, header) = parse_btree_header(data)
            .finish()
            .map_err(|e| anyhow!("parse error: {:?}", e))?;
        Ok(header)
    }
}

#[derive(Debug, Clone, Copy)]
pub enum BtreePageKind {
    InteriorIndex,
    InteriorTable,
    LeafIndex,
    LeafTable,
}

impl BtreePageKind {
    const fn is_interior(self) -> bool {
        match self {
            Self::InteriorIndex | Self::InteriorTable => true,
            _ => false,
        }
    }
}

impl TryFrom<u8> for BtreePageKind {
    type Error = Error;

    fn try_from(value: u8) -> std::result::Result<Self, Self::Error> {
        use BtreePageKind::*;
        match value {
            2 => Ok(InteriorIndex),
            5 => Ok(InteriorTable),
            10 => Ok(LeafIndex),
            13 => Ok(LeafTable),
            _ => bail!("invalid b-tree page type: {}", value),
        }
    }
}

#[derive(Debug)]
pub struct BtreeHeader {
    pub kind: BtreePageKind,
    pub first_freeblock: u16,
    pub cell_count: u16,
    pub cell_contents: u16,
    pub fragmented_free_bytes: u8,
    pub rightmost_pointer: Option<u32>,
}

macro_rules! parse_steps {
    ($input:ident: $($var:ident = $parser:expr;)*) => {
        $(let ($input, $var) = $parser($input)?;)*
    };
}

fn parse_btree_header(input: &[u8]) -> IResult<&[u8], BtreeHeader> {
    parse_steps! {
        input:
        kind = map_res(byte, |n| BtreePageKind::try_from(n));
        first_freeblock = be_u16;
        cell_count = be_u16;
        cell_contents = be_u16;
        fragmented_free_bytes = byte;
        rightmost_pointer = be_u32;
    }
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
