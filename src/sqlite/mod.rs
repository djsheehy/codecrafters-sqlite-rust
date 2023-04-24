use anyhow::{anyhow, bail, Error, Result};
use nom::IResult;
use nom::{
    combinator::map_res,
    multi::count,
    number::complete::{be_u16, be_u32, u8},
    sequence::tuple,
};
use std::cell::RefCell;
use std::io::{Read, Seek, SeekFrom};
use std::{fs::File, ops::Deref};

use self::cells::Cell;

pub(crate) mod cells;
pub(crate) mod record;
pub(crate) mod varint;

/// An SQLite database file. Top level thingy that gets everything else.
pub struct SqliteFile {
    file: RefCell<File>,
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
        Ok(Self {
            file: RefCell::new(file),
            page_size,
        })
    }

    /// Get the page size.
    pub fn page_size(&self) -> u16 {
        self.page_size
    }

    /// Get a page. `page_id` starts at 1.
    pub fn get_page(&self, page_id: u64) -> Result<Page> {
        let mut data = vec![0u8; self.page_size as usize];
        self.file.borrow_mut().seek(SeekFrom::Start(
            ((page_id - 1) * self.page_size as u64) as u64,
        ))?;
        self.file.borrow_mut().read_exact(&mut data[..])?;
        let hdata = if page_id == 1 {
            &data[100..]
        } else {
            &data[..]
        };
        let (_, header) =
            parse_btree_header(hdata).map_err(|e| anyhow!("parse header: {:?}", e))?;
        Ok(Page {
            page_id,
            data,
            header,
        })
    }
}

pub struct Page {
    pub page_id: u64,
    pub data: Vec<u8>,
    pub header: BtreeHeader,
}

pub struct CellIter<'p> {
    page: &'p Page,
    ptr_array: &'p [u8],
}

impl<'p> Iterator for CellIter<'p> {
    type Item = Cell<'p>;

    fn next(&mut self) -> Option<Self::Item> {
        let (input, ptr) = be_u16::<&[u8], ()>(self.ptr_array).ok()?;
        let data = &self.page[ptr as usize..];
        let (_, cell) = self.page.header.parse_cell(data).ok()?;
        self.ptr_array = input;
        Some(cell)
    }
}

impl Page {
    pub fn cells<'p>(&'p self) -> CellIter<'p> {
        // start of cell pointer array
        let start = if self.page_id == 1 { 108 } else { 8 };
        let count = self.header.cell_count as usize;
        let ptr_array = &self[start..count * 2 + start];
        CellIter {
            page: self,
            ptr_array,
        }
    }
}

impl Deref for Page {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.data
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

#[derive(Debug, PartialEq)]
pub struct Select {
    pub columns: Vec<String>,
    pub table: String,
}

#[derive(Debug, PartialEq)]
pub struct Create {
    pub name: String,
    pub columns: Vec<String>,
}

peg::parser! {
    grammar sql() for str {
        rule _ -> () = $([' '|'\n'|'\t']+) { () }

        rule __ -> () = $([' '|'\n'|'\t']*) { () }

        rule word() -> String
            = w:$(['a'..='z'|'A'..='Z']+) { w.to_owned() }

        rule names() -> Vec<String>
            = n:(word() ** ("," _)) { n.to_owned() }

        pub rule select() -> Select
            = "select" _ cols:names() _ "from" _ table:word() {
                Select {columns: cols, table }
        }

        rule column() -> String
            = col:(word() ++ _) { col[0].to_owned() }

        pub rule create() -> Create
            = "create" _ "table" _ name:word() __ "(" __ columns:(column() ++ ("," __)) __ ");" {
                Create { name, columns }
        }
    }
}

#[test]
fn test_sql_select() {
    let sel = sql::select("select asdf from stuff").unwrap();
    assert_eq!(sel.columns, vec!["asdf".to_owned()]);
    assert_eq!(sel.table, "stuff".to_owned());
    let sel = sql::select("select apple, banana, camel from stuff").unwrap();
    assert_eq!(sel.columns, vec!["apple", "banana", "camel"]);
    assert_eq!(sel.table, "stuff");
}

#[test]
fn test_sql_create() {
    let table = sql::create(
        &"CREATE TABLE oranges
    (
            id integer primary key autoincrement,
            name text,
            description text
    );"
        .to_ascii_lowercase(),
    )
    .unwrap();
    assert_eq!(table.columns, vec!["id", "name", "description"]);
    assert_eq!(table.name, "oranges");
}
