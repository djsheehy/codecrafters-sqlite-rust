use anyhow::{anyhow, bail, Error, Result};
use nom::IResult;
use nom::{
    combinator::map_res,
    multi::count,
    number::complete::{be_u16, be_u32, u8},
    sequence::tuple,
};
use regex::RegexBuilder;
use std::cell::RefCell;
use std::io::{Read, Seek, SeekFrom};
use std::num::NonZeroU64;
use std::str::FromStr;
use std::{fs::File, ops::Deref};

use self::cells::Cell;

pub(crate) mod cells;
pub(crate) mod record;
pub(crate) mod varint;

/// An SQLite database file. Top level thingy that gets everything else.
pub struct SqliteFile {
    file: RefCell<File>,
    page_size: u16,
    page1: Page,
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
        let mut data = vec![0u8; page_size as usize];
        file.by_ref().read_exact(&mut data)?;
        let (_, header) = parse_btree_header(&data[100..]).map_err(|_| anyhow!("parse header"))?;

        Ok(Self {
            file: RefCell::new(file),
            page_size,
            page1: Page {
                page_id: 1,
                data,
                header,
            },
        })
    }

    /// Get the page size.
    pub fn page_size(&self) -> u16 {
        self.page_size
    }

    /// Get a page. `page_id` starts at 1.
    pub fn get_page(&self, page_id: NonZeroU64) -> Result<Page> {
        let page_id = page_id.get();
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

    pub fn get_schema(&self) -> Vec<Schema> {
        self.page1
            .cells()
            .map(|c| {
                let row = c.get_payload().unwrap().parse().unwrap().1;
                Schema {
                    stype: row[0].to_string().parse().unwrap(),
                    name: row[1].to_string(),
                    table_name: row[2].to_string(),
                    rootpage: u64::from(row[3].clone()),
                    sql: row[4].to_string(),
                }
            })
            .collect()
    }
}

#[derive(Clone, Copy, Debug)]
pub enum SchemaType {
    Table,
    Index,
    View,
    Trigger,
}

impl FromStr for SchemaType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        use SchemaType::*;
        match s {
            "table" => Ok(Table),
            "index" => Ok(Index),
            "view" => Ok(View),
            "trigger" => Ok(Trigger),
            _ => bail!("schema type must be table, index, view or trigger"),
        }
    }
}

#[derive(Debug)]
pub struct Schema {
    pub stype: SchemaType,
    pub name: String,
    pub table_name: String,
    pub rootpage: u64,
    pub sql: String,
}

pub struct Page {
    pub page_id: u64,
    pub data: Vec<u8>,
    pub header: BtreeHeader,
}

/// Iterates over the cells in a page.
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
        // start of cell pointer array.
        // First page contains 100 byte file header.
        // Page header is 8 bytes if a leaf page or 12 bytes if interior.
        // I assume the first page is a leaf page, which is usually true unless you have a crapload of tables.
        let start = if self.page_id == 1 {
            108
        } else if self.header.kind.is_interior() {
            12
        } else {
            8
        };
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

/// Header of a B-tree page.
pub struct BtreeHeader {
    /// Page type
    pub kind: PageKind,
    /// Offset to first freeblock in the page, or 0 if none.
    pub first_freeblock: u16,
    /// Number of cells on the page.
    pub cell_count: u16,
    /// Start of cell content area
    pub cell_contents: u16,
    /// Number of fragmented free bytes.
    pub fragmented_free_bytes: u8,
    /// Child page whose keys are greater than the keys on this page.
    /// Only exists if it's an internal page.
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

/// Compiled `SELECT` statement
#[derive(Debug, PartialEq)]
pub struct Select {
    pub name: String,
    pub columns: Vec<String>,
}

/// Compiled `CREATE TABLE` statement
#[derive(Debug, PartialEq)]
pub struct CreateTable {
    pub name: String,
    pub columns: Vec<String>,
    pub key: Option<String>,
}

impl CreateTable {
    /// Get index of corresponding columns in a [`Select`]
    pub fn select(&self, sel: &Select) -> Vec<usize> {
        sel.columns
            .iter()
            .flat_map(|sc| self.columns.iter().position(|cc| cc == sc))
            .collect()
    }
}

impl TryFrom<&Schema> for CreateTable {
    type Error = Error;

    fn try_from(value: &Schema) -> std::result::Result<Self, Self::Error> {
        value.sql.parse()
    }
}

impl FromStr for Select {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let rx = RegexBuilder::new("SELECT ([A-Za-z, ]+) FROM ([A-Za-z]+)")
            .case_insensitive(true)
            .build()?;
        let caps = rx
            .captures(s)
            .ok_or_else(|| anyhow!("failed to parse SELECT: {:?}", s))?;
        let name = caps.get(2).unwrap().as_str().to_owned();
        let columns = caps.get(1).unwrap();
        let columns: Vec<String> = columns.as_str().split(", ").map(String::from).collect();
        Ok(Select { name, columns })
    }
}

impl FromStr for CreateTable {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let rx = RegexBuilder::new(r"create\s+table\s+(?P<name>\w+)\s*\(\s*(?P<columns>[^\)]*)\)")
            .case_insensitive(true)
            .build()?;
        let caps = rx
            .captures(s)
            .ok_or_else(|| anyhow!("failed to parse CREATE TABLE"))?;
        let name = caps.name("name").unwrap().as_str().to_owned();
        let columns: Vec<_> = caps
            .name("columns")
            .unwrap()
            .as_str()
            .split(",")
            .map(|s| s.trim())
            .collect();
        let colnames: Vec<_> = columns
            .iter()
            .map(|s| s.split(" ").next().unwrap().to_string())
            .collect();
        let mut table = CreateTable {
            name,
            columns: colnames,
            key: None,
        };
        for (i, col) in columns.iter().enumerate() {
            if col.contains("primary key") {
                table.key = Some(table.columns[i].clone());
                break;
            }
        }
        Ok(table)
    }
}

#[test]
fn sql_create_table() -> Result<()> {
    let sql = "CREATE TABLE apples
    (
            id integer primary key autoincrement,
            name text,
            color text
    )";
    let table: CreateTable = sql.parse()?;
    let expected = CreateTable {
        name: "apples".to_string(),
        columns: vec!["id".to_owned(), "name".to_owned(), "color".to_owned()],
        key: Some("id".to_owned()),
    };
    assert_eq!(table, expected);
    Ok(())
}

#[test]
fn sql_select() -> Result<()> {
    let sql = "SELECT name FROM apples";
    let sel: Select = sql.parse()?;
    let expected = Select {
        name: "apples".to_owned(),
        columns: vec!["name".to_owned()],
    };
    assert_eq!(sel, expected);
    Ok(())
}

#[test]
fn sql_multi_select() -> Result<()> {
    let sql = "SELECT name, description FROM apples";
    let sel: Select = sql.parse()?;
    let expected = Select {
        name: "apples".to_owned(),
        columns: vec!["name".to_owned(), "description".to_owned()],
    };
    assert_eq!(sel, expected);
    Ok(())
}
