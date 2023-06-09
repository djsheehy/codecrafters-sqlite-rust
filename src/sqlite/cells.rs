use crate::record::{parse_payload, Value};
use crate::varint::varint;
use crate::BtreeHeader;
use crate::PageKind;

use nom::bytes::complete::take;
use nom::number::complete::be_u32;
use nom::sequence::tuple;
use nom::IResult;

/// Contains the payload part of the [Cell].
pub struct Payload<'a> {
    pub size: u64,
    pub payload: &'a [u8],
    pub overflow: Option<u32>,
}

impl<'a> Payload<'a> {
    pub fn parse(&'a self) -> IResult<&'a [u8], Vec<Value>> {
        parse_payload(self.payload)
    }
}

impl<'a> std::fmt::Debug for Payload<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Payload")
            .field("size", &self.size)
            .field("payload", &String::from_utf8_lossy(&self.payload))
            .finish()
    }
}

#[derive(Debug)]
/// Represents a cell in a table or index.
pub enum Cell<'a> {
    /// Table Leaf cell
    TableLeaf { rowid: u64, payload: Payload<'a> },
    /// Table Interior cell
    TableInterior { left_child_page: u32, rowid: u64 },
    /// Index Leaf cell
    IndexLeaf { payload: Payload<'a> },
    /// Index Interior cell
    IndexInterior {
        left_child_page: u32,
        payload: Payload<'a>,
    },
}

impl<'a> Cell<'a> {
    pub fn get_payload(&self) -> Option<&Payload<'a>> {
        match self {
            Cell::TableLeaf { ref payload, .. } => Some(payload),
            Cell::TableInterior { .. } => None,
            Cell::IndexLeaf { ref payload, .. } => Some(payload),
            Cell::IndexInterior { ref payload, .. } => Some(payload),
        }
    }
}

impl<'a> TryFrom<Cell<'a>> for Vec<Value> {
    type Error = anyhow::Error;

    fn try_from(value: Cell<'a>) -> Result<Self, Self::Error> {
        let pl = value
            .get_payload()
            .ok_or_else(|| anyhow::anyhow!("Table Interior cells have no payload"))?;
        let (_, row) = pl
            .parse()
            .map_err(|e| anyhow::anyhow!("parse payload error: {}", e.to_string()))?;
        Ok(row)
    }
}

impl<'a> BtreeHeader {
    /// Parse a cell based on the type of Btree.
    pub fn parse_cell(&'a self, input: &'a [u8]) -> IResult<&[u8], Cell<'a>> {
        match self.kind {
            PageKind::TableLeaf => {
                let (input, (size, rowid)) = tuple((varint, varint))(input)?;
                let (input, payload) = take(size)(input)?;
                let payload = Payload {
                    size,
                    payload,
                    overflow: None,
                };
                Ok((input, Cell::TableLeaf { rowid, payload }))
            }
            PageKind::TableInterior => {
                let (input, (left_child_page, rowid)) = tuple((be_u32, varint))(input)?;
                Ok((
                    input,
                    Cell::TableInterior {
                        left_child_page,
                        rowid,
                    },
                ))
            }
            PageKind::IndexLeaf => {
                let (input, size) = varint(input)?;
                let (input, payload) = take(size)(input)?;
                let payload = Payload {
                    size,
                    payload,
                    overflow: None,
                };
                Ok((input, Cell::IndexLeaf { payload }))
            }
            PageKind::IndexInterior => {
                let (input, (left_child_page, size)) = tuple((be_u32, varint))(input)?;
                let (input, payload) = take(size)(input)?;
                let payload = Payload {
                    size,
                    payload,
                    overflow: None,
                };
                Ok((
                    input,
                    Cell::IndexInterior {
                        left_child_page,
                        payload,
                    },
                ))
            }
        }
    }
}
