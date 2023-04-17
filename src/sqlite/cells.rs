use crate::record::{parse_payload, Record};
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
    pub fn parse(&self) -> IResult<&'a [u8], Vec<Record>> {
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
    TableLeaf {
        rowid: u64,
        payload: Payload<'a>,
    },
    TableInterior {
        left_child_page: u32,
        rowid: u64,
    },
    IndexLeaf {
        payload: Payload<'a>,
    },
    IndexInterior {
        left_child_page: u32,
        payload: Payload<'a>,
    },
}

impl<'a> BtreeHeader {
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
