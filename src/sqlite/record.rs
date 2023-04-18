use std::{borrow::Cow, fmt::Display};

use crate::varint::varint;
use nom::{
    bytes::complete::take,
    combinator::into,
    multi::many1,
    number::complete::{be_f64, be_i16, be_i24, be_i32, be_i64, i8},
    IResult,
};

/// Record from an SQLite database.
#[derive(Debug)]
pub enum Record<'a> {
    /// `NULL` value
    Null,
    /// Integer value
    Integer(i64),
    /// Floating point value
    Float(f64),
    /// `BLOB` value (binary data)
    Blob(&'a [u8]),
    /// `TEXT` value (unicode text)
    String(Cow<'a, str>),
}

impl<'a> Display for Record<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Record::Null => write!(f, "NULL"),
            Record::Integer(n) => write!(f, "{}", *n),
            Record::Float(n) => write!(f, "{}", *n),
            Record::Blob(b) => write!(f, "{:?}", b),
            Record::String(s) => write!(f, "{}", s),
        }
    }
}

#[derive(Clone, Copy)]
enum RecordCode {
    Null,
    I8,
    I16,
    I24,
    I32,
    I48,
    I64,
    F64,
    Zero,
    One,
    Blob(usize),
    String(usize),
}

impl From<u64> for RecordCode {
    fn from(value: u64) -> Self {
        use RecordCode::*;
        match value {
            0 => Null,
            1 => I8,
            2 => I16,
            3 => I24,
            4 => I32,
            5 => I48,
            6 => I64,
            7 => F64,
            8 => Zero,
            9 => One,
            n if n >= 12 && n % 2 == 0 => Blob((n as usize - 12) / 2),
            n if n >= 13 && n % 2 == 1 => String((n as usize - 13) / 2),
            _ => unreachable!("serial type 10 and 11 are reserved."),
        }
    }
}

impl<'a> RecordCode {
    fn parse(self, input: &'a [u8]) -> IResult<&'a [u8], Record> {
        match self {
            RecordCode::Null => Ok((input, Record::Null)),
            RecordCode::I8 => {
                let (input, n) = i8(input)?;
                Ok((input, Record::Integer(n.into())))
            }

            RecordCode::I16 => {
                let (input, n) = be_i16(input)?;
                Ok((input, Record::Integer(n.into())))
            }
            RecordCode::I24 => {
                let (input, n) = be_i24(input)?;
                Ok((input, Record::Integer(n.into())))
            }
            RecordCode::I32 => {
                let (input, n) = be_i32(input)?;
                Ok((input, Record::Integer(n.into())))
            }
            RecordCode::I48 => {
                let (input, n) = take(6 as usize)(input)?;
                let mut x = 0u64;
                for b in n {
                    x = (x << 8) | (*b as u64);
                }
                if n[0] >= 0x80 {
                    x |= 0xff_ff_00_00_00_00_00_00;
                }
                Ok((input, Record::Integer(x as i64)))
            }
            RecordCode::I64 => {
                let (input, n) = be_i64(input)?;
                Ok((input, Record::Integer(n.into())))
            }
            RecordCode::F64 => {
                let (input, n) = be_f64(input)?;
                Ok((input, Record::Float(n)))
            }
            RecordCode::Zero => Ok((input, Record::Integer(0))),
            RecordCode::One => Ok((input, Record::Integer(1))),
            RecordCode::Blob(n) => {
                let (input, b) = take(n)(input)?;
                Ok((input, Record::Blob(b)))
            }
            RecordCode::String(n) => {
                let (input, s) = take(n)(input)?;
                Ok((input, Record::String(String::from_utf8_lossy(s))))
            }
        }
    }
}

/// Parse a [Cell][crate::cells::Cell] payload into a series of [Record]s.
pub fn parse_payload<'a>(input: &'a [u8]) -> IResult<&'a [u8], Vec<Record<'a>>> {
    let (_, header_size) = varint(input)?;
    let header = &input[..header_size as usize];
    let (header, _) = varint(header)?;
    let (_, codes): (_, Vec<RecordCode>) = many1(into(varint))(header)?;
    let mut body = &input[header_size as usize..];
    let mut records = vec![];
    for code in codes {
        let (input, rec) = code.parse(body)?;
        body = input;
        records.push(rec);
    }

    Ok((body, records))
}
