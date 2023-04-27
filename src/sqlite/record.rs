use std::fmt::Display;

use crate::varint::varint;
use nom::{
    bytes::complete::take,
    combinator::into,
    multi::many1,
    number::complete::{be_f64, be_i16, be_i24, be_i32, be_i64, i8},
    IResult,
};

/// Record from an SQLite database.
#[derive(Debug, Clone)]
pub enum Value {
    /// `NULL` value
    Null,
    /// Integer value
    Integer(i64),
    /// Floating point value
    Float(f64),
    /// `BLOB` value (binary data)
    Blob(Vec<u8>),
    /// `TEXT` value (unicode text)
    String(String),
}

impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Integer(n) => write!(f, "{}", *n),
            Value::Float(n) => write!(f, "{}", *n),
            Value::Blob(b) => write!(f, "{:?}", b),
            Value::String(s) => write!(f, "{}", s),
        }
    }
}

macro_rules! impl_from_value {
    ($($t:ty),* $(,)?) => {
        $(
            impl From<Value> for $t {
                fn from(v: Value) -> $t {
                    match v {
                        Value::Null => Default::default(),
                        Value::Integer(n) => n as $t,
                        Value::Float(n) => n as $t,
                        Value::Blob(_) => Default::default(),
                        Value::String(s) => s.parse::<$t>().unwrap_or_default()
                    }
                }
            }
        )*
    }
}

impl_from_value!(u8, i8, u16, i16, u32, i32, u64, i64, f32, f64, usize);

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
    fn parse(self, input: &'a [u8]) -> IResult<&'a [u8], Value> {
        match self {
            RecordCode::Null => Ok((input, Value::Null)),
            RecordCode::I8 => {
                let (input, n) = i8(input)?;
                Ok((input, Value::Integer(n.into())))
            }

            RecordCode::I16 => {
                let (input, n) = be_i16(input)?;
                Ok((input, Value::Integer(n.into())))
            }
            RecordCode::I24 => {
                let (input, n) = be_i24(input)?;
                Ok((input, Value::Integer(n.into())))
            }
            RecordCode::I32 => {
                let (input, n) = be_i32(input)?;
                Ok((input, Value::Integer(n.into())))
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
                Ok((input, Value::Integer(x as i64)))
            }
            RecordCode::I64 => {
                let (input, n) = be_i64(input)?;
                Ok((input, Value::Integer(n.into())))
            }
            RecordCode::F64 => {
                let (input, n) = be_f64(input)?;
                Ok((input, Value::Float(n)))
            }
            RecordCode::Zero => Ok((input, Value::Integer(0))),
            RecordCode::One => Ok((input, Value::Integer(1))),
            RecordCode::Blob(n) => {
                let (input, b) = take(n)(input)?;
                Ok((input, Value::Blob(b.to_vec())))
            }
            RecordCode::String(n) => {
                let (input, s) = take(n)(input)?;
                Ok((input, Value::String(String::from_utf8_lossy(s).to_string())))
            }
        }
    }
}

/// Parse a [`Cell`][crate::cells::Cell] payload into a series of [`Value`]s.
pub fn parse_payload<'a>(input: &'a [u8]) -> IResult<&'a [u8], Vec<Value>> {
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
