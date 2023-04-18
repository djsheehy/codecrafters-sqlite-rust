use nom::{bytes::complete::take_while_m_n, number::complete::u8, IResult};

/// Parse a variable sized integer (varint) based on SQLite's format.
pub fn varint(input: &[u8]) -> IResult<&[u8], u64> {
    // get bytes with high bit set
    let (input, hibytes) = take_while_m_n(0, 8, |b| b >= 0x80)(input)?;
    let mut ans: u64 = 0;
    for b in hibytes {
        ans = (ans << 7) | (b & 0x7f) as u64;
    }
    let (input, last) = u8(input)?;
    // if there are 9 bits, get all 8 bits of the last byte
    // otherwise get 7 bits of last byte
    if hibytes.len() == 8 {
        ans = (ans << 8) | last as u64;
    } else {
        ans = (ans << 7) | (last & 0x7f) as u64;
    }
    Ok((input, ans))
}

#[cfg(test)]
fn assert_varint(input: &[u8], expected: u64) {
    let (_, answer) = varint(input).unwrap();
    assert_eq!(answer, expected);
}

#[test]
fn test_varint() {
    assert_varint(&[0x00], 0);
    assert_varint(&[0x7f], 0x7f);
    assert_varint(&[0b1_0000001, 0b0_0000000], 0x80);
    assert_varint(&[0b1_1111111, 0b0_1111111], 0b1111111_1111111);
    assert_varint(
        &[0b1_1010101, 0b1_0011001, 0b1_1110011, 0b0_1001100],
        0b1010101_0011001_1110011_1001100,
    );
    assert_varint(
        &[
            0b1_1111111,
            0b1_0000000,
            0b1_1111111,
            0b1_0000000,
            0b1_1111111,
            0b1_0000000,
            0b1_1111111,
            0b1_0000000,
            0b11111111,
        ],
        0b1111111_0000000_1111111_0000000_1111111_0000000_1111111_0000000_11111111,
    );
}
