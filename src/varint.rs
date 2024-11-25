use binrw::{BinRead, BinResult as Result, Endian};
use std::io::{Read, Seek};

/// Variable length u64 integers
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct VarInt {
    /// Value of varint
    pub value: u64,
    /// Number of bytes required to encode the value
    pub width: u8,
}

impl VarInt {
    pub fn new(value: u64) -> Self {
        let width = VarInt::encode(value).len() as u8;
        assert!(width <= 9);
        VarInt { value, width }
    }

    pub fn encode(value: u64) -> Vec<u8> {
        let mut buf = [0u8; 10];
        let mut n = 0;
        let mut value = value;

        // Build bytes in reverse order
        while value != 0 {
            buf[n] = ((value & 0x7f) as u8) | 0x80;
            n += 1;
            value >>= 7;
        }

        if n == 0 {
            return vec![0];
        }

        // Clear high bit of what will be the last byte
        buf[0] &= 0x7f;

        // Create result array with bytes in correct order
        let mut result = Vec::with_capacity(n);
        for i in (0..n).rev() {
            result.push(buf[i]);
        }

        result
    }
}

/// A custom parser for VarInt
///
/// Note: Implicitly big endian, even though not specified anywhere
impl BinRead for VarInt {
    type Args<'a> = ();

    fn read_options<R: Read + Seek>(reader: &mut R, _: Endian, _: Self::Args<'_>) -> Result<Self> {
        let mut value: u64 = 0;
        for i in 0..9 {
            let width = i + 1;
            let byte = {
                let mut buf = [0u8; 1];
                reader.read_exact(&mut buf).map_err(binrw::Error::Io)?;
                buf[0]
            };

            // Shift 7 bits left ++ 7 low order bits of byte
            value = (value << 7) | ((byte & 0x7F) as u64);

            // If the high-order bit is clear, we've reached the end of the varint.
            if byte & 0x80 == 0 {
                return Ok(VarInt { value, width });
            }

            // If this is the 9th byte, include all 8 bits.
            if i == 8 {
                value = (value << 8) | (byte as u64);
                return Ok(VarInt { value, width });
            }
        }

        // If we exit the loop, this is an error (invalid varint format).
        Err(binrw::Error::AssertFail {
            pos: reader.stream_position()?,
            message: "Invalid varint format".into(),
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn decode() {
        let t = vec![
            (VarInt::new(0), vec![0]), // Tiny numbers
            (VarInt::new(1), vec![1]),
            (VarInt::new(26), vec![26]),
            (VarInt::new(26), vec![26, 0, 1, 2, 3]), // Ignore trailing bytes
            (VarInt::new(127), vec![127]),           // Ignore trailing bytes
            (VarInt::new(128), vec![0x81, 0x00]),    // 2 bytes
            (VarInt::new(200), vec![0x81, 0x48]),    // 2 bytes
            (VarInt::new(9901644408), vec![0xA4, 0xF1, 0xBC, 0xB4, 0x78]),
        ];

        for (exp, buf) in t.into_iter() {
            assert_eq!(
                exp,
                VarInt::read_be(&mut Cursor::new(buf)).expect("Failed to parse into varint")
            )
        }
    }

    #[test]
    fn encode() {
        let t = vec![
            (0, vec![0]),
            (1, vec![1]),
            (127, vec![0x7f]),
            (128, vec![0x81, 0x00]),
            (216, vec![0x81, 0x58]),
        ];

        for (num, exp) in t.into_iter() {
            assert_eq!(VarInt::encode(num), exp)
        }
    }
}
