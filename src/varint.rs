use binrw::{BinRead, BinResult as Result, Endian};
use std::io::{Read, Seek};

/// Variable length u64 integers
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct VarInt {
    /// Value of varint
    pub value: u64,
    /// Number of bytes required to encode the value
    pub width: u64,
}

impl VarInt {
    pub fn new(value: u64) -> Self {
        assert!(value < 128);
        VarInt { value, width: 1 }
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
                reader.read_exact(&mut buf)?;
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
