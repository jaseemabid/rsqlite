use binrw::BinResult;

/**
 * Variable length integers
 */
pub type VarInt = usize;

/// A custom parser for VarInt
#[binrw::parser(reader)]
pub fn varint() -> BinResult<VarInt> {
    // let what = <_>::read_options(reader, endian, ());

    let mut value: usize = 0;
    for i in 0..9 {
        let byte = {
            let mut buf = [0u8; 1];
            reader.read_exact(&mut buf)?;
            buf[0]
        };

        value = (value << 7) | ((byte & 0x7F) as usize);

        // If the high-order bit is clear, we've reached the end of the varint.
        if byte & 0x80 == 0 {
            return Ok(value);
        }

        // If this is the 9th byte, include all 8 bits.
        if i == 8 {
            return Ok((value << 8) | (byte as usize));
        }
    }

    // If we exit the loop, this is an error (invalid varint format).
    Err(binrw::Error::AssertFail {
        pos: reader.stream_position()?,
        message: "Invalid varint format".into(),
    })
}
