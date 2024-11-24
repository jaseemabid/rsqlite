//! # A very naive SQLite database reader.
//!
//! A SQLite [Database] is a sequence of [Page]s. The first 100 bytes of the
//! first [Page] contains a [Header] with global metadata.
//!
//! Each [Page] can be one of the 5 types, but only [TableLeaf] is implemented
//! for now to handle the simplest databases. [TableLeaf] page starts with a
//! [BTreePageHeader], followed by [TableLeaf::cell_pointers] pointing to
//! [TableLeaf::cells] with actual data.
//!
//! [TableLeafCell] holds metadata like `row_id` and `size` for a database row,
//! along with a [Record] containing ([SerialType], [SerialValue]) pairs holding
//! data itself.

use crate::varint::VarInt;
use binrw::{file_ptr::parse_from_iter, helpers::args_iter_with, io::SeekFrom, *};
use std::io::{Cursor, Read, Seek};

/** A SQLite Database */
#[derive(BinRead, Debug, PartialEq)]
#[br(big)]
pub struct Database {
    /// A database starts with a header ...
    pub db_header: Header,

    /// ... followed by a number of pages.
    // The header is part of first page, so start from the beginning again.
    #[br(seek_before = SeekFrom::Start(0),
         align_before = db_header.page_size,
         count = db_header.database_page_count)]
    pub pages: Vec<Page>,
}

/**
 * A page can be of 5 types as described
 * [here](https://www.sqlite.org/fileformat2.html#pages), but only table leaf
 * pages are implemented for now.
 *
 * 1. B tree page
 *      1. Table interior
 *      2. Table leaf [TableLeaf] ⭐
 *      3. Index interior
 *      4. Index leaf
 * 2. Freelist page
 *      1. Trunk Page
 *      2. Leaf Page
 * 3. Payload overflow page
 * 4. A pointer map page
 * 5. The lock-byte page
 */
#[derive(BinRead, Debug, PartialEq)]
#[br(big)]
pub enum Page {
    TableLeaf(TableLeaf),
}

/**
 * A B tree table leaf page is divided into regions in the following order
 *
 * 1. The 100-byte database file [Header] (found on page 1 only)
 * 2. The 8 or 12 byte [b-tree page header][BTreePageHeader]
 * 3. The cell pointer array
 * 4. Unallocated space
 * 5. The cell content area
 * 6. The reserved region
 *
 * See more [docs](https://www.sqlite.org/fileformat2.html#b_tree_pages)
 *
 * TODO: `#[binread]` instead of `#[derive(BinRead)]` breaks markdown docs on
 * the type. Fix this upstream.
 */
#[binread]
#[br(big, stream = s)]
#[derive(Debug, PartialEq)]
pub struct TableLeaf {
    // Page start offset for internal offset calculations.
    //
    // `cell_pointers` are offsets from the start of the page, so store starting
    // offset here which can be referenced later.
    #[br(temp, try_calc = s.stream_position())]
    _page_start: u64,

    /// DB Header is only present on first page
    #[br(try)]
    pub db_header: Option<Header>,

    /// Page header
    pub page_header: BTreePageHeader,

    // 🎉 It's really cool that previous values can be referred for count. binrw is awesome!
    #[br(count = page_header.num_cells)]

    /// The cell pointer array is K 2-byte integer offsets to the cell contents.
    pub cell_pointers: Vec<u16>,

    /// [ Unallocated space ]

    /// Cells with metadata + (type, value) pairs in a record
    #[br(parse_with = parse_from_iter(cell_pointers.iter().copied()),
          seek_before(SeekFrom::Start(_page_start)))]
    pub cells: Vec<TableLeafCell>,
}

/**
 * The first 100 bytes of the database file comprise the database file header.
 *
 * - [Docs](https://www.sqlite.org/fileformat.html#the_database_header)
 * - [SQLite Source](https://github.com/sqlite/sqlite/blob/e69b4d7/src/btreeInt.h#L45-L82)
 */
#[derive(BinRead, Copy, Clone, Debug, PartialEq)]
#[br(big, magic = b"SQLite format 3\0")]
pub struct Header {
    pub page_size: u16,            // Page size in bytes.  (1 means 65536)
    pub write_format: u8,          // File format write version
    pub read_format: u8,           // File format read version
    pub reserved_bytes: u8,        // Bytes of unused space at the end of each page
    pub max_payload_fraction: u8,  // Maximum embedded payload fraction
    pub min_payload_fraction: u8,  // Minimum embedded payload fraction
    pub leaf_payload_fraction: u8, // Min leaf payload fraction
    pub file_change_counter: u32,  // File change counter
    pub database_page_count: u32,  // Size of the database in pages
    pub freelist_trunk_page: u32,  // First freelist page
    pub freelist_page_count: u32,  // Number of freelist pages in file
    pub schema_cookie: u32,        // Schema cookie
    pub schema_format: u32,        // Schema format number
    pub default_page_cache: u32,   // Default page cache size
    pub autovacuum_top_root: u32,  // Largest root b-tree page when in auto-vacuum
    pub text_encoding: u32,        // The database text encoding.
    pub user_version: u32,         // User version
    pub incremental_vacuum: u32,   // True (non-zero) for incremental-vacuum mode
    pub application_id: u32,       // The "Application ID" set by PRAGMA application_id.
    pub reserved: [u8; 20],        // Reserved for expansion. Must be zero.
    // TODO: Unsure if this is equal to `data version`
    pub version_valid_for: u32, // The version-valid-for number.
    pub sqlite_version: u32,    // SQLITE_VERSION_NUMBER
}

/**
 * B tree Page Header Format
 *
 * [Docs](https://www.sqlite.org/fileformat2.html#b_tree_pages)
 *
 * | Offset | Size | Description                                                         |
 * |--------|------|---------------------------------------------------------------------|
 * | 0      | 1    | The one-byte flag indicating the b-tree page type:                  |
 * |        |      | - 2 (0x02): Interior index b-tree page                              |
 * |        |      | - 5 (0x05): Interior table b-tree page                              |
 * |        |      | - 10 (0x0a): Leaf index b-tree page                                 |
 * |        |      | - 13 (0x0d): Leaf table b-tree page                                 |
 * |        |      | Any other value is an error.                                        |
 * | 1      | 2    | Start of the first freeblock (0 if none).                           |
 * | 3      | 2    | Number of cells on the page.                                        |
 * | 5      | 2    | Start of the cell content area (0 interpreted as 65536).            |
 * | 7      | 1    | Number of fragmented free bytes in the cell content area.           |
 * | 8      | 4    | Right-most pointer (interior b-tree pages only, omitted otherwise). |
 */
#[derive(BinRead, Debug, PartialEq)]
#[br(big)]
pub struct BTreePageHeader {
    pub page_type: PageType,
    pub first_freeblock: u16,
    pub num_cells: u16,
    pub cell_content_start: u16,
    pub fragmented_free_bytes: u8,
    // Only present for interior pages
    #[br(if(page_type == PageType::InteriorIndex || page_type == PageType::InteriorTable))]
    pub right_most_pointer: Option<u32>,
}

/**
 * A b-tree page is either an interior page or a leaf page.
 */
#[derive(BinRead, Debug, PartialEq)]
#[br(repr(u8))]
pub enum PageType {
    // An interior page contains K keys together with K+1 pointers to child
    // b-tree pages. A "pointer" in an interior b-tree page is just the 32-bit
    // unsigned integer page number of the child page.
    InteriorIndex = 0x02,
    InteriorTable = 0x05,
    // A leaf page contains keys ...
    LeafIndex = 0x0a,
    // ... and in the case of a table b-tree each key  has associated data
    LeafTable = 0x0d,
}

/**
 * Leaf cell for a [PageType::LeafTable]
 *
 * A cell represents a row in the database. Each cell has 4 regions in the
 * following order.
 *
 * 1. A varint for the total number of bytes of payload, including any overflow
 * 2. A varint which is the integer key, a.k.a. `rowid`
 * 3. The initial portion of the payload that does not spill to overflow pages.
 * 4. A 4-byte big-endian integer page number for the first page of the overflow
 *    page list - omitted if all payload fits on the b-tree page.
 */
#[derive(BinRead, Debug, PartialEq)]
#[br(big)]
pub struct TableLeafCell {
    pub size: VarInt,
    pub row_id: VarInt,
    pub record: Record,
}

/**
 * A Record holds the contents of a row along with type info.
 *
 * [See schema layer docs](https://www.sqlite.org/fileformat2.html#schema_layer) for more info.
 */
#[derive(BinRead, Debug, PartialEq)]
#[br(big)]
pub struct Record {
    /// The header begins with a single varint which determines the total number
    /// of bytes in the header. The varint value is the size of the header in
    /// bytes including the size varint itself.
    pub header_size: VarInt,

    /// Following the size varint are one or more additional varints, one per
    /// column. These additional varints are called "serial type" numbers and
    /// determine the datatype of each column
    // There is a lot going on here!
    //   1. Since the size of SerialType is variadic, you can't tell upfront how
    //      many of them will be parsed here.
    //   2. So read the expected number of bytes first into a buffer
    //   3. Parse this temp buffer till it is exhausted.
    //   4. Varints make this code far trickier, could have been a trivial
    //      (count=N) with fixed size numbers
    #[br(count = header_size.value - header_size.width,
        map = |buffer: Vec<u8>| -> Vec<SerialType> {
            let mut cursor = Cursor::new(buffer);
            std::iter::from_fn(|| cursor.read_be().ok()).collect()
        })]
    pub columns: Vec<SerialType>,

    /// Payload cells, based on types inferred from the `columns`
    #[br(parse_with = args_iter_with(&columns, |reader, options, kind|
            SerialValue::read_options(reader, options, *kind)))]
    pub payload: Vec<SerialValue>,
}

/**
 * Serial types for parsing cell contents
 *
 * Types are described [here](https://www.sqlite.org/fileformat2.html#record_format)
 *
 * - 0-9 maps the first constants
 * - 10,11 are reserved
 * - N > 12 and even for blobs
 * - N > 13 and odd for strings
 */
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum SerialType {
    Null,
    I8,
    I16,
    I24,
    I32,
    I48,
    I64,
    Float,
    Zero,
    One,
    Reserved,
    String(usize),
    Blob(usize),
}

/**
 * Serial values holding table data.
 */
#[derive(Debug, PartialEq)]
pub enum SerialValue {
    Null,
    Number(i64),
    Float(f64),
    Reserved,
    String(String),
    Blob(Vec<u8>),
}

impl BinRead for SerialType {
    type Args<'a> = ();

    fn read_options<R: Read + Seek>(r: &mut R, _: Endian, _: Self::Args<'_>) -> BinResult<Self> {
        // TODO: Figure out how to pass [endian] through.
        let magic = VarInt::read_be(r)?;

        match usize::try_from(magic.value).unwrap() {
            0 => Ok(SerialType::Null),
            1 => Ok(SerialType::I8),
            2 => Ok(SerialType::I16),
            3 => Ok(SerialType::I24),
            4 => Ok(SerialType::I32),
            5 => Ok(SerialType::I48),
            6 => Ok(SerialType::I64),
            7 => Ok(SerialType::Float),
            8 => Ok(SerialType::Zero),
            9 => Ok(SerialType::One),
            10 | 11 => Ok(SerialType::Reserved),
            // Even: Blob with (N-12)/2 bytes
            m if m % 2 == 0 => Ok(SerialType::Blob((m - 12) / 2)),
            // Odd: String with (N-13)/2 bytes
            m => Ok(SerialType::String((m - 13) / 2)),
        }
    }
}

impl BinRead for SerialValue {
    type Args<'a> = SerialType;

    fn read_options<R: Read + Seek>(r: &mut R, _: Endian, serial_type: Self::Args<'_>) -> BinResult<Self> {
        use {SerialType as T, SerialValue as V};

        match serial_type {
            T::Null => Ok(V::Null),
            T::I8 => Ok(V::Number(i8::read_be(r)?.into())),
            T::I16 => Ok(V::Number(i16::read_be(r)?.into())),
            T::I24 => Ok(V::Number(read_u24_be(r)?.into())),
            T::I32 => Ok(V::Number(i32::read_be(r)?.into())),
            T::I48 => Ok(V::Number(read_i48_be(r)?)),
            T::I64 => Ok(V::Number(i64::read_be(r)?)),
            T::Float => Ok(V::Float(f64::read_be(r)?)),
            T::Zero => Ok(V::Number(0)),
            T::One => Ok(V::Number(1)),
            T::Reserved => Ok(V::Reserved),
            T::String(n) => {
                let mut buf = vec![0; n];
                r.read_exact(&mut buf)?;
                let str = String::from_utf8(buf).map_err(|err| binrw::Error::Custom {
                    pos: r.stream_position().unwrap_or_default(),
                    err: Box::new(format!("Invalid String: {err}")),
                })?;
                Ok(V::String(str))
            }
            T::Blob(n) => {
                let mut buf = vec![0; n];
                r.read_exact(&mut buf)?;
                Ok(V::Blob(buf))
            }
        }
    }
}

// * Helper functions and Traits * //

fn read_u24_be<R: Read>(r: &mut R) -> BinResult<u32> {
    let mut buf = [0u8; 3];
    r.read_exact(&mut buf)?;
    Ok(u32::from_be_bytes([0, buf[0], buf[1], buf[2]]))
}

fn read_i48_be<R: Read>(r: &mut R) -> BinResult<i64> {
    let mut buf = [0u8; 6];
    r.read_exact(&mut buf)?;
    Ok(i64::from_be_bytes([
        0, 0, buf[0], buf[1], buf[2], buf[3], buf[4], buf[5],
    ]))
}

// * Tests * //

#[cfg(test)]
mod planets {
    use super::{SerialType as T, *};
    use pretty_assertions::assert_eq;
    use std::fs::File;

    impl From<&str> for SerialValue {
        fn from(value: &str) -> Self {
            SerialValue::String(value.to_string())
        }
    }

    impl From<i64> for SerialValue {
        fn from(value: i64) -> Self {
            SerialValue::Number(value)
        }
    }

    impl From<Vec<u8>> for SerialValue {
        fn from(value: Vec<u8>) -> Self {
            SerialValue::Blob(value)
        }
    }

    impl From<()> for SerialValue {
        fn from(_: ()) -> Self {
            SerialValue::Null
        }
    }

    // $ sqlite3 data/planets.db .dbinfo
    const DB_HEADER: Header = Header {
        page_size: 4096,
        write_format: 1,
        read_format: 1,
        reserved_bytes: 0,
        max_payload_fraction: 64,
        min_payload_fraction: 32,
        leaf_payload_fraction: 32,
        file_change_counter: 2,
        database_page_count: 2,
        freelist_trunk_page: 0, // Not in the .dbinfo
        freelist_page_count: 0,
        schema_cookie: 1,
        schema_format: 4,
        default_page_cache: 0,
        autovacuum_top_root: 0,
        incremental_vacuum: 0,
        text_encoding: 1,
        user_version: 0,
        application_id: 0,
        reserved: [0; 20],
        version_valid_for: 2,
        sqlite_version: 3047000,
    };

    #[test]
    fn read_database() {
        let mut file = File::open("data/planets.db").expect("Failed to open planets.db");
        let database: Database = file.read_be().expect("Failed to read db header at start of file");

        assert_eq!(database.db_header, DB_HEADER);
        assert_eq!(database.pages.len(), DB_HEADER.database_page_count as usize);
    }

    #[test]
    fn read_page_1() {
        let mut file = File::open("data/planets.db").expect("Failed to open planets.db");
        let db: Database = file.read_be().expect("Failed to parse 1st page");

        /*
        # Schema table
        https://www.sqlite.org/fileformat2.html#storage_of_the_sql_database_schema

        Page 1 of a database file is the root page of a table b-tree that holds
        a special table named "sqlite_schema". This b-tree is known as the
        "schema table" since it stores the complete database schema. The
        structure of the sqlite_schema table is as if it had been created using
        the following SQL:

            CREATE TABLE sqlite_schema(
                type text,
                name text,
                tbl_name text,
                rootpage integer,
                sql text
            );
         */

        let sql_file = include_bytes!("../data/planets.sql");
        let query = SerialValue::String(String::from_utf8_lossy(&sql_file[0..189]).into_owned());

        assert_eq!(
            db.pages[0],
            Page::TableLeaf(TableLeaf {
                db_header: Some(DB_HEADER),
                page_header: BTreePageHeader {
                    page_type: PageType::LeafTable,
                    first_freeblock: 0,
                    num_cells: 1,
                    cell_content_start: 3877,
                    fragmented_free_bytes: 0,
                    right_most_pointer: None
                },
                cell_pointers: vec![3877],
                cells: vec![TableLeafCell {
                    size: VarInt { value: 216, width: 2 },
                    row_id: VarInt::new(1),
                    record: Record {
                        header_size: VarInt::new(7),
                        columns: vec![T::String(5), T::String(7), T::String(7), T::I8, T::String(189)],
                        payload: vec!["table".into(), "planets".into(), "planets".into(), 2.into(), query]
                    }
                }],
            })
        );
    }

    #[test]
    fn read_page_2() {
        let mut file = File::open("data/planets.db").expect("Failed to open planets.db");
        let db: Database = file.read_be().expect("Failed to parse 2nd page");

        let page_header = BTreePageHeader {
            page_type: PageType::LeafTable,
            first_freeblock: 0,
            num_cells: 8,
            cell_content_start: 3836,
            fragmented_free_bytes: 0,
            right_most_pointer: None,
        };

        let cell_pointers = vec![4063, 4032, 4001, 3970, 3937, 3905, 3871, 3836];

        let cells = vec![
            TableLeafCell {
                size: VarInt::new(31),
                row_id: VarInt::new(1),
                record: Record {
                    header_size: VarInt::new(7),
                    // TODO: 🔥 This null byte at the start of column is a mystery
                    columns: vec![T::Null, T::String(7), T::String(11), T::I16, T::I32, T::Zero],
                    payload: vec![
                        ().into(),
                        "Mercury".into(),
                        "Terrestrial".into(),
                        4879.into(),
                        57910000.into(),
                        0.into(),
                    ],
                },
            },
            TableLeafCell {
                size: VarInt::new(29),
                row_id: VarInt::new(2),
                record: Record {
                    header_size: VarInt::new(7),
                    columns: vec![T::Null, T::String(5), T::String(11), T::I16, T::I32, T::Zero],
                    payload: vec![
                        ().into(),
                        "Venus".into(),
                        "Terrestrial".into(),
                        12104.into(),
                        108200000.into(),
                        0.into(),
                    ],
                },
            },
            TableLeafCell {
                size: VarInt::new(29),
                row_id: VarInt::new(3),
                record: Record {
                    header_size: VarInt::new(7),
                    columns: vec![T::Null, T::String(5), T::String(11), T::I16, T::I32, T::One],
                    payload: vec![
                        ().into(),
                        "Earth".into(),
                        "Terrestrial".into(),
                        12742.into(),
                        149600000.into(),
                        1.into(),
                    ],
                },
            },
            TableLeafCell {
                size: VarInt::new(29),
                row_id: VarInt::new(4),
                record: Record {
                    header_size: VarInt::new(7),
                    columns: vec![T::Null, T::String(4), T::String(11), T::I16, T::I32, T::I8],
                    payload: vec![
                        ().into(),
                        "Mars".into(),
                        "Terrestrial".into(),
                        6779.into(),
                        227900000.into(),
                        2.into(),
                    ],
                },
            },
            TableLeafCell {
                size: VarInt::new(31),
                row_id: VarInt::new(5),
                record: Record {
                    header_size: VarInt::new(7),
                    columns: vec![T::Null, T::String(7), T::String(9), T::I24, T::I32, T::I8],
                    payload: vec![
                        ().into(),
                        "Jupiter".into(),
                        "Gas Giant".into(),
                        139820.into(),
                        778500000.into(),
                        79.into(),
                    ],
                },
            },
            TableLeafCell {
                size: VarInt::new(30),
                row_id: VarInt::new(6),
                record: Record {
                    header_size: VarInt::new(7),
                    columns: vec![T::Null, T::String(6), T::String(9), T::I24, T::I32, T::I8],
                    payload: vec![
                        ().into(),
                        "Saturn".into(),
                        "Gas Giant".into(),
                        116460.into(),
                        1433000000.into(),
                        83.into(),
                    ],
                },
            },
            TableLeafCell {
                size: VarInt::new(32),
                row_id: VarInt::new(7),
                record: Record {
                    header_size: VarInt::new(7),
                    columns: vec![T::Null, T::String(6), T::String(9), T::I24, T::I48, T::I8],
                    payload: vec![
                        ().into(),
                        "Uranus".into(),
                        "Ice Giant".into(),
                        50724.into(),
                        2871000000.into(),
                        27.into(),
                    ],
                },
            },
            TableLeafCell {
                size: VarInt::new(33),
                row_id: VarInt::new(8),
                record: Record {
                    header_size: VarInt::new(7),
                    columns: vec![T::Null, T::String(7), T::String(9), T::I24, T::I48, T::I8],
                    payload: vec![
                        ().into(),
                        "Neptune".into(),
                        "Ice Giant".into(),
                        49244.into(),
                        4495000000.into(),
                        14.into(),
                    ],
                },
            },
        ];

        assert_eq!(
            db.pages[1],
            Page::TableLeaf(TableLeaf {
                db_header: None,
                page_header,
                cell_pointers,
                cells
            })
        );
    }
}
