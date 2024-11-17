mod varint;

use binrw::{io::SeekFrom, *};
use varint::{varint, VarInt};

/**
 * DB Header
 *
 * https://www.sqlite.org/fileformat.html#the_database_header
 *
 * The first 100 bytes of the database file comprise the database file header.
 *
 * Source: https://github.com/sqlite/sqlite/blob/e69b4d7/src/btreeInt.h#L45-L82
 */

#[derive(BinRead, Debug, PartialEq)]
#[br(big, magic = b"SQLite format 3\0")]
pub struct Header {
    page_size: u16,            // Page size in bytes.  (1 means 65536)
    write_format: u8,          // File format write version
    read_format: u8,           // File format read version
    reserved_bytes: u8,        // Bytes of unused space at the end of each page
    max_payload_fraction: u8,  // Maximum embedded payload fraction
    min_payload_fraction: u8,  // Minimum embedded payload fraction
    leaf_payload_fraction: u8, // Min leaf payload fraction
    file_change_counter: u32,  // File change counter
    database_page_count: u32,  // Size of the database in pages
    freelist_trunk_page: u32,  // First freelist page
    freelist_page_count: u32,  // Number of freelist pages in file
    schema_cookie: u32,        // Schema cookie
    schema_format: u32,        // Schema format number
    default_page_cache: u32,   // Default page cache size
    autovacuum_top_root: u32,  // Largest root b-tree page when in auto-vacuum
    text_encoding: u32,        // The database text encoding.
    user_version: u32,         // User version
    incremental_vacuum: u32,   // True (non-zero) for incremental-vacuum mode
    application_id: u32,       // The "Application ID" set by PRAGMA application_id.
    reserved: [u8; 20],        // Reserved for expansion. Must be zero.
    // TODO: Unsure if this is equal to `data version`
    version_valid_for: u32, // The version-valid-for number.
    sqlite_version: u32,    // SQLITE_VERSION_NUMBER
}

/**
 * A page can be of 5 types:
 *
 * 1. B tree page
 *      Table interior |  Table leaf  | Index interior | Index leaf
 * 2. Freelist page
 *      Trunk Page | Leaf Page
 * 3. Payload overflow page
 * 4. A pointer map page
 * 5. The lock-byte page
 *
 * https://www.sqlite.org/fileformat.html#pages
 */
#[derive(BinRead, Debug, PartialEq)]
#[br(big)]
pub enum Page {
    TableLeaf(TableLeaf),
}

/**
 * A B tree table leaf page is divided into regions in the following order
 *
 * 1. The 100-byte database file header (found on page 1 only)
 * 2. The 8 or 12 byte b-tree page header
 * 3. The cell pointer array
 * 4. Unallocated space
 * 5. The cell content area
 * 6. The reserved region
 *
 * See more docs https://www.sqlite.org/fileformat.html#b_tree_pages
 */

#[derive(BinRead, Debug, PartialEq)]
#[br(big)]
pub struct TableLeaf {
    #[br(try)]
    pub db_header: Option<Header>, // DB Header is only present on first page
    page_header: BTreePageHeader,
    // 🎉 It's really cool that previous values can be referred for count. binrw is awesome!
    #[br(count = page_header.num_cells)]
    // The cell pointer array is K 2-byte integer offsets to the cell contents.
    cell_pointers: Vec<u16>,
    // [ Unallocated space ]
    #[br(calc = *cell_pointers.last().unwrap())]
    unallocated_: u16,
    // 🔥 TODO: Fix seek offset, this should't have a 4096 in here.
    #[br(seek_before = SeekFrom::Start(4096 + *cell_pointers.last().unwrap() as u64),
         count = cell_pointers.len())]
    cells: Vec<TableLeafCell>,
}

/**
 *
 * B tree Page Header Format
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
 * Leaf cell for a PageType::LeafTable
 *
 * Each cell has 4 regions in the following order.
 *
 * 1. A varint for the total number of bytes of payload, including any overflow
 * 2. A varint which is the integer key, a.k.a. "rowid"
 * 3. The initial portion of the payload that does not spill to overflow pages.
 * 4. A 4-byte big-endian integer page number for the first page of the overflow
 *    page list - omitted if all payload fits on the b-tree page.
 */
#[derive(BinRead, Debug, PartialEq)]
#[br(big)]
pub struct TableLeafCell {
    #[br(parse_with = varint)]
    pub size: VarInt,
    #[br(parse_with = varint)]
    pub row_id: VarInt,
    #[br(count = size)] // 🪄 This is NEAT!
    pub payload: Vec<u8>,
}

#[cfg(test)]
mod planets {
    use super::*;
    use io::Seek;
    use std::fs::File;

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
    fn test_db_header() {
        let mut file = File::open("data/planets.db").expect("Failed to open planets.db");
        let header: Header = file
            .read_be()
            .expect("Failed to read db header at start of file");

        assert_eq!(header, DB_HEADER);
    }

    #[test]
    fn test_btree_page_1() {
        let mut file = File::open("data/planets.db").expect("Failed to open planets.db");
        let page: Page = file.read_be().expect("Failed to parse 1st page");

        assert_eq!(
            page,
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
                unallocated_: 3877,
                cells: vec![TableLeafCell {
                    size: 3,
                    row_id: 5,
                    payload: vec![1, 85, 114]
                }]
            })
        );
    }

    #[test]
    fn test_btree_page_2() {
        let mut file = File::open("data/planets.db").expect("Failed to open planets.db");

        // Seek ahead to 2nd page, which should be a btree leaf for planets.db
        file.seek(io::SeekFrom::Start(4096))
            .expect("Failed to seek to second page");

        let page: Page = file.read_be().expect("Failed to parse 2nd page");

        assert_eq!(
            page,
            Page::TableLeaf(TableLeaf {
                db_header: None,
                page_header: BTreePageHeader {
                    page_type: PageType::LeafTable,
                    first_freeblock: 0,
                    num_cells: 8,
                    cell_content_start: 3836,
                    fragmented_free_bytes: 0,
                    right_most_pointer: None,
                },
                cell_pointers: vec![4063, 4032, 4001, 3970, 3937, 3905, 3871, 3836],
                unallocated_: 3836,
                cells: vec![
                    TableLeafCell {
                        size: 33,
                        row_id: 8,
                        payload: vec![
                            7, 0, 27, 31, 3, 5, 1, 78, 101, 112, 116, 117, 110, 101, 73, 99, 101,
                            32, 71, 105, 97, 110, 116, 0, 192, 92, 0, 1, 11, 236, 65, 192, 14
                        ]
                    },
                    TableLeafCell {
                        size: 32,
                        row_id: 7,
                        payload: vec![
                            7, 0, 25, 31, 3, 5, 1, 85, 114, 97, 110, 117, 115, 73, 99, 101, 32, 71,
                            105, 97, 110, 116, 0, 198, 36, 0, 0, 171, 31, 251, 192, 27
                        ]
                    },
                    TableLeafCell {
                        size: 30,
                        row_id: 6,
                        payload: vec![
                            7, 0, 25, 31, 3, 4, 1, 83, 97, 116, 117, 114, 110, 71, 97, 115, 32, 71,
                            105, 97, 110, 116, 1, 198, 236, 85, 105, 216, 64, 83
                        ]
                    },
                    TableLeafCell {
                        size: 31,
                        row_id: 5,
                        payload: vec![
                            7, 0, 27, 31, 3, 4, 1, 74, 117, 112, 105, 116, 101, 114, 71, 97, 115,
                            32, 71, 105, 97, 110, 116, 2, 34, 44, 46, 102, 247, 160, 79
                        ]
                    },
                    TableLeafCell {
                        size: 29,
                        row_id: 4,
                        payload: vec![
                            7, 0, 21, 35, 2, 4, 1, 77, 97, 114, 115, 84, 101, 114, 114, 101, 115,
                            116, 114, 105, 97, 108, 26, 123, 13, 149, 122, 96, 2
                        ]
                    },
                    TableLeafCell {
                        size: 29,
                        row_id: 3,
                        payload: vec![
                            7, 0, 23, 35, 2, 4, 9, 69, 97, 114, 116, 104, 84, 101, 114, 114, 101,
                            115, 116, 114, 105, 97, 108, 49, 198, 8, 234, 183, 0
                        ]
                    },
                    TableLeafCell {
                        size: 29,
                        row_id: 2,
                        payload: vec![
                            7, 0, 23, 35, 2, 4, 8, 86, 101, 110, 117, 115, 84, 101, 114, 114, 101,
                            115, 116, 114, 105, 97, 108, 47, 72, 6, 115, 0, 64
                        ]
                    },
                    TableLeafCell {
                        size: 31,
                        row_id: 1,
                        payload: vec![
                            7, 0, 27, 35, 2, 4, 8, 77, 101, 114, 99, 117, 114, 121, 84, 101, 114,
                            114, 101, 115, 116, 114, 105, 97, 108, 19, 15, 3, 115, 162, 240
                        ]
                    }
                ]
            })
        );
    }
}
