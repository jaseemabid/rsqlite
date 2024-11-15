use binrw::*;

/**
 * DB Header
 *
 * https://www.sqlite.org/fileformat.html#the_database_header
 *
 * The first 100 bytes of the database file comprise the database file header.
 *
 * Source: https://github.com/sqlite/sqlite/blob/e69b4d7/src/btreeInt.h#L45-L82
 */

#[derive(Debug, PartialEq)]
#[binread]
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

#[cfg(test)]
mod planets {
    use super::*;
    use std::fs::File;

    #[test]
    fn test_dbinfo() {
        // $ sqlite3 data/planets.db .dbinfo
        let expect = Header {
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

        let mut file = File::open("data/planets.db").expect("Failed to open planets.db");
        let header: Header = file.read_be().expect("Failed to read header");

        assert_eq!(header, expect);
    }
}
