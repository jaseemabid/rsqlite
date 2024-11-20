use crate::*;
use std::fmt;

impl fmt::Display for Header {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "database page size:  {}",
            if self.page_size == 1 {
                65536
            } else {
                self.page_size as u32
            }
        )?;
        writeln!(f, "write format:        {}", self.write_format)?;
        writeln!(f, "read format:         {}", self.read_format)?;
        writeln!(f, "reserved bytes:      {}", self.reserved_bytes)?;
        writeln!(f, "file change counter: {}", self.file_change_counter)?;
        writeln!(f, "database page count: {}", self.database_page_count)?;
        writeln!(f, "freelist page count: {}", self.freelist_trunk_page)?;
        writeln!(f, "freelist page count: {}", self.freelist_page_count)?;
        writeln!(f, "schema cookie:       {}", self.schema_cookie)?;
        writeln!(f, "schema format:       {}", self.schema_format)?;
        writeln!(f, "default cache size:  {}", self.default_page_cache)?;
        writeln!(f, "autovacuum top root: {}", self.autovacuum_top_root)?;
        writeln!(f, "incremental vacuum:  {}", self.incremental_vacuum)?;
        writeln!(
            f,
            "text encoding:       {} ({})",
            self.text_encoding,
            match self.text_encoding {
                1 => "utf8",
                2 => "utf16le",
                3 => "utf16be",
                _ => "unknown",
            }
        )?;
        writeln!(f, "user version:        {}", self.user_version)?;
        writeln!(f, "application id:      {}", self.application_id)?;
        writeln!(f, "software version:    {}", self.sqlite_version)?;
        writeln!(f, "number of tables:    ?")?;
        writeln!(f, "number of indexes:   ?")?;
        writeln!(f, "number of triggers:  ?")?;
        writeln!(f, "number of views:     ?")?;
        writeln!(f, "schema size:         ?")?;
        writeln!(f, "data version:        ?")?;
        Ok(())
    }
}

impl fmt::Display for BTreePageHeader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "page type:               {:?}", self.page_type)?;
        writeln!(f, "first freeblock:         {}", self.first_freeblock)?;
        writeln!(f, "number of cells:         {}", self.num_cells)?;
        writeln!(f, "cell content start:      {}", self.cell_content_start)?;
        writeln!(f, "fragmented free bytes:   {}", self.fragmented_free_bytes)?;
        if let Some(pointer) = self.right_most_pointer {
            writeln!(f, "right-most pointer:  {}", pointer)?;
        }
        Ok(())
    }
}

impl fmt::Display for Page {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Page::TableLeaf(leaf) => {
                write!(f, "{}", leaf.page_header)?;
                writeln!(f, "cell pointers:           {:?}", leaf.cell_pointers)?;
                writeln!(f, "cells:")?;
                for (i, cell) in leaf.cells.iter().enumerate() {
                    writeln!(f, "  [{}] row_id:{} {:?}", i, cell.row_id.value, cell.payload.payload)?;
                }

                Ok(())
            }
        }
    }
}
