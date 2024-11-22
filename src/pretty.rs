use crate::*;
use std::fmt;

struct Indent(usize);

impl Indent {
    fn new(level: usize) -> Self {
        Indent(level * 2)
    }
}

impl fmt::Display for Indent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:width$}", "", width = self.0 * 2)
    }
}

impl fmt::Display for Database {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "SQLite Database")?;
        writeln!(f, "{}Database Header", Indent::new(1))?;
        writeln!(f, "{}", HeaderDisplay(self.db_header, 2))?;
        for (i, page) in self.pages.iter().enumerate() {
            writeln!(f, "{}Page {}", Indent::new(1), i)?;
            write!(f, "{}", page)?;
        }
        Ok(())
    }
}

impl fmt::Display for Page {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Page::TableLeaf(leaf) => write!(f, "{}", leaf)?,
        }
        Ok(())
    }
}

impl fmt::Display for TableLeaf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.page_header)?;

        // Cell pointers
        writeln!(
            f,
            "{}Cell Pointers:               {:?}",
            Indent::new(2),
            self.cell_pointers
        )?;

        // Cells types, just few row for a sample
        writeln!(f, "{}Sample Cell Types", Indent::new(2))?;
        for cell in self.cells.iter().take(3) {
            writeln!(f, "{}{:?}", Indent::new(3), cell.record.columns)?;
        }

        // Cells as table
        writeln!(f, "{}Cells\n", Indent::new(2))?;
        let indent = Indent::new(3);

        // Header
        write!(f, "{}│ {:8} │ {:8} │", indent, "Size", "Row ID")?;
        for i in 0..self.cells[0].record.columns.len() {
            write!(f, " {:14} │", format!("Column {}", i))?;
        }
        writeln!(f)?;

        // Separator
        write!(f, "{}├─", indent)?;
        write!(f, "{:─<8}─┼", "")?;
        write!(f, "{:─<10}┼", "")?;
        for _ in 0..self.cells[0].record.columns.len() {
            write!(f, "{:─<16}┼", "")?;
        }
        writeln!(f)?;

        // Cells
        for cell in &self.cells {
            write!(f, "{}│ {:8} │ {:8} │", indent, cell.size.value, cell.row_id.value)?;
            for value in &cell.record.payload {
                write!(f, " {} │", truncate(&value.to_string(), 14))?;
            }
            writeln!(f)?;
        }

        Ok(())
    }
}

impl fmt::Display for BTreePageHeader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "{}Page Header:", Indent::new(2))?;
        let indent = Indent::new(3);
        writeln!(f, "{}Type:                    {:?}", indent, self.page_type)?;
        writeln!(f, "{}First freeblock:         {}", indent, self.first_freeblock)?;
        writeln!(f, "{}Number of cells:         {}", indent, self.num_cells)?;
        writeln!(f, "{}Cell content start:      {}", indent, self.cell_content_start)?;
        writeln!(f, "{}Fragmented free bytes:   {}", indent, self.fragmented_free_bytes)?;
        if let Some(ptr) = self.right_most_pointer {
            writeln!(f, "{}Right most pointer:  {}", indent, ptr)?;
        }
        Ok(())
    }
}

impl fmt::Display for SerialType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl fmt::Display for SerialValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SerialValue::Null => write!(f, "NULL"),
            SerialValue::Number(n) => write!(f, "{}", n),
            SerialValue::Float(x) => write!(f, "{}", x),
            SerialValue::String(s) => write!(f, "\"{}\"", s),
            SerialValue::Blob(b) => write!(f, "<BLOB:{}>", b.len()),
            SerialValue::Reserved => write!(f, "<RESERVED>"),
        }
    }
}

pub struct HeaderDisplay(pub Header, pub usize);

impl fmt::Display for HeaderDisplay {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (header, indent) = (&self.0, Indent::new(self.1));

        writeln!(
            f,
            "{}database page size:  {}",
            indent,
            if header.page_size == 1 {
                65536
            } else {
                header.page_size as u32
            }
        )?;
        writeln!(f, "{}write format:        {}", indent, header.write_format)?;
        writeln!(f, "{}read format:         {}", indent, header.read_format)?;
        writeln!(f, "{}file change counter: {}", indent, header.file_change_counter)?;
        writeln!(f, "{}database page count: {}", indent, header.database_page_count)?;
        writeln!(f, "{}freelist page count: {}", indent, header.freelist_trunk_page)?;
        writeln!(f, "{}freelist page count: {}", indent, header.freelist_page_count)?;
        writeln!(f, "{}schema cookie:       {}", indent, header.schema_cookie)?;
        writeln!(f, "{}schema format:       {}", indent, header.schema_format)?;
        writeln!(f, "{}default cache size:  {}", indent, header.default_page_cache)?;
        writeln!(f, "{}autovacuum top root: {}", indent, header.autovacuum_top_root)?;
        writeln!(f, "{}incremental vacuum:  {}", indent, header.incremental_vacuum)?;
        writeln!(
            f,
            "{}text encoding:       {} ({})",
            indent,
            header.text_encoding,
            match header.text_encoding {
                1 => "utf8",
                2 => "utf16le",
                3 => "utf16be",
                _ => "unknown",
            }
        )?;
        writeln!(f, "{}user version:        {}", indent, header.user_version)?;
        writeln!(f, "{}application id:      {}", indent, header.application_id)?;
        writeln!(f, "{}software version:    {}", indent, header.sqlite_version)?;
        writeln!(f, "{}number of tables:    ?", indent)?;
        writeln!(f, "{}number of indexes:   ?", indent)?;
        writeln!(f, "{}number of triggers:  ?", indent)?;
        writeln!(f, "{}number of views:     ?", indent)?;
        writeln!(f, "{}schema size:         ?", indent)?;
        writeln!(f, "{}data version:        ?", indent)
    }
}

fn truncate(s: &str, width: usize) -> String {
    if s.len() <= width {
        format!("{:width$}", s, width = width)
    } else {
        format!("{:.<width$}", &s[..width - 3], width = width)
    }
}
