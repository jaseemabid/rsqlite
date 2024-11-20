use binrw::BinRead;
use rsqlite::{Header, Page};
use std::{
    env,
    fs::File,
    io::{BufReader, Seek, SeekFrom},
    process,
};

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() != 3 {
        eprintln!("Usage: {} <file> <command>", args[0]);
        process::exit(1);
    }

    let (file_path, command) = (&args[1], &args[2]);

    // Open the file
    let file = File::open(file_path).unwrap_or_else(|err| {
        eprintln!("Failed to open file {}: {}", file_path, err);
        process::exit(1);
    });
    let mut reader = BufReader::new(file);

    match command.as_str() {
        ".dbinfo" => match Header::read_be(&mut reader) {
            Ok(header) => println!("{}", header),
            Err(err) => {
                eprintln!("Failed to read header: {}", err);
                process::exit(1);
            }
        },
        ".dump" => {
            // Seek ahead to 2nd page, which should be a btree leaf for planets.db
            reader
                .seek(SeekFrom::Start(4096))
                .expect("Failed to seek to second page");

            match Page::read_be(&mut reader) {
                Ok(header) => println!("{}", header),
                Err(err) => {
                    eprintln!("Failed to read 2nd page: {}", err);
                    process::exit(1);
                }
            }
        }
        _ => {
            eprintln!("Unknown command: {}", command);
            process::exit(1);
        }
    }
}
