use binrw::BinRead;
use rsqlite::{pretty::HeaderDisplay, schema::*};
use std::{env, fs::File, io::BufReader, process};

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() != 3 {
        eprintln!("Usage: {} <file> <command>", args[0]);
        process::exit(1);
    }

    let (file_path, command) = (&args[1], &args[2]);
    let file = File::open(file_path).unwrap_or_else(|err| {
        eprintln!("Failed to open file {}: {}", file_path, err);
        process::exit(1);
    });
    let mut reader = BufReader::new(file);

    match command.as_str() {
        ".dbinfo" => match Header::read_be(&mut reader) {
            Ok(header) => println!("{}", HeaderDisplay(header, 0)),
            Err(err) => {
                eprintln!("Failed to read header: {}", err);
                process::exit(1);
            }
        },
        ".dump" => match Database::read_be(&mut reader) {
            Ok(db) => println!("{}", db),
            Err(err) => {
                eprintln!("Failed to read database: {}", err);
                process::exit(1);
            }
        },
        _ => {
            eprintln!("Unknown command: {}", command);
            process::exit(1);
        }
    }
}
