mod anonymize;
mod cli;
mod elastic;
mod parser;

use log::error;
use std::env;
use std::fs::File;

use env_logger::{Builder, Target};
use std::io::{ Write};

#[allow(unused_imports)]
use std::io::{self, Read};

use serde_json::Value;

use crate::cli::Output;
use crate::elastic::collect;
use crate::parser::MultiLine;
use cli::Commands::Send;
use linemux::MuxedLines;


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut lines = MuxedLines::new()?;

    let cli = cli::cli();

    match env::var("RUST_LOG") {
        Err(_) => {
            Builder::new()
                .target(Target::Stdout)
                .filter_level(cli.log_level)
                .init();
        }
        _ => {
            env_logger::init();
        }
    }

    match &cli.command {
        Send(name) => {
            if let Some(config_path) = name.input_file.as_deref() {
                match File::open(config_path) {
                    Err(why) => error!("couldn't open {}: {}", config_path.display(), why),
                    Ok(_file) => {
                        //let file = TailedFile::new(file);
                        //let locked = file.lock();
                        lines.add_file(config_path).await?;
                        let mut file_write;

                        file_write = File::create(name.output_file.clone().unwrap()).unwrap();

                        let mut collected_data: Vec<Value> = Vec::new();
                        let mut log_entries: MultiLine = MultiLine {
                            log_entries: Vec::new(),
                            multi_line: false,
                            sql: "".to_string(),
                            temp_entry: None,
                        };

                        //for line in locked.lines() {
                        while let Ok(Some(line)) = lines.next_line().await {
                            let opened_line = line.line();

                            match name.input {
                                cli::Input::General => {
                                    log_entries =
                                        parser::parse_mysql_log_entry(&opened_line, log_entries);
                                }
                                cli::Input::Slow => {
                                    log_entries = parser::parse_mysql_slow_log_entry(
                                        &opened_line,
                                        log_entries,
                                    );
                                }
                            }

                            for log_entry in log_entries.log_entries.iter_mut() {
                                if !name.query {
                                    log_entry.original_query = "".to_string();
                                }
                                match name.output {
                                    Output::File => {
                                        if !log_entry.replaced_query.is_empty() {
                                            let _ = File::write(
                                                &mut file_write,
                                                (serde_json::to_string(&log_entry).unwrap() + "\n")
                                                    .as_bytes(),
                                            );
                                            log_entry.replaced_query = "".to_string();
                                        }
                                    }
                                    Output::Elastic => {
                                        collected_data = collect(collected_data, log_entry).await;
                                    }
                                }
                            }

                            let _ = File::flush(&mut file_write);
                        }

                    }
                };
            }
        }
    }

    Ok(())
}
