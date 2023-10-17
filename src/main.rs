mod parser;
mod cli;
mod anonymize;
mod elastic;


use std::env;
use std::fs::File;
use log::{ error};

use std::io::{BufRead, Write};
use env_logger::{Builder, Target};



#[allow(unused_imports)]
use std::io::{self, Read};
use fs_tail::TailedFile;
use serde_json::Value;

use cli::Commands::Send;
use crate::cli::Output;
use crate::elastic::collect;
use crate::parser::{ MultiLine};


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = cli::cli();

    match env::var("RUST_LOG") {
        Err(_) => {
            Builder::new()
            .target(Target::Stdout)
            .filter_level(cli.log_level)
            .init();

        },
        _ => {
            env_logger::init();
        },
    }


    match &cli.command {
        Send(name) => {
            if let Some(config_path) = name.input_file.as_deref() {
                match File::open(config_path) {
                    Err(why) => error!("couldn't open {}: {}", config_path.display(), why),
                    Ok(file) => {
                        let file = TailedFile::new(file);
                        let locked = file.lock();
                        let mut file_write ;

                        file_write = File::create(name.output_file.clone().unwrap()).unwrap();



                        let mut  collected_data: Vec<Value> = Vec::new();
                        let mut log_entries: MultiLine = MultiLine {log_entries: Vec::new(), multi_line:false, sql: "".to_string(), temp_entry: None};

                        for line in locked.lines() {
                            match name.input {
                                cli::Input::General => {
                                    log_entries = parser::parse_mysql_log_entry(&line.unwrap(), log_entries);
                                },
                                cli::Input::Slow => {
                                    log_entries = parser::parse_mysql_slow_log_entry(&line.unwrap(), log_entries);

                                }
                            }

                            for log_entry in log_entries.log_entries.iter_mut() {
                                match name.output {
                                    Output::File => {
                                        if !log_entry.replaced_query.is_empty() {
                                            log_entry.original_query = "".to_string();
                                            let _ = File::write(&mut file_write,(serde_json::to_string(&log_entry).unwrap() + "\n").as_bytes());
                                            log_entry.replaced_query = "".to_string();
                                        }
                                    },
                                    Output::Elastic => {
                                        log_entry.original_query = "".to_string();
                                        collected_data = collect(collected_data, log_entry).await;
                                    }
                                }
                            }
                            let _ = File::flush(&mut file_write);
                        }

                    },
                };
            }
        }
    }

    Ok(())
}
