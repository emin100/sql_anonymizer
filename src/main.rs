mod parser;
mod cli;
mod anonymize;
mod elastic;


use std::env;
use std::fs::File;
use log::{error, info};


use sqlparser::dialect::MySqlDialect;

use std::io::{BufRead, Write};
use env_logger::{Builder, Target};



#[allow(unused_imports)]
use std::io::{self, Read};
use fs_tail::TailedFile;
use sqlparser::parser::Parser;
use elastic::collect;
use serde_json::Value;

use cli::Commands::Send;


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {



    // log::set_max_level(LevelFilter::Debug);
    // env_logger::init();



    let cli = cli::cli();

    let dialect = MySqlDialect {};


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

                        let mut file_write = File::create(name.output_file.clone().unwrap()).unwrap();

                        let mut  collected_data: Vec<Value> = Vec::new();

                        for line in locked.lines() {
                            if let Some(log_entry) = parser::parse_mysql_log_entry(&line.unwrap()) {
                                match parser::parse_timestamp(&log_entry.timestamp) {
                                    Ok(_parsed_timestamp) => {
                                        match Parser::parse_sql(&dialect, &log_entry.sql_query) {
                                            Ok(mut ast) => {
                                                for statement in ast.iter_mut() {
                                                    let replaced = anonymize::rec(statement);
                                                    *statement = replaced.statement;
                                                    match name.output {
                                                        cli::Output::Elastic => {
                                                            collected_data = collect(collected_data,&log_entry.user, &log_entry.timestamp, statement.to_string(), replaced.statement_type).await;
                                                        },
                                                        cli::Output::File => {
                                                            File::write(&mut file_write, (statement.to_string() + "\n").as_bytes()).expect("TODO: panic message");
                                                        }
                                                    }

                                                };

                                                info!("Modified SQL Tree: {:?}", ast);
                                                info!("Modified SQL: {:?}", ast[0].to_string());
                                            }
                                            Err(err) => {
                                                error!("Error parsing timestamp: {}", err);
                                            }
                                        }
                                    }
                                    Err(err) => {
                                        error!("Error parsing timestamp: {}", err);
                                    }
                                }
                            }
                        }
                        file_write.flush().expect("TODO: panic message");
                    },
                };
            }
        }
    }

    // match cli.debug {
    //     0 => println!("Debug mode is off"),
    //     1 => println!("Debug mode is kind of on"),
    //     2 => println!("Debug mode is on"),
    //     _ => println!("Don't be crazy"),
    // }
    Ok(())
}
