use crate::{anonymize, cli};
use chrono::{NaiveDateTime, ParseError};
use log::error;
use regex::Regex;
use serde::Serialize;
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::{Parser, ParserError};
use std::env;
use std::net::IpAddr;
use uuid::Uuid;

#[derive(Debug, Serialize, Clone)]
pub struct MultiLine {
    pub log_entries: Vec<LogEntry>,
    pub temp_entry: Option<LogEntry>,
    pub multi_line: bool,
    pub sql: String,
}

pub fn parse_mysql_log_entry(log_line: &str, mut multilines: MultiLine) -> MultiLine {
    let mut parts: Vec<&str> = log_line.split_whitespace().collect();

    let new_line = multilines.sql.clone();

    match parse_timestamp(parts[0]) {
        Ok(_) => {
            parts = new_line.split_whitespace().collect();
            multilines = MultiLine {
                log_entries: Vec::new(),
                multi_line: false,
                sql: log_line.to_string(),
                temp_entry: None,
            };
        }
        Err(_) => {
            multilines.multi_line = true;
            multilines.sql += log_line;
            parts.truncate(0);
        }
    }

    let mut log_entries: Vec<LogEntry> = Vec::new();

    if parts.len() >= 4 {
        let timestamp = parts[0];
        let _id = parts[1];
        let _command = parts[1];
        let sql_query = parts[3..].join(" ");

        match anonymize_sql(sql_query.to_string()) {
            Ok(result) => {
                for ast in result {
                    log_entries.push(LogEntry {
                        timestamp: parse_timestamp(timestamp).unwrap(),
                        command: ast.statement_type,
                        original_query: sql_query.to_string(),
                        replaced_query: ast.statement.to_string(),
                        ..LogEntry::default()
                    });
                }
            }
            Err(_error) => {
                // error!("{} - {}", log_line.to_string(),error);
            }
        }
    }
    multilines.log_entries = log_entries;
    multilines
}

pub fn parse_mysql_slow_log_entry(log_line: &str, mut multilines: MultiLine) -> MultiLine {
    let re = Regex::new(
        r"^# Time: (.+)$|^# User@Host: (.+)Id:|^# Query_time: (.+)$|^SET timestamp=(.+);$",
    )
    .unwrap();
    let mut log_entries: Vec<LogEntry> = Vec::new();
    let mut log_entry = LogEntry::default();
    if multilines.temp_entry.is_some() {
        log_entry = multilines.temp_entry.clone().unwrap();
    }
    if let Some(captures) = re.captures(log_line) {
        if let Some(match_time) = captures.get(1) {
            if multilines.multi_line && log_entry.timestamp != NaiveDateTime::default() {
                // log_entry[0].original_query = log_line.to_string();

                match anonymize_sql(log_entry.original_query.clone()) {
                    Ok(result) => {
                        for ast in result {
                            let mut c_entry = log_entry.clone();
                            c_entry.replaced_query = ast.statement.to_string();
                            c_entry.command = ast.statement_type;
                            log_entries.push(c_entry);
                        }
                    }
                    Err(_error) => {}
                }
            }
            multilines.multi_line = false;
            log_entry.original_query = "".to_string();

            log_entry = LogEntry::default();
            log_entry.timestamp = parse_timestamp(match_time.as_str()).unwrap();
        }
        if let Some(match_time) = captures.get(2) {
            log_entry.user = Some(match_time.as_str().trim().to_string());
        }
        if let Some(match_time) = captures.get(3) {
            let line_re =
                Regex::new(r"^(.+)Lock_time:(.+)Rows_sent:(.+)Rows_examined:\s(.+)").unwrap();
            let parse_info = line_re.captures(match_time.as_str()).unwrap();
            log_entry.query_time = Some(
                parse_info
                    .get(1)
                    .unwrap()
                    .as_str()
                    .trim()
                    .parse::<f32>()
                    .unwrap(),
            );
            log_entry.lock_time = Some(
                parse_info
                    .get(2)
                    .unwrap()
                    .as_str()
                    .trim()
                    .parse::<f32>()
                    .unwrap(),
            );
            log_entry.row_sent = Some(
                parse_info
                    .get(3)
                    .unwrap()
                    .as_str()
                    .trim()
                    .parse::<u32>()
                    .unwrap(),
            );
            let last_part: Vec<&str> = parse_info
                .get(4)
                .unwrap()
                .as_str()
                .split_whitespace()
                .collect();
            log_entry.row_examined = Some(last_part.first().unwrap().parse::<u32>().unwrap());
        }

        if let Some(_match_time) = captures.get(4) {
            multilines.multi_line = true;
        }

        multilines.temp_entry = Some(log_entry);
        multilines.log_entries = log_entries;
        multilines
    } else {
        log_entry.original_query += log_line.replace('\t', " ").as_str();
        multilines.temp_entry = Some(log_entry);
        multilines
    }
}

#[derive(Debug, Serialize, Clone)]
pub struct LogEntry {
    pub timestamp: NaiveDateTime,
    pub id: Uuid,
    pub command: Command,
    pub replaced_query: String,
    pub original_query: String,
    pub host: String,
    pub ip: IpAddr,
    pub user: Option<String>,
    pub query_time: Option<f32>,
    pub lock_time: Option<f32>,
    pub row_sent: Option<u32>,
    pub row_examined: Option<u32>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub enum Command {
    Query,
    Insert,
    Update,
    Other,
    Explain,
    Delete,
}

fn hostname() -> Option<String> {
    match env::var("HOSTNAME") {
        Ok(val) => Some(val),
        Err(_) => match hostname::get() {
            Ok(host) => Some(host.to_string_lossy().into_owned()),
            Err(_) => None,
        },
    }
}

impl Default for LogEntry {
    fn default() -> LogEntry {
        LogEntry {
            timestamp: Default::default(),
            id: Uuid::new_v4(),
            command: Command::Other,
            replaced_query: "".to_string(),
            original_query: "".to_string(),
            host: hostname().unwrap(),
            ip: local_ip_address::local_ip().unwrap(),
            user: None,
            query_time: None,
            lock_time: None,
            row_sent: None,
            row_examined: None,
        }
    }
}

pub fn parse_timestamp(timestamp_str: &str) -> Result<NaiveDateTime, ParseError> {
    let format_str = "%Y-%m-%dT%H:%M:%S%.6fZ"; // Adjust this format to match your timestamp format
    NaiveDateTime::parse_from_str(timestamp_str, format_str)
}

fn anonymize_sql(sql: String) -> Result<Vec<anonymize::Replaced>, ParserError> {
    let cli = cli::cli();

    let dialect = MySqlDialect {};

    let cli::Commands::Send(_name) = cli.command;
    // sql = sql.replace("  ", " ");

    if !sql.starts_with('#') {
        return match Parser::parse_sql(&dialect, sql.as_str()) {
            Ok(mut ast) => {
                let mut replaced = Vec::new();
                for statement in ast.iter_mut() {
                    replaced.push(anonymize::rec(statement));
                }
                Ok(replaced)
            }
            Err(err) => {
                error!("Error parsing sql: {} - {}", err, sql);
                /*error!("Error parsing sql: {}", err);*/
                Err(err)
            }
        };
    }

    Ok(Vec::new())
}
