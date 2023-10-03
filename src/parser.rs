use chrono::{NaiveDateTime, ParseError};


pub fn parse_mysql_log_entry(log_line: &str) -> Option<LogEntry> {
    // Example: MySQL general log format "[timestamp] user@host: SQL query"
    // You should customize this logic to match your log format
    let parts: Vec<&str> = log_line.split_whitespace().collect();

    if parts.len() >= 4 {
        let timestamp = parts[0].trim_matches('[').trim_matches(']');
        let user_host = parts[1];
        let sql_query = parts[3..].join(" ");

        // Extract user from "user@host"
        let user_parts: Vec<&str> = user_host.split('@').collect();
        let user = user_parts.first().unwrap_or(&"").trim();

        Some(LogEntry {
            timestamp: timestamp.to_string(),
            user: user.to_string(),
            sql_query: sql_query.to_string(),
        })
    } else {
        None
    }
}


#[derive(Debug)]
pub struct LogEntry {
    pub timestamp: String,
    pub user: String,
    pub sql_query: String,
}

pub fn parse_timestamp(timestamp_str: &str) -> Result<NaiveDateTime, ParseError> {
    let format_str = "%Y-%m-%dT%H:%M:%S%.6fZ"; // Adjust this format to match your timestamp format
    NaiveDateTime::parse_from_str(timestamp_str, format_str)
}