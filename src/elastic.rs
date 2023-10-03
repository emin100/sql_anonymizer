#![crate_name = "elastic"]

use elasticsearch::{BulkParts, Elasticsearch};
use elasticsearch::http::transport::{SingleNodeConnectionPool, TransportBuilder};
use elasticsearch::http::request::JsonBody;
use elasticsearch::http::response::Response;

use serde_json::{json, Value};
use uuid::Uuid;

use std::env;
use chrono::{DateTime, FixedOffset, Local};
use elasticsearch::auth::Credentials;
use log::error;
use crate::{cli, parser};
use crate::cli::Commands::Send;

pub async fn collect(mut body: Vec<Value>, user: &str, date: &str, sql: String, statement: String) -> Vec<Value> {
    let id = Uuid::new_v4();
    let local_time: DateTime<Local> = Local::now();
    let log_time = parser::parse_timestamp(date.clone()).unwrap();

    let datetime_with_timezone =
        DateTime::<FixedOffset>::from_naive_utc_and_offset(log_time,
                                                           local_time.offset().clone());

    body.push(json!({"index": {"_id": id}}));

    body.push(json!({
        "id": id,
        "host": hostname(),
        "statement": statement,
        "ip": local_ip_address::local_ip().unwrap(),
        "user": user,
        "log_time": date,
        "sql": sql
    }));


    if body.len() > 1000
        || local_time.signed_duration_since(datetime_with_timezone).num_seconds() > 15 {
        match send(body.to_vec()).await {
            Ok(_) => {}
            Err(e) => error!("{:?}", e)
        }
        body.truncate(0);
    }
    body
}

fn hostname() -> Option<String> {
    match env::var("HOSTNAME") {
        Ok(val) => Some(val),
        Err(_) => {
            match hostname::get() {
                Ok(host) => Some(host.to_string_lossy().into_owned()),
                Err(_) => None,
            }
        }
    }
}

async fn elastic_connect() -> Result<Elasticsearch, Box<dyn std::error::Error>> {
    let cli = cli::cli();

    match &cli.command {
        Send(name) => {
            let conn_pool = SingleNodeConnectionPool::new(name.elastic_host.clone().unwrap());
            let mut transport = TransportBuilder::new(conn_pool)
                .disable_proxy();


            if name.elastic_user.is_some() {
                transport = transport.auth(Credentials::Basic(name.elastic_user.clone().unwrap(), name.elastic_password.clone().unwrap()));
            }

            Ok(Elasticsearch::new(transport.build()?))
        }
    }

}

async fn send(body: Vec<serde_json::Value>) -> Result<Response, Box<dyn std::error::Error>> {
    let mut request_body: Vec<JsonBody<_>> = Vec::with_capacity(body.len());
    for body_datum in body {
        request_body.push(body_datum.into())
    }

    let client = elastic_connect().await?;

    let response = client
        .bulk(BulkParts::Index("mysql_logs"))
        .body(request_body)
        .send()
        .await?;
    if response.status_code() != 200 {
        return Err(format!("{:?}",response).into())
    }

    Ok(response)
}
