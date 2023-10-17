use chrono::{DateTime, FixedOffset, Local};
use crate::{cli};
use crate::cli::Commands::Send;
use elasticsearch::{BulkParts, Elasticsearch};
use elasticsearch::auth::Credentials;
use elasticsearch::http::request::JsonBody;
use elasticsearch::http::response::Response;
use elasticsearch::http::transport::{SingleNodeConnectionPool, TransportBuilder};
use log::{error, info};
use crate::parser::LogEntry;
use serde_json::{json, Value};
use uuid::Uuid;


pub async fn collect(mut body: Vec<Value>, log_entry: &LogEntry) -> Vec<Value> {
    let _id = Uuid::new_v4();
    let local_time: DateTime<Local> = Local::now();
    let Send(client) = cli::cli().command;

    let datetime_with_timezone =
        DateTime::<FixedOffset>::from_naive_utc_and_offset(log_entry.timestamp,
                                                           *local_time.offset());

    body.push(json!({"index": {"_id": log_entry.id }}));

    body.push(json!(log_entry));
    if body.len() > client.elastic_push_size as usize
        || local_time.signed_duration_since(datetime_with_timezone).num_seconds() > client.elastic_push_seconds as i64 {
        match send(body.to_vec()).await {
            Ok(_) => {}
            Err(e) => error!("{:?}", e)
        }
        body.truncate(0);
    }

    body
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
    info!("Elastic send statistic - {}",body.len());

    for body_datum in body {
        request_body.push(body_datum.into())
    }


    let client = elastic_connect().await?;
    let cli = cli::cli();

    let Send(options) = cli.command;

    let response = client
        .bulk(BulkParts::Index(options.elastic_index_name.unwrap().as_str()))
        .body(request_body)
        .send()
        .await?;
    if response.status_code() != 200 {
        return Err(format!("{:?}",response).into())
    }

    Ok(response)
}
