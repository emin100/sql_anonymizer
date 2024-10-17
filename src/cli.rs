use clap::{Args, Parser, Subcommand};
use elasticsearch::http::Url;
use log::LevelFilter;
use std::path::PathBuf;

#[derive(Args, Debug)]
pub struct AddArgs {
    /// Elastic host. Env: ELASTIC_HOST
    #[arg(short, long, env = "ELASTIC_HOST", required_if_eq("output", "elastic"))]
    pub elastic_host: Option<Url>,

    /// Sets Elastic username.
    #[arg(short = 'u', long, env = "ELASTIC_USER", requires = "elastic_password")]
    pub elastic_user: Option<String>,

    /// Sets Elastic password.
    #[arg(short = 'p', long, env = "ELASTIC_PASSWORD", requires = "elastic_user")]
    pub elastic_password: Option<String>,

    /// Sets an output type
    #[arg(short = 't', long, default_value = "file")]
    #[clap(value_enum)]
    pub output: Output,

    /// Sets an input type
    #[arg(short, long, default_value = "general", required = true)]
    #[clap(value_enum)]
    pub input: Input,

    /// Sets a input file path
    #[arg(short = 'f', long, value_name = "FILE", required_if_eq_any([("input", "general"), ("input", "slow")]))]
    pub input_file: Option<PathBuf>,

    /// Sets a output file path
    #[arg(
        short = 'o',
        long,
        value_name = "FILE",
        required_if_eq("output", "file"),
        default_value = "output.txt"
    )]
    pub output_file: Option<PathBuf>,

    /// Sets a push size
    #[arg(short = 's', long, default_value = "1000")]
    pub elastic_push_size: u16,

    /// Sets a push seconds
    #[arg(short = 'c', long, default_value = "15")]
    pub elastic_push_seconds: u16,

    /// Sets Elastic password.
    #[arg(short = 'n', long, default_value = "mysql_logs", env = "ELASTIC_INDEX")]
    pub elastic_index_name: Option<String>,

    /// Log original query
    #[arg(short = 'q', long, default_value = "false")]
    pub query: bool,
}

#[derive(Args, Debug)]
pub struct ReportArgs {
    /// Sets a input file path
    #[arg(short = 'f', long, value_name = "FILE", required = true)]
    pub input_file: Option<PathBuf>,

    /// Sets a output file path
    #[arg(short = 'o', long, value_name = "FILE", default_value = "output.txt")]
    pub output_file: Option<PathBuf>,
}

#[derive(Args, Debug)]
pub struct PFArgs {
    /// Sets a input file path
    #[arg(short = 'f', long, value_name = "FILE", required = true)]
    pub input_file: Option<PathBuf>,

    /// Sets a output file path
    #[arg(short = 'o', long, value_name = "FILE", default_value = "output.txt")]
    pub output_file: Option<PathBuf>,

    /// Sets an input type
    #[arg(short, long, default_value = "general", required = true)]
    #[clap(value_enum)]
    pub input: Input,

    /// Sets a start date
    #[arg(short = 'd', long, default_value_t = 0, required = false)]
    pub start_date: u16,
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub struct Client {
    /// Turn debugging information on
    #[arg(short, long, default_value = "error", global = true)]
    pub log_level: LevelFilter,

    /// Commands
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    Report(ReportArgs),
    FileParse(PFArgs),
    Send(AddArgs),
}

#[derive(clap::ValueEnum, Clone, Debug)]
pub enum Input {
    Slow,
    General,
}

#[derive(clap::ValueEnum, Clone, Debug, PartialEq)]
pub enum Output {
    File,
    Elastic,
}

pub fn cli() -> Client {
    Client::parse()
}
