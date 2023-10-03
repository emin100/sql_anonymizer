use std::path::PathBuf;
use clap::{Args, Parser, Subcommand};
use elasticsearch::http::Url;
use log::LevelFilter;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub struct Client {
    /// Turn debugging information on
    #[arg(short, long, default_value="error", global=true)]
    pub log_level: LevelFilter,

    /// Commands
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Adds files to myapp
    Send(AddArgs),
}


#[derive(Args,Debug)]
pub struct AddArgs {

    /// Elastic host. Env: ELASTIC_HOST
    #[arg(short, long, env = "ELASTIC_HOST", required_if_eq("output", "elastic"))]
    pub elastic_host: Option<Url>,

    /// Sets Elastic username.
    #[arg(short='u', long, env = "ELASTIC_USER", requires = "elastic_password")]
    pub elastic_user: Option<String>,

    /// Sets Elastic password.
    #[arg(short='p', long, env = "ELASTIC_PASSWORD", requires = "elastic_user")]
    pub elastic_password: Option<String>,

    /// Sets an output type
    #[arg(short='t', long, default_value = "file")]
    #[clap(value_enum)]
    pub output: Output,

    /// Sets an input type
    #[arg(short, long, default_value = "general", required = true)]
    #[clap(value_enum)]
    pub input: Input,

    /// Sets a custom config file
    #[arg(short = 'f', long, value_name = "FILE", required_if_eq_any([("input", "general"), ("input", "slow")]))]
    pub input_file: Option<PathBuf>,

    /// Sets a custom config file
    #[arg(short = 'o', long, value_name = "FILE", required_if_eq("output", "file"), default_value="output.txt")]
    pub output_file: Option<PathBuf>,

}


#[derive(clap::ValueEnum, Clone, Debug)]
pub enum Output {
    File,
    Elastic
}

#[derive(clap::ValueEnum, Clone, Debug)]
pub enum Input {
    Slow,
    General,
    Syslog
}


pub fn cli() -> Client {
    Client::parse()
}