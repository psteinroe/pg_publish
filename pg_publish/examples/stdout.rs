use std::error::Error;

use clap::{Args, Parser, Subcommand};
use pg_publish::pipeline::{
    data_pipeline::DataPipeline,
    destinations::stdout::StdoutDestination,
    sources::postgres::{PostgresSource, TableNamesFrom},
    stores::memory::MemoryStore,
};
use tracing::error;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Debug, Parser)]
#[command(name = "stdout", version, about, arg_required_else_help = true)]
struct AppArgs {
    #[clap(flatten)]
    db_args: DbArgs,

    #[clap(subcommand)]
    command: Command,
}

#[derive(Debug, Args)]
struct DbArgs {
    /// Host on which Postgres is running
    #[arg(long)]
    db_host: String,

    /// Port on which Postgres is running
    #[arg(long)]
    db_port: u16,

    /// Postgres database name
    #[arg(long)]
    db_name: String,

    /// Postgres database user name
    #[arg(long)]
    db_username: String,

    /// Postgres database user password
    #[arg(long)]
    db_password: Option<String>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Start a change data capture
    Cdc {
        publication: String,
        slot_name: String,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    if let Err(e) = main_impl().await {
        error!("{e}");
    }

    Ok(())
}

fn init_tracing() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "stdout=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();
}

fn set_log_level() {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }
}

async fn main_impl() -> Result<(), Box<dyn Error>> {
    set_log_level();
    init_tracing();
    let args = AppArgs::parse();
    let db_args = args.db_args;

    let postgres_source = match args.command {
        Command::Cdc {
            publication,
            slot_name,
        } => {
            PostgresSource::new(
                &db_args.db_host,
                db_args.db_port,
                &db_args.db_name,
                &db_args.db_username,
                db_args.db_password,
                Some(slot_name),
                TableNamesFrom::Publication(publication),
            )
            .await?
        }
    };

    let store = MemoryStore::default();
    let destination = StdoutDestination;

    let mut pipeline = DataPipeline::new(postgres_source, destination, store).await?;

    pipeline.run().await?;

    Ok(())
}
