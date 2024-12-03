mod config;
mod consumer;

use anyhow::{bail, Result};
use clap::{Parser, Subcommand};
use consumer::run_consumer;
use std::collections::HashMap;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Configuration parameters
    #[arg(short, long)]
    config: Vec<String>,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    Producer,
    Consumer,
    ProducerConsumer,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let config = parse_config(cli.config)?;

    // Start thread to handle ctrl-c/SIGINT
    let cancellation = CancellationToken::new();
    let cancellation_c = cancellation.clone();
    let ctrlc_handler: JoinHandle<std::io::Result<()>> = tokio::spawn(async move {
        tokio::signal::ctrl_c().await?;
        println!("Received SIGINT, shutting down");
        cancellation_c.cancel();
        Ok(())
    });

    println!("Running command");
    match &cli.command {
        Some(Commands::Producer) => bail!("Producer not implemented"),
        Some(Commands::Consumer) => run_consumer(config, cancellation.clone()).await?,
        Some(Commands::ProducerConsumer) => bail!("Producer consumer not implemented"),
        None => bail!("No command given"),
    }

    Ok(ctrlc_handler.await??)
}

fn parse_config(config_args: Vec<String>) -> Result<HashMap<String, String>> {
    let mut config_map = HashMap::with_capacity(config_args.len());
    for arg in config_args {
        let parts = arg.splitn(2, '=').collect::<Vec<_>>();
        match parts.as_slice() {
            &[key, value] => {
                config_map.insert(key.to_owned(), value.to_owned());
            }
            _ => bail!("Invalid config value: {}", arg),
        }
    }
    Ok(config_map)
}
