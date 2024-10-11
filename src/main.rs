use std::env;
use std::path::PathBuf;

use anyhow::{Context as _, Result};
use tokio::net::TcpListener;
use tokio::signal;
use tokio::task;

use redis_starter_rust::handle_connection;
use redis_starter_rust::logs;
use redis_starter_rust::properties::ServerProperties;

#[tokio::main]
async fn main() -> Result<()> {
    let server_properties = env::args()
        .find(|arg| arg.contains("server.properties"))
        .map(PathBuf::from)
        .context("missing required server.properties argument")?;

    // TODO: separate storage layer from the properties structure
    //  - `ServerProperties::load` and similar should take an abstract async reader, not a fs path
    let server_properties = ServerProperties::load(&server_properties)
        .await
        .with_context(|| format!("parse {server_properties:?}"))?;

    println!("Loaded {server_properties:?}");

    let log_dirs = server_properties.log_dirs.iter().cloned();
    let log_dirs = logs::load_metadata(log_dirs)
        .await
        .context("reading log directory metadata")?;

    println!("Found log dir metadata {log_dirs:?}");

    let listener = TcpListener::bind("127.0.0.1:9092")
        .await
        .context("failed to bind TCP listener")?;

    loop {
        tokio::select! {
            // handle connections
            conn = listener.accept() => match conn {
                Ok((stream, addr)) => {
                    println!("Accepted new connection: {addr}");

                    stream.set_nodelay(true).context("enable TCP_NODELAY on connection")?;

                    task::spawn(async move {
                        if let Err(e) = handle_connection(stream).await {
                            eprintln!("Task handling connection failed with {e:?}");
                        }
                    });
                },

                Err(e) => eprintln!("Instance cannot get client: {e:?}"),
            },

            // handle signals
            sig = signal::ctrl_c() => match sig {
                Ok(()) => {
                    println!("Received SIGINT, shutting down...");
                    break Ok(());
                },
                Err(e) => {
                    eprintln!("Terminating after error: {e}");
                    break Err(e).context("failed while receiving an interrupt signal");
                },
            }
        }

        // cooperatively yield from the main loop
        task::yield_now().await;
    }
}
