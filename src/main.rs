use std::env;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context as _, Result};
use tokio::net::TcpListener;
use tokio::signal;
use tokio::task;

use redis_starter_rust::Server;

#[tokio::main]
async fn main() -> Result<()> {
    let server_properties = env::args()
        .find(|arg| arg.contains("server.properties"))
        .map(PathBuf::from)
        .context("missing required server.properties argument")?;

    let server = Server::new(server_properties)
        .await
        .map(Arc::new)
        .context("initialize server")?;

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

                    let server = Arc::clone(&server);

                    task::spawn(async move {
                        if let Err(e) = server.handle_connection(stream).await {
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
