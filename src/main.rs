use anyhow::{Context as _, Result};
use tokio::net::TcpListener;
use tokio::signal;
use tokio::task;

use redis_starter_rust::handle_connection;

#[tokio::main]
async fn main() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:9092")
        .await
        .context("failed to bind TCP listener")?;

    loop {
        tokio::select! {
            // handle connections
            conn = listener.accept() => match conn {
                Ok((stream, addr)) => {
                    println!("accepted new connection: {addr}");

                    stream.set_nodelay(true).context("enable TCP_NODELAY on connection")?;

                    task::spawn(async move {
                        if let Err(e) = handle_connection(stream).await {
                            eprintln!("task handling connection failed with {e:?}");
                        }
                    });
                },

                Err(e) => eprintln!("instance cannot get client: {e:?}"),
            },

            // handle signals
            sig = signal::ctrl_c() => match sig {
                Ok(()) => {
                    println!("received SIGINT, shutting down...");
                    break Ok(());
                },
                Err(e) => {
                    eprintln!("terminating after error: {e}");
                    break Err(e).context("failed while receiving an interrupt signal");
                },
            }
        }

        // cooperatively yield from the main loop
        task::yield_now().await;
    }
}
