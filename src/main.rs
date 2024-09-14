use anyhow::{Context as _, Result};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:9092")
        .await
        .context("failed to bind TCP listener")?;

    loop {
        tokio::select! {
            conn = listener.accept() => match conn {
                Ok((_stream, _addr)) => {
                    println!("accepted new connection");
                },
                Err(e) => eprintln!("connection failed: {e:?}"),
            }
        }
    }
}
