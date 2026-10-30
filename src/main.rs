use std::net::{Ipv4Addr, SocketAddr};

use liaison::{Config, app};
use tokio::net::TcpListener;
use tokio::signal::unix::{SignalKind, signal};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt()
        .with_target(false)
        .with_ansi(false)
        .init();
    let port = std::env::var("PORT")
        .ok()
        .and_then(|port| port.parse().ok())
        .unwrap_or(8080);
    let listener = TcpListener::bind((Ipv4Addr::UNSPECIFIED, port)).await?;
    tracing::info!("listening on {}", listener.local_addr()?);
    let service = app(Config::from_env()).into_make_service_with_connect_info::<SocketAddr>();
    axum::serve(listener, service)
        .with_graceful_shutdown(terminated())
        .await
}

/// Docker stops the container with SIGTERM; Ctrl-C does the same when run by hand. Open
/// WebSockets are not waited for: they are handshakes the browsers can start again.
async fn terminated() {
    let mut term = signal(SignalKind::terminate()).expect("SIGTERM handler");
    tokio::select! {
        _ = term.recv() => {}
        _ = tokio::signal::ctrl_c() => {}
    }
    tracing::info!("shutting down");
}
