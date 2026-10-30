//! Smoke test for a running liaison, used by CI against the production image:
//! `cargo run --example smoke -- ws://127.0.0.1:18080/ws`. Two clients meet in a room and pass a
//! signal, and a page from a foreign origin is turned away.

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::{Error, Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

async fn open(url: &str, origin: &str) -> Result<Socket, Error> {
    let mut request = url.into_client_request()?;
    request
        .headers_mut()
        .insert("origin", origin.parse().unwrap());
    Ok(tokio_tungstenite::connect_async(request).await?.0)
}

async fn send(socket: &mut Socket, message: Value) {
    socket
        .send(Message::text(message.to_string()))
        .await
        .unwrap();
}

async fn recv(socket: &mut Socket) -> Value {
    let message = tokio::time::timeout(Duration::from_secs(5), socket.next())
        .await
        .expect("no answer in time")
        .expect("connection ended")
        .expect("connection failed");
    serde_json::from_str(message.to_text().unwrap()).unwrap()
}

#[tokio::main]
async fn main() {
    let url = std::env::args().nth(1).expect("usage: smoke <ws-url>");
    let build = json!({"version": "smoke", "commit": "smoke"});

    let mut host = open(&url, "https://neser.rmstdope.eu").await.unwrap();
    send(&mut host, json!({"type": "create", "build": build})).await;
    let created = recv(&mut host).await;
    let room = created["room"].as_str().expect("a room code").to_string();

    let mut joiner = open(&url, "https://neser.rmstdope.eu").await.unwrap();
    send(
        &mut joiner,
        json!({"type": "join", "room": room, "build": build}),
    )
    .await;
    // A relay is there when outpost answers, as in production, and left out otherwise.
    let joined = recv(&mut joiner).await;
    assert_eq!(joined["type"], "joined");
    assert_eq!(
        joined.get("iceServers"),
        created.get("iceServers"),
        "the same relay for both"
    );
    if let Some(servers) = joined.get("iceServers") {
        assert!(
            servers[0]["urls"].is_array(),
            "a relay with urls: {servers}"
        );
        println!("relay: {}", servers[0]["urls"]);
    }
    assert_eq!(recv(&mut host).await, json!({"type": "peer-joined"}));

    send(&mut host, json!({"type": "signal", "data": "offer"})).await;
    assert_eq!(
        recv(&mut joiner).await,
        json!({"type": "signal", "data": "offer"})
    );

    match open(&url, "https://evil.example").await {
        Err(Error::Http(response)) => assert_eq!(response.status(), 403),
        _ => panic!("a foreign origin was not refused"),
    }
    println!("smoke test passed");
}
