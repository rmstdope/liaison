//! End-to-end tests: a real server on a random port, real WebSocket clients, and the protocol in
//! README.md as the specification.

use std::net::SocketAddr;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use liaison::{Config, app};
use serde_json::{Value, json};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::{self, Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

const ORIGIN: &str = "https://neser.rmstdope.eu";
const WAIT: Duration = Duration::from_secs(2);

fn config() -> Config {
    Config {
        allowed_origins: vec![ORIGIN.to_string()],
        ..Config::default()
    }
}

async fn start(config: Config) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let service = app(config).into_make_service_with_connect_info::<SocketAddr>();
    tokio::spawn(async move { axum::serve(listener, service).await.unwrap() });
    addr
}

struct Client(WebSocketStream<MaybeTlsStream<TcpStream>>);

async fn connect_with(
    addr: SocketAddr,
    headers: &[(&'static str, &str)],
) -> Result<Client, tungstenite::Error> {
    let mut request = format!("ws://{addr}/ws").into_client_request().unwrap();
    for (name, value) in headers {
        request.headers_mut().insert(*name, value.parse().unwrap());
    }
    let (stream, _) = tokio_tungstenite::connect_async(request).await?;
    Ok(Client(stream))
}

async fn connect(addr: SocketAddr) -> Client {
    connect_with(addr, &[("origin", ORIGIN)]).await.unwrap()
}

fn rejected_status(result: Result<Client, tungstenite::Error>) -> u16 {
    match result {
        Err(tungstenite::Error::Http(response)) => response.status().as_u16(),
        Err(other) => panic!("unexpected error {other}"),
        Ok(_) => panic!("connection was accepted"),
    }
}

impl Client {
    async fn send(&mut self, message: Value) {
        self.0
            .send(Message::text(message.to_string()))
            .await
            .unwrap();
    }

    async fn send_raw(&mut self, text: &str) {
        self.0.send(Message::text(text)).await.unwrap();
    }

    async fn recv(&mut self) -> Value {
        loop {
            let message = tokio::time::timeout(WAIT, self.0.next())
                .await
                .expect("no message in time")
                .expect("connection ended")
                .expect("connection failed");
            match message {
                Message::Text(text) => return serde_json::from_str(&text).unwrap(),
                Message::Ping(_) | Message::Pong(_) => continue,
                other => panic!("unexpected frame {other:?}"),
            }
        }
    }

    /// The server ends the connection: a close frame, the end of the stream or a reset.
    async fn closed(&mut self) {
        loop {
            match tokio::time::timeout(WAIT, self.0.next())
                .await
                .expect("still open")
            {
                None | Some(Err(_)) | Some(Ok(Message::Close(_))) => return,
                Some(Ok(Message::Ping(_) | Message::Pong(_))) => continue,
                Some(Ok(other)) => panic!("expected close, got {other:?}"),
            }
        }
    }

    async fn close(mut self) {
        self.0.close(None).await.unwrap();
        while let Ok(Some(Ok(_))) = tokio::time::timeout(WAIT, self.0.next()).await {}
    }
}

fn build() -> Value {
    json!({"version": "1.2.3", "commit": "abc123"})
}

fn error(reason: &str) -> Value {
    json!({"type": "error", "reason": reason})
}

async fn host(addr: SocketAddr) -> (Client, String) {
    let mut host = connect(addr).await;
    host.send(json!({"type": "create", "build": build()})).await;
    let created = host.recv().await;
    assert_eq!(created["type"], "created");
    (host, created["room"].as_str().unwrap().to_string())
}

async fn pair(addr: SocketAddr) -> (Client, Client) {
    let (mut host, room) = host(addr).await;
    let mut joiner = connect(addr).await;
    joiner
        .send(json!({"type": "join", "room": room, "build": build()}))
        .await;
    assert_eq!(joiner.recv().await, json!({"type": "joined"}));
    assert_eq!(host.recv().await, json!({"type": "peer-joined"}));
    (host, joiner)
}

#[tokio::test]
async fn a_created_room_has_a_six_character_code_in_two_halves() {
    let addr = start(config()).await;
    let (_host, room) = host(addr).await;

    let (left, right) = room.split_once('-').expect("a dash");
    for half in [left, right] {
        assert_eq!(half.len(), 3, "{room}");
        assert!(
            half.chars()
                .all(|c| "ABCDEFGHJKLMNPQRSTUVWXYZ23456789".contains(c)),
            "{room}"
        );
    }
}

#[tokio::test]
async fn host_and_joiner_pass_signals_both_ways_unchanged() {
    let addr = start(config()).await;
    let (mut host, mut joiner) = pair(addr).await;

    let offer = json!({"kind": "offer", "sdp": "v=0\r\n...", "nested": [1, null, true]});
    host.send(json!({"type": "signal", "data": offer})).await;
    assert_eq!(
        joiner.recv().await,
        json!({"type": "signal", "data": offer})
    );

    joiner
        .send(json!({"type": "signal", "data": "answer"}))
        .await;
    assert_eq!(
        host.recv().await,
        json!({"type": "signal", "data": "answer"})
    );
}

#[tokio::test]
async fn a_room_code_is_found_whatever_its_case_spacing_and_dashes() {
    let addr = start(config()).await;
    let (_host, room) = host(addr).await;
    let typed = format!(" {} ", room.replace('-', " ").to_lowercase());

    let mut joiner = connect(addr).await;
    joiner
        .send(json!({"type": "join", "room": typed, "build": build()}))
        .await;
    assert_eq!(joiner.recv().await, json!({"type": "joined"}));
}

#[tokio::test]
async fn joining_an_unknown_room_is_refused_and_the_joiner_may_try_again() {
    let addr = start(config()).await;
    let (_host, room) = host(addr).await;
    let mut joiner = connect(addr).await;

    joiner
        .send(json!({"type": "join", "room": "ZZZ-ZZZ", "build": build()}))
        .await;
    assert_eq!(joiner.recv().await, error("room-not-found"));
    joiner
        .send(json!({"type": "join", "room": "not a code", "build": build()}))
        .await;
    assert_eq!(joiner.recv().await, error("room-not-found"));

    joiner
        .send(json!({"type": "join", "room": room, "build": build()}))
        .await;
    assert_eq!(joiner.recv().await, json!({"type": "joined"}));
}

#[tokio::test]
async fn too_many_joins_to_unknown_rooms_end_the_connection() {
    let addr = start(Config {
        max_failed_joins: 3,
        ..config()
    })
    .await;
    let mut guesser = connect(addr).await;

    for _ in 0..2 {
        guesser
            .send(json!({"type": "join", "room": "ZZZ-ZZZ", "build": build()}))
            .await;
        assert_eq!(guesser.recv().await, error("room-not-found"));
    }
    guesser
        .send(json!({"type": "join", "room": "ZZZ-ZZZ", "build": build()}))
        .await;
    assert_eq!(guesser.recv().await, error("too-many-attempts"));
    guesser.closed().await;
}

#[tokio::test]
async fn a_room_takes_one_joiner() {
    let addr = start(config()).await;
    let (_host, room) = host(addr).await;
    let mut first = connect(addr).await;
    first
        .send(json!({"type": "join", "room": room, "build": build()}))
        .await;
    assert_eq!(first.recv().await, json!({"type": "joined"}));

    let mut second = connect(addr).await;
    second
        .send(json!({"type": "join", "room": room, "build": build()}))
        .await;
    assert_eq!(second.recv().await, error("room-full"));
}

#[tokio::test]
async fn a_joiner_on_another_build_is_told_the_hosts_build_and_the_room_stays_open() {
    let addr = start(config()).await;
    let (_host, room) = host(addr).await;
    let mut other = connect(addr).await;

    let theirs = json!({"version": "1.2.4", "commit": "def456"});
    other
        .send(json!({"type": "join", "room": room, "build": theirs}))
        .await;
    assert_eq!(
        other.recv().await,
        json!({"type": "error", "reason": "other-build", "theirs": build()})
    );

    let mut same = connect(addr).await;
    same.send(json!({"type": "join", "room": room, "build": build()}))
        .await;
    assert_eq!(same.recv().await, json!({"type": "joined"}));
}

#[tokio::test]
async fn when_the_joiner_leaves_the_host_is_told_and_the_room_is_gone() {
    let addr = start(config()).await;
    let (mut host, joiner) = pair(addr).await;

    joiner.close().await;
    assert_eq!(host.recv().await, json!({"type": "peer-left"}));
    host.closed().await;
}

#[tokio::test]
async fn when_the_host_leaves_the_joiner_is_told_and_the_code_no_longer_works() {
    let addr = start(config()).await;
    let (mut host, room) = host(addr).await;
    let mut joiner = connect(addr).await;
    joiner
        .send(json!({"type": "join", "room": room, "build": build()}))
        .await;
    assert_eq!(joiner.recv().await, json!({"type": "joined"}));
    assert_eq!(host.recv().await, json!({"type": "peer-joined"}));

    host.close().await;
    assert_eq!(joiner.recv().await, json!({"type": "peer-left"}));
    joiner.closed().await;

    let mut late = connect(addr).await;
    late.send(json!({"type": "join", "room": room, "build": build()}))
        .await;
    assert_eq!(late.recv().await, error("room-not-found"));
}

#[tokio::test]
async fn a_room_that_outlives_its_lifetime_expires_for_both() {
    let addr = start(Config {
        room_lifetime: Duration::from_millis(300),
        ..config()
    })
    .await;
    let (mut host, room) = host(addr).await;
    let mut joiner = connect(addr).await;
    joiner
        .send(json!({"type": "join", "room": room, "build": build()}))
        .await;
    assert_eq!(joiner.recv().await, json!({"type": "joined"}));
    assert_eq!(host.recv().await, json!({"type": "peer-joined"}));

    assert_eq!(host.recv().await, error("room-expired"));
    assert_eq!(joiner.recv().await, error("room-expired"));
    host.closed().await;
    joiner.closed().await;

    let mut late = connect(addr).await;
    late.send(json!({"type": "join", "room": room, "build": build()}))
        .await;
    assert_eq!(late.recv().await, error("room-not-found"));
}

#[tokio::test]
async fn signals_need_a_room_and_someone_in_it() {
    let addr = start(config()).await;
    let mut lonely = connect(addr).await;
    lonely.send(json!({"type": "signal", "data": 1})).await;
    assert_eq!(lonely.recv().await, error("not-in-room"));

    let (mut host, _) = host(addr).await;
    host.send(json!({"type": "signal", "data": 1})).await;
    assert_eq!(host.recv().await, error("no-peer"));
}

#[tokio::test]
async fn one_connection_is_in_one_room_at_most() {
    let addr = start(config()).await;
    let (mut host, room) = host(addr).await;

    host.send(json!({"type": "create", "build": build()})).await;
    assert_eq!(host.recv().await, error("already-in-room"));
    host.send(json!({"type": "join", "room": room, "build": build()}))
        .await;
    assert_eq!(host.recv().await, error("already-in-room"));
}

#[tokio::test]
async fn a_message_that_is_not_in_the_protocol_is_refused_and_the_connection_stays() {
    let addr = start(config()).await;
    let mut client = connect(addr).await;

    for text in [
        "not json",
        r#"{"type":"dance"}"#,
        r#"{"type":"create"}"#,
        "[]",
    ] {
        client.send_raw(text).await;
        assert_eq!(client.recv().await, error("bad-message"), "{text}");
    }
    client.0.send(Message::binary(vec![1, 2, 3])).await.unwrap();
    assert_eq!(client.recv().await, error("bad-message"));

    client
        .send(json!({"type": "create", "build": build()}))
        .await;
    assert_eq!(client.recv().await["type"], "created");
}

#[tokio::test]
async fn too_many_signals_end_the_connection_and_the_room() {
    let addr = start(Config {
        max_signals: 2,
        ..config()
    })
    .await;
    let (mut host, mut joiner) = pair(addr).await;

    for n in 0..2 {
        host.send(json!({"type": "signal", "data": n})).await;
        assert_eq!(joiner.recv().await, json!({"type": "signal", "data": n}));
    }
    host.send(json!({"type": "signal", "data": 2})).await;
    assert_eq!(host.recv().await, error("too-many-signals"));
    host.closed().await;
    assert_eq!(joiner.recv().await, json!({"type": "peer-left"}));
}

#[tokio::test]
async fn an_oversized_message_ends_the_connection() {
    let addr = start(Config {
        max_message_bytes: 1024,
        ..config()
    })
    .await;
    let (mut host, mut joiner) = pair(addr).await;

    host.send(json!({"type": "signal", "data": "x".repeat(2048)}))
        .await;
    host.closed().await;
    assert_eq!(joiner.recv().await, json!({"type": "peer-left"}));
}

#[tokio::test]
async fn no_more_rooms_open_than_the_limit() {
    let addr = start(Config {
        max_rooms: 1,
        ..config()
    })
    .await;
    let (_host, _) = host(addr).await;

    let mut second = connect(addr).await;
    second
        .send(json!({"type": "create", "build": build()}))
        .await;
    assert_eq!(second.recv().await, error("server-busy"));
}

#[tokio::test]
async fn only_allowed_page_origins_may_connect() {
    let addr = start(config()).await;

    assert_eq!(
        rejected_status(connect_with(addr, &[("origin", "https://evil.example")]).await),
        403
    );
    assert_eq!(rejected_status(connect_with(addr, &[]).await), 403);
    for dev in [
        "http://localhost:5173",
        "http://127.0.0.1:8080",
        "http://localhost",
    ] {
        assert!(
            connect_with(addr, &[("origin", dev)]).await.is_ok(),
            "{dev}"
        );
    }
    assert_eq!(
        rejected_status(connect_with(addr, &[("origin", "http://localhost.evil.example")]).await),
        403
    );
}

#[tokio::test]
async fn each_client_address_has_a_limited_number_of_connections() {
    let addr = start(Config {
        max_connections_per_ip: 2,
        ..config()
    })
    .await;
    let from = |ip: &'static str| [("origin", ORIGIN), ("x-forwarded-for", ip)];

    let first = connect_with(addr, &from("203.0.113.1")).await.unwrap();
    let _second = connect_with(addr, &from("203.0.113.1")).await.unwrap();
    assert_eq!(
        rejected_status(connect_with(addr, &from("203.0.113.1")).await),
        429
    );
    assert!(connect_with(addr, &from("203.0.113.2")).await.is_ok());

    first.close().await;
    let mut freed = None;
    for _ in 0..20 {
        if let Ok(client) = connect_with(addr, &from("203.0.113.1")).await {
            freed = Some(client);
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(freed.is_some(), "a closed connection gives its place back");
}

mod http {
    use super::*;
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode, header};
    use tower::ServiceExt;

    async fn get(config: Config, path: &str) -> axum::http::Response<Body> {
        app(config)
            .oneshot(Request::get(path).body(Body::empty()).unwrap())
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn health_is_ok() {
        let response = get(config(), "/healthz").await;
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn release_json_names_the_revision_and_is_never_cached() {
        let revision = "0123456789abcdef0123456789abcdef01234567";
        let response = get(
            Config {
                revision: revision.to_string(),
                ..config()
            },
            "/release.json",
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[header::CONTENT_TYPE], "application/json");
        assert_eq!(response.headers()[header::CACHE_CONTROL], "no-cache");
        let body = to_bytes(response.into_body(), 1024).await.unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&body).unwrap(),
            json!({"revision": revision})
        );
    }
}
