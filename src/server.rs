//! The HTTP side: health, release marker, and the WebSocket each browser talks through.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{ConnectInfo, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::get;
use tokio::sync::mpsc::unbounded_channel;

use crate::Config;
use crate::protocol::{ClientMessage, Reason, ServerMessage};
use crate::rooms::{Outbox, Rooms, Seat};

struct Shared {
    config: Config,
    rooms: Arc<Rooms>,
    connections: Mutex<HashMap<IpAddr, usize>>,
}

pub fn app(config: Config) -> Router {
    let rooms = Arc::new(Rooms::new(config.max_rooms, config.room_lifetime));
    let shared = Shared {
        config,
        rooms,
        connections: Mutex::default(),
    };
    Router::new()
        .route("/", get(|| async { Html(include_str!("index.html")) }))
        .route("/healthz", get(|| async { "ok" }))
        .route("/release.json", get(release))
        .route("/ws", get(upgrade))
        .with_state(Arc::new(shared))
}

async fn release(State(shared): State<Arc<Shared>>) -> Response {
    let body = serde_json::json!({ "revision": shared.config.revision }).to_string();
    let headers = [
        (header::CONTENT_TYPE, "application/json"),
        (header::CACHE_CONTROL, "no-cache"),
    ];
    (headers, body).into_response()
}

async fn upgrade(
    State(shared): State<Arc<Shared>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    let origin = headers
        .get(header::ORIGIN)
        .and_then(|value| value.to_str().ok());
    if !origin.is_some_and(|origin| origin_allowed(&shared.config, origin)) {
        return StatusCode::FORBIDDEN.into_response();
    }
    let Some(slot) = Slot::take(&shared, client_ip(&headers, peer)) else {
        return StatusCode::TOO_MANY_REQUESTS.into_response();
    };
    let limit = shared.config.max_message_bytes;
    ws.max_message_size(limit)
        .max_frame_size(limit)
        .on_upgrade(move |socket| connection(socket, shared, slot))
}

fn origin_allowed(config: &Config, origin: &str) -> bool {
    config
        .allowed_origins
        .iter()
        .any(|allowed| allowed == origin)
        || is_local(origin)
}

/// A page served from this computer, on any port, so development builds can connect.
fn is_local(origin: &str) -> bool {
    ["http://localhost", "http://127.0.0.1"].iter().any(|host| {
        origin.strip_prefix(host).is_some_and(|rest| {
            rest.is_empty()
                || rest.strip_prefix(':').is_some_and(|port| {
                    !port.is_empty() && port.bytes().all(|b| b.is_ascii_digit())
                })
        })
    })
}

/// The browser's address. Only Caddy can reach the container, and Caddy appends the address it
/// saw to `X-Forwarded-For`, so the last entry is the one to trust.
fn client_ip(headers: &HeaderMap, peer: SocketAddr) -> IpAddr {
    headers
        .get("x-forwarded-for")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.rsplit(',').next())
        .and_then(|ip| ip.trim().parse().ok())
        .unwrap_or(peer.ip())
}

/// One of a client address's connections; its place is given back when it is dropped.
struct Slot {
    shared: Arc<Shared>,
    ip: IpAddr,
}

impl Slot {
    fn take(shared: &Arc<Shared>, ip: IpAddr) -> Option<Slot> {
        let mut connections = shared.connections.lock().unwrap();
        let count = connections.entry(ip).or_default();
        if *count >= shared.config.max_connections_per_ip {
            return None;
        }
        *count += 1;
        Some(Slot {
            shared: shared.clone(),
            ip,
        })
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        let mut connections = self.shared.connections.lock().unwrap();
        if let Some(count) = connections.get_mut(&self.ip) {
            *count -= 1;
            if *count == 0 {
                connections.remove(&self.ip);
            }
        }
    }
}

async fn connection(mut socket: WebSocket, shared: Arc<Shared>, _slot: Slot) {
    let (outbox, mut queued) = unbounded_channel();
    let mut state = Connection::default();
    loop {
        tokio::select! {
            incoming = socket.recv() => match incoming {
                Some(Ok(Message::Text(text))) => state.handle(&shared, &outbox, text.as_str()).await,
                Some(Ok(Message::Binary(_))) => {
                    let _ = outbox.send(ServerMessage::error(Reason::BadMessage));
                }
                Some(Ok(Message::Ping(_) | Message::Pong(_))) => {}
                Some(Ok(Message::Close(_)) | Err(_)) | None => break,
            },
            Some(message) = queued.recv() => {
                let text = serde_json::to_string(&message).expect("server messages serialise");
                if socket.send(Message::Text(text.into())).await.is_err() || message.is_final() {
                    break;
                }
            }
        }
    }
    if let Some(seat) = &state.seat {
        shared.rooms.leave(seat);
    }
    let _ = socket.send(Message::Close(None)).await;
}

/// The relay for a new room, if outpost gives one in time.
async fn relay(config: &Config) -> Option<serde_json::Value> {
    let url = config.outpost_url.as_deref()?;
    crate::outpost::ice_servers(url, config.outpost_timeout).await
}

/// What one WebSocket has done so far.
#[derive(Default)]
struct Connection {
    seat: Option<Seat>,
    failed_joins: u32,
    signals: u32,
}

impl Connection {
    async fn handle(&mut self, shared: &Arc<Shared>, outbox: &Outbox, text: &str) {
        let reply = |message| {
            let _ = outbox.send(message);
        };
        let Ok(message) = serde_json::from_str::<ClientMessage>(text) else {
            return reply(ServerMessage::error(Reason::BadMessage));
        };
        match message {
            ClientMessage::Create { .. } | ClientMessage::Join { .. } if self.seat.is_some() => {
                reply(ServerMessage::error(Reason::AlreadyInRoom))
            }
            ClientMessage::Create { build } => {
                match shared
                    .rooms
                    .create(build, relay(&shared.config).await, outbox)
                {
                    Ok(seat) => self.seat = Some(seat),
                    Err(refusal) => reply(refusal),
                }
            }
            ClientMessage::Join { room, build } => match shared.rooms.join(&room, build, outbox) {
                Ok(seat) => self.seat = Some(seat),
                Err(ServerMessage::Error {
                    reason: Reason::RoomNotFound,
                    ..
                }) => {
                    self.failed_joins += 1;
                    let reason = if self.failed_joins >= shared.config.max_failed_joins {
                        Reason::TooManyAttempts
                    } else {
                        Reason::RoomNotFound
                    };
                    reply(ServerMessage::error(reason))
                }
                Err(refusal) => reply(refusal),
            },
            ClientMessage::Signal { data } => {
                self.signals += 1;
                if self.signals > shared.config.max_signals {
                    return reply(ServerMessage::error(Reason::TooManySignals));
                }
                let Some(seat) = &self.seat else {
                    return reply(ServerMessage::error(Reason::NotInRoom));
                };
                if let Err(reason) = shared.rooms.signal(seat, data) {
                    reply(ServerMessage::error(reason))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_last_forwarded_address_is_the_client() {
        let peer: SocketAddr = "10.0.0.2:4000".parse().unwrap();
        let mut headers = HeaderMap::new();
        assert_eq!(
            client_ip(&headers, peer),
            "10.0.0.2".parse::<IpAddr>().unwrap()
        );

        headers.insert(
            "x-forwarded-for",
            "198.51.100.7, 203.0.113.9".parse().unwrap(),
        );
        assert_eq!(
            client_ip(&headers, peer),
            "203.0.113.9".parse::<IpAddr>().unwrap()
        );

        headers.insert("x-forwarded-for", "garbage".parse().unwrap());
        assert_eq!(
            client_ip(&headers, peer),
            "10.0.0.2".parse::<IpAddr>().unwrap()
        );
    }

    #[test]
    fn local_pages_on_any_port_are_allowed_but_lookalike_hosts_are_not() {
        for origin in [
            "http://localhost",
            "http://localhost:5173",
            "http://127.0.0.1:8080",
        ] {
            assert!(is_local(origin), "{origin}");
        }
        for origin in [
            "http://localhost.evil.example",
            "http://localhost:",
            "http://localhost:80x",
            "https://localhost",
        ] {
            assert!(!is_local(origin), "{origin}");
        }
    }
}
