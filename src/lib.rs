//! liaison: a signaling server where two browsers meet in a room and swap the messages that set
//! up a direct WebRTC connection. The protocol is described in README.md.

mod budget;
mod outpost;
mod protocol;
mod room_code;
mod rooms;
mod server;

use std::time::Duration;

pub use server::app;

#[derive(Clone, Debug)]
pub struct Config {
    /// Page origins allowed to open a WebSocket, besides local development pages.
    pub allowed_origins: Vec<String>,
    /// Source commit served from `/release.json`.
    pub revision: String,
    pub room_lifetime: Duration,
    pub max_rooms: usize,
    pub max_connections_per_ip: usize,
    pub max_message_bytes: usize,
    /// Joins to unknown rooms one WebSocket may make; the last one closes it.
    pub max_failed_joins: u32,
    pub max_signals: u32,
    /// outpost's `POST /rooms`, which opens a TURN relay for one game; `None` leaves the relay
    /// out. outpost is reached over the internal Docker network.
    pub outpost_url: Option<String>,
    /// How long a new room waits for outpost before it opens without a relay.
    pub outpost_timeout: Duration,
    /// Relays one client address may be given; beyond that its rooms open without one. Each
    /// relay is a room on outpost, which opens a limited number for everyone, so this keeps one
    /// address from using them all up.
    pub relays_per_address: Rate,
}

/// A steady rate with room for bursts.
#[derive(Clone, Copy, Debug)]
pub struct Rate {
    pub per_second: f64,
    pub burst: f64,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            allowed_origins: vec!["https://neser.rmstdope.eu".to_string()],
            revision: "unknown".to_string(),
            room_lifetime: Duration::from_secs(15 * 60),
            max_rooms: 1000,
            max_connections_per_ip: 16,
            max_message_bytes: 16 * 1024,
            max_failed_joins: 5,
            max_signals: 200,
            outpost_url: Some("http://outpost-web:8081/rooms".to_string()),
            outpost_timeout: Duration::from_secs(1),
            relays_per_address: Rate {
                per_second: 0.1,
                burst: 3.0,
            },
        }
    }
}

impl Config {
    /// The defaults, with `LIAISON_ALLOWED_ORIGINS`, `LIAISON_REVISION` and
    /// `LIAISON_OUTPOST_URL` (empty for no relay) applied.
    pub fn from_env() -> Self {
        let mut config = Config::default();
        if let Ok(origins) = std::env::var("LIAISON_ALLOWED_ORIGINS") {
            config.allowed_origins = origins
                .split(',')
                .map(str::trim)
                .filter(|origin| !origin.is_empty())
                .map(String::from)
                .collect();
        }
        if let Ok(revision) = std::env::var("LIAISON_REVISION") {
            config.revision = revision;
        }
        if let Ok(url) = std::env::var("LIAISON_OUTPOST_URL") {
            config.outpost_url = Some(url).filter(|url| !url.is_empty());
        }
        config
    }
}
