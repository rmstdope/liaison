//! liaison: a signaling server where two browsers meet in a room and swap the messages that set
//! up a direct WebRTC connection. The protocol is described in README.md.

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
        }
    }
}

impl Config {
    /// The defaults, with `LIAISON_ALLOWED_ORIGINS` and `LIAISON_REVISION` applied.
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
        config
    }
}
