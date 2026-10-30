//! The messages on the WebSocket, as README.md describes them.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Which NESER (or other client) build a browser runs; host and joiner must match.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Build {
    pub version: String,
    pub commit: String,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum ClientMessage {
    Create { build: Build },
    Join { room: String, build: Build },
    Signal { data: Value },
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum ServerMessage {
    Created {
        room: String,
    },
    Joined,
    PeerJoined,
    Signal {
        data: Value,
    },
    PeerLeft,
    Error {
        reason: Reason,
        #[serde(skip_serializing_if = "Option::is_none")]
        theirs: Option<Build>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Reason {
    BadMessage,
    AlreadyInRoom,
    NotInRoom,
    NoPeer,
    RoomNotFound,
    RoomFull,
    OtherBuild,
    ServerBusy,
    TooManyAttempts,
    TooManySignals,
    RoomExpired,
}

impl ServerMessage {
    pub fn error(reason: Reason) -> Self {
        ServerMessage::Error {
            reason,
            theirs: None,
        }
    }

    /// After a final message the server closes the WebSocket.
    pub fn is_final(&self) -> bool {
        matches!(
            self,
            ServerMessage::PeerLeft
                | ServerMessage::Error {
                    reason: Reason::TooManyAttempts | Reason::TooManySignals | Reason::RoomExpired,
                    ..
                }
        )
    }
}
