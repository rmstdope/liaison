//! The open rooms. Every message caused by a change to a room is queued while the room table is
//! locked, so the two browsers see events in the order they happened (a joiner always hears
//! `joined` before the host's first signal).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::Value;
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::AbortHandle;

use crate::protocol::{Build, Reason, ServerMessage};
use crate::room_code;

/// Where messages for one WebSocket are queued.
pub type Outbox = UnboundedSender<ServerMessage>;

/// A connection's place in a room. `serial` tells this room apart from a later one that happens
/// to get the same code.
#[derive(Clone, Debug)]
pub struct Seat {
    code: String,
    serial: u64,
    host: bool,
}

struct Room {
    serial: u64,
    build: Build,
    host: Outbox,
    joiner: Option<Outbox>,
    expiry: AbortHandle,
    /// The relay both players are given.
    ice_servers: Option<Value>,
}

#[derive(Default)]
struct Table {
    rooms: HashMap<String, Room>,
    next_serial: u64,
}

pub struct Rooms {
    table: Mutex<Table>,
    max_rooms: usize,
    lifetime: Duration,
}

impl Rooms {
    pub fn new(max_rooms: usize, lifetime: Duration) -> Self {
        Rooms {
            table: Mutex::default(),
            max_rooms,
            lifetime,
        }
    }

    /// Whether no more rooms can be opened now.
    pub fn is_full(&self) -> bool {
        self.table.lock().unwrap().rooms.len() >= self.max_rooms
    }

    /// Opens a room hosted by `outbox`, tells it the code, and expires the room after its lifetime.
    pub fn create(
        self: &Arc<Self>,
        build: Build,
        ice_servers: Option<Value>,
        outbox: &Outbox,
    ) -> Result<Seat, ServerMessage> {
        let mut table = self.table.lock().unwrap();
        if table.rooms.len() >= self.max_rooms {
            return Err(ServerMessage::error(Reason::ServerBusy));
        }
        let code = loop {
            let code = room_code::generate();
            if !table.rooms.contains_key(&code) {
                break code;
            }
        };
        table.next_serial += 1;
        let seat = Seat {
            code: code.clone(),
            serial: table.next_serial,
            host: true,
        };
        let (rooms, timed, lifetime) = (Arc::downgrade(self), seat.clone(), self.lifetime);
        let expiry = tokio::spawn(async move {
            tokio::time::sleep(lifetime).await;
            if let Some(rooms) = rooms.upgrade() {
                rooms.expire(&timed);
            }
        })
        .abort_handle();
        let room = Room {
            serial: seat.serial,
            build,
            host: outbox.clone(),
            joiner: None,
            expiry,
            ice_servers: ice_servers.clone(),
        };
        table.rooms.insert(code.clone(), room);
        tracing::info!(open = table.rooms.len(), "room opened");
        let _ = outbox.send(ServerMessage::Created {
            room: code,
            ice_servers,
        });
        Ok(seat)
    }

    /// Seats `outbox` as the joiner of the room the player typed, and tells both browsers.
    pub fn join(&self, typed: &str, build: Build, outbox: &Outbox) -> Result<Seat, ServerMessage> {
        let mut table = self.table.lock().unwrap();
        let not_found = || ServerMessage::error(Reason::RoomNotFound);
        let code = room_code::normalise(typed).ok_or_else(not_found)?;
        let room = table.rooms.get_mut(&code).ok_or_else(not_found)?;
        if room.joiner.is_some() {
            return Err(ServerMessage::error(Reason::RoomFull));
        }
        if room.build != build {
            return Err(ServerMessage::Error {
                reason: Reason::OtherBuild,
                theirs: Some(room.build.clone()),
            });
        }
        room.joiner = Some(outbox.clone());
        let _ = outbox.send(ServerMessage::Joined {
            ice_servers: room.ice_servers.clone(),
        });
        let _ = room.host.send(ServerMessage::PeerJoined);
        Ok(Seat {
            code,
            serial: room.serial,
            host: false,
        })
    }

    /// Passes `data` to the other browser in the seat's room.
    pub fn signal(&self, seat: &Seat, data: Value) -> Result<(), Reason> {
        let table = self.table.lock().unwrap();
        let room = table
            .rooms
            .get(&seat.code)
            .filter(|room| room.serial == seat.serial)
            .ok_or(Reason::NotInRoom)?;
        let other = if seat.host {
            room.joiner.as_ref().ok_or(Reason::NoPeer)?
        } else {
            &room.host
        };
        let _ = other.send(ServerMessage::Signal { data });
        Ok(())
    }

    /// The seat's browser has gone: the room ends and the other browser is told.
    pub fn leave(&self, seat: &Seat) {
        if let Some(room) = self.remove(seat) {
            let other = if seat.host {
                room.joiner
            } else {
                Some(room.host)
            };
            if let Some(other) = other {
                let _ = other.send(ServerMessage::PeerLeft);
            }
        }
    }

    /// The room has lived too long: it ends and both browsers are told.
    pub fn expire(&self, seat: &Seat) {
        if let Some(room) = self.remove(seat) {
            for outbox in std::iter::once(room.host).chain(room.joiner) {
                let _ = outbox.send(ServerMessage::error(Reason::RoomExpired));
            }
        }
    }

    fn remove(&self, seat: &Seat) -> Option<Room> {
        let mut table = self.table.lock().unwrap();
        let current = table
            .rooms
            .get(&seat.code)
            .is_some_and(|room| room.serial == seat.serial);
        let room = current.then(|| table.rooms.remove(&seat.code)).flatten()?;
        room.expiry.abort();
        tracing::info!(open = table.rooms.len(), "room closed");
        Some(room)
    }
}
