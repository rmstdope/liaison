//! Asking outpost, the TURN relay, for a room for one game. outpost is reached over the internal
//! Docker network with plain HTTP and answers with a `Content-Length`, so a small HTTP/1.1
//! request is all it takes.

use std::time::Duration;

use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// The most of outpost's answer that is read.
const MAX_ANSWER: u64 = 16 * 1024;

/// An `iceServers` list holding the relay outpost opened, or `None` if it gave none within
/// `timeout`. Without one the room still works, only without a relay.
pub async fn ice_servers(url: &str, timeout: Duration) -> Option<Value> {
    match tokio::time::timeout(timeout, open_room(url)).await {
        Ok(Ok(server)) => Some(Value::Array(vec![server])),
        Ok(Err(reason)) => {
            tracing::warn!("a room opens without a relay: {reason}");
            None
        }
        Err(_) => {
            tracing::warn!("a room opens without a relay: outpost did not answer in time");
            None
        }
    }
}

/// `POST`s to `url` and answers its `RTCIceServer`: a JSON object with a list of `urls`.
async fn open_room(url: &str) -> Result<Value, String> {
    let rest = url
        .strip_prefix("http://")
        .ok_or("the outpost URL must start with http://")?;
    let (authority, path) = match rest.split_once('/') {
        Some((authority, path)) => (authority, format!("/{path}")),
        None => (rest, "/".to_string()),
    };
    let mut stream = TcpStream::connect(authority)
        .await
        .map_err(|error| format!("cannot reach outpost: {error}"))?;
    let request = format!(
        "POST {path} HTTP/1.1\r\nHost: {authority}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
    );
    stream
        .write_all(request.as_bytes())
        .await
        .map_err(|error| format!("cannot ask outpost: {error}"))?;
    let mut answer = Vec::new();
    stream
        .take(MAX_ANSWER)
        .read_to_end(&mut answer)
        .await
        .map_err(|error| format!("cannot read outpost's answer: {error}"))?;
    let answer = String::from_utf8(answer).map_err(|_| "outpost's answer is not text")?;
    let (head, body) = answer
        .split_once("\r\n\r\n")
        .ok_or("outpost's answer is not HTTP")?;
    let status = head.split(' ').nth(1).unwrap_or("none");
    if status != "200" {
        return Err(format!("outpost answered {status}"));
    }
    let server: Value =
        serde_json::from_str(body).map_err(|_| "outpost's answer is not JSON".to_string())?;
    if !server.get("urls").is_some_and(Value::is_array) {
        return Err("outpost's answer has no urls".to_string());
    }
    Ok(server)
}
