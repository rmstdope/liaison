# liaison

Signaling server for NESER network play (and anything else that needs two browsers to find each
other). Two browsers meet in a room named by a short code and swap the messages that set up a
direct WebRTC connection. Once that connection is open they leave, and liaison never sees what
they send each other.

Rooms live in memory only: nothing is stored, nothing is logged about who met whom, and a restart
only drops handshakes that are still in progress.

## Running

```sh
cargo run                      # listens on 0.0.0.0:8080
cargo test                     # unit and end-to-end tests
```

| Environment variable | Default | Meaning |
|---|---|---|
| `PORT` | `8080` | Port to listen on. |
| `LIAISON_ALLOWED_ORIGINS` | `https://neser.rmstdope.eu` | Comma-separated page origins allowed to connect. `http://localhost` and `http://127.0.0.1` on any port are always allowed, for development. |
| `LIAISON_REVISION` | `unknown` | Source commit served from `/release.json`; the image sets it at build time. |
| `LIAISON_OUTPOST_URL` | `http://outpost-web:8081/rooms` | outpost's internal `POST /rooms`, which opens a TURN relay for each new room. Empty leaves the relay out. |

## HTTP endpoints

| Path | Answer |
|---|---|
| `GET /` | A short page saying what liaison is, linking to NESER |
| `GET /healthz` | `200 ok` |
| `GET /release.json` | `{"revision":"<commit>"}`, never cached |
| `GET /ws` | WebSocket upgrade; requires an allowed `Origin` header |

## Protocol

One WebSocket per browser. Every message is a JSON object with a `type`.

```text
Host                         liaison                          Joiner
 |-- create {build} ----------->|                                |
 |<-- created {room} -----------|                                |
 |                              |<------------ join {room,build} -|
 |<-- peer-joined --------------|-- joined ---------------------->|
 |-- signal {data} ------------>|-- signal {data} --------------->|
 |<-- signal {data} ------------|<------------ signal {data} -----|
 |   ... until the WebRTC connection is open, then both close the WebSocket ...
```

### From a browser

| Message | Meaning |
|---|---|
| `{"type":"create","build":{"version":"…","commit":"…"}}` | Open a new room as its host. |
| `{"type":"join","room":"ABC-DEF","build":{…}}` | Join a room. The code is case-insensitive, and spaces and dashes are ignored. |
| `{"type":"signal","data":…}` | Pass any JSON value (an offer, an answer, a connection candidate) to the other browser in the room. |

### From liaison

| Message | Meaning |
|---|---|
| `{"type":"created","room":"ABC-DEF","iceServers":[…]}` | The room is open; show the code to the player. |
| `{"type":"joined","iceServers":[…]}` | To the joiner: you are in the room. |
| `{"type":"peer-joined"}` | To the host: someone joined; send your offer now. |
| `{"type":"signal","data":…}` | The other browser's `data`, unchanged. |
| `{"type":"peer-left"}` | The other browser left; the room is gone. **Final.** |
| `{"type":"error","reason":"…"}` | See below. |

`iceServers`, when present, is the room's TURN relay as `RTCPeerConnection` takes it:
`[{"urls":["turn:…?transport=udp","turn:…?transport=tcp"],"username":"…","credential":"…"}]`.
Both browsers get the same entry. They can then connect through
[outpost](https://github.com/rmstdope/outpost) when their networks cannot connect directly.
liaison asks outpost for it once, on `create`. If outpost does not answer within a second, or
refuses, the field is left out and the room works as before, for direct connections only. A full
liaison asks for nothing. Each relay is a room on outpost, which opens only a limited number for
everyone, so a client address gets a limited number of relays (see Limits). An attacker with
many addresses can still use up outpost's rooms for a while; outpost's own limits bound that.

A room holds two browsers and ends when either leaves; a host whose joiner went away creates a new
room. After a **final** message liaison closes the WebSocket.

| Error `reason` | When | Final |
|---|---|---|
| `bad-message` | Not JSON, or not a message above. | no |
| `already-in-room` | A second `create` or `join` on the same WebSocket. | no |
| `not-in-room` | `signal` before `create` or `join`. | no |
| `no-peer` | `signal` from a host nobody has joined yet. | no |
| `room-not-found` | No room with that code. | no |
| `room-full` | The room already has a joiner. | no |
| `other-build` | The joiner's build differs from the host's; `theirs` carries the host's build. | no |
| `server-busy` | Too many rooms are open. | no |
| `too-many-attempts` | Too many joins to unknown rooms on one WebSocket. | yes |
| `too-many-signals` | Too many `signal` messages on one WebSocket. | yes |
| `room-expired` | The room outlived its lifetime (15 minutes). | yes |

### Limits

| Limit | Value |
|---|---|
| Room lifetime | 15 minutes |
| Open rooms | 1000 |
| WebSockets per client address | 16 |
| Message size | 16 KiB (larger closes the WebSocket) |
| Joins to unknown rooms per WebSocket | 5 |
| `signal` messages per WebSocket | 200 |
| Relays per client address | 3 at once, one more every 10 s; beyond that rooms open without one |

Room codes are six characters from `ABCDEFGHJKLMNPQRSTUVWXYZ23456789` (no `I`, `O`, `0` or `1`),
shown as `ABC-DEF`.

## Deployment

CI (`.github/workflows/ci.yml`) checks formatting, runs clippy and every test, then builds the
amd64 image, starts it and probes it: health, `/release.json`, the non-root user, a real room
handshake (`cargo run --example smoke -- ws://…/ws`) and a quick clean stop. On `main` it then
pushes that tested image to `ghcr.io/rmstdope/liaison` and uploads the `deployment-manifest`
artifact that `rmstdope-infra` deploys to `liaison.rmstdope.eu`. The container serves plain HTTP
on port 8080 behind Caddy, which terminates TLS and passes WebSocket upgrades through.

liaison is a standard static app for `rmstdope-infra` (one HTTP container, no volumes, no
secrets). Once main CI has published a build:

```sh
rmstdope-deploy app register liaison \
  --repository rmstdope/liaison --image-repository ghcr.io/rmstdope/liaison
rmstdope-deploy release liaison --latest
rmstdope-deploy auto enroll liaison
rmstdope-deploy auto resume liaison
```

Before that: give the VPS's GHCR identity read access to the `liaison` package (or make it
public), give the VPS polling token read access to `rmstdope/liaison`, and point DNS for
`liaison.rmstdope.eu` at the VPS.
