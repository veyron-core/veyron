use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use metrics::counter;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::auth::frame_mac::{compute_tag, verify_tag};
use crate::auth::jwt::JwtValidator;
use crate::ipc::connection::{Outbound, SessionKeyCell};
use crate::ipc::framing::{
    serialize_header, Frame, FLAG_COMPRESSED, FLAG_FRAGMENTED, FLAG_MAC_PRESENT, MAX_PAYLOAD_SIZE,
};
use crate::ipc::messages::IncomingMessage;
use crate::utils::sync::recover_poison;

const FRAME_HEADER_SIZE: usize = 44;
const MAX_WS_PARSE_ERRORS: u32 = 16;
pub const WS_CONN_ID_BASE: u64 = 1_000_000_000;

pub struct WsGateway {
    pub router_tx: mpsc::Sender<IncomingMessage>,
    pub disconnect_tx: mpsc::Sender<u64>,
    pub conn_counter: Arc<AtomicU64>,
    pub jwt_validator: Option<Arc<JwtValidator>>,
    /// E-01: per-device credential store — a token whose `sub` names a revoked
    /// or expired device is rejected before the upgrade completes.
    pub device_store: Option<Arc<crate::auth::device_store::DeviceStore>>,
    /// Current open WS connection count, gated against `max_connections` before
    /// the upgrade completes (T-09; mirrors the UDS listener's `max_connections`).
    pub open_conns: Arc<AtomicU64>,
    pub max_connections: usize,
    /// D-07: a JWT-authenticated connection that doesn't complete
    /// registration within this window is dropped — a client that never
    /// registers never gets a session frame-MAC key.
    pub register_timeout_secs: u64,
}

/// Extract JWT from `Sec-WebSocket-Protocol: vynkor, <jwt>`.
/// Token is the first comma-separated entry that isn't "vynkor".
/// Credentials stay in a request header, never in the URL.
fn extract_ws_token(headers: &HeaderMap) -> &str {
    headers
        .get("sec-websocket-protocol")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.split(',').map(str::trim).find(|p| *p != "vynkor"))
        .unwrap_or("")
}

pub async fn ws_handler(
    ws: WebSocketUpgrade,
    headers: HeaderMap,
    State(state): State<Arc<WsGateway>>,
) -> Response {
    if let Some(validator) = &state.jwt_validator {
        let token = extract_ws_token(&headers);
        match validator.validate(token) {
            Ok(claims) => {
                // E-01: a known device row must be active — revoked/expired
                // devices die here, before any socket exists. Unknown subs pass
                // (local clients and pre-pairing tokens).
                if let Some(store) = &state.device_store {
                    if let Err(e) = store.active_secret(&claims.sub) {
                        warn!("WS: device rejected at upgrade");
                        let _ = e; // don't log token contents or device state details
                        counter!("ws_connections_rejected_total", "reason" => "device")
                            .increment(1);
                        return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
                    }
                }
            }
            Err(e) => {
                warn!("WS: JWT rejected");
                let _ = e; // don't log token contents
                return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
            }
        }
    }

    // Reserve a connection slot before upgrading (T-09). fetch_add first, then
    // back out if it pushed past the cap — avoids a check-then-increment race
    // between concurrent upgrade requests.
    if state.open_conns.fetch_add(1, Ordering::Relaxed) as usize >= state.max_connections {
        state.open_conns.fetch_sub(1, Ordering::Relaxed);
        warn!(
            "WS connection limit ({}) reached — rejecting upgrade",
            state.max_connections
        );
        counter!("ws_connections_rejected_total").increment(1);
        return (StatusCode::SERVICE_UNAVAILABLE, "too many connections").into_response();
    }

    let conn_id = state.conn_counter.fetch_add(1, Ordering::Relaxed) + WS_CONN_ID_BASE;
    let router_tx = state.router_tx.clone();
    let disconnect_tx = state.disconnect_tx.clone();
    let open_conns = Arc::clone(&state.open_conns);
    // D-07: the register-or-drop deadline only bites on authenticated
    // connections — unauthenticated local ones can already route nothing.
    let register_timeout = if state.jwt_validator.is_some() {
        state.register_timeout_secs
    } else {
        0
    };

    ws.protocols(["vynkor"])
        .on_upgrade(move |socket| async move {
            handle_socket(socket, conn_id, router_tx, disconnect_tx, register_timeout).await;
            open_conns.fetch_sub(1, Ordering::Relaxed);
        })
}

async fn handle_socket(
    mut socket: WebSocket,
    conn_id: u64,
    router_tx: mpsc::Sender<IncomingMessage>,
    disconnect_tx: mpsc::Sender<u64>,
    register_timeout_secs: u64,
) {
    info!(conn_id = conn_id, "WS client connected");

    let mut ws_parse_errors: u32 = 0;
    let (write_tx, mut write_rx) = mpsc::channel::<Outbound>(64);
    let session_key: SessionKeyCell = std::sync::Arc::new(std::sync::Mutex::new(None));
    // Key stored locally for tagging outbound frames; populated via Outbound::EnableMac.
    let mut outbound_key: Option<[u8; 32]> = None;
    // D-07: registration deadline. EnableMac (sent by the router on accepted
    // registration) both arms the MAC keys and marks the connection
    // registered; a client that never registers within the window is dropped
    // so every WS frame is MAC'd or the connection is short-lived.
    let deadline = (register_timeout_secs > 0)
        .then(|| tokio::time::Instant::now() + Duration::from_secs(register_timeout_secs));
    let mut registered = false;

    loop {
        tokio::select! {
            msg = socket.recv() => {
                match msg {
                    Some(Ok(Message::Binary(data))) => {
                        match parse_frame(&data) {
                            Ok(frame) => {
                                // Verify MAC on inbound frames once session key is active.
                                let key = *session_key.lock().unwrap_or_else(recover_poison);
                                if let Some(k) = key {
                                    let valid = frame.flags & FLAG_MAC_PRESENT != 0
                                        && match &frame.mac {
                                            Some(tag) => {
                                                let header = serialize_header(&frame);
                                                verify_tag(&k, &header, &frame.payload, tag)
                                            }
                                            None => false,
                                        };
                                    if !valid {
                                        warn!(conn_id, "WS: frame MAC invalid — dropping connection");
                                        counter!("ipc_frame_errors_total", "error" => "mac").increment(1);
                                        break;
                                    }
                                }
                                let incoming = IncomingMessage {
                                    conn_id,
                                    frame,
                                    write_tx: write_tx.clone(),
                                    session_key: session_key.clone(),
                                };
                                if router_tx.send(incoming).await.is_err() {
                                    break;
                                }
                            }
                            Err(e) => {
                                ws_parse_errors += 1;
                                warn!(
                                    conn_id = conn_id,
                                    errors = ws_parse_errors,
                                    "WS: bad frame: {e}"
                                );
                                counter!("ipc_frame_errors_total", "error" => "parse").increment(1);
                                if ws_parse_errors >= MAX_WS_PARSE_ERRORS {
                                    warn!(
                                        conn_id = conn_id,
                                        "WS: parse error budget exhausted — closing connection"
                                    );
                                    break;
                                }
                            }
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Err(e)) => {
                        warn!(conn_id = conn_id, "WS recv error: {e}");
                        break;
                    }
                    Some(Ok(_)) => {} // ping/pong/text ignored
                }
            }
            item = write_rx.recv() => {
                match item {
                    Some(Outbound::Frame(f)) => {
                        let mut frame = *f;
                        if let Some(k) = &outbound_key {
                            frame.flags |= FLAG_MAC_PRESENT;
                            let header = serialize_header(&frame);
                            frame.mac = Some(compute_tag(k, &header, &frame.payload));
                        }
                        if socket.send(Message::Binary(frame_to_bytes(&frame).into())).await.is_err() {
                            break;
                        }
                    }
                    Some(Outbound::EnableMac(k, cell)) => {
                        registered = true;
                        outbound_key = Some(k);
                        *cell.lock().unwrap_or_else(recover_poison) = Some(k);
                    }
                    None => break,
                }
            }
            _ = async {
                // the select! guard is only consulted once the future is
                // ready, and the future expression runs unconditionally — so
                // a None deadline must be a pending future, not a panic
                if let Some(d) = deadline {
                    tokio::time::sleep_until(d).await;
                } else {
                    std::future::pending::<()>().await;
                }
            }, if deadline.is_some() && !registered => {
                warn!(
                    conn_id,
                    "WS: client never registered within {register_timeout_secs}s — closing"
                );
                break;
            }
        }
    }

    info!(conn_id = conn_id, "WS client disconnected");
    let _ = disconnect_tx.send(conn_id).await;
}

pub(crate) fn parse_frame(data: &[u8]) -> Result<Frame, &'static str> {
    if data.len() < FRAME_HEADER_SIZE {
        return Err("frame too short");
    }
    let magic = u16::from_be_bytes([data[0], data[1]]);
    if magic != 0x5652 {
        return Err("bad magic");
    }
    let flags = u16::from_be_bytes([data[2], data[3]]);
    // WS has native message framing (no fragment reassembly needed) and the
    // gateway does not normalize compressed payloads before MAC verification —
    // reject rather than silently mishandle. See docs/FRAMING.md.
    if flags & FLAG_COMPRESSED != 0 {
        return Err("compressed inbound frames not supported over WebSocket");
    }
    if flags & FLAG_FRAGMENTED != 0 {
        return Err("fragmented inbound frames not supported over WebSocket");
    }
    let length = u32::from_be_bytes([data[4], data[5], data[6], data[7]]) as usize;
    if length > MAX_PAYLOAD_SIZE {
        return Err("payload too large");
    }
    let payload_end = FRAME_HEADER_SIZE + length;
    if data.len() < payload_end {
        return Err("truncated payload");
    }
    let mut target = [0u8; 32];
    target.copy_from_slice(&data[8..40]);
    let crc32 = u32::from_be_bytes([data[40], data[41], data[42], data[43]]);
    let payload = data[FRAME_HEADER_SIZE..payload_end].to_vec();
    let computed = crc32fast::hash(&payload);
    if computed != crc32 {
        return Err("CRC mismatch");
    }
    let mac = if flags & FLAG_MAC_PRESENT != 0 {
        if data.len() < payload_end + 32 {
            return Err("frame too short for MAC tag");
        }
        let mut tag = [0u8; 32];
        tag.copy_from_slice(&data[payload_end..payload_end + 32]);
        Some(tag)
    } else {
        None
    };
    Ok(Frame {
        magic,
        flags,
        length: length as u32,
        target,
        crc32,
        payload: payload.into(),
        mac,
    })
}

pub(crate) fn frame_to_bytes(frame: &Frame) -> Vec<u8> {
    let mac_len = if frame.mac.is_some() { 32 } else { 0 };
    let mut out = Vec::with_capacity(FRAME_HEADER_SIZE + frame.payload.len() + mac_len);
    out.extend_from_slice(&frame.magic.to_be_bytes());
    out.extend_from_slice(&frame.flags.to_be_bytes());
    out.extend_from_slice(&frame.length.to_be_bytes());
    out.extend_from_slice(&frame.target);
    out.extend_from_slice(&frame.crc32.to_be_bytes());
    out.extend_from_slice(&frame.payload);
    if let Some(tag) = &frame.mac {
        out.extend_from_slice(tag);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderMap;

    fn headers_with_protocol(val: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert("sec-websocket-protocol", val.parse().unwrap());
        h
    }

    #[test]
    fn extract_ws_token_vynkor_prefix() {
        let h = headers_with_protocol("vynkor, my.jwt.token");
        assert_eq!(extract_ws_token(&h), "my.jwt.token");
    }

    #[test]
    fn extract_ws_token_trims_spaces() {
        let h = headers_with_protocol("vynkor,  spaced.token  ");
        assert_eq!(extract_ws_token(&h), "spaced.token");
    }

    #[test]
    fn extract_ws_token_empty_when_only_vynkor() {
        let h = headers_with_protocol("vynkor");
        assert_eq!(extract_ws_token(&h), "");
    }

    #[test]
    fn extract_ws_token_empty_when_no_header() {
        let h = HeaderMap::new();
        assert_eq!(extract_ws_token(&h), "");
    }
}
