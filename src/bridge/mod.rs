//! D-06: `role: client` — mirror local plugins on a remote host kernel.
//! One WS connection per mirrored capability, registered on the host as
//! `device.<cap>`. See docs/REMOTE_DEVICES_ROADMAP.md (D-06).

use crate::api::websocket::frame_to_bytes;
use crate::auth::frame_mac::{compute_tag, derive_session_key, verify_tag};
use crate::auth::permissions::normalize_permission;
use crate::ipc::connection::{out_frame, Outbound, SessionKeyCell};
use crate::ipc::framing::{
    build_frame, parse_frame, serialize_header, target_as_str, target_bytes, Frame,
    FLAG_MAC_PRESENT,
};
use crate::ipc::messages::IncomingMessage;
use crate::plugins::registry::{DeviceMeta, PluginRegistry};
use crate::proto::vynkor::{envelope, Envelope, PluginManifest, PluginRegister};
use crate::utils::config::BridgeConfig;
use crate::utils::errors::VynkorError;
use crate::utils::sync::recover_poison;
use axum::http::HeaderValue;
use futures_util::{SinkExt, StreamExt};
use prost::Message;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
use tracing::{info, warn};

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// conn_ids for bridge connections: above the WS gateway's base so the two
/// id spaces never collide in the registry's by_conn_id map.
const BRIDGE_CONN_ID_BASE: u64 = 2_000_000_000;

const BRIDGE_MAX_BACKOFF: Duration = Duration::from_secs(30);

#[derive(Debug)]
enum BridgeError {
    Connect(String),
    Register(String),
    Wire(VynkorError),
    Mac,
    Disconnected,
    Shutdown,
}

impl std::fmt::Display for BridgeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BridgeError::Connect(e) => write!(f, "connect: {e}"),
            BridgeError::Register(e) => write!(f, "registration rejected: {e}"),
            BridgeError::Wire(e) => write!(f, "wire: {e}"),
            BridgeError::Mac => write!(f, "frame MAC verification failed"),
            BridgeError::Disconnected => write!(f, "disconnected"),
            BridgeError::Shutdown => write!(f, "shutdown"),
        }
    }
}

impl std::error::Error for BridgeError {}

/// Host-outbound channels of every live bridge connection. The router's
/// forward() miss path falls through to this to reach the host.
#[derive(Clone, Default)]
pub struct BridgeHandle {
    conns: Arc<Mutex<Vec<mpsc::Sender<Outbound>>>>,
}

impl BridgeHandle {
    pub fn new() -> Self {
        Self::default()
    }

    /// Best-effort relay of an unroutable local frame. v1: sends via the
    /// first live connection — the host sees the frame's sender as whichever
    /// capability owns that connection.
    pub fn relay_to_host(&self, frame: &Frame) -> bool {
        let conns = self.conns.lock().unwrap_or_else(recover_poison);
        let Some(tx) = conns.first() else {
            return false;
        };
        // strip MAC: the bridge write loop re-tags with the host session key
        let stripped = Frame {
            magic: frame.magic,
            flags: frame.flags & !FLAG_MAC_PRESENT,
            length: frame.length,
            target: frame.target,
            crc32: frame.crc32,
            payload: frame.payload.clone(),
            mac: None,
        };
        tx.try_send(out_frame(stripped)).is_ok()
    }

    pub fn register_conn(&self, tx: mpsc::Sender<Outbound>) {
        self.conns.lock().unwrap_or_else(recover_poison).push(tx);
    }

    pub fn unregister_conn(&self, tx: &mpsc::Sender<Outbound>) {
        self.conns
            .lock()
            .unwrap_or_else(recover_poison)
            .retain(|c| !c.same_channel(tx));
    }

    pub fn is_connected(&self) -> bool {
        !self.conns.lock().unwrap_or_else(recover_poison).is_empty()
    }
}

pub struct Bridge {
    config: BridgeConfig,
    device_id: String,
    registry: Arc<PluginRegistry>,
    router_tx: mpsc::Sender<IncomingMessage>,
    handle: BridgeHandle,
}

impl Clone for Bridge {
    fn clone(&self) -> Self {
        Bridge {
            config: self.config.clone(),
            device_id: self.device_id.clone(),
            registry: Arc::clone(&self.registry),
            router_tx: self.router_tx.clone(),
            handle: self.handle.clone(),
        }
    }
}

impl Bridge {
    pub fn new(
        config: BridgeConfig,
        device_id: String,
        registry: Arc<PluginRegistry>,
        router_tx: mpsc::Sender<IncomingMessage>,
        handle: BridgeHandle,
    ) -> Self {
        Bridge {
            config,
            device_id,
            registry,
            router_tx,
            handle,
        }
    }

    /// One mirror task per mirrored capability; the task set outlives every
    /// individual connection (reconnect with backoff).
    pub async fn run(self) {
        let mut tasks = Vec::new();
        for (idx, cap) in self.config.mirror.iter().enumerate() {
            let bridge = self.clone();
            let cap = cap.clone();
            tasks.push(tokio::spawn(bridge.mirror_cap(cap, idx)));
        }
        for task in tasks {
            let _ = task.await;
        }
    }

    async fn mirror_cap(self, cap: String, idx: usize) {
        let conn_id = BRIDGE_CONN_ID_BASE + idx as u64;
        let mut backoff = Duration::from_secs(1);
        loop {
            match self.one_cycle(&cap, conn_id).await {
                Ok(()) => info!(cap = %cap, "bridge connection closed"),
                Err(BridgeError::Shutdown) => return,
                Err(e) => warn!(cap = %cap, error = %e, "bridge connection failed"),
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(BRIDGE_MAX_BACKOFF);
        }
    }

    async fn one_cycle(&self, cap: &str, conn_id: u64) -> Result<(), BridgeError> {
        // mirror only a live local plugin: the manifest is registered on the
        // host verbatim (so its actions resolve there) and locally stripped
        // of the provider surface (so <device_id>.<cap> never wins action lookup)
        let (host_manifest, local_user_id) = wait_for_local_plugin(&self.registry, cap).await;
        let url = resolve_ws_url(&self.config.host_url)?;
        self.run_conn(cap, conn_id, &url, host_manifest, local_user_id)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_conn(
        &self,
        cap: &str,
        conn_id: u64,
        url: &str,
        host_manifest: PluginManifest,
        local_user_id: String,
    ) -> Result<(), BridgeError> {
        // <device_id>.<cap> — D-14 naming decision, globally unique per device
        let device_plugin_id = format!("{}.{}", self.device_id, cap);

        let mut req = url
            .into_client_request()
            .map_err(|e| BridgeError::Connect(e.to_string()))?;
        // same handshake as the SDK: the JWT rides the subprotocol header,
        // never the URL (access-log hygiene)
        let token = self.config.token.clone().unwrap_or_default();
        let protocol = if token.is_empty() {
            "vynkor".to_string()
        } else {
            format!("vynkor, {token}")
        };
        let value =
            HeaderValue::from_str(&protocol).map_err(|e| BridgeError::Connect(e.to_string()))?;
        req.headers_mut().insert("sec-websocket-protocol", value);
        let (ws, _resp) = connect_async(req)
            .await
            .map_err(|e| BridgeError::Connect(e.to_string()))?;
        let (mut ws_write, mut ws_read) = ws.split();

        // register on the host, unauthenticated at the frame level (MAC arms
        // only after the ack) — mirrors the SDK register_full flow
        let reg = PluginRegister {
            plugin_id: device_plugin_id.clone(),
            version: "1.0.0".to_string(),
            manifest: Some(host_manifest.clone()),
            jwt_token: token,
            device_id: self.device_id.clone(),
            capabilities: vec![cap.to_string()],
            ..Default::default()
        };
        let env = Envelope {
            payload: Some(envelope::Payload::PluginRegister(reg)),
            ..Default::default()
        };
        let mut payload = Vec::new();
        env.encode(&mut payload)
            .map_err(|_| BridgeError::Wire(VynkorError::Internal("encode register".into())))?;
        let frame = build_frame("kernel", 0, payload);
        ws_write
            .send(WsMessage::Binary(frame_to_bytes(&frame)))
            .await
            .map_err(|e| BridgeError::Connect(e.to_string()))?;

        let session_key: Option<[u8; 32]> = loop {
            let bytes = next_binary(&mut ws_read).await?;
            let frame = parse_frame(&bytes).await.map_err(BridgeError::Wire)?;
            let env = Envelope::decode(frame.payload.as_ref()).map_err(|_| {
                BridgeError::Wire(VynkorError::Internal("decode register ack".into()))
            })?;
            match env.payload {
                Some(envelope::Payload::PluginRegisterAck(ack)) => {
                    if !ack.accepted {
                        return Err(BridgeError::Register(ack.reject_reason));
                    }
                    let key = match &self.config.secret {
                        Some(secret) if !ack.session_nonce.is_empty() => Some(derive_session_key(
                            secret.as_bytes(),
                            &ack.session_nonce,
                            &device_plugin_id,
                        )),
                        _ => None,
                    };
                    info!(plugin_id = %device_plugin_id, "registered on host");
                    break key;
                }
                Some(envelope::Payload::Error(err)) => {
                    return Err(BridgeError::Register(format!(
                        "{}: {}",
                        err.message, err.details
                    )))
                }
                _ => {
                    warn!(cap = %cap, "unexpected frame before registration ack");
                    continue;
                }
            }
        };

        // local registration makes the router resolve <device_id>.<cap> without
        // round-tripping the host — local-to-local traffic stays local
        let (host_tx, host_rx) = mpsc::channel::<Outbound>(64);
        let local_manifest = local_entry_manifest(&host_manifest, cap);
        let meta = DeviceMeta {
            device_id: self.device_id.clone(),
            user_id: local_user_id,
            capabilities: vec![cap.to_string()],
            ..Default::default()
        };
        self.registry
            .register_with_device(
                device_plugin_id.clone(),
                conn_id,
                local_manifest,
                host_tx.clone(),
                meta,
            )
            .map_err(|e| BridgeError::Register(e.to_string()))?;
        self.handle.register_conn(host_tx.clone());

        // host-bound frames: the local kernel addresses its peers as "client"
        // (build_outbound), but the host only accepts SDK-style frames that
        // address it as "kernel" — rewrite here, once, on the way out
        let write_task = tokio::spawn(write_loop(ws_write, host_rx, session_key));

        let session_cell: SessionKeyCell = Arc::new(Mutex::new(None));
        let result = read_loop(
            &mut ws_read,
            conn_id,
            cap,
            session_key,
            host_tx.clone(),
            self.router_tx.clone(),
            session_cell,
        )
        .await;

        self.registry.unregister(&device_plugin_id);
        self.handle.unregister_conn(&host_tx);
        write_task.abort();
        result
    }
}

/// Poll until the local plugin registers, then take its manifest + user id.
async fn wait_for_local_plugin(registry: &PluginRegistry, cap: &str) -> (PluginManifest, String) {
    loop {
        if let Some(entry) = registry.get(cap) {
            return (entry.manifest.clone(), entry.user_id.clone());
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// The local registry entry is a routing proxy, not a provider: strip the
/// action/event surface so find_action_provider/event delivery never resolve
/// <device_id>.<cap>, and force the send gate so host->client frames pass
/// forward()'s permission checks.
fn local_entry_manifest(host_manifest: &PluginManifest, cap: &str) -> PluginManifest {
    let mut m = host_manifest.clone();
    m.actions = Vec::new();
    m.action_specs = Vec::new();
    m.events = Vec::new();
    m.ipc_targets = vec![cap.to_string()];
    if !m
        .permissions
        .iter()
        .any(|p| normalize_permission(p) == "ipc_send")
    {
        m.permissions.push("PERMISSION_IPC_SEND".to_string());
    }
    m
}

/// http(s):// base URLs get their scheme swapped and default to the /ws
/// gateway path; ws(s):// URLs are used verbatim.
fn resolve_ws_url(host_url: &str) -> Result<String, BridgeError> {
    if host_url.starts_with("ws://") || host_url.starts_with("wss://") {
        return Ok(host_url.to_string());
    }
    let bad = || {
        BridgeError::Wire(VynkorError::Internal(
            "host_url must start with ws://, wss://, http://, or https://".into(),
        ))
    };
    let (scheme, rest) = host_url.split_once("://").ok_or_else(bad)?;
    let ws_scheme = crate::utils::url::ws_scheme_for(scheme).ok_or_else(bad)?;
    let (host, path) = match rest.split_once('/') {
        Some((h, p)) => (h, format!("/{p}")),
        None => (rest, crate::utils::url::DEFAULT_WS_PATH.to_string()),
    };
    Ok(format!("{ws_scheme}://{host}{path}"))
}

async fn next_binary(
    ws_read: &mut futures_util::stream::SplitStream<WsStream>,
) -> Result<Vec<u8>, BridgeError> {
    loop {
        match ws_read.next().await {
            Some(Ok(WsMessage::Binary(b))) => return Ok(b),
            Some(Ok(WsMessage::Close(_))) | None => return Err(BridgeError::Disconnected),
            Some(Ok(_)) => continue,
            Some(Err(e)) => return Err(BridgeError::Connect(e.to_string())),
        }
    }
}

async fn write_loop(
    mut ws_write: futures_util::stream::SplitSink<WsStream, WsMessage>,
    mut rx: mpsc::Receiver<Outbound>,
    key: Option<[u8; 32]>,
) {
    while let Some(item) = rx.recv().await {
        let mut frame = match item {
            Outbound::Frame(f) => *f,
            // the local router never arms bridge conns (registration bypasses
            // the register arm) — nothing to do
            Outbound::EnableMac(_, _) => continue,
        };
        if target_as_str(&frame) == Some("client") {
            frame.target = target_bytes("kernel");
        }
        if let Some(k) = &key {
            frame.flags |= FLAG_MAC_PRESENT;
            let header = serialize_header(&frame);
            frame.mac = Some(compute_tag(k, &header, &frame.payload));
        }
        if ws_write
            .send(WsMessage::Binary(frame_to_bytes(&frame)))
            .await
            .is_err()
        {
            break;
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn read_loop(
    ws_read: &mut futures_util::stream::SplitStream<WsStream>,
    conn_id: u64,
    cap: &str,
    key: Option<[u8; 32]>,
    host_tx: mpsc::Sender<Outbound>,
    router_tx: mpsc::Sender<IncomingMessage>,
    session_cell: SessionKeyCell,
) -> Result<(), BridgeError> {
    while let Some(item) = ws_read.next().await {
        let bytes = match item {
            Ok(WsMessage::Binary(b)) => b,
            Ok(WsMessage::Close(_)) => return Err(BridgeError::Disconnected),
            Ok(_) => continue,
            Err(e) => return Err(BridgeError::Connect(e.to_string())),
        };
        let mut frame = parse_frame(&bytes).await.map_err(BridgeError::Wire)?;
        if let Some(k) = &key {
            let tag_present = frame.flags & FLAG_MAC_PRESENT != 0 && frame.mac.is_some();
            let valid = tag_present
                && verify_tag(
                    k,
                    &serialize_header(&frame),
                    &frame.payload,
                    frame.mac.as_ref().unwrap(),
                );
            if !valid {
                warn!(conn_id, "bridge frame MAC invalid — dropping connection");
                return Err(BridgeError::Mac);
            }
        }
        // the local router re-tags with the local plugin's key on the way
        // out — strip the host's tag here
        frame.flags &= !FLAG_MAC_PRESENT;
        frame.mac = None;
        // host kernel-generated frames carry target "client"; route them to
        // the local kernel or the mirrored plugin, never to "client"
        frame.target = target_bytes(if is_kernel_routed(&frame) {
            "kernel"
        } else {
            cap
        });
        let msg = IncomingMessage {
            conn_id,
            frame,
            write_tx: host_tx.clone(),
            session_key: session_cell.clone(),
        };
        if router_tx.send(msg).await.is_err() {
            return Err(BridgeError::Shutdown);
        }
    }
    Err(BridgeError::Disconnected)
}

/// Payloads the host's kernel arms handle (mirrors the local router's
/// "kernel" arm). Everything else (Event, raw binary) is device traffic.
fn is_kernel_routed(frame: &Frame) -> bool {
    Envelope::decode(frame.payload.as_ref())
        .ok()
        .and_then(|env| env.payload)
        .is_some_and(|p| {
            matches!(
                p,
                envelope::Payload::ActionRequest(_)
                    | envelope::Payload::ActionRequestChunk(_)
                    | envelope::Payload::ActionResponse(_)
                    | envelope::Payload::ActionResponseChunk(_)
                    | envelope::Payload::SessionClose(_)
                    | envelope::Payload::ActionStreamAbort(_)
                    | envelope::Payload::Ping(_)
                    | envelope::Payload::Pong(_)
                    | envelope::Payload::Error(_)
                    | envelope::Payload::EventPublishAck(_)
                    | envelope::Payload::KernelCommandAck(_)
                    | envelope::Payload::EventAck(_)
                    | envelope::Payload::PluginRegisterAck(_)
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::vynkor::{Event, PluginRegisterAck};
    use tokio::net::TcpListener;

    fn frame_target(frame: &Frame) -> String {
        target_as_str(frame).unwrap_or_default().to_string()
    }

    fn manifest_with(actions: &[&str]) -> PluginManifest {
        PluginManifest {
            actions: actions.iter().map(|s| s.to_string()).collect(),
            permissions: vec!["PERMISSION_IPC_SEND".to_string()],
            ipc_targets: vec!["device-1.geo".to_string()],
            ..Default::default()
        }
    }

    fn plain_frame(target: &str, env: &Envelope) -> Frame {
        let mut payload = Vec::new();
        env.encode(&mut payload).unwrap();
        build_frame(target, 0, payload)
    }

    #[test]
    fn ws_url_resolution() {
        assert_eq!(resolve_ws_url("ws://h:8080/ws").unwrap(), "ws://h:8080/ws");
        assert_eq!(resolve_ws_url("wss://h").unwrap(), "wss://h");
        assert_eq!(resolve_ws_url("http://h:8080").unwrap(), "ws://h:8080/ws");
        assert_eq!(
            resolve_ws_url("http://h:8080/ws").unwrap(),
            "ws://h:8080/ws"
        );
        assert_eq!(
            resolve_ws_url("https://h/custom").unwrap(),
            "wss://h/custom"
        );
        assert!(resolve_ws_url("ftp://h").is_err());
    }

    #[test]
    fn local_manifest_strips_provider_surface_and_forces_send_gate() {
        let m = local_entry_manifest(&manifest_with(&["get_position"]), "geo");
        assert!(m.actions.is_empty(), "actions must not resolve locally");
        assert!(m.action_specs.is_empty());
        assert!(m.events.is_empty());
        assert_eq!(m.ipc_targets, vec!["geo".to_string()]);
        assert!(m.permissions.iter().any(|p| p == "PERMISSION_IPC_SEND"));
    }

    #[test]
    fn kernel_routed_classification() {
        let action = Envelope {
            payload: Some(envelope::Payload::ActionRequest(
                crate::proto::vynkor::ActionRequest::default(),
            )),
            ..Default::default()
        };
        assert!(is_kernel_routed(&plain_frame("client", &action)));

        let event = Envelope {
            payload: Some(envelope::Payload::Event(Event::default())),
            ..Default::default()
        };
        assert!(!is_kernel_routed(&plain_frame("client", &event)));

        let garbage = Frame {
            payload: b"not an envelope".to_vec().into(),
            ..plain_frame("client", &event)
        };
        assert!(!is_kernel_routed(&garbage));
    }

    #[tokio::test]
    // accept_hdr_async pins the large tungstenite::Error in the fake-host callback's Result
    #[allow(clippy::result_large_err)]
    async fn bridge_registers_and_shuttles_frames_both_ways() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let host_secret = b"host-secret".to_vec();

        let registry = Arc::new(PluginRegistry::new());
        let (router_tx, mut router_rx) = mpsc::channel::<IncomingMessage>(64);
        let handle = BridgeHandle::new();

        // the local plugin is already registered (its manifest goes to the host)
        let (cap_tx, _cap_rx) = mpsc::channel::<Outbound>(8);
        registry
            .register(
                "geo".to_string(),
                1,
                manifest_with(&["get_position"]),
                cap_tx,
                "local",
                "default",
            )
            .unwrap();

        let bridge = Bridge::new(
            BridgeConfig {
                host_url: format!("http://{addr}"),
                token: Some("test-token".to_string()),
                secret: Some(String::from_utf8(host_secret.clone()).unwrap()),
                mirror: vec!["geo".to_string()],
            },
            "device-1".to_string(),
            Arc::clone(&registry),
            router_tx.clone(),
            handle.clone(),
        );
        let bridge_task = tokio::spawn(bridge.run());

        // fake host: accept, read the register, ack with a nonce, then drive
        // the two directions. Must echo the subprotocol the client requested
        // (the real axum host does `ws.protocols(["vynkor"])`) or the client
        // aborts the handshake.
        let host = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let ws = tokio_tungstenite::accept_hdr_async(
                stream,
                |_: &tokio_tungstenite::tungstenite::handshake::server::Request,
                 mut resp: tokio_tungstenite::tungstenite::handshake::server::Response| {
                    resp.headers_mut().insert(
                        "sec-websocket-protocol",
                        HeaderValue::from_static("vynkor"),
                    );
                    Ok(resp)
                },
            )
            .await
            .unwrap();
            let (mut ws_write, mut ws_read) = ws.split();

            let msg = tokio::time::timeout(Duration::from_secs(5), ws_read.next())
                .await
                .expect("register frame timeout")
                .unwrap()
                .unwrap();
            let bytes = match msg {
                WsMessage::Binary(b) => b,
                other => panic!("expected binary register, got {other:?}"),
            };
            let frame = parse_frame(&bytes).await.unwrap();
            let env = Envelope::decode(frame.payload.as_ref()).unwrap();
            let reg = match env.payload {
                Some(envelope::Payload::PluginRegister(r)) => r,
                other => panic!("expected PluginRegister, got {other:?}"),
            };
            assert_eq!(reg.plugin_id, "device-1.geo");
            assert_eq!(reg.capabilities, vec!["geo"]);
            assert_eq!(reg.device_id, "device-1");

            let ack = Envelope {
                payload: Some(envelope::Payload::PluginRegisterAck(PluginRegisterAck {
                    accepted: true,
                    session_nonce: b"nonce-aaaaaaaaaa".to_vec(),
                    ..Default::default()
                })),
                ..Default::default()
            };
            ws_write
                .send(WsMessage::Binary(frame_to_bytes(&plain_frame(
                    "client", &ack,
                ))))
                .await
                .unwrap();

            let key = derive_session_key(&host_secret, b"nonce-aaaaaaaaaa", "device-1.geo");

            // host kernel -> device: an Event (device traffic)
            let event = Envelope {
                payload: Some(envelope::Payload::Event(Event {
                    event_id: "evt-1".to_string(),
                    event_type: "plugin.geo.updated".to_string(),
                    payload_json: b"{}".to_vec(),
                    retry_count: 0,
                })),
                ..Default::default()
            };
            let mut event_frame = plain_frame("client", &event);
            event_frame.flags |= FLAG_MAC_PRESENT;
            let header = serialize_header(&event_frame);
            event_frame.mac = Some(compute_tag(&key, &header, &event_frame.payload));
            ws_write
                .send(WsMessage::Binary(frame_to_bytes(&event_frame)))
                .await
                .unwrap();

            // host kernel -> device: a watchdog Ping (kernel traffic)
            let ping = Envelope {
                payload: Some(envelope::Payload::Ping(crate::proto::vynkor::Ping {
                    timestamp: 1,
                })),
                ..Default::default()
            };
            let mut ping_frame = plain_frame("client", &ping);
            ping_frame.flags |= FLAG_MAC_PRESENT;
            let header = serialize_header(&ping_frame);
            ping_frame.mac = Some(compute_tag(&key, &header, &ping_frame.payload));
            ws_write
                .send(WsMessage::Binary(frame_to_bytes(&ping_frame)))
                .await
                .unwrap();

            // device -> host: a kernel reply frame addressed "client" must
            // arrive re-addressed "kernel", MAC'd
            let msg = tokio::time::timeout(Duration::from_secs(5), ws_read.next())
                .await
                .expect("outbound frame timeout")
                .unwrap()
                .unwrap();
            let bytes = match msg {
                WsMessage::Binary(b) => b,
                other => panic!("expected binary outbound, got {other:?}"),
            };
            let out_frame = parse_frame(&bytes).await.unwrap();
            assert_eq!(frame_target(&out_frame), "kernel");
            assert!(out_frame.flags & FLAG_MAC_PRESENT != 0);
            let tag = out_frame.mac.expect("outbound frame must be MAC'd");
            assert!(verify_tag(
                &key,
                &serialize_header(&out_frame),
                &out_frame.payload,
                &tag
            ));

            (ws_write, ws_read)
        });

        // event should arrive locally with target rewritten to the cap
        let msg = tokio::time::timeout(Duration::from_secs(5), router_rx.recv())
            .await
            .expect("inbound event timeout")
            .unwrap();
        assert_eq!(frame_target(&msg.frame), "geo");
        assert!(msg.frame.mac.is_none(), "host MAC must be stripped");
        let env = Envelope::decode(msg.frame.payload.as_ref()).unwrap();
        assert!(matches!(env.payload, Some(envelope::Payload::Event(_))));

        // ping should arrive with target rewritten to the kernel
        let msg = tokio::time::timeout(Duration::from_secs(5), router_rx.recv())
            .await
            .expect("inbound ping timeout")
            .unwrap();
        assert_eq!(frame_target(&msg.frame), "kernel");

        // the local registry now resolves device-1.geo to the bridge
        let entry = registry
            .get("device-1.geo")
            .expect("bridge entry registered");
        assert_eq!(entry.plugin_id, "device-1.geo");
        assert!(entry.manifest.actions.is_empty());

        // push a kernel-reply frame through the bridge entry's write channel
        // (what the local ActionResponse arm does after resolving a pending)
        let reply = Envelope {
            payload: Some(envelope::Payload::EventPublishAck(
                crate::proto::vynkor::EventPublishAck {
                    event_id: "evt-x".to_string(),
                    status: 0,
                    error: String::new(),
                },
            )),
            ..Default::default()
        };
        entry
            .write_tx
            .send(out_frame(plain_frame("client", &reply)))
            .await
            .unwrap();

        // the host task already verified the relay (re-addressed "kernel",
        // MAC'd with the session key) — awaiting it confirms the shuttling
        let (ws_write, _ws_read) = host.await.unwrap();

        drop(ws_write);
        bridge_task.abort();
    }
}
