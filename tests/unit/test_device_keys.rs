//! E-01: per-device keys — registration gates + session-key derivation.
//!
//! The oracle for "kernel and agent agree byte-for-byte" is the frame-MAC
//! round trip: the client derives its key from the device_secret exactly the
//! way the Android agent does (`derive_session_key(secret, ack.nonce,
//! plugin_id)`), tags a Ping, and the router must answer with a Pong — not an
//! ERR_MAC_INVALID.

use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::timeout;

use prost::Message as _;
use vynkor::auth::device_store::DeviceStore;
use vynkor::auth::frame_mac::derive_session_key;
use vynkor::auth::jwt::mint_device_token;
use vynkor::events::bus::EventBus;
use vynkor::ipc::connection::Outbound;
use vynkor::ipc::framing::Frame;
use vynkor::ipc::messages::IncomingMessage;
use vynkor::ipc::protocol::MessageRouter;
use vynkor::plugins::registry::PluginRegistry;
use vynkor::proto::vynkor::{
    envelope, Envelope, PluginManifest, PluginRegister, PluginRegisterAck,
};

const MASTER: &str = "e01-master-secret-0123456789abcdef";

// ── harness ─────────────────────────────────────────────────────────────────

fn make_write_pair() -> (mpsc::Sender<Outbound>, mpsc::Receiver<Outbound>) {
    mpsc::channel::<Outbound>(16)
}

fn encode_envelope(env: Envelope) -> Vec<u8> {
    let mut buf = Vec::new();
    env.encode(&mut buf).unwrap();
    buf
}

fn make_frame(target: &str, payload: Vec<u8>) -> Frame {
    let crc = crc32fast::hash(&payload);
    let mut t = [0u8; 32];
    let b = target.as_bytes();
    t[..b.len().min(32)].copy_from_slice(&b[..b.len().min(32)]);
    Frame {
        magic: 0x5652,
        flags: 0,
        length: payload.len() as u32,
        target: t,
        crc32: crc,
        payload: payload.into(),
        mac: None,
    }
}

fn kernel_frame(payload: Envelope) -> Frame {
    make_frame("kernel", encode_envelope(payload))
}

async fn recv_frame(rx: &mut mpsc::Receiver<Outbound>) -> Frame {
    let item = timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("recv timed out")
        .expect("channel closed");
    match item {
        Outbound::Frame(f) => *f,
        _ => panic!("expected Outbound::Frame"),
    }
}

/// Drain the post-ack channel item: the key the router installed for this
/// connection (what inbound verification will enforce).
async fn recv_installed_key(rx: &mut mpsc::Receiver<Outbound>) -> [u8; 32] {
    let item = timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("recv timed out")
        .expect("channel closed");
    match item {
        Outbound::EnableMac(key, _) => key,
        _ => panic!("expected Outbound::EnableMac"),
    }
}

fn decode_envelope(frame: &Frame) -> Envelope {
    Envelope::decode(frame.payload.as_ref()).expect("failed to decode envelope")
}

struct RouterFixture {
    router_tx: mpsc::Sender<IncomingMessage>,
    /// keeps the tempdir alive for the store's path
    _store_dir: tempfile::TempDir,
    store: Arc<DeviceStore>,
}

fn spawn_router_with_store(
    mac_secret: Option<Arc<Vec<u8>>>,
    device_store: Option<Arc<DeviceStore>>,
) -> mpsc::Sender<IncomingMessage> {
    let (tx, rx) = mpsc::channel::<IncomingMessage>(16);
    tokio::spawn(MessageRouter::run_with_context(
        rx,
        Arc::new(PluginRegistry::new()),
        Arc::new(EventBus::new()),
        None,
        std::time::Instant::now(),
        None,
        None,
        mac_secret,
        None,
        None,
        None,
        None,
        30_000,
        16,
        8192,
        None,
        60,
        device_store,
        None,
    ));
    tx
}

fn fixture() -> RouterFixture {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(DeviceStore::new(dir.path(), MASTER));
    let router_tx = spawn_router_with_store(
        Some(Arc::new(MASTER.as_bytes().to_vec())),
        Some(Arc::clone(&store)),
    );
    RouterFixture {
        router_tx,
        _store_dir: dir,
        store,
    }
}

async fn register_device(
    router_tx: &mpsc::Sender<IncomingMessage>,
    conn_id: u64,
    plugin_id: &str,
    device_id: &str,
    token: &str,
) -> (mpsc::Receiver<Outbound>, PluginRegisterAck) {
    let (write_tx, mut write_rx) = make_write_pair();
    let env = Envelope {
        payload: Some(envelope::Payload::PluginRegister(PluginRegister {
            plugin_id: plugin_id.to_string(),
            manifest: Some(PluginManifest::default()),
            jwt_token: token.to_string(),
            device_id: device_id.to_string(),
            ..Default::default()
        })),
        ..Default::default()
    };
    router_tx
        .send(IncomingMessage {
            conn_id,
            frame: kernel_frame(env),
            write_tx: write_tx.clone(),
            session_key: Arc::new(Mutex::new(None)),
        })
        .await
        .unwrap();

    // the ack is the first frame; EnableMac (if any) follows as a channel item
    let frame = recv_frame(&mut write_rx).await;
    let ack_env = decode_envelope(&frame);
    match ack_env.payload {
        Some(envelope::Payload::PluginRegisterAck(ack)) => (write_rx, ack),
        other => panic!("expected register ack, got {other:?}"),
    }
}

fn mint(device_id: &str, ttl: u64) -> String {
    mint_device_token(
        MASTER.as_bytes(),
        device_id,
        vec!["PERMISSION_IPC_SEND".into()],
        vec![],
        ttl,
        "vynkor",
    )
    .unwrap()
}

// ── tests ───────────────────────────────────────────────────────────────────

#[tokio::test]
async fn paired_device_key_derives_from_device_secret_not_master() {
    let fx = fixture();
    let secret = fx.store.issue("dev-1", "phone", 3600).unwrap();

    let (mut write_rx, ack) =
        register_device(&fx.router_tx, 1, "dev-1.geo", "dev-1", &mint("dev-1", 600)).await;
    assert!(ack.accepted, "{:?}", ack.reject_reason);

    // byte-for-byte agreement: the key the kernel installs equals what the
    // Android agent derives from its device_secret + the ack nonce
    let installed = recv_installed_key(&mut write_rx).await;
    assert_eq!(
        installed,
        derive_session_key(secret.as_bytes(), &ack.session_nonce, "dev-1.geo"),
        "kernel must key the session off the device_secret"
    );
    assert_ne!(
        installed,
        derive_session_key(MASTER.as_bytes(), &ack.session_nonce, "dev-1.geo"),
        "the master jwt_secret must no longer derive device session keys"
    );
}

#[tokio::test]
async fn unknown_device_registration_is_rejected() {
    let fx = fixture();
    let (_, ack) =
        register_device(&fx.router_tx, 1, "ghost.geo", "ghost", &mint("ghost", 600)).await;
    assert!(!ack.accepted);
    assert!(
        ack.reject_reason.contains("unknown device"),
        "got: {}",
        ack.reject_reason
    );
}

#[tokio::test]
async fn revoked_device_registration_is_rejected() {
    let fx = fixture();
    fx.store.issue("dev-2", "phone", 3600).unwrap();
    fx.store.set_revoked("dev-2", true).unwrap();

    let (_, ack) =
        register_device(&fx.router_tx, 1, "dev-2.geo", "dev-2", &mint("dev-2", 600)).await;
    assert!(!ack.accepted);
    assert!(
        ack.reject_reason.contains("revoked"),
        "got: {}",
        ack.reject_reason
    );
}

#[tokio::test]
async fn expired_device_registration_is_rejected() {
    let fx = fixture();
    fx.store.issue("dev-3", "phone", 1).unwrap();
    tokio::time::sleep(Duration::from_millis(1200)).await;

    let (_, ack) =
        register_device(&fx.router_tx, 1, "dev-3.geo", "dev-3", &mint("dev-3", 600)).await;
    assert!(!ack.accepted);
    assert!(
        ack.reject_reason.contains("expired"),
        "got: {}",
        ack.reject_reason
    );
}

#[tokio::test]
async fn local_plugins_still_derive_from_master_secret() {
    let fx = fixture();
    // local plugin: token sub = plugin_id, registration carries NO device_id
    let token = mint("local-plugin", 600);
    let (mut write_rx, ack) = register_device(&fx.router_tx, 7, "local-plugin", "", &token).await;
    assert!(ack.accepted, "{:?}", ack.reject_reason);

    let installed = recv_installed_key(&mut write_rx).await;
    assert_eq!(
        installed,
        derive_session_key(MASTER.as_bytes(), &ack.session_nonce, "local-plugin"),
        "empty-device_id registrations keep the master-secret derivation"
    );
}

#[tokio::test]
async fn revocation_after_registration_blocks_the_next_connection() {
    let fx = fixture();
    let secret = fx.store.issue("dev-4", "phone", 3600).unwrap();
    let token = mint("dev-4", 600);

    let (_, ack) = register_device(&fx.router_tx, 1, "dev-4.geo", "dev-4", &token).await;
    assert!(ack.accepted);

    // revoke from "another process" (the CLI path) — no kernel restart
    fx.store.set_revoked("dev-4", true).unwrap();

    // a fresh connection with the SAME valid token must now be rejected
    let (_, ack2) = register_device(&fx.router_tx, 2, "dev-4.battery", "dev-4", &token).await;
    assert!(!ack2.accepted);
    assert!(ack2.reject_reason.contains("revoked"));

    // re-pairing (issue rotates + un-revokes) restores access
    fx.store.issue("dev-4", "phone", 3600).unwrap();
    let (_, ack3) = register_device(&fx.router_tx, 3, "dev-4.clipboard", "dev-4", &token).await;
    assert!(ack3.accepted, "{:?}", ack3.reject_reason);
    let _ = secret;
}
