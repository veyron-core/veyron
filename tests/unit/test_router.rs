use prost::Message;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::timeout;
use vynkor::auth::jwt::JwtValidator;
use vynkor::events::bus::EventBus;
use vynkor::ipc::connection::{out_frame, Outbound};
use vynkor::ipc::framing::{target_as_str, Frame, FLAG_MAC_PRESENT};
use vynkor::ipc::messages::IncomingMessage;
use vynkor::ipc::protocol::MessageRouter;
use vynkor::plugins::registry::PluginRegistry;
use vynkor::proto::vynkor::{
    envelope, ActionRequest, Envelope, Ping, PluginManifest, PluginRegister, PluginRegisterAck,
};

// ── helpers ─────────────────────────────────────────────────────────────────

fn dummy_manifest() -> PluginManifest {
    PluginManifest::default()
}

fn ipc_manifest_with_targets(targets: Vec<&str>) -> PluginManifest {
    PluginManifest {
        permissions: vec!["PERMISSION_IPC_SEND".to_string()],
        ipc_targets: targets.iter().map(|s| s.to_string()).collect(),
        ..Default::default()
    }
}

fn action_manifest(actions: Vec<&str>) -> PluginManifest {
    PluginManifest {
        actions: actions.iter().map(|s| s.to_string()).collect(),
        ..Default::default()
    }
}

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

fn plug_frame(target: &str, payload: Vec<u8>) -> Frame {
    make_frame(target, payload)
}

fn incoming(conn_id: u64, frame: Frame, write_tx: mpsc::Sender<Outbound>) -> IncomingMessage {
    IncomingMessage {
        conn_id,
        frame,
        write_tx,
        session_key: Arc::new(Mutex::new(None)),
    }
}

fn spawn_router(
    registry: Arc<PluginRegistry>,
    event_bus: Arc<EventBus>,
) -> mpsc::Sender<IncomingMessage> {
    let (tx, rx) = mpsc::channel::<IncomingMessage>(64);
    tokio::spawn(MessageRouter::run(rx, registry, event_bus, None));
    tx
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

fn decode_envelope(frame: &Frame) -> Envelope {
    Envelope::decode(frame.payload.as_ref()).expect("failed to decode envelope")
}

// ── tests ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn router_handles_plugin_register_and_sends_ack() {
    let reg = Arc::new(PluginRegistry::new());
    let bus = Arc::new(EventBus::new());
    let router_tx = spawn_router(Arc::clone(&reg), bus);

    let (write_tx, mut write_rx) = make_write_pair();

    let env = Envelope {
        payload: Some(envelope::Payload::PluginRegister(PluginRegister {
            plugin_id: "weather".to_string(),
            version: "1.0.0".to_string(),
            manifest: Some(dummy_manifest()),
            ..Default::default()
        })),
        ..Default::default()
    };

    router_tx
        .send(incoming(1, kernel_frame(env), write_tx))
        .await
        .unwrap();

    let frame = recv_frame(&mut write_rx).await;
    let ack_env = decode_envelope(&frame);

    match ack_env.payload {
        Some(envelope::Payload::PluginRegisterAck(PluginRegisterAck { accepted, .. })) => {
            assert!(accepted, "ack must be accepted=true");
        }
        other => panic!("expected PluginRegisterAck, got {:?}", other),
    }

    assert!(
        reg.is_registered(1),
        "conn_id 1 must be in registry after registration"
    );
}

#[tokio::test]
async fn router_forwards_frame_to_target_plugin() {
    let reg = Arc::new(PluginRegistry::new());
    let bus = Arc::new(EventBus::new());

    // pre-register plugin B in registry
    let (b_write_tx, mut b_write_rx) = make_write_pair();
    reg.register(
        "plugin_b".to_string(),
        2,
        dummy_manifest(),
        b_write_tx,
        "",
        "",
    )
    .unwrap();

    // plugin A (conn_id=1): ipc_send + plugin_b in allowlist
    let (a_write_tx, _a_write_rx) = make_write_pair();
    reg.register(
        "plugin_a".to_string(),
        1,
        ipc_manifest_with_targets(vec!["plugin_b"]),
        a_write_tx.clone(),
        "",
        "",
    )
    .unwrap();

    let router_tx = spawn_router(Arc::clone(&reg), bus);

    let raw_payload = b"arbitrary bytes".to_vec();
    let frame = plug_frame("plugin_b", raw_payload.clone());

    router_tx
        .send(incoming(1, frame, a_write_tx))
        .await
        .unwrap();

    let received = recv_frame(&mut b_write_rx).await;
    assert_eq!(&*received.payload, raw_payload);
    assert_eq!(target_as_str(&received), Some("plugin_b"));
}

#[tokio::test]
async fn slow_target_does_not_stall_router() {
    let reg = Arc::new(PluginRegistry::new());
    let bus = Arc::new(EventBus::new());

    // slow target: capacity-1 channel, pre-filled and never drained. Keep the
    // receiver alive so the channel stays open and full (send blocks → times out).
    let (slow_tx, _slow_rx) = mpsc::channel::<Outbound>(1);
    slow_tx
        .send(out_frame(make_frame("x", b"prefill".to_vec())))
        .await
        .unwrap();
    reg.register("slow".to_string(), 2, dummy_manifest(), slow_tx, "", "")
        .unwrap();

    // fast target: drained normally
    let (fast_tx, mut fast_rx) = make_write_pair();
    reg.register("fast".to_string(), 3, dummy_manifest(), fast_tx, "", "")
        .unwrap();

    // sender: ipc_send + both targets in allowlist
    let (a_tx, _a_rx) = make_write_pair();
    reg.register(
        "sender".to_string(),
        1,
        ipc_manifest_with_targets(vec!["slow", "fast"]),
        a_tx.clone(),
        "",
        "",
    )
    .unwrap();

    let router_tx = spawn_router(Arc::clone(&reg), bus);

    // frame to the stuck target (router must give up after the send timeout)...
    router_tx
        .send(incoming(
            1,
            plug_frame("slow", b"to-slow".to_vec()),
            a_tx.clone(),
        ))
        .await
        .unwrap();
    // ...then a frame to a healthy target.
    router_tx
        .send(incoming(
            1,
            plug_frame("fast", b"to-fast".to_vec()),
            a_tx.clone(),
        ))
        .await
        .unwrap();

    // With an unbounded send the router would block on "slow" forever and "fast"
    // would never arrive. The bounded send must let it through promptly.
    let f = recv_frame(&mut fast_rx).await;
    assert_eq!(&*f.payload, b"to-fast");
    assert_eq!(target_as_str(&f), Some("fast"));
}

#[tokio::test]
async fn forward_to_full_channel_returns_without_waiting() {
    let reg = Arc::new(PluginRegistry::new());
    let bus = Arc::new(EventBus::new());

    // stuck target: capacity-1 channel, pre-filled and never drained.
    let (stuck_tx, _stuck_rx) = mpsc::channel::<Outbound>(1);
    stuck_tx
        .send(out_frame(make_frame("x", b"prefill".to_vec())))
        .await
        .unwrap();
    reg.register("stuck".to_string(), 2, dummy_manifest(), stuck_tx, "", "")
        .unwrap();

    let (a_tx, _a_rx) = make_write_pair();
    reg.register(
        "sender".to_string(),
        1,
        ipc_manifest_with_targets(vec!["stuck"]),
        a_tx.clone(),
        "",
        "",
    )
    .unwrap();

    let router_tx = spawn_router(Arc::clone(&reg), bus);

    let start = std::time::Instant::now();
    router_tx
        .send(incoming(
            1,
            plug_frame("stuck", b"to-stuck".to_vec()),
            a_tx.clone(),
        ))
        .await
        .unwrap();

    // Sending a second message to the SAME router channel and waiting for the
    // router to accept it (mpsc send completing) proves the router's internal
    // loop iteration for "stuck" has finished — if forward() blocked 50ms
    // internally, this whole round trip takes >= 50ms.
    router_tx
        .send(incoming(
            1,
            plug_frame("stuck", b"to-stuck-2".to_vec()),
            a_tx,
        ))
        .await
        .unwrap();

    assert!(
        start.elapsed() < Duration::from_millis(20),
        "two forwards to a full channel must not block the router on the 50ms \
         send timeout, took {:?}",
        start.elapsed()
    );
}

#[tokio::test]
async fn router_denies_forward_without_ipc_permission() {
    let reg = Arc::new(PluginRegistry::new());
    let bus = Arc::new(EventBus::new());

    // target plugin B registered
    let (b_write_tx, mut b_write_rx) = make_write_pair();
    reg.register(
        "plugin_b".to_string(),
        2,
        dummy_manifest(),
        b_write_tx,
        "",
        "",
    )
    .unwrap();

    // sender plugin A lacks PERMISSION_IPC_SEND (default-deny)
    let (a_write_tx, mut a_write_rx) = make_write_pair();
    reg.register(
        "plugin_a".to_string(),
        1,
        dummy_manifest(),
        a_write_tx.clone(),
        "",
        "",
    )
    .unwrap();

    let router_tx = spawn_router(Arc::clone(&reg), bus);

    let frame = plug_frame("plugin_b", b"blocked".to_vec());
    router_tx
        .send(incoming(1, frame, a_write_tx))
        .await
        .unwrap();

    // sender gets an error frame; target receives nothing
    let err = recv_frame(&mut a_write_rx).await;
    let env = Envelope::decode(err.payload.as_ref()).unwrap();
    assert!(matches!(env.payload, Some(envelope::Payload::Error(_))));
    assert!(b_write_rx.try_recv().is_err());
}

#[tokio::test]
async fn router_broadcasts_star_to_all_except_sender() {
    let reg = Arc::new(PluginRegistry::new());
    let bus = Arc::new(EventBus::new());

    let (a_tx, mut a_rx) = make_write_pair();
    let (b_tx, mut b_rx) = make_write_pair();
    let (c_tx, mut c_rx) = make_write_pair();

    // sender holds PERMISSION_IPC_SEND and lists both recipients in ipc_targets
    reg.register(
        "plugin_a".to_string(),
        1,
        ipc_manifest_with_targets(vec!["plugin_b", "plugin_c"]),
        a_tx.clone(),
        "",
        "",
    )
    .unwrap();
    reg.register("plugin_b".to_string(), 2, dummy_manifest(), b_tx, "", "")
        .unwrap();
    reg.register("plugin_c".to_string(), 3, dummy_manifest(), c_tx, "", "")
        .unwrap();

    let router_tx = spawn_router(Arc::clone(&reg), bus);

    let payload = b"broadcast payload".to_vec();
    let frame = make_frame("*", payload.clone());

    // sender is plugin_a (conn_id=1)
    router_tx.send(incoming(1, frame, a_tx)).await.unwrap();

    // B and C must receive it
    let fb = recv_frame(&mut b_rx).await;
    let fc = recv_frame(&mut c_rx).await;
    assert_eq!(&*fb.payload, payload);
    assert_eq!(&*fc.payload, payload);

    // A (sender) must NOT receive it
    let a_result = timeout(Duration::from_millis(100), a_rx.recv()).await;
    assert!(
        a_result.is_err(),
        "sender must not receive its own broadcast"
    );
}

#[tokio::test]
async fn router_denies_broadcast_without_ipc_permission() {
    let reg = Arc::new(PluginRegistry::new());
    let bus = Arc::new(EventBus::new());

    let (a_tx, mut a_rx) = make_write_pair();
    let (b_tx, mut b_rx) = make_write_pair();

    // sender plugin_a lacks PERMISSION_IPC_SEND (default-deny)
    reg.register(
        "plugin_a".to_string(),
        1,
        dummy_manifest(),
        a_tx.clone(),
        "",
        "",
    )
    .unwrap();
    reg.register("plugin_b".to_string(), 2, dummy_manifest(), b_tx, "", "")
        .unwrap();

    let router_tx = spawn_router(Arc::clone(&reg), bus);

    let frame = make_frame("*", b"blocked".to_vec());
    router_tx.send(incoming(1, frame, a_tx)).await.unwrap();

    // sender gets an error; no peer receives the broadcast
    let err = recv_frame(&mut a_rx).await;
    let env = Envelope::decode(err.payload.as_ref()).unwrap();
    assert!(matches!(env.payload, Some(envelope::Payload::Error(_))));
    assert!(b_rx.try_recv().is_err());
}

#[tokio::test]
async fn router_throttles_connection_after_error_burst() {
    let reg = Arc::new(PluginRegistry::new());
    let bus = Arc::new(EventBus::new());
    let router_tx = spawn_router(Arc::clone(&reg), bus);

    // wide channel so all error responses buffer without blocking the router
    let (tx, mut rx) = mpsc::channel::<Outbound>(64);

    // 16 malformed kernel frames (invalid protobuf) -> 16 error responses
    for _ in 0..16 {
        let frame = make_frame("kernel", vec![0xff, 0xff, 0xff, 0xff]);
        router_tx
            .send(incoming(1, frame, tx.clone()))
            .await
            .unwrap();
    }
    for _ in 0..16 {
        let f = recv_frame(&mut rx).await;
        let env = Envelope::decode(f.payload.as_ref()).unwrap();
        assert!(matches!(env.payload, Some(envelope::Payload::Error(_))));
    }

    // 17th crosses the budget -> dropped, no response
    let frame = make_frame("kernel", vec![0xff, 0xff, 0xff, 0xff]);
    router_tx
        .send(incoming(1, frame, tx.clone()))
        .await
        .unwrap();
    let throttled = timeout(Duration::from_millis(200), rx.recv()).await;
    assert!(
        throttled.is_err(),
        "throttled connection must receive no further responses"
    );
}

#[tokio::test]
async fn router_resets_error_budget_on_success() {
    let reg = Arc::new(PluginRegistry::new());
    let bus = Arc::new(EventBus::new());
    reg.register(
        "pinger".to_string(),
        1,
        dummy_manifest(),
        make_write_pair().0,
        "",
        "",
    )
    .unwrap();
    let router_tx = spawn_router(Arc::clone(&reg), bus);

    let (tx, mut rx) = mpsc::channel::<Outbound>(64);

    // alternate: malformed (error) then valid ping (success resets budget) x many.
    // never accrues to the throttle threshold, so every ping still gets a pong.
    for _ in 0..30 {
        let bad = make_frame("kernel", vec![0xff, 0xff, 0xff, 0xff]);
        router_tx.send(incoming(1, bad, tx.clone())).await.unwrap();
        let _err = recv_frame(&mut rx).await;

        let ping = kernel_frame(Envelope {
            payload: Some(envelope::Payload::Ping(Ping { timestamp: 1 })),
            ..Default::default()
        });
        router_tx.send(incoming(1, ping, tx.clone())).await.unwrap();
        let pong = recv_frame(&mut rx).await;
        let env = Envelope::decode(pong.payload.as_ref()).unwrap();
        assert!(matches!(env.payload, Some(envelope::Payload::Pong(_))));
    }
}

#[tokio::test]
async fn router_responds_to_ping_with_pong() {
    let reg = Arc::new(PluginRegistry::new());
    let bus = Arc::new(EventBus::new());

    let (write_tx, mut write_rx) = make_write_pair();
    reg.register(
        "pinger".to_string(),
        5,
        dummy_manifest(),
        write_tx.clone(),
        "",
        "",
    )
    .unwrap();

    let router_tx = spawn_router(Arc::clone(&reg), bus);

    let ping_env = Envelope {
        payload: Some(envelope::Payload::Ping(Ping { timestamp: 999 })),
        ..Default::default()
    };

    router_tx
        .send(incoming(5, kernel_frame(ping_env), write_tx))
        .await
        .unwrap();

    let frame = recv_frame(&mut write_rx).await;
    let env = decode_envelope(&frame);

    assert!(
        matches!(env.payload, Some(envelope::Payload::Pong(_))),
        "expected Pong, got {:?}",
        env.payload
    );
}

// PERF-1: the shared router task must never await a peer's full write
// channel — a stuck plugin loses its own kernel replies, but every other
// connection keeps being served. (With the old blocking send this test
// hangs on the healthy plugin's pong.)
#[tokio::test]
async fn kernel_replies_do_not_block_router_on_full_peer_channel() {
    let reg = Arc::new(PluginRegistry::new());
    let bus = Arc::new(EventBus::new());
    let router_tx = spawn_router(Arc::clone(&reg), bus);

    let (stuck_tx, mut stuck_rx) = make_write_pair();
    let (healthy_tx, mut healthy_rx) = make_write_pair();
    reg.register(
        "stuck".to_string(),
        1,
        dummy_manifest(),
        stuck_tx.clone(),
        "",
        "",
    )
    .unwrap();
    reg.register(
        "healthy".to_string(),
        2,
        dummy_manifest(),
        healthy_tx.clone(),
        "",
        "",
    )
    .unwrap();

    // saturate "stuck" (capacity 16) — nobody drains it
    for i in 0..16u8 {
        stuck_tx
            .send(out_frame(make_frame("kernel", vec![i])))
            .await
            .unwrap();
    }

    let ping = |ts: u64| {
        kernel_frame(Envelope {
            payload: Some(envelope::Payload::Ping(Ping { timestamp: ts })),
            ..Default::default()
        })
    };

    // stuck plugin pings — its pong hits the full channel and is dropped
    router_tx
        .send(incoming(1, ping(1), stuck_tx))
        .await
        .unwrap();

    // the router must stay live for other connections
    router_tx
        .send(incoming(2, ping(2), healthy_tx))
        .await
        .unwrap();
    let pong = recv_frame(&mut healthy_rx).await;
    assert!(matches!(
        decode_envelope(&pong).payload,
        Some(envelope::Payload::Pong(_))
    ));

    // the saturated channel holds exactly the pre-filled frames — the
    // dropped pong never enqueued
    let mut drained = 0;
    while stuck_rx.try_recv().is_ok() {
        drained += 1;
    }
    assert_eq!(
        drained, 16,
        "dropped reply must not appear on the full channel"
    );
}

#[tokio::test]
async fn router_rejects_non_register_from_unregistered_conn() {
    let reg = Arc::new(PluginRegistry::new());
    let bus = Arc::new(EventBus::new());
    let router_tx = spawn_router(Arc::clone(&reg), bus);

    let (write_tx, mut write_rx) = make_write_pair();

    // send arbitrary frame targeting "kernel" without registering first
    let env = Envelope {
        payload: Some(envelope::Payload::Ping(Ping { timestamp: 1 })),
        ..Default::default()
    };

    router_tx
        .send(incoming(99, kernel_frame(env), write_tx))
        .await
        .unwrap();

    let frame = recv_frame(&mut write_rx).await;
    let env = decode_envelope(&frame);

    assert!(
        matches!(env.payload, Some(envelope::Payload::Error(_))),
        "expected ErrorMessage, got {:?}",
        env.payload
    );
}

#[tokio::test]
async fn router_sends_error_for_unknown_target_plugin() {
    let reg = Arc::new(PluginRegistry::new());
    let bus = Arc::new(EventBus::new());

    let (write_tx, mut write_rx) = make_write_pair();
    reg.register(
        "sender".to_string(),
        7,
        dummy_manifest(),
        write_tx.clone(),
        "",
        "",
    )
    .unwrap();

    let router_tx = spawn_router(Arc::clone(&reg), bus);

    let frame = plug_frame("nonexistent_plugin", b"data".to_vec());
    router_tx.send(incoming(7, frame, write_tx)).await.unwrap();

    let response = recv_frame(&mut write_rx).await;
    let env = decode_envelope(&response);

    assert!(
        matches!(env.payload, Some(envelope::Payload::Error(_))),
        "expected ErrorMessage for unknown target, got {:?}",
        env.payload
    );
}

// ── T-04: per-plugin IPC allowlist ───────────────────────────────────────────

#[tokio::test]
async fn router_denies_forward_with_empty_ipc_targets() {
    let reg = Arc::new(PluginRegistry::new());
    let bus = Arc::new(EventBus::new());

    let (b_tx, mut b_rx) = make_write_pair();
    reg.register("plugin_b".to_string(), 2, dummy_manifest(), b_tx, "", "")
        .unwrap();

    // sender: ipc_send granted but ipc_targets empty → deny-all
    let (a_tx, mut a_rx) = make_write_pair();
    reg.register(
        "plugin_a".to_string(),
        1,
        ipc_manifest_with_targets(vec![]),
        a_tx.clone(),
        "",
        "",
    )
    .unwrap();

    let router_tx = spawn_router(Arc::clone(&reg), bus);
    router_tx
        .send(incoming(
            1,
            plug_frame("plugin_b", b"blocked".to_vec()),
            a_tx,
        ))
        .await
        .unwrap();

    let err = recv_frame(&mut a_rx).await;
    let env = Envelope::decode(err.payload.as_ref()).unwrap();
    assert!(matches!(env.payload, Some(envelope::Payload::Error(_))));
    assert!(b_rx.try_recv().is_err());
}

#[tokio::test]
async fn router_allows_forward_to_listed_ipc_target() {
    let reg = Arc::new(PluginRegistry::new());
    let bus = Arc::new(EventBus::new());

    let (b_tx, mut b_rx) = make_write_pair();
    reg.register("plugin_b".to_string(), 2, dummy_manifest(), b_tx, "", "")
        .unwrap();

    // sender: ipc_send + plugin_b in allowlist
    let (a_tx, _a_rx) = make_write_pair();
    reg.register(
        "plugin_a".to_string(),
        1,
        ipc_manifest_with_targets(vec!["plugin_b"]),
        a_tx.clone(),
        "",
        "",
    )
    .unwrap();

    let router_tx = spawn_router(Arc::clone(&reg), bus);
    let payload = b"hello".to_vec();
    router_tx
        .send(incoming(1, plug_frame("plugin_b", payload.clone()), a_tx))
        .await
        .unwrap();

    let received = recv_frame(&mut b_rx).await;
    assert_eq!(&*received.payload, payload);
}

#[tokio::test]
async fn router_denies_forward_to_unlisted_ipc_target() {
    let reg = Arc::new(PluginRegistry::new());
    let bus = Arc::new(EventBus::new());

    let (b_tx, mut b_rx) = make_write_pair();
    reg.register("plugin_b".to_string(), 2, dummy_manifest(), b_tx, "", "")
        .unwrap();
    let (c_tx, mut c_rx) = make_write_pair();
    reg.register("plugin_c".to_string(), 3, dummy_manifest(), c_tx, "", "")
        .unwrap();

    // sender: ipc_send + only plugin_b allowed, not plugin_c
    let (a_tx, mut a_rx) = make_write_pair();
    reg.register(
        "plugin_a".to_string(),
        1,
        ipc_manifest_with_targets(vec!["plugin_b"]),
        a_tx.clone(),
        "",
        "",
    )
    .unwrap();

    let router_tx = spawn_router(Arc::clone(&reg), bus);
    router_tx
        .send(incoming(
            1,
            plug_frame("plugin_c", b"blocked".to_vec()),
            a_tx,
        ))
        .await
        .unwrap();

    let err = recv_frame(&mut a_rx).await;
    let env = Envelope::decode(err.payload.as_ref()).unwrap();
    assert!(matches!(env.payload, Some(envelope::Payload::Error(_))));
    assert!(b_rx.try_recv().is_err());
    assert!(c_rx.try_recv().is_err());
}

// ── T-19: mutex poison hardening (VULN-013) ─────────────────────────────────

/// VULN-013: if the SessionKeyCell mutex is poisoned before registration,
/// `if let Ok(...)` silently skips key installation — MAC verification is
/// never activated and the connection proceeds unprotected.
/// After the fix, the key must be installed despite the prior poison.
#[tokio::test]
async fn poisoned_session_key_cell_still_installs_mac_key() {
    use vynkor::ipc::messages::IncomingMessage;

    // Poison the mutex on a dedicated OS thread, then join to confirm.
    let cell: Arc<Mutex<Option<[u8; 32]>>> = Arc::new(Mutex::new(None));
    let cell_for_poison = Arc::clone(&cell);
    let _ = std::thread::spawn(move || {
        let _guard = cell_for_poison.lock().unwrap();
        panic!("intentionally poison the mutex");
    })
    .join();
    assert!(cell.lock().is_err(), "precondition: mutex must be poisoned");

    // Router with a mac_secret so it will derive + install a session key.
    let reg = Arc::new(PluginRegistry::new());
    let bus = Arc::new(EventBus::new());
    let mac_secret = Arc::new(b"t19-test-secret-key-32bytesXXXXX".to_vec());

    let (router_tx, rx) = mpsc::channel::<IncomingMessage>(16);
    tokio::spawn(MessageRouter::run_with_context(
        rx,
        Arc::clone(&reg),
        Arc::clone(&bus),
        None,
        std::time::Instant::now(),
        None,
        None,
        Some(Arc::clone(&mac_secret)),
        None,
        None,
        None,
        None,
        30_000,
        16,
        8192,
        None,
        60,
        None, // device_store
        None,
    ));

    let (write_tx, mut write_rx) = make_write_pair();
    let env = Envelope {
        payload: Some(envelope::Payload::PluginRegister(PluginRegister {
            plugin_id: "poison-test-plugin".to_string(),
            manifest: Some(dummy_manifest()),
            ..Default::default()
        })),
        ..Default::default()
    };

    router_tx
        .send(IncomingMessage {
            conn_id: 42,
            frame: kernel_frame(env),
            write_tx: write_tx.clone(),
            session_key: Arc::clone(&cell),
        })
        .await
        .unwrap();

    // Ack confirms registration was accepted.
    let frame = recv_frame(&mut write_rx).await;
    let ack_env = decode_envelope(&frame);
    match ack_env.payload {
        Some(envelope::Payload::PluginRegisterAck(PluginRegisterAck {
            accepted,
            session_nonce,
            ..
        })) => {
            assert!(accepted, "registration must be accepted");
            assert!(
                !session_nonce.is_empty(),
                "nonce must be present when mac_secret set"
            );
        }
        other => panic!("expected PluginRegisterAck, got {:?}", other),
    }

    // EnableMac is enqueued after the ack (VULN-020 fix). In production the
    // write_loop processes it. Here we simulate that: drain it and install the key.
    let enable_item = timeout(Duration::from_secs(2), write_rx.recv())
        .await
        .expect("EnableMac timed out")
        .expect("channel closed");
    match enable_item {
        Outbound::EnableMac(k, c) => {
            *c.lock().unwrap_or_else(|p| p.into_inner()) = Some(k);
        }
        _ => panic!("expected Outbound::EnableMac after ack"),
    }

    // Key must be installed despite the earlier poison — recover with unwrap_or_else.
    let installed = cell.lock().unwrap_or_else(|p| p.into_inner());
    assert!(
        installed.is_some(),
        "session key must be installed even when mutex was previously poisoned"
    );
}

// ── T-18: broadcast security (VULN-012, VULN-015) ───────────────────────────

/// VULN-012: sender with PERMISSION_IPC_SEND but empty ipc_targets must not
/// deliver a broadcast to any recipient — the per-target allowlist check must
/// apply inside the broadcast send loop, same as in forward().
#[tokio::test]
async fn broadcast_denied_with_empty_ipc_targets() {
    let reg = Arc::new(PluginRegistry::new());
    let bus = Arc::new(EventBus::new());

    let (b_tx, mut b_rx) = make_write_pair();
    reg.register("plugin_b".to_string(), 2, dummy_manifest(), b_tx, "", "")
        .unwrap();

    // sender: ipc_send granted but ipc_targets empty → deny-all
    let (a_tx, _a_rx) = make_write_pair();
    reg.register(
        "plugin_a".to_string(),
        1,
        ipc_manifest_with_targets(vec![]),
        a_tx.clone(),
        "",
        "",
    )
    .unwrap();

    let router_tx = spawn_router(Arc::clone(&reg), bus);
    let frame = make_frame("*", b"secret".to_vec());
    router_tx.send(incoming(1, frame, a_tx)).await.unwrap();

    // plugin_b must receive nothing
    let result = timeout(Duration::from_millis(150), b_rx.recv()).await;
    assert!(
        result.is_err(),
        "broadcast from plugin with empty ipc_targets must not deliver to any recipient"
    );
}

/// VULN-015: broadcast must strip FLAG_MAC_PRESENT from the cloned frame's
/// flags so that the recipient's write_loop re-tags it under its own session
/// key rather than forwarding a stale/wrong tag.
#[tokio::test]
async fn broadcast_strips_flag_mac_present() {
    let reg = Arc::new(PluginRegistry::new());
    let bus = Arc::new(EventBus::new());

    let (b_tx, mut b_rx) = make_write_pair();
    reg.register("plugin_b".to_string(), 2, dummy_manifest(), b_tx, "", "")
        .unwrap();

    // sender: ipc_send + plugin_b in allowlist
    let (a_tx, _a_rx) = make_write_pair();
    reg.register(
        "plugin_a".to_string(),
        1,
        ipc_manifest_with_targets(vec!["plugin_b"]),
        a_tx.clone(),
        "",
        "",
    )
    .unwrap();

    let router_tx = spawn_router(Arc::clone(&reg), bus);

    // send a frame that has FLAG_MAC_PRESENT set in flags but mac: None
    let mut frame = make_frame("*", b"data".to_vec());
    frame.flags |= FLAG_MAC_PRESENT;

    router_tx.send(incoming(1, frame, a_tx)).await.unwrap();

    let received = recv_frame(&mut b_rx).await;
    assert_eq!(
        received.flags & FLAG_MAC_PRESENT,
        0,
        "broadcast must strip FLAG_MAC_PRESENT from cloned frame"
    );
}

/// AUDIT M-04: forward() (unicast) must strip FLAG_MAC_PRESENT the same way
/// broadcast() does — otherwise the recipient's write_loop sees a stale tag
/// flag from the sender's own session key.
#[tokio::test]
async fn forward_strips_flag_mac_present() {
    let reg = Arc::new(PluginRegistry::new());
    let bus = Arc::new(EventBus::new());

    let (b_tx, mut b_rx) = make_write_pair();
    reg.register("plugin_b".to_string(), 2, dummy_manifest(), b_tx, "", "")
        .unwrap();

    let (a_tx, _a_rx) = make_write_pair();
    reg.register(
        "plugin_a".to_string(),
        1,
        ipc_manifest_with_targets(vec!["plugin_b"]),
        a_tx.clone(),
        "",
        "",
    )
    .unwrap();

    let router_tx = spawn_router(Arc::clone(&reg), bus);

    let mut frame = plug_frame("plugin_b", b"data".to_vec());
    frame.flags |= FLAG_MAC_PRESENT;

    router_tx.send(incoming(1, frame, a_tx)).await.unwrap();

    let received = recv_frame(&mut b_rx).await;
    assert_eq!(
        received.flags & FLAG_MAC_PRESENT,
        0,
        "forward must strip FLAG_MAC_PRESENT from the sender's frame"
    );
}

#[tokio::test]
async fn router_rejects_duplicate_plugin_id_registration() {
    let reg = Arc::new(PluginRegistry::new());
    let bus = Arc::new(EventBus::new());
    let router_tx = spawn_router(Arc::clone(&reg), bus);

    let (tx1, mut rx1) = make_write_pair();
    let (tx2, mut rx2) = make_write_pair();

    let reg_env = |id: &str| Envelope {
        payload: Some(envelope::Payload::PluginRegister(PluginRegister {
            plugin_id: id.to_string(),
            manifest: Some(dummy_manifest()),
            ..Default::default()
        })),
        ..Default::default()
    };

    router_tx
        .send(incoming(10, kernel_frame(reg_env("clash")), tx1))
        .await
        .unwrap();
    let f1 = recv_frame(&mut rx1).await;
    let e1 = decode_envelope(&f1);
    assert!(matches!(
        e1.payload,
        Some(envelope::Payload::PluginRegisterAck(PluginRegisterAck {
            accepted: true,
            ..
        }))
    ));

    router_tx
        .send(incoming(11, kernel_frame(reg_env("clash")), tx2))
        .await
        .unwrap();
    let f2 = recv_frame(&mut rx2).await;
    let e2 = decode_envelope(&f2);
    match e2.payload {
        Some(envelope::Payload::PluginRegisterAck(PluginRegisterAck { accepted, .. })) => {
            assert!(!accepted, "duplicate registration must be rejected");
        }
        other => panic!(
            "expected PluginRegisterAck(accepted=false), got {:?}",
            other
        ),
    }
}

// ── T-04: config.yaml permission allowlist clamps JWT claims ───────────────

const T04_SECRET: &[u8] = b"t04-test-secret-key-for-unit-tests";

fn spawn_router_with_jwt_and_config_perms(
    registry: Arc<PluginRegistry>,
    event_bus: Arc<EventBus>,
    config_permissions: std::collections::HashMap<String, Vec<String>>,
) -> mpsc::Sender<IncomingMessage> {
    use vynkor::auth::jwt::JwtValidator;

    let (tx, rx) = mpsc::channel::<IncomingMessage>(64);
    tokio::spawn(MessageRouter::run_with_context(
        rx,
        registry,
        event_bus,
        Some(Arc::new(JwtValidator::new(T04_SECRET))),
        std::time::Instant::now(),
        None,
        None,
        None,
        Some(Arc::new(config_permissions)),
        None,
        None,
        None,
        30_000,
        16,
        8192,
        None,
        60,
        None, // device_store
        None,
    ));
    tx
}

#[tokio::test]
async fn registration_clamps_jwt_permissions_to_config_allowlist() {
    let reg = Arc::new(PluginRegistry::new());
    let bus = Arc::new(EventBus::new());
    let mut config_permissions = std::collections::HashMap::new();
    // N2: both the exact proto form and the lowercase documented form must
    // match a token claiming PERMISSION_NETWORK.
    config_permissions.insert(
        "net-plugin".to_string(),
        vec!["PERMISSION_NETWORK".to_string()],
    );
    config_permissions.insert("net-plugin-lower".to_string(), vec!["network".to_string()]);
    let router_tx =
        spawn_router_with_jwt_and_config_perms(Arc::clone(&reg), bus, config_permissions);

    // Token claims kernel_admin too, but config.yaml only grants network.
    register_and_assert_clamped(&router_tx, 1, "net-plugin").await;
    // lowercase config form clamps the same way (failed before N2)
    register_and_assert_clamped(&router_tx, 2, "net-plugin-lower").await;
}

/// Register `plugin_id` on `conn_id` with a token claiming
/// PERMISSION_NETWORK + PERMISSION_KERNEL_ADMIN and assert the ack clamps the
/// grant to PERMISSION_NETWORK.
async fn register_and_assert_clamped(
    router_tx: &mpsc::Sender<IncomingMessage>,
    conn_id: u64,
    plugin_id: &str,
) {
    let (write_tx, mut write_rx) = make_write_pair();
    let token = crate::jwt_helper::create_test_token(
        plugin_id,
        vec![
            "PERMISSION_NETWORK".to_string(),
            "PERMISSION_KERNEL_ADMIN".to_string(),
        ],
        T04_SECRET,
        3600,
    );
    let env = Envelope {
        payload: Some(envelope::Payload::PluginRegister(PluginRegister {
            plugin_id: plugin_id.to_string(),
            jwt_token: token,
            manifest: Some(dummy_manifest()),
            ..Default::default()
        })),
        ..Default::default()
    };

    router_tx
        .send(incoming(conn_id, kernel_frame(env), write_tx))
        .await
        .unwrap();

    let frame = recv_frame(&mut write_rx).await;
    let ack_env = decode_envelope(&frame);
    match ack_env.payload {
        Some(envelope::Payload::PluginRegisterAck(PluginRegisterAck {
            accepted,
            granted_permissions,
            ..
        })) => {
            assert!(accepted);
            assert_eq!(granted_permissions, vec!["PERMISSION_NETWORK".to_string()]);
        }
        other => panic!("expected PluginRegisterAck, got {:?}", other),
    }
}

#[tokio::test]
async fn registration_leaves_permissions_unclamped_for_plugin_not_in_config() {
    let reg = Arc::new(PluginRegistry::new());
    let bus = Arc::new(EventBus::new());
    // No entry for "other-plugin" — same convention as validate_plugin_def's
    // empty-list case: no restriction.
    let router_tx = spawn_router_with_jwt_and_config_perms(
        Arc::clone(&reg),
        bus,
        std::collections::HashMap::new(),
    );

    let (write_tx, mut write_rx) = make_write_pair();
    let token = crate::jwt_helper::create_test_token(
        "other-plugin",
        vec!["PERMISSION_KERNEL_ADMIN".to_string()],
        T04_SECRET,
        3600,
    );
    let env = Envelope {
        payload: Some(envelope::Payload::PluginRegister(PluginRegister {
            plugin_id: "other-plugin".to_string(),
            jwt_token: token,
            manifest: Some(dummy_manifest()),
            ..Default::default()
        })),
        ..Default::default()
    };

    router_tx
        .send(incoming(1, kernel_frame(env), write_tx))
        .await
        .unwrap();

    let frame = recv_frame(&mut write_rx).await;
    let ack_env = decode_envelope(&frame);
    match ack_env.payload {
        Some(envelope::Payload::PluginRegisterAck(PluginRegisterAck {
            accepted,
            granted_permissions,
            ..
        })) => {
            assert!(accepted);
            assert_eq!(
                granted_permissions,
                vec!["PERMISSION_KERNEL_ADMIN".to_string()]
            );
        }
        other => panic!("expected PluginRegisterAck, got {:?}", other),
    }
}

// ── T-08: error-budget prune must not be resettable by an unregistered
// connection staying unregistered ────────────────────────────────────────

#[tokio::test]
async fn unregistered_connection_error_budget_survives_map_prune() {
    let reg = Arc::new(PluginRegistry::new());
    let bus = Arc::new(EventBus::new());

    // max_tracked_error_conns = 1 forces the size-triggered prune on every
    // single error from this one never-registered connection. Before the
    // fix, prune kept only registered conn_ids (`registry.is_registered`),
    // so an unregistered attacker's own entry was evicted and reset to zero
    // on every message, and it could never cross max_conn_errors.
    let (router_tx, rx) = mpsc::channel::<IncomingMessage>(64);
    tokio::spawn(MessageRouter::run_with_context(
        rx,
        Arc::clone(&reg),
        Arc::clone(&bus),
        None,
        std::time::Instant::now(),
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        30_000,
        /* max_conn_errors */ 3,
        /* max_tracked_error_conns */ 1,
        None,
        60,
        None, // device_store
        None,
    ));

    let (tx, mut rx_out) = mpsc::channel::<Outbound>(64);

    for _ in 0..3 {
        let frame = make_frame("kernel", vec![0xff, 0xff, 0xff, 0xff]);
        router_tx
            .send(incoming(1, frame, tx.clone()))
            .await
            .unwrap();
        let f = recv_frame(&mut rx_out).await;
        let env = decode_envelope(&f);
        assert!(matches!(env.payload, Some(envelope::Payload::Error(_))));
    }

    // 4th malformed frame crosses the budget -> dropped, no response.
    // Keep `tx` alive past the send so the channel doesn't close and make
    // `recv()` return `None` immediately instead of genuinely timing out.
    let frame = make_frame("kernel", vec![0xff, 0xff, 0xff, 0xff]);
    router_tx
        .send(incoming(1, frame, tx.clone()))
        .await
        .unwrap();
    let throttled = timeout(Duration::from_millis(200), rx_out.recv()).await;
    drop(tx);
    assert!(
        throttled.is_err(),
        "unregistered connection must still be throttled after crossing the error budget"
    );
}

#[tokio::test]
async fn broadcast_to_many_stuck_targets_does_not_multiply_delay() {
    let reg = Arc::new(PluginRegistry::new());
    let bus = Arc::new(EventBus::new());

    // Five stuck targets: each capacity-1, pre-filled, never drained.
    // Keep receivers alive throughout the test.
    let mut _stuck_rxs = Vec::new();
    for i in 0..5u64 {
        let (stuck_tx, stuck_rx) = mpsc::channel::<Outbound>(1);
        stuck_tx
            .send(out_frame(make_frame("x", b"prefill".to_vec())))
            .await
            .unwrap();
        reg.register(
            format!("stuck{i}"),
            100 + i,
            dummy_manifest(),
            stuck_tx,
            "",
            "",
        )
        .unwrap();
        _stuck_rxs.push(stuck_rx);
    }

    // Sender (conn_id=1): broadcasts to stuck0..stuck4 only. Does NOT include "pong"
    // in ipc_targets, so "pong" is never a broadcast recipient.
    let (a_tx, _a_rx) = make_write_pair();
    reg.register(
        "sender".to_string(),
        1,
        PluginManifest {
            permissions: vec!["PERMISSION_IPC_SEND".to_string()],
            ipc_targets: (0..5u64).map(|i| format!("stuck{i}")).collect(),
            ..Default::default()
        },
        a_tx.clone(),
        "",
        "",
    )
    .unwrap();

    // Register a separate "pong" target that is NOT in sender's broadcast allowlist.
    let (pong_tx, mut pong_rx) = make_write_pair();
    reg.register("pong".to_string(), 50, dummy_manifest(), pong_tx, "", "")
        .unwrap();

    // Sender2 (conn_id=2): a second plugin with IPC permission, ipc_targets=[pong].
    // It sends the unicast forward to pong after the broadcast completes.
    let (sender2_tx, _sender2_rx) = make_write_pair();
    reg.register(
        "sender2".to_string(),
        2,
        PluginManifest {
            permissions: vec!["PERMISSION_IPC_SEND".to_string()],
            ipc_targets: vec!["pong".to_string()],
            ..Default::default()
        },
        sender2_tx.clone(),
        "",
        "",
    )
    .unwrap();

    let router_tx = spawn_router(Arc::clone(&reg), bus);

    let start = std::time::Instant::now();

    // Message 1: Broadcast from sender (conn_id=1) to stuck0..stuck4.
    // "pong" is NOT in the broadcast recipients — it only receives the stuck targets.
    router_tx
        .send(incoming(
            1,
            make_frame("*", b"broadcast payload".to_vec()),
            a_tx,
        ))
        .await
        .unwrap();

    // Message 2: Unicast forward from sender2 (conn_id=2) to "pong".
    // Because MessageRouter processes messages one at a time from a single
    // mpsc::Receiver, message 2 is only dequeued and processed after message 1's
    // broadcast() call has fully returned (including all 5 stuck send attempts).
    // Thus, when pong receives its frame, the entire broadcast loop is guaranteed
    // complete, independent of DashMap iteration order. "pong" reaches its receiver
    // only via this explicit unicast (message 2), never as a broadcast recipient.
    router_tx
        .send(incoming(
            2,
            make_frame("pong", b"pong payload".to_vec()),
            sender2_tx,
        ))
        .await
        .unwrap();

    // Wait for pong's receiver to get the forwarded frame.
    // This proves the broadcast loop has fully completed.
    let _ = timeout(Duration::from_secs(1), pong_rx.recv())
        .await
        .expect("timed out waiting for pong forward")
        .expect("channel closed");

    let elapsed = start.elapsed();

    assert!(
        elapsed < Duration::from_millis(50),
        "broadcast to 5 stuck subscribers must not cost 5 * 50ms, took {:?}",
        elapsed
    );
}

// N1 regression: forward() must share the payload allocation, not deep-copy it.
// Frame.payload is Arc<[u8]> (wire v0.2.0); a per-hop Vec copy would make large
// plugin-to-plugin messages O(n) at every hop.
#[tokio::test]
async fn forward_shares_payload_without_copy() {
    let reg = Arc::new(PluginRegistry::new());
    let bus = Arc::new(EventBus::new());

    let (b_write_tx, mut b_write_rx) = make_write_pair();
    reg.register(
        "plugin_b".to_string(),
        2,
        dummy_manifest(),
        b_write_tx,
        "",
        "",
    )
    .unwrap();

    let (a_write_tx, _a_write_rx) = make_write_pair();
    reg.register(
        "plugin_a".to_string(),
        1,
        ipc_manifest_with_targets(vec!["plugin_b"]),
        a_write_tx.clone(),
        "",
        "",
    )
    .unwrap();

    let router_tx = spawn_router(Arc::clone(&reg), bus);

    // 64 KiB — large enough that a deep copy would dominate the routing cost
    let original: Arc<[u8]> = vec![0xABu8; 64 * 1024].into();
    let mut frame = make_frame("plugin_b", Vec::new());
    frame.payload = original.clone();
    frame.length = original.len() as u32;
    frame.crc32 = crc32fast::hash(&original);

    router_tx
        .send(incoming(1, frame, a_write_tx))
        .await
        .unwrap();

    let received = recv_frame(&mut b_write_rx).await;
    assert!(
        Arc::ptr_eq(&original, &received.payload),
        "forward() must share the payload Arc, not deep-copy it (N1)"
    );
    assert_eq!(&*received.payload, &*original);
}

// N1 regression (broadcast side): every recipient's frame must reference the
// sender's payload allocation — fan-out must be O(1) per recipient, not O(n).
#[tokio::test]
async fn broadcast_shares_payload_without_copy() {
    let reg = Arc::new(PluginRegistry::new());
    let bus = Arc::new(EventBus::new());

    let (a_tx, _a_rx) = make_write_pair();
    let (b_tx, mut b_rx) = make_write_pair();
    let (c_tx, mut c_rx) = make_write_pair();

    reg.register(
        "plugin_a".to_string(),
        1,
        ipc_manifest_with_targets(vec!["plugin_b", "plugin_c"]),
        a_tx.clone(),
        "",
        "",
    )
    .unwrap();
    reg.register("plugin_b".to_string(), 2, dummy_manifest(), b_tx, "", "")
        .unwrap();
    reg.register("plugin_c".to_string(), 3, dummy_manifest(), c_tx, "", "")
        .unwrap();

    let router_tx = spawn_router(Arc::clone(&reg), bus);

    let original: Arc<[u8]> = vec![0xCDu8; 64 * 1024].into();
    let mut frame = make_frame("*", Vec::new());
    frame.payload = original.clone();
    frame.length = original.len() as u32;
    frame.crc32 = crc32fast::hash(&original);

    router_tx.send(incoming(1, frame, a_tx)).await.unwrap();

    let fb = recv_frame(&mut b_rx).await;
    let fc = recv_frame(&mut c_rx).await;
    assert!(
        Arc::ptr_eq(&original, &fb.payload) && Arc::ptr_eq(&original, &fc.payload),
        "broadcast() must share the payload Arc with every recipient, not deep-copy it (N1)"
    );
    assert_eq!(&*fb.payload, &*fc.payload);
}

// ── D-03: protocol version + device metadata on the wire ────────────────────

#[tokio::test]
async fn router_accepts_matching_protocol_major() {
    let reg = Arc::new(PluginRegistry::new());
    let bus = Arc::new(EventBus::new());
    let router_tx = spawn_router(Arc::clone(&reg), bus);

    let (write_tx, mut write_rx) = make_write_pair();

    let env = Envelope {
        payload: Some(envelope::Payload::PluginRegister(PluginRegister {
            plugin_id: "weather".to_string(),
            version: "1.0.0".to_string(),
            manifest: Some(dummy_manifest()),
            protocol_version: "1.6".to_string(),
            ..Default::default()
        })),
        ..Default::default()
    };

    router_tx
        .send(incoming(1, kernel_frame(env), write_tx))
        .await
        .unwrap();

    let frame = recv_frame(&mut write_rx).await;
    let ack_env = decode_envelope(&frame);
    match ack_env.payload {
        Some(envelope::Payload::PluginRegisterAck(PluginRegisterAck { accepted, .. })) => {
            assert!(accepted, "v1.6 register must be accepted");
        }
        other => panic!("expected PluginRegisterAck, got {:?}", other),
    }
}

#[tokio::test]
async fn router_accepts_minor_variant_of_protocol_major() {
    let reg = Arc::new(PluginRegistry::new());
    let bus = Arc::new(EventBus::new());
    let router_tx = spawn_router(Arc::clone(&reg), bus);

    let (write_tx, mut write_rx) = make_write_pair();

    let env = Envelope {
        payload: Some(envelope::Payload::PluginRegister(PluginRegister {
            plugin_id: "weather".to_string(),
            version: "1.0.0".to_string(),
            manifest: Some(dummy_manifest()),
            protocol_version: "1.5.2".to_string(),
            ..Default::default()
        })),
        ..Default::default()
    };

    router_tx
        .send(incoming(1, kernel_frame(env), write_tx))
        .await
        .unwrap();

    let frame = recv_frame(&mut write_rx).await;
    let ack_env = decode_envelope(&frame);
    match ack_env.payload {
        Some(envelope::Payload::PluginRegisterAck(PluginRegisterAck { accepted, .. })) => {
            assert!(accepted, "minor/patch variant must be accepted");
        }
        other => panic!("expected PluginRegisterAck, got {:?}", other),
    }
}

#[tokio::test]
async fn router_rejects_protocol_major_mismatch() {
    let reg = Arc::new(PluginRegistry::new());
    let bus = Arc::new(EventBus::new());
    let router_tx = spawn_router(Arc::clone(&reg), bus);

    let (write_tx, mut write_rx) = make_write_pair();

    let env = Envelope {
        payload: Some(envelope::Payload::PluginRegister(PluginRegister {
            plugin_id: "weather".to_string(),
            version: "1.0.0".to_string(),
            manifest: Some(dummy_manifest()),
            protocol_version: "2.0".to_string(),
            ..Default::default()
        })),
        ..Default::default()
    };

    router_tx
        .send(incoming(1, kernel_frame(env), write_tx))
        .await
        .unwrap();

    let frame = recv_frame(&mut write_rx).await;
    let ack_env = decode_envelope(&frame);
    match ack_env.payload {
        Some(envelope::Payload::Error(err)) => {
            assert_eq!(
                err.code,
                vynkor::proto::vynkor::ErrorCode::ErrProtocolMismatch as i32,
                "major mismatch must use ERR_PROTOCOL_MISMATCH"
            );
            assert!(
                err.message.contains("2.0") && err.message.contains(vynkor_wire::PROTOCOL_VERSION),
                "message must carry both versions, got: {}",
                err.message
            );
        }
        other => panic!("expected Error payload, got {:?}", other),
    }
    assert!(
        !reg.is_registered(1),
        "major-mismatch register must not leave a registry entry"
    );
}

#[tokio::test]
async fn router_stores_device_metadata_from_wire() {
    let reg = Arc::new(PluginRegistry::new());
    let bus = Arc::new(EventBus::new());
    let router_tx = spawn_router(Arc::clone(&reg), bus);

    let (write_tx, mut write_rx) = make_write_pair();

    let env = Envelope {
        payload: Some(envelope::Payload::PluginRegister(PluginRegister {
            plugin_id: "geo".to_string(),
            version: "1.0.0".to_string(),
            manifest: Some(dummy_manifest()),
            device_id: "phone-7f3a".to_string(),
            os: vynkor::proto::vynkor::DeviceOs::Android as i32,
            arch: "aarch64".to_string(),
            os_version: "14".to_string(),
            capabilities: vec!["geo".to_string()],
            user_id: "behzod".to_string(),
            protocol_version: "1.6".to_string(),
            ..Default::default()
        })),
        ..Default::default()
    };

    router_tx
        .send(incoming(1, kernel_frame(env), write_tx))
        .await
        .unwrap();

    let frame = recv_frame(&mut write_rx).await;
    let ack_env = decode_envelope(&frame);
    match ack_env.payload {
        Some(envelope::Payload::PluginRegisterAck(PluginRegisterAck { accepted, .. })) => {
            assert!(accepted, "device register must be accepted");
        }
        other => panic!("expected PluginRegisterAck, got {:?}", other),
    }

    let dev = reg.get_device("phone-7f3a").expect("device must exist");
    assert_eq!(dev.os, vynkor::proto::vynkor::DeviceOs::Android as i32);
    assert_eq!(dev.arch, "aarch64");
    assert_eq!(dev.capabilities, vec!["geo".to_string()]);
    let entry = reg.get("geo").expect("plugin must exist");
    assert_eq!(entry.device_id, "phone-7f3a");
    assert_eq!(entry.user_id, "behzod");
}

#[tokio::test]
async fn router_accepts_device_scoped_jwt() {
    let reg = Arc::new(PluginRegistry::new());
    let bus = Arc::new(EventBus::new());
    let (tx, rx) = mpsc::channel::<IncomingMessage>(64);
    let validator = Arc::new(JwtValidator::new(b"test-secret"));
    tokio::spawn(MessageRouter::run(
        rx,
        Arc::clone(&reg),
        bus,
        Some(validator),
    ));

    let (write_tx, mut write_rx) = make_write_pair();
    let token = crate::jwt_helper::create_test_token(
        "phone-7f3a", // device-scoped: sub == device_id, not plugin_id
        vec!["PERMISSION_IPC_SEND".to_string()],
        b"test-secret",
        3600,
    );

    let env = Envelope {
        payload: Some(envelope::Payload::PluginRegister(PluginRegister {
            plugin_id: "geo".to_string(),
            version: "1.0.0".to_string(),
            manifest: Some(dummy_manifest()),
            jwt_token: token,
            device_id: "phone-7f3a".to_string(),
            user_id: "behzod".to_string(),
            protocol_version: "1.6".to_string(),
            ..Default::default()
        })),
        ..Default::default()
    };

    tx.send(incoming(1, kernel_frame(env), write_tx))
        .await
        .unwrap();

    let frame = recv_frame(&mut write_rx).await;
    let ack_env = decode_envelope(&frame);
    match ack_env.payload {
        Some(envelope::Payload::PluginRegisterAck(PluginRegisterAck { accepted, .. })) => {
            assert!(accepted, "device-scoped token must authorize its plugins");
        }
        other => panic!("expected PluginRegisterAck, got {:?}", other),
    }
    let entry = reg.get("geo").expect("plugin must exist");
    assert_eq!(
        entry.manifest.permissions,
        vec!["PERMISSION_IPC_SEND".to_string()],
        "device token claims must override the manifest"
    );
}

#[tokio::test]
async fn router_rejects_cross_device_token() {
    let reg = Arc::new(PluginRegistry::new());
    let bus = Arc::new(EventBus::new());
    let (tx, rx) = mpsc::channel::<IncomingMessage>(64);
    let validator = Arc::new(JwtValidator::new(b"test-secret"));
    tokio::spawn(MessageRouter::run(
        rx,
        Arc::clone(&reg),
        bus,
        Some(validator),
    ));

    let (write_tx, mut write_rx) = make_write_pair();
    let token = crate::jwt_helper::create_test_token("other-phone", vec![], b"test-secret", 3600);

    let env = Envelope {
        payload: Some(envelope::Payload::PluginRegister(PluginRegister {
            plugin_id: "geo".to_string(),
            version: "1.0.0".to_string(),
            manifest: Some(dummy_manifest()),
            jwt_token: token,
            device_id: "phone-7f3a".to_string(),
            protocol_version: "1.6".to_string(),
            ..Default::default()
        })),
        ..Default::default()
    };

    tx.send(incoming(1, kernel_frame(env), write_tx))
        .await
        .unwrap();

    let frame = recv_frame(&mut write_rx).await;
    let ack_env = decode_envelope(&frame);
    match ack_env.payload {
        Some(envelope::Payload::PluginRegisterAck(PluginRegisterAck { accepted, .. })) => {
            assert!(
                !accepted,
                "another device's token must not register plugins here"
            );
        }
        other => panic!("expected PluginRegisterAck, got {:?}", other),
    }
    assert!(!reg.is_registered(1));
}

// ── D-10: message_id propagation + hop logging ─────────────────────────────

#[tokio::test]
async fn kernel_action_request_preserves_message_id_to_provider() {
    let reg = Arc::new(PluginRegistry::new());
    let bus = Arc::new(EventBus::new());

    // provider declares the action
    let (provider_tx, mut provider_rx) = make_write_pair();
    reg.register(
        "plugin_b".to_string(),
        2,
        action_manifest(vec!["do.thing"]),
        provider_tx,
        "",
        "",
    )
    .unwrap();

    // caller sends a device-stamped ActionRequest to the kernel
    let (caller_tx, _caller_rx) = make_write_pair();
    reg.register(
        "caller".to_string(),
        1,
        dummy_manifest(),
        caller_tx.clone(),
        "",
        "",
    )
    .unwrap();

    let router_tx = spawn_router(Arc::clone(&reg), bus);

    let env = Envelope {
        message_id: "trace-42".to_string(),
        payload: Some(envelope::Payload::ActionRequest(ActionRequest {
            action_id: "a1".to_string(),
            action: "do.thing".to_string(),
            params_json: b"{}".to_vec(),
            timeout_ms: 1000,
            streaming: false,
            caller_plugin_id: String::new(),
        })),
        ..Default::default()
    };

    router_tx
        .send(incoming(1, kernel_frame(env), caller_tx))
        .await
        .unwrap();

    let frame = recv_frame(&mut provider_rx).await;
    let forwarded = decode_envelope(&frame);
    match forwarded.payload {
        Some(envelope::Payload::ActionRequest(req)) => {
            assert_eq!(req.action, "do.thing");
            // internal correlation id, never the caller's "a1"
            assert!(
                req.action_id.starts_with("kact-"),
                "expected internal action_id, got {}",
                req.action_id
            );
            // D-10: the provider sees the device's message_id — the kernel
            // hop must not mint a fresh id over a preserved trace.
            assert_eq!(forwarded.message_id, "trace-42");
        }
        other => panic!("expected ActionRequest, got {:?}", other),
    }
}

#[tokio::test]
async fn kernel_action_request_stamps_message_id_when_absent() {
    let reg = Arc::new(PluginRegistry::new());
    let bus = Arc::new(EventBus::new());

    let (provider_tx, mut provider_rx) = make_write_pair();
    reg.register(
        "plugin_b".to_string(),
        2,
        action_manifest(vec!["do.thing"]),
        provider_tx,
        "",
        "",
    )
    .unwrap();
    let (caller_tx, _caller_rx) = make_write_pair();
    reg.register(
        "caller".to_string(),
        1,
        dummy_manifest(),
        caller_tx.clone(),
        "",
        "",
    )
    .unwrap();

    let router_tx = spawn_router(Arc::clone(&reg), bus);

    // no message_id on the wire — a host plugin or stale SDK
    let env = Envelope {
        payload: Some(envelope::Payload::ActionRequest(ActionRequest {
            action_id: "a1".to_string(),
            action: "do.thing".to_string(),
            params_json: b"{}".to_vec(),
            timeout_ms: 1000,
            streaming: false,
            caller_plugin_id: String::new(),
        })),
        ..Default::default()
    };

    router_tx
        .send(incoming(1, kernel_frame(env), caller_tx))
        .await
        .unwrap();

    let frame = recv_frame(&mut provider_rx).await;
    let forwarded = decode_envelope(&frame);
    assert!(
        forwarded.message_id.starts_with("k-"),
        "kernel must stamp a trace id when the sender provides none, got {:?}",
        forwarded.message_id
    );
}

#[tokio::test]
async fn ping_pong_preserves_message_id() {
    let reg = Arc::new(PluginRegistry::new());
    let bus = Arc::new(EventBus::new());

    let (write_tx, mut write_rx) = make_write_pair();
    reg.register(
        "pinger".to_string(),
        5,
        dummy_manifest(),
        write_tx.clone(),
        "",
        "",
    )
    .unwrap();

    let router_tx = spawn_router(Arc::clone(&reg), bus);

    let ping_env = Envelope {
        message_id: "ping-1".to_string(),
        payload: Some(envelope::Payload::Ping(Ping { timestamp: 999 })),
        ..Default::default()
    };

    router_tx
        .send(incoming(5, kernel_frame(ping_env), write_tx))
        .await
        .unwrap();

    let frame = recv_frame(&mut write_rx).await;
    let env = decode_envelope(&frame);
    assert!(
        matches!(env.payload, Some(envelope::Payload::Pong(_))),
        "expected Pong, got {:?}",
        env.payload
    );
    assert_eq!(
        env.message_id, "ping-1",
        "pong must echo the ping's trace id"
    );
}

#[tokio::test]
async fn forward_preserves_message_id() {
    let reg = Arc::new(PluginRegistry::new());
    let bus = Arc::new(EventBus::new());

    let (b_tx, mut b_rx) = make_write_pair();
    reg.register("plugin_b".to_string(), 2, dummy_manifest(), b_tx, "", "")
        .unwrap();
    let (a_tx, _a_rx) = make_write_pair();
    reg.register(
        "plugin_a".to_string(),
        1,
        ipc_manifest_with_targets(vec!["plugin_b"]),
        a_tx.clone(),
        "",
        "",
    )
    .unwrap();

    let router_tx = spawn_router(Arc::clone(&reg), bus);

    // zero-parse relay: the envelope bytes (message_id included) pass through
    let env = Envelope {
        message_id: "fwd-9".to_string(),
        payload: Some(envelope::Payload::Ping(Ping { timestamp: 1 })),
        ..Default::default()
    };
    router_tx
        .send(incoming(
            1,
            plug_frame("plugin_b", encode_envelope(env)),
            a_tx,
        ))
        .await
        .unwrap();

    let frame = recv_frame(&mut b_rx).await;
    assert_eq!(decode_envelope(&frame).message_id, "fwd-9");
}

#[tokio::test]
async fn broadcast_preserves_message_id() {
    let reg = Arc::new(PluginRegistry::new());
    let bus = Arc::new(EventBus::new());

    let (a_tx, _a_rx) = make_write_pair();
    let (b_tx, mut b_rx) = make_write_pair();
    let (c_tx, mut c_rx) = make_write_pair();
    reg.register(
        "plugin_a".to_string(),
        1,
        ipc_manifest_with_targets(vec!["plugin_b", "plugin_c"]),
        a_tx.clone(),
        "",
        "",
    )
    .unwrap();
    reg.register("plugin_b".to_string(), 2, dummy_manifest(), b_tx, "", "")
        .unwrap();
    reg.register("plugin_c".to_string(), 3, dummy_manifest(), c_tx, "", "")
        .unwrap();

    let router_tx = spawn_router(Arc::clone(&reg), bus);

    let env = Envelope {
        message_id: "bcast-7".to_string(),
        payload: Some(envelope::Payload::Ping(Ping { timestamp: 1 })),
        ..Default::default()
    };
    router_tx
        .send(incoming(1, plug_frame("*", encode_envelope(env)), a_tx))
        .await
        .unwrap();

    let fb = recv_frame(&mut b_rx).await;
    let fc = recv_frame(&mut c_rx).await;
    assert_eq!(decode_envelope(&fb).message_id, "bcast-7");
    assert_eq!(decode_envelope(&fc).message_id, "bcast-7");
}

#[tokio::test]
async fn routing_logs_ingress_and_relay_hops_with_message_id() {
    use std::io::Write;
    use std::sync::{Mutex, Once, OnceLock};

    // traced_test can't capture debug! logs from the spawned router task (its
    // env filter is target-scoped and it matches by test span) — install our
    // own TRACE-level subscriber writing into a process-global buffer.
    static LOG_BUF: OnceLock<Mutex<Vec<u8>>> = OnceLock::new();
    static INIT: Once = Once::new();

    #[derive(Clone, Copy)]
    struct BufWriter(&'static OnceLock<Mutex<Vec<u8>>>);
    impl tracing_subscriber::fmt::MakeWriter<'_> for BufWriter {
        type Writer = BufWriter;
        fn make_writer(&self) -> Self::Writer {
            *self
        }
    }
    impl Write for BufWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.get().unwrap().lock().unwrap().write(buf)
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    INIT.call_once(|| {
        LOG_BUF.set(Mutex::new(Vec::new())).unwrap();
        let _ = tracing::subscriber::set_global_default(
            tracing_subscriber::fmt()
                .with_max_level(tracing::Level::TRACE)
                .with_ansi(false)
                .with_writer(BufWriter(&LOG_BUF))
                .finish(),
        );
    });

    let reg = Arc::new(PluginRegistry::new());
    let bus = Arc::new(EventBus::new());

    let (b_tx, mut b_rx) = make_write_pair();
    reg.register("plugin_b".to_string(), 2, dummy_manifest(), b_tx, "", "")
        .unwrap();
    let (a_tx, _a_rx) = make_write_pair();
    reg.register(
        "plugin_a".to_string(),
        1,
        ipc_manifest_with_targets(vec!["plugin_b"]),
        a_tx.clone(),
        "",
        "",
    )
    .unwrap();

    let router_tx = spawn_router(Arc::clone(&reg), bus);

    let env = Envelope {
        message_id: "hop-trace-1".to_string(),
        payload: Some(envelope::Payload::Ping(Ping { timestamp: 1 })),
        ..Default::default()
    };
    router_tx
        .send(incoming(
            1,
            plug_frame("plugin_b", encode_envelope(env)),
            a_tx,
        ))
        .await
        .unwrap();

    // draining the relay proves the router finished both hops before we assert
    let _ = recv_frame(&mut b_rx).await;
    let logs = String::from_utf8(LOG_BUF.get().unwrap().lock().unwrap().to_vec()).unwrap();
    assert!(
        logs.contains("message ingress"),
        "hop-0 ingress log missing:\n{logs}"
    );
    assert!(
        logs.contains("message relayed"),
        "hop-1 relay log missing:\n{logs}"
    );
    assert!(
        logs.contains("hop=0"),
        "hop field missing on ingress:\n{logs}"
    );
    assert!(
        logs.contains("hop=1"),
        "hop field missing on relay:\n{logs}"
    );
    assert!(
        logs.contains("hop-trace-1"),
        "trace id must appear in hop logs:\n{logs}"
    );
    assert!(
        logs.contains("plugin_b"),
        "target must appear in hop logs:\n{logs}"
    );
}

#[tokio::test]
async fn forward_mux_geo_to_device_conn() {
    let reg = Arc::new(PluginRegistry::new());
    let bus = Arc::new(EventBus::new());

    let caps = vec![
        "battery".to_string(),
        "geo".to_string(),
        "clipboard".to_string(),
    ];
    let (dev_tx, mut dev_rx) = make_write_pair();
    use vynkor::plugins::registry::DeviceMeta;
    use vynkor::proto::vynkor::DeviceOs;
    reg.register_device(
        "dev-xxx".to_string(),
        10,
        caps.clone(),
        dummy_manifest(),
        dev_tx,
        DeviceMeta {
            device_id: "dev-xxx".to_string(),
            user_id: "default".to_string(),
            os: DeviceOs::Android,
            arch: "aarch64".to_string(),
            os_version: "14".to_string(),
            capabilities: caps.clone(),
        },
    )
    .unwrap();

    let (sender_tx, _sender_rx) = make_write_pair();
    reg.register(
        "sender".to_string(),
        1,
        ipc_manifest_with_targets(vec!["dev-xxx.geo"]),
        sender_tx.clone(),
        "",
        "",
    )
    .unwrap();

    let router_tx = spawn_router(Arc::clone(&reg), bus);
    let payload = b"mux hello".to_vec();
    router_tx
        .send(incoming(
            1,
            plug_frame("dev-xxx.geo", payload.clone()),
            sender_tx.clone(),
        ))
        .await
        .unwrap();

    let received = recv_frame(&mut dev_rx).await;
    assert_eq!(&*received.payload, payload);
    assert_eq!(target_as_str(&received), Some("dev-xxx.geo"));

    // also allowlist by device id should work (prefix fallback allowlist)
    let (sender2_tx, _sender2_rx) = make_write_pair();
    reg.register(
        "sender2".to_string(),
        2,
        ipc_manifest_with_targets(vec!["dev-xxx"]),
        sender2_tx.clone(),
        "",
        "",
    )
    .unwrap();
    router_tx
        .send(incoming(
            2,
            plug_frame("dev-xxx.battery", payload.clone()),
            sender2_tx.clone(),
        ))
        .await
        .unwrap();
    let received2 = recv_frame(&mut dev_rx).await;
    assert_eq!(&*received2.payload, payload);
}

#[tokio::test]
async fn forward_mux_battery_to_device_conn_via_device_allowlist() {
    let reg = Arc::new(PluginRegistry::new());
    let bus = Arc::new(EventBus::new());
    let caps = vec!["battery".to_string()];
    let (dev_tx, mut dev_rx) = make_write_pair();
    use vynkor::plugins::registry::DeviceMeta;
    reg.register_device(
        "dev-yyy".to_string(),
        5,
        caps.clone(),
        dummy_manifest(),
        dev_tx,
        DeviceMeta {
            device_id: "dev-yyy".to_string(),
            ..Default::default()
        },
    )
    .unwrap();
    let (sender_tx, _sender_rx) = make_write_pair();
    reg.register(
        "sender".to_string(),
        1,
        ipc_manifest_with_targets(vec!["dev-yyy"]),
        sender_tx.clone(),
        "",
        "",
    )
    .unwrap();
    let router_tx = spawn_router(Arc::clone(&reg), bus);
    router_tx
        .send(incoming(
            1,
            plug_frame("dev-yyy.battery", b"data".to_vec()),
            sender_tx,
        ))
        .await
        .unwrap();
    let f = recv_frame(&mut dev_rx).await;
    assert_eq!(target_as_str(&f), Some("dev-yyy.battery"));
}

#[tokio::test]
async fn register_mux_via_router_single_ws_creates_one_entry() {
    let reg = Arc::new(PluginRegistry::new());
    let bus = Arc::new(EventBus::new());
    let router_tx = spawn_router(Arc::clone(&reg), bus);
    let (write_tx, mut write_rx) = make_write_pair();
    let caps = vec!["battery".to_string(), "geo".to_string()];
    let env = Envelope {
        payload: Some(envelope::Payload::PluginRegister(PluginRegister {
            plugin_id: "dev-mux".to_string(),
            device_id: "dev-mux".to_string(),
            capabilities: caps.clone(),
            protocol_version: vynkor_wire::PROTOCOL_VERSION.to_string(),
            manifest: Some(dummy_manifest()),
            ..Default::default()
        })),
        ..Default::default()
    };
    router_tx
        .send(incoming(77, kernel_frame(env), write_tx))
        .await
        .unwrap();
    let ack_frame = recv_frame(&mut write_rx).await;
    let ack = decode_envelope(&ack_frame);
    match ack.payload {
        Some(envelope::Payload::PluginRegisterAck(a)) => assert!(
            a.accepted,
            "mux register must be accepted: {}",
            a.reject_reason
        ),
        other => panic!("expected ack, got {other:?}"),
    }
    assert_eq!(reg.list().len(), 1);
    let entry = reg.get("dev-mux").expect("must exist");
    assert!(entry
        .manifest
        .actions
        .contains(&"dev-mux.battery".to_string()));
    assert!(entry.manifest.actions.contains(&"dev-mux.geo".to_string()));
    let dev = reg.get_device("dev-mux").unwrap();
    assert_eq!(dev.capabilities, caps);
}
